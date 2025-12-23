use std::{borrow::Cow, collections::BTreeMap};

use log::info;
use shvclient::{ClientCommandSender, clientapi::{CallRpcMethodError, RpcCall}, clientnode::{METH_DIR, METH_LS, MetaMethods, Method, RequestHandlerResult}};
use shvproto::RpcValue;
use shvrpc::{RpcMessage, RpcMessageMetaTags, metamethod::{AccessLevel, DirAttribute, MetaMethod, SignalsDefinition}, rpcmessage::{RpcError, RpcErrorCode}, util::join_path};

use crate::{AppState, state::EventId};

pub struct EventRpcProxy{
    pub app_state: AppState,
    pub event_id: EventId,
}

impl EventRpcProxy {

    pub async fn request_handler(
        &self,
        rq: RpcMessage,
        client_cmd_tx: ClientCommandSender,
    ) -> RequestHandlerResult {
        let event_mount_point = self.app_state.read().await.event_mount_point(self.event_id);
        let shv_path = join_path(event_mount_point, rq.shv_path().unwrap_or_default());
        info!("rq: {}", rq.to_cpon());
        match Method::from_request(&rq) {
            Method::Dir(dir) => dir.resolve(methods_on_path(&shv_path, client_cmd_tx).await.unwrap_or_default()),
            Method::Ls(ls) => {
                let methods = methods_on_path(&shv_path, client_cmd_tx.clone()).await.unwrap_or_default();
                let children = children_on_path(&shv_path, client_cmd_tx).await.unwrap_or_default();
                ls.resolve(methods, async move || { Ok(children) })
            },
            Method::Other(m) => {
                let methods = methods_on_path(&shv_path, client_cmd_tx.clone()).await.unwrap_or_default();
                let mut rq = rq;
                rq.set_shvpath(&shv_path);
                m.resolve(methods, async move || {
                    forward_rpc_call(rq, client_cmd_tx).await
                })
            }
        }
    }
}

async fn methods_on_path(shv_path: &str, client_cmd_tx: ClientCommandSender) -> Result<MetaMethods, CallRpcMethodError> {
    let result: RpcValue = RpcCall::new(shv_path, METH_DIR)
        // .param(getlog_params.clone())
        // .timeout(std::time::Duration::from_secs(60))
        .exec(&client_cmd_tx)
        .await?;
    let v = result.as_list().iter().map(|v| metamethod_from_rpcvalue(v))
        .collect::<Vec<_>>();
    Ok(MetaMethods::Owned(v))
}

async fn children_on_path(shv_path: &str, client_cmd_tx: ClientCommandSender) -> Result<Vec<String>, CallRpcMethodError> {
    info!("children_on_path: {}", shv_path);
    let result: RpcValue = RpcCall::new(shv_path, METH_LS)
        // .param(getlog_params.clone())
        // .timeout(std::time::Duration::from_secs(60))
        .exec(&client_cmd_tx)
        .await?;
    let v = result.as_list().iter().map(|v| v.as_str().to_string())
        .collect::<Vec<_>>();
    Ok(v)
}

async fn forward_rpc_call(rq: RpcMessage, client_cmd_tx: ClientCommandSender) -> Result<RpcValue, RpcError> {
    let result: Result<RpcValue, CallRpcMethodError> = RpcCall::new(rq.shv_path().unwrap_or_default(), rq.method().unwrap_or_default())
        .param(rq.param())
        // .timeout(std::time::Duration::from_secs(60))
        .exec(&client_cmd_tx)
        .await;
    match result {
        Ok(v) => Ok(v),
        Err(e) => match e.error() {
            shvclient::clientapi::CallRpcMethodErrorKind::ConnectionClosed => Err(RpcError::new(RpcErrorCode::MethodCallCancelled, "ConnectionClosed")),
            shvclient::clientapi::CallRpcMethodErrorKind::InvalidMessage(e) => Err(RpcError::new(RpcErrorCode::MethodCallCancelled, e.to_string())),
            shvclient::clientapi::CallRpcMethodErrorKind::RpcError(rpc_error) => Err(rpc_error.clone()),
            shvclient::clientapi::CallRpcMethodErrorKind::ResultTypeMismatch(e) => Err(RpcError::new(RpcErrorCode::MethodCallCancelled, e.to_string())),
        },
    }
}

fn metamethod_from_rpcvalue(value: &RpcValue) -> MetaMethod {
    fn to_cow_str(v: Option<&RpcValue>) -> Cow<'static, str> {
        v.map(|v| Cow::Owned(v.as_str().to_string()))
        .unwrap_or(Cow::Borrowed(""))
    }
    let name = to_cow_str(value.get(DirAttribute::Name as i32));
    let flags = value.get(DirAttribute::Flags as i32).unwrap_or_default().as_u32();
    let access_level = value.get(DirAttribute::AccessLevel as i32).unwrap_or_default();
    let access_level = AccessLevel::try_from(access_level).unwrap_or(AccessLevel::Browse);
    let param = to_cow_str(value.get(DirAttribute::Param as i32));
    let result = to_cow_str(value.get(DirAttribute::Result as i32));
    let signals = value.get(DirAttribute::Signals as i32)
        .unwrap_or_default().as_map()
            .iter()
            .map(|(k, v)| {
                (k.to_string(), match v.value {
                    shvproto::Value::String(ref s) => Some(s.to_string()),
                    _ => None,
                })
            })
            .collect::<BTreeMap<String, Option<String>>>();
    let signals = SignalsDefinition::Dynamic(signals);
    MetaMethod::new(name, flags, access_level, param, result, signals, Cow::Borrowed(""))
}
