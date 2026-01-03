use log::warn;
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvproto::{RpcValue, make_map};
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::{anyhow_to_rpc_error, string_to_rpc_error};
use crate::state::{EventId, SharedAppState, event_mount_point};
use anyhow::anyhow;


#[derive(Debug)]
enum NodeType {
    Root,
    Event(EventId),
}

impl NodeType {
    fn from_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        let event_id = path.parse::<i64>()?;
        Ok(Self::Event(event_id))
    }
}

// fn split_first_fragment(path: &str, sep: char) -> (&str, &str) {
//     if let Some(ix) = path.find(sep) {
//         let dir = &path[0 .. ix];
//         let rest = &path[ix + 1..];
//         (dir, rest)
//     } else {
//         (path, "")
//     }
// }

// fn split_first_path_fragment(path: &str) -> (&str, &str) {
//     split_first_fragment(path, '/')
// }

// fn split_last_fragment(path: &str) -> (&str, &str) {
//     if let Some(ix) = path.rfind('/') {
//         let dir = &path[ix + 1..];
//         let prefix = &path[..ix];
//         (prefix, dir)
//     } else {
//         ("", path)
//     }
// }

const METH_CREATE_EVENT: &str = "createEvent";
const METH_OPEN_EVENT: &str = "openEvent";
const METH_OPEN_EVENT_API_KEY: &str = "openEventApiKey";
const METH_DELETE_EVENT: &str = "deleteEvent";
const ROOT_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_CREATE_EVENT, Flag::None as u32, AccessLevel::Write, "s:owner", "[i:event_id,s:api_token]", &[], "",
    ),
    MetaMethod::new_static(
        METH_OPEN_EVENT, Flag::None as u32, AccessLevel::Read, "i:event_id", "s:mount_point", &[], "",
    ),
    MetaMethod::new_static(
        METH_OPEN_EVENT_API_KEY, Flag::None as u32, AccessLevel::Read, "s:api_token", "[i:event_id,s:mount_point]", &[], "",
    ),
    MetaMethod::new_static(
        METH_DELETE_EVENT, Flag::None as u32, AccessLevel::Config, "s:api_token", "b:was_deleted", &[], "",
    ),
];

const METH_EVENT_DATA: &str = "data";
const METH_EVENT_CLOSE: &str = "close";
const EVENT_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_EVENT_DATA, Flag::None as u32, AccessLevel::Read, "", "{?}", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_CLOSE, Flag::None as u32, AccessLevel::Read, "",  "", &[], "",
    ),
];

pub(crate) async fn request_handler(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    app_state: SharedAppState,
) -> RequestHandlerResult {
    if !rq.is_request() {
        warn!("Not request");
        return err_unresolved_request();
    }
    let shv_path = rq.shv_path().unwrap_or_default().to_string();
    // info!("shv_path2: {shv_path}");
    let node_type = match NodeType::from_path(&shv_path) {
        Ok(node_type) => node_type,
        Err(err) => {
            warn!("Invalid path: {shv_path}, error: {}", err);
            return err_unresolved_request();
        }
    };
    match node_type {
        NodeType::Root => {
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(ROOT_METHODS),
                Method::Ls(ls) => ls.resolve(ROOT_METHODS, async move || {
                    Ok(list_events(app_state).await)
                }),
                Method::Other(m) => {
                    let method = m.method();
                    match method {
                        METH_CREATE_EVENT => m.resolve(ROOT_METHODS, async move || {
                            let owner = rq.param().unwrap_or_default().as_str().to_owned();
                            let (event_id, api_token) = app_state.write().await.create_event(owner, client_cmd_tx.clone()).await
                                .map_err(anyhow_to_rpc_error)?;
                            // add api token to broker mounts
                            let mount_point = event_mount_point(event_id);
                            let param: Vec<RpcValue> = vec![
                                (&api_token).into(),
                                make_map!( "mountPoint".to_string() => RpcValue::from(mount_point),).into(),
                            ];
                            let _res: RpcValue = client_cmd_tx.call_rpc_method(".broker/access/mounts", "setValue", Some(param.into()), None, None::<fn(f64)>)
                                .await.map_err(|e| string_to_rpc_error(format!("{e}")))?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(api_token)]))
                        }),
                        METH_OPEN_EVENT => m.resolve(ROOT_METHODS, async move || {
                            let event_id = rq.param().unwrap_or_default().as_i64();
                            app_state.write().await.open_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(event_mount_point(event_id)))
                        }),
                        METH_OPEN_EVENT_API_KEY => m.resolve(ROOT_METHODS, async move || {
                            let api_token = rq.param().unwrap_or_default().as_str();
                            let event_id = app_state.read().await.api_token_to_event_id(api_token).await
                                .map_err(anyhow_to_rpc_error)?;
                            app_state.write().await.open_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(event_mount_point(event_id))]))
                        }),
                        METH_DELETE_EVENT => m.resolve(ROOT_METHODS, async move || {
                            let api_token = rq.param().unwrap_or_default().as_str();
                            let event_id = app_state.read().await.api_token_to_event_id(api_token).await
                                .map_err(anyhow_to_rpc_error)?;
                            let was_deleted = app_state.write().await.delete_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(was_deleted))
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
        NodeType::Event(event_id) => {
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENT_METHODS),
                Method::Ls(ls) => ls.resolve(EVENT_METHODS, async move || { Ok(vec![]) }),
                Method::Other(m) => {
                    let method = m.method();
                    match method {
                        METH_EVENT_DATA => m.resolve(EVENT_METHODS, async move || {
                            let event_data = app_state.read().await.open_events.get(&event_id)
                                .ok_or_else(|| anyhow_to_rpc_error(anyhow!("Event not found")))?.data.clone();
                            let info = RpcValue::from(&event_data);
                            Ok(info)
                        }),
                        METH_EVENT_CLOSE => m.resolve(EVENT_METHODS, async move || {
                            let res = app_state.write().await.close_event(event_id, client_cmd_tx.clone()).await;
                            res.map_err(anyhow_to_rpc_error)
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
    }
}

async fn list_events(app_state: SharedAppState) -> Vec<String> {
    let mut events = app_state.read().await.open_events.keys().cloned().collect::<Vec<_>>();
    events.sort();

    events
        .into_iter()
        .map(|id| format!("{id}"))
        .collect()
}
