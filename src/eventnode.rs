use log::warn;
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvrpc::metamethod::MetaMethod;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::AppState;
use crate::eventrpcproxy::EventRpcProxy;
use crate::qxappsql::QxAppSql;
use qxsql::{sql::{QxSqlApi}};


// History site node methods
// const METH_NAME: &str = "name";
// const NODE_SQL: &str = "sql";

// const META_METHOD_NAME: MetaMethod = MetaMethod {
//     name: Cow::Borrowed(METH_NAME),
//     flags: shvrpc::metamethod::Flag::None as u32,
//     access: shvrpc::metamethod::AccessLevel::Read,
//     param: Cow::Borrowed("n"),
//     result: Cow::Borrowed("s"),
//     signals: SignalsDefinition::Static(&[]),
//     description: Cow::Borrowed(""),
// };

// type RpcRequestResult = Result<RpcValue, RpcError>;

enum NodeType<'a> {
    Root,
    Event(i32, &'a str),
}

impl<'a> NodeType<'a> {
    fn from_path(path: &'a str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        let (first, rest) = split_first_fragment(path);
        let event_id = first.parse::<i32>()?;
        Ok(Self::Event(event_id, rest))
    }
}

fn split_first_fragment(path: &str) -> (&str, &str) {
    if let Some(ix) = path.find('/') {
        let dir = &path[0 .. ix];
        let rest = &path[ix + 1..];
        (dir, rest)
    } else {
        ("", path)
    }
}

// fn split_last_fragment(path: &str) -> (&str, &str) {
//     if let Some(ix) = path.rfind('/') {
//         let dir = &path[ix + 1..];
//         let prefix = &path[..ix];
//         (prefix, dir)
//     } else {
//         ("", path)
//     }
// }



// fn rpc_error_unknown_method(method: impl AsRef<str>) -> RpcError {
//     RpcError::new(
//         RpcErrorCode::MethodNotFound,
//         format!("Unknown method '{}'", method.as_ref())
//     )
// }

// async fn event_methods_getter(
//     path: impl AsRef<str>,
//     // app_state: Option<AppState<crate::state::State>>,
// ) -> Option<MetaMethods> {
//     let path = path.as_ref();
//     if path.is_empty() {
//         return Some(MetaMethods::from(&[
//                 &META_METHOD_NAME,
//                 ]));
//     }
//     if path == NODE_SQL {
//         return Some(MetaMethods::from(&[
//                 &META_METHOD_NAME,
//                 ]));
//     } else {
//         None
//     }
// }

// async fn event_request_handler(
//     method: &str,
// ) -> RpcRequestResult {
//     match method {
//         METH_LS => Ok(shvproto::List::new().into()),
//         METH_NAME => {
//             Ok("event".into())
//         }
//         _ => Err(rpc_error_unknown_method(method)),
//     }
// }

// struct ScopedLog { msg: String }
// impl ScopedLog {
//     fn new(msg: impl AsRef<str>) -> Self {
//         let msg = msg.as_ref();
//         log::debug!("init: {msg}");
//         Self { msg: msg.into() }
//     }
// }
// impl Drop for ScopedLog {
//     fn drop(&mut self) {
//         log::debug!("drop: {}", self.msg);
//     }
// }

// pub(crate) async fn methods_getter(
//     path: String,
//     client_cmd_tx: crate::state::ClientCommandSender,
//     app_state: Option<AppState<crate::state::State>>,
// ) -> Option<MetaMethods> {
//     let Ok(node_type) = NodeType::from_path(&path) else {
//         return None;
//     };
//     match node_type {
//         NodeType::Root => Some(MetaMethods::from(&[])),
//         NodeType::Event(_) => Some(MetaMethods::from(&[&META_METHOD_NAME])),
//         NodeType::Sql(event_id, shv_path) => {
//             let event_mount = "test/hsh2025";
//             let new_path = shvrpc::util::join_path(event_mount, shv_path);
//             let log: RpcValue = RpcCall::new(&new_path, METH_DIR)
//                 // .param(getlog_params.clone())
//                 // .timeout(std::time::Duration::from_secs(60))
//                 .exec(&client_cmd_tx)
//                 .await;
//             None
//         }
//     }
// }

const DIR_LS_METHODS: &[MetaMethod] = &[META_METHOD_DIR, META_METHOD_LS];

// pub(crate) async fn children_on_path(
//     path: String,
//     app_state: AppState,
// ) -> Vec<String> {
//     let Ok(node_type) = NodeType::from_path(&path) else {
//         return vec![];
//     };
//     match node_type {
//         NodeType::Root => list_events(app_state).await,
//         NodeType::Event(id) => event_ls(id).await,
//         NodeType::Sql(event_id, shv_path) => {
//             let event_mount = "test/hsh2025";
//             let new_path = shvrpc::util::join_path(event_mount, shv_path);
//             let log: RpcValue = RpcCall::new(&new_path, METH_DIR)
//                 // .param(getlog_params.clone())
//                 // .timeout(std::time::Duration::from_secs(60))
//                 .exec(&client_cmd_tx)
//                 .await;
//             None
//         }
//     }
// }

pub(crate) async fn request_handler(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    app_state: AppState,
) -> RequestHandlerResult {
    if !rq.is_request() {
        warn!("Not request");
        return err_unresolved_request();
    }
    let shv_path = rq.shv_path().unwrap_or_default().to_string();
    // if shv_path.is_empty() {
    //     return err_unresolved_request();
    // }
    let Ok(node_type) = NodeType::from_path(&shv_path) else {
        warn!("Invalid path: {shv_path}");
        return err_unresolved_request();
    };
    if let NodeType::Event(id, path) = node_type {
        let px = EventRpcProxy{ app_state, event_id: id as i64 };
        let mut rq = rq;
        rq.set_shvpath(path);
        return px.request_handler(rq, client_cmd_tx).await;
    }

    match Method::from_request(&rq) {
        Method::Dir(dir) => dir.resolve(DIR_LS_METHODS),
        Method::Ls(ls) => ls.resolve(DIR_LS_METHODS, async move || {
            Ok(list_events(app_state).await)
        }),
        _ => {
            err_unresolved_request()
        }
    }
}

async fn list_events(app_state: AppState) -> Vec<String> {
    let qxsql = QxAppSql(app_state);
    let fields = Some(vec!["id"]);
    let result = qxsql.list_records("events", fields, None, None)
        .await
        .unwrap_or(vec![])
        .into_iter()
        .map(|record| format!("{}", record["id"].to_int().unwrap_or_default()))
        .collect();
    result
}

// async fn event_ls(event_id: i64) -> Vec<String> {
//     let qxsql = QxAppSql(app_state);
//     let fields = Some(vec!["id"]);
//     let result = qxsql.list_records("events", fields, None, None)
//         .await
//         .unwrap_or(vec![])
//         .into_iter()
//         .map(|record| format!("{}", record["id"].to_int().unwrap_or_default()))
//         .collect();
//     result
// }
// async fn request_handler_impl(
//     rq: RpcMessage,
//     client_cmd_tx: crate::state::ClientCommandSender,
//     app_state: AppState<crate::state::State>,
// ) -> RpcRequestResult {
//     let path = rq.shv_path().unwrap_or_default();
//     let method = rq.method().unwrap_or_default();
//     let param = rq.param().map_or_else(RpcValue::null, RpcValue::clone);

//     match NodeType::from_path(path) {
//         NodeType::Event =>
//             event_request_handler(method, None).await,
//         NodeType::Sql => {
//             if method == shvclient::clientnode::METH_LS {
//                 Ok(shvproto::List::new().into())
//             } else {
//                 DOT_APP_NODE
//                     .process_request(&rq)
//                     .unwrap_or_else(|| Err(rpc_error_unknown_method(method)))
//             }
//         }
//     }
// }
