use log::{info, warn};
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvrpc::metamethod::MetaMethod;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::AppState;
use crate::eventrpcproxy::EventRpcProxy;
use crate::qxappsql::QxAppSql;
use qxsql::{sql::{QxSqlApi}};


#[derive(Debug)]
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
        (path, "")
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

const DIR_LS_METHODS: &[MetaMethod] = &[META_METHOD_DIR, META_METHOD_LS];

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
    // info!("shv_path2: {shv_path}");
    let node_type = match NodeType::from_path(&shv_path) {
        Ok(node_type) => node_type,
        Err(err) => {
            warn!("Invalid path: {shv_path}, error: {}", err);
            return err_unresolved_request();
        }
    };
    info!("node type: {:?}", node_type);
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
    let mut result: Vec<String> = qxsql.list_records("events", fields, None, None)
        .await
        .unwrap_or(vec![])
        .into_iter()
        .map(|record| format!("{}", record["id"].to_int().unwrap_or_default()))
        .collect();
    result.push("2025".to_string());
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
