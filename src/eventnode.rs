use log::{info, warn};
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvproto::RpcValue;
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::{AppState, anyhow_to_rpc_error};
use crate::eventrpcproxy::EventRpcProxy;
use crate::qxappsql::QxAppSql;
use crate::state::{EventId};
use qxsql::{sql::{QxSqlApi}};
use anyhow::anyhow;


#[derive(Debug)]
enum NodeType<'a> {
    Root,
    Event(EventId),
    EventDb(EventId, &'a str),
}

impl<'a> NodeType<'a> {
    fn from_path(path: &'a str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        let (first, rest) = split_first_path_fragment(path);
        let event_id = first.parse::<i64>()?;
        if rest.is_empty() {
            Ok(Self::Event(event_id))
        } else {
            let (first, rest) = split_first_path_fragment(rest);
            if first == "db" {
                Ok(Self::EventDb(event_id, rest))
            } else {
                Err(anyhow::anyhow!("Invalid path: {}", path))
            }
        }
    }
}

fn split_first_fragment(path: &str, sep: char) -> (&str, &str) {
    if let Some(ix) = path.find(sep) {
        let dir = &path[0 .. ix];
        let rest = &path[ix + 1..];
        (dir, rest)
    } else {
        (path, "")
    }
}

fn split_first_path_fragment(path: &str) -> (&str, &str) {
    split_first_fragment(path, '/')
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

const METH_CREATE_EVENT: &str = "createEvent";
const MM_CREATE_EVENT: MetaMethod = MetaMethod::new_static(
    METH_CREATE_EVENT,
    Flag::None as u32,
    AccessLevel::Write,
    "s", // owner
    "i", // event_id
    &[],
    "",
);
const METH_OPEN_EVENT: &str = "openEvent";
const MM_OPEN_EVENT: MetaMethod = MetaMethod::new_static(
    METH_OPEN_EVENT, Flag::None as u32, AccessLevel::Read, "i", "", &[], "",
);

const ROOT_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MM_CREATE_EVENT,
    MM_OPEN_EVENT,
];

const EVENT_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MM_EVENT_INFO,
    MM_EVENT_CLOSE,
];
const METH_EVENT_INFO: &str = "info";
const MM_EVENT_INFO: MetaMethod = MetaMethod::new_static(
    METH_EVENT_INFO, Flag::None as u32, AccessLevel::Read, "n", "{}", &[], "",
);
const METH_EVENT_CLOSE: &str = "close";
const MM_EVENT_CLOSE: MetaMethod = MetaMethod::new_static(
    METH_EVENT_CLOSE, Flag::None as u32, AccessLevel::Read, "",  "", &[], "",
);

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
                            let event_id = app_state.write().await.create_event(owner).await
                                .map_err(|e| anyhow_to_rpc_error(e))?;
                            Ok(RpcValue::from(event_id))
                        }),
                        METH_OPEN_EVENT => m.resolve(ROOT_METHODS, async move || {
                            let event_id = rq.param().unwrap_or_default().as_i64();
                            app_state.write().await.open_event(event_id).await
                                .map_err(|e| anyhow_to_rpc_error(e))?;
                            let message = RpcMessage::new_signal("event", "lsmod", Some(true.into()));
                            client_cmd_tx.send_message(message)
                                .map_err(|e| anyhow_to_rpc_error(anyhow!("Failed to send message {}", e)))?;
                            Ok(RpcValue::from(()))
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
                        METH_EVENT_INFO => m.resolve(EVENT_METHODS, async move || {
                            let mut event_data = app_state.read().await.open_events.get(&event_id)
                                .ok_or_else(|| anyhow_to_rpc_error(anyhow!("Event not found")))?.data.clone();
                            event_data.api_token = "".into(); // hide api token
                            let info = RpcValue::from(&event_data);
                            Ok(info)
                        }),
                        METH_EVENT_CLOSE => m.resolve(EVENT_METHODS, async move || {
                            app_state.write().await.close_event(event_id).await
                                .map_err(|e| anyhow_to_rpc_error(e))?;
                            let message = RpcMessage::new_signal("event", "lsmod", Some(false.into()));
                            client_cmd_tx.send_message(message)
                                .map_err(|e| anyhow_to_rpc_error(anyhow!("Failed to send message {}", e)))?;
                            Ok(RpcValue::from(()))
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
        NodeType::EventDb(id, path) => {
            let px = EventRpcProxy{ app_state, event_id: id as i64 };
            let mut rq = rq;
            rq.set_shvpath(path);
            return px.request_handler(rq, client_cmd_tx).await;
        }
    }
}

async fn list_events(app_state: AppState) -> Vec<String> {
    let qxsql = QxAppSql(app_state.read().await.db_pool.clone());
    let fields = Some(vec!["id"]);
    let result: Vec<String> = qxsql.list_records("events", fields, None, None)
        .await
        .unwrap_or(vec![])
        .into_iter()
        .map(|record| format!("{}", record["id"].to_int().unwrap_or_default()))
        .collect();
    result
}
