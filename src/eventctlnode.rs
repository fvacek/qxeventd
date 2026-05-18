use log::warn;
use qxsql::sql::{EXEC_PARAMS, EXEC_RESULT, QUERY_PARAMS, QUERY_RESULT, READ_PARAMS, READ_RESULT};
use qxsql::{QueryAndParams, QxSqlApi, QxSqlApiRecChng, RecDeleteParam, RecInsertParam, RecReadParam, RecUpdateParam, Record};
use serde::{Deserialize, Serialize};
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvproto::{RpcValue, from_rpcvalue, to_rpcvalue};
use shvrpc::metamethod::{AccessLevel, MetaMethod, Flags};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::eventsqlapi::EventSqlApi;
use crate::{anyhow_to_rpc_error, str_to_rpc_error, string_to_rpc_error};
use crate::state::{EventId, EventRecordChange, SharedAppState, event_api_shv_path};


#[derive(Debug)]
enum EventCtlNode {
    Root,
    Event(EventId),
    EventSql(EventId),
}

impl EventCtlNode {
    fn from_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        if let Some(prefix) = path.strip_suffix("/sql") {
            let event_id = prefix.parse::<i64>()?;
            return Ok(Self::EventSql(event_id));
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
const METH_READ_EVENT_RECORD: &str = "readEventRecord";
const METH_UPDATE_EVENT_RECORD: &str = "updateEventRecord";
const METH_DELETE_EVENT: &str = "deleteEvent";
const METH_LIST_EVENTS: &str = "listEvents";
const METH_EVENT_DATA: &str = "eventData";

const EVENTCTL_DIR_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_CREATE_EVENT, Flags::None, AccessLevel::Write, "s:owner", "[i:event_id,s:api_token]", &[], "",
    ),
    MetaMethod::new_static(
        METH_OPEN_EVENT, Flags::None, AccessLevel::Read, "i:event_id", "s:mount_point", &[], "",
    ),
    MetaMethod::new_static(
        METH_OPEN_EVENT_API_KEY, Flags::None, AccessLevel::Read, "s:api_token", "[i:event_id,s:mount_point]", &[], "",
    ),
    MetaMethod::new_static(
        // user with Service access or user_id == event_owner can read event records
        // read event record is privileged operation, because it exposes api_key
        METH_READ_EVENT_RECORD, Flags::None, AccessLevel::Service, "i:event_id", "{?}", &[], "",
    ),
    MetaMethod::new_static(
        // user with Service access or user_id == event_owner can update event records
        METH_UPDATE_EVENT_RECORD, Flags::None, AccessLevel::Service, "[i:event_id,{?}:event_record]", "b", &[], "",
    ),
    MetaMethod::new_static(
        METH_DELETE_EVENT, Flags::None, AccessLevel::Write, "s:api_token", "b:was_deleted", &[], "",
    ),
    MetaMethod::new_static(
        METH_LIST_EVENTS, Flags::None, AccessLevel::Read, "n", "[{?}]", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_DATA, Flags::None, AccessLevel::Read, "n", "{?}", &[], "",
    ),
];

const METH_EVENT_STATUS: &str = "status";
const METH_EVENT_ADD_ENTRY: &str = "addEntry";
const METH_EVENT_CLOSE: &str = "close";
const EVENTCTL_NODE_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_EVENT_ADD_ENTRY, Flags::None, AccessLevel::Write, "{?}", "i", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_STATUS, Flags::None, AccessLevel::Read, "", "{?}", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_CLOSE, Flags::None, AccessLevel::Read, "",  "", &[], "",
    ),
];

const METH_SQL_QUERY: &str = "query";
const METH_SQL_EXEC: &str = "exec";
const METH_SQL_CREATE: &str = "create";
const METH_SQL_READ: &str = "read";
const METH_SQL_UPDATE: &str = "update";
const METH_SQL_DELETE: &str = "delete";

const EVENTCTL_SQL_NODE_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_SQL_QUERY, Flags::None, AccessLevel::Read, QUERY_PARAMS, QUERY_RESULT, &[], "",
    ),
    MetaMethod::new_static(
        METH_SQL_EXEC, Flags::None, AccessLevel::Write, EXEC_PARAMS, EXEC_RESULT, &[], "",
    ),
    MetaMethod::new_static(
        METH_SQL_READ, Flags::None, AccessLevel::Read, READ_PARAMS, READ_RESULT, &[], "",
    ),
];

fn sanitize_user_id(user_id: &str) -> &str {
    user_id.split(':').next().unwrap_or(user_id)
}

const QX_API_TOKEN: &str = "qx_api_token";

fn get_event_id_from_param(rq: &RpcMessage, method_name: &'static str) -> Option<i64> {
    let event_id = rq.param().map(|event_id| event_id.as_int());
    if event_id.is_none() {
        warn!("Event id is missing in method: {method_name} parameters");
    }
    event_id
}

#[derive(Debug, Clone, Serialize,Deserialize)]
struct UpdateEventRecordParams(i64, EventRecordChange);
impl_rpcvalue_conversions!(UpdateEventRecordParams);

async fn escalate_event_owner_rights(rq: &RpcMessage, app_state: SharedAppState, method_name: &'static str, methods: &'static [MetaMethod]) -> Vec<MetaMethod> {
    // info!("escalate_event_owner_rights, method_name: {method_name}");
    let event_id = if method_name == METH_READ_EVENT_RECORD || method_name == METH_UPDATE_EVENT_RECORD {
        get_event_id_from_param(rq, method_name)
    } else {
        None
    };
    if let Some(event_id) = event_id
        && let Ok(record) = app_state.read().await.event_record(event_id).await {
            let api_token = rq.meta().get(QX_API_TOKEN).map(|v| v.as_str());
            let user_id = rq.user_id().map(sanitize_user_id);
            // info!("event_id: {event_id}, user_id: {user_id:?}");
            if let Some(mm) = methods.iter().find(|&m| m.name == method_name) {
                let called_by_event_owner = api_token.map(|t| t == record.api_token).unwrap_or(false)
                    || user_id.map(|u| u == record.owner).unwrap_or(false);
                if !called_by_event_owner {
                    warn!("Method: {method_name} not called by event owner");
                    warn!("User id: {:?}", user_id);
                }
                return vec![
                    MetaMethod::new_static(
                        method_name, mm.flags, if called_by_event_owner { AccessLevel::Read } else { mm.access }, mm.param.as_ref(), mm.result.as_ref(), &[], mm.description.as_ref(),
                    )
                ]
            }
        }
    methods.to_vec()
}

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
    let node_type = match EventCtlNode::from_path(&shv_path) {
        Ok(node_type) => node_type,
        Err(err) => {
            warn!("Invalid path: {shv_path}, error: {}", err);
            return err_unresolved_request();
        }
    };
    match node_type {
        EventCtlNode::Root => {
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENTCTL_DIR_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_DIR_METHODS, async move || {
                    Ok(list_events(app_state).await)
                }),
                Method::Other(m) => {
                    let method = m.method();
                    match method {
                        METH_CREATE_EVENT => m.resolve(EVENTCTL_DIR_METHODS, async move || {
                            let owner = rq.param().unwrap_or_default().as_str().to_owned();
                            let (event_id, api_token) = app_state.write().await.create_event(owner, client_cmd_tx.clone()).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(api_token)]))
                        }),
                        METH_OPEN_EVENT => m.resolve(EVENTCTL_DIR_METHODS, async move || {
                            let event_id = rq.param().unwrap_or_default().as_str().parse::<EventId>()
                                .map_err(|e| string_to_rpc_error(e.to_string()))?;
                            app_state.write().await.open_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(event_api_shv_path(event_id)))
                        }),
                        METH_OPEN_EVENT_API_KEY => m.resolve(EVENTCTL_DIR_METHODS, async move || {
                            let api_token = rq.param().unwrap_or_default().as_str();
                            let event_id = app_state.read().await.api_token_to_event_id(api_token).await
                                .map_err(anyhow_to_rpc_error)?;
                            app_state.write().await.open_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(event_api_shv_path(event_id))]))
                        }),
                        METH_READ_EVENT_RECORD => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_READ_EVENT_RECORD, EVENTCTL_DIR_METHODS).await, async move || {
                            let event_id = rq.param().unwrap_or_default().as_str().parse::<EventId>()
                                .map_err(|e| string_to_rpc_error(e.to_string()))?;
                            let res = app_state.read().await.event_record(event_id).await;
                            res.map_err(anyhow_to_rpc_error)
                        }),
                        METH_UPDATE_EVENT_RECORD => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_UPDATE_EVENT_RECORD, EVENTCTL_DIR_METHODS).await, async move || {
                            let p = UpdateEventRecordParams::try_from(rq.param())
                                .map_err(anyhow_to_rpc_error)?;
                            let (event_id, change) = (p.0, p.1);
                            // info!("update_event_record, event_id: {event_id:?}, change: {change:?}");
                            let res = app_state.write().await.update_event_record(event_id, change, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(res)
                        }),

                        METH_DELETE_EVENT => m.resolve(EVENTCTL_DIR_METHODS, async move || {
                            let api_token = rq.param().unwrap_or_default().as_str();
                            let event_id = app_state.read().await.api_token_to_event_id(api_token).await
                                .map_err(anyhow_to_rpc_error)?;
                            let was_deleted = app_state.write().await.delete_event(event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(was_deleted))
                        }),
                        METH_LIST_EVENTS => m.resolve(EVENTCTL_DIR_METHODS, async move || {
                            let events = app_state.write().await.list_events().await
                                .map_err(anyhow_to_rpc_error)?;
                            to_rpcvalue(&events).map_err(|e| string_to_rpc_error(e.to_string()))
                        }),
                        METH_EVENT_DATA => m.resolve(EVENTCTL_NODE_METHODS, async move || {
                            let event_id = rq.param().unwrap_or_default().as_str().parse::<EventId>()
                                .map_err(|e| string_to_rpc_error(e.to_string()))?;
                            let res = app_state.read().await.event_record(event_id).await;
                            res.map(|mut rec| {
                                rec.api_token = "".into();
                                rec
                            }).map_err(anyhow_to_rpc_error)
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
        EventCtlNode::Event(event_id) => {
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENTCTL_NODE_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_NODE_METHODS, async move || {
                    Ok(vec!["sql".into()])
                }),
                Method::Other(m) => {
                    let method = m.method();
                    match method {
                        METH_EVENT_STATUS => m.resolve(EVENTCTL_NODE_METHODS, async move || {
                            let res = app_state.read().await.open_event_status(event_id);
                            res.map_err(anyhow_to_rpc_error)
                        }),
                        METH_EVENT_ADD_ENTRY => m.resolve(EVENTCTL_NODE_METHODS, async move || {
                            let Some(param) = rq.param() else {
                                return Err(str_to_rpc_error("Invalid parameters"));
                            };
                            let record: Record = from_rpcvalue(param).map_err(|e| string_to_rpc_error(e.to_string()))?;
                            let qxsql = EventSqlApi::new(event_id, app_state.clone(), client_cmd_tx.clone());
                            let id = qxsql.create_record_with_recchng("late_entries", &record, client_cmd_tx, None).await.map_err(anyhow_to_rpc_error)?;
                            Ok(id)
                        }),
                        METH_EVENT_CLOSE => m.resolve(EVENTCTL_NODE_METHODS, async move || {
                            let res = app_state.write().await.close_event(event_id, client_cmd_tx.clone()).await;
                            res.map_err(anyhow_to_rpc_error)
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
        EventCtlNode::EventSql(event_id) => {
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENTCTL_SQL_NODE_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_SQL_NODE_METHODS, async move || { Ok(vec![]) }),
                Method::Other(m) => {
                    let method = m.method();
                    match method {
                        METH_SQL_QUERY => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_QUERY, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let query = QueryAndParams::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.query(query.query(), query.params()).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
                        }),

                        METH_SQL_EXEC => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_EXEC, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let query = QueryAndParams::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.exec(query.query(), query.params()).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_CREATE => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_CREATE, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let param = RecInsertParam::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx.clone());
                            sql_api.create_record_with_recchng(&param.table, &param.record, client_cmd_tx, param.issuer).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_READ => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_READ, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let param = RecReadParam::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let fields = qxsql::string_list_to_ref_vec(&param.fields);
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.read_record(&param.table, param.id, fields).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_UPDATE => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_UPDATE, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let param = RecUpdateParam::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx.clone());
                            sql_api.update_record_with_recchng(&param.table, param.id, &param.record, client_cmd_tx.clone(), param.issuer).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_DELETE => m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), METH_SQL_DELETE, EVENTCTL_SQL_NODE_METHODS).await, async move || {
                            let param = RecDeleteParam::try_from(rq.param().unwrap_or_default())
                                .map_err(string_to_rpc_error)?;
                            let sql_api = EventSqlApi::new(event_id, app_state, client_cmd_tx.clone());
                            sql_api.delete_record_with_recchng(&param.table, param.id, client_cmd_tx.clone(), param.issuer).await
                                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                                .map_err(anyhow_to_rpc_error)
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
