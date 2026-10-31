
use log::warn;
use serde::{Serialize};
use serde::Deserialize;
use crate::rpcvalueext::RpcValueExt;
use crate::rpctype::RpcType;
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, MethodResolver, RequestHandlerResult, UnresolvedRequest, err_unresolved_request};
use shvproto::{RpcValue, from_rpcvalue, to_rpcvalue};
use shvrpc::metamethod::{AccessLevel, MetaMethod, Flags};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::qxchange::{self, Data, LateEntry, QxChangeRecord};
use crate::qxeventconfig::Role;
use crate::remotedbsqlapi::RemoteEventSqlApi;
use crate::{anyhow_to_rpc_error, str_to_rpc_error, string_to_rpc_error};
use crate::state::{open_event, EventId, EventRecordChange, SharedAppState, State};


#[derive(Debug)]
enum EventCtlNode {
    Root,
    Event(EventId),
}

impl EventCtlNode {
    fn from_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        let event_id = path.parse::<i64>()?;
        Ok(Self::Event(event_id))
    }

}

const METH_CREATE_EVENT: &str = "createEvent";
const METH_OPEN_EVENT: &str = "openEvent";
const METH_OPEN_EVENT_API_KEY: &str = "openEventApiKey";
const METH_READ_EVENT_RECORD: &str = "readEventRecord";
const METH_UPDATE_EVENT_RECORD: &str = "updateEventRecord";
const METH_SET_EVENT_DB_MODE: &str = "setEventDbMode";
const METH_DELETE_EVENT: &str = "deleteEvent";
const METH_LIST_EVENTS: &str = "listEvents";
const METH_EVENT_DATA: &str = "eventData";
const METH_PUSH_EVENT_DB: &str = "pushEventDb";
const METH_PULL_EVENT_DB: &str = "pullEventDb";

const EVENTCTL_ROOT_METHODS: &[MetaMethod] = &[
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
        METH_READ_EVENT_RECORD, Flags::UserIDRequired, AccessLevel::Service, "i:event_id", "{?}", &[], "",
    ),
    MetaMethod::new_static(
        // user with Service access or user_id == event_owner can update event records
        METH_UPDATE_EVENT_RECORD, Flags::UserIDRequired, AccessLevel::Service, "{i:event_id,{?}:change}", "b", &[], "",
    ),
    MetaMethod::new_static(
        // user with Service access or user_id == event_owner can change the database mode
        METH_SET_EVENT_DB_MODE, Flags::UserIDRequired, AccessLevel::Service, "{i:event_id,s:db_mode}", "b", &[], "",
    ),
    MetaMethod::new_static(
        METH_DELETE_EVENT, Flags::UserIDRequired, AccessLevel::Read, "s:api_token", "b:was_deleted", &[], "",
    ),
    MetaMethod::new_static(
        METH_PUSH_EVENT_DB, Flags::UserIDRequired, AccessLevel::Service, PushEventDbParams::RPC_TYPE, "n", &[], "",
    ),
    MetaMethod::new_static(
        METH_PULL_EVENT_DB, Flags::UserIDRequired, AccessLevel::Service, "i:event_id", "[i:uncompressed_data_size,x:compressed_data]", &[], "",
    ),
    MetaMethod::new_static(
        METH_LIST_EVENTS, Flags::None, AccessLevel::Read, "n", "[{?}]", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_DATA, Flags::None, AccessLevel::Read, "n", "{?}", &[], "",
    ),
];

const METH_EVENT_STATUS: &str = "status";
const METH_EVENT_UPDATE_LATE_ENTRY: &str = "updateLateEntry";
const METH_EVENT_CLOSE: &str = "close";
const EVENTCTL_NODE_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_EVENT_UPDATE_LATE_ENTRY, Flags::UserIDRequired, AccessLevel::Write, "{i:run_id,{?}:record}", "b|i", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_STATUS, Flags::None, AccessLevel::Read, "", "{?}", &[], "",
    ),
    MetaMethod::new_static(
        METH_EVENT_CLOSE, Flags::None, AccessLevel::Read, "",  "", &[], "",
    ),
];

fn sanitize_user_id(rq: &RpcMessage) -> Option<&str> {
    rq.user_id().map(|user_id| user_id.split(':').next().unwrap_or(user_id))
}

const QX_API_TOKEN: &str = "qx_api_token";


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct JustEventIdParam {
    event_id: EventId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateEventRecordParams {
    event_id: EventId,
    change: EventRecordChange,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetEventDbModeParams {
    event_id: EventId,
    db_mode: crate::state::EventDbMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcType)]
struct PushEventDbParams {
    event_id: EventId,
    compressed_data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct DeleteEventParams {
    api_token: String,
}

async fn escalate_event_owner_rights(rq: &RpcMessage, app_state: SharedAppState, event_id: Option<EventId>, method_name: String, methods: &'static [MetaMethod]) -> Vec<MetaMethod> {
    // log::info!("escalate_event_owner_rights, method_name: {method_name}, event id: {event_id:?}");
    let Some(mm) = methods.iter().find(|&m| m.name == method_name) else {
        warn!("Unknown method: {method_name}");
        return vec![];
    };

    if !mm.flags.contains(Flags::UserIDRequired) {
        warn!("Method: {method_name} should require a user id, if it uses user rights escalation");
        return vec![];
    }

    let Some(event_id) = event_id else {
        warn!("Event id not provided for method: {method_name}");
        return vec![];
    };

    // 1. Resolve the event; reject if unavailable.
    let Ok(event_record) = app_state.write().await.event_record(event_id).await else {
        warn!("Invalid event id: {event_id} for method: {method_name}");
        return vec![];
    };
    let event_record = event_record.read().await;

    // 2. Identify the user and reject if banned, regardless of API token.
    let user_id = sanitize_user_id(rq);
    let user_role = user_id.and_then(|u| event_record.config.members.get(u));
    if user_role == Some(&Role::Banned) {
        warn!("User: {user_id:?} is banned");
        return vec![];
    }

    // 3. Validate the API token if supplied.
    let api_token = rq.meta().get(QX_API_TOKEN).map(|v| v.as_str());
    let valid_api_token = match api_token {
        Some(api_token) if api_token == event_record.api_token => true,
        Some(_) => {
            warn!("API token does not match event_id: {event_id}");
            return vec![];
        }
        None => false,
    };


    // 4. Allow access if either the token is valid or the user is an organizer.
    let is_organizer = user_id.map(|u| u == event_record.owner).unwrap_or(false)
        || user_role == Some(&Role::Organizer);
    let called_by_event_organizer = valid_api_token || is_organizer;

    // 5. Apply the method's access-level logic.
    if !called_by_event_organizer {
        warn!("Method: {method_name} not called by event organizer");
        warn!("User id: {:?}", user_id);
    }

    vec![
        MetaMethod::new_static(
            &mm.name,
            mm.flags,
            if called_by_event_organizer { AccessLevel::Read } else { mm.access },
            mm.param.as_ref(),
            mm.result.as_ref(),
            &[],
            mm.description.as_ref(),
        )
    ]
}

fn resolve_error(m: &MethodResolver, methods: &'static [MetaMethod], err: anyhow::Error) -> RequestHandlerResult {
    m.resolve(methods, async move || { Err::<RpcValue, _>(anyhow_to_rpc_error(err)) })
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
                Method::Dir(dir) => dir.resolve(EVENTCTL_ROOT_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_ROOT_METHODS, async move || {
                    Ok(list_events(app_state).await)
                }),
                Method::Other(m) => {
                    let method = m.method();

                    match method {
                        METH_CREATE_EVENT => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let owner = rq.param().unwrap_or_default().as_str().to_owned();
                            let (event_id, api_token) = app_state.write().await.create_event(owner, client_cmd_tx.clone()).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(api_token)]))
                        }),
                        METH_OPEN_EVENT => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let event_id = rq.param().unwrap_or_default().as_int();
                            let event_shv_path = open_event(app_state, event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(event_shv_path))
                        }),
                        METH_OPEN_EVENT_API_KEY => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let api_token = rq.param().unwrap_or_default().as_str();
                            let event_id = app_state.read().await.api_token_to_event_id(api_token).await
                                .map_err(anyhow_to_rpc_error)?;
                            let event_shv_path = open_event(app_state, event_id, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::from(vec![RpcValue::from(event_id), RpcValue::from(event_shv_path)]))
                        }),
                        METH_READ_EVENT_RECORD => {
                            let p = match JustEventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), Some(p.event_id), method.to_owned(), EVENTCTL_ROOT_METHODS).await, async move || {
                                let res = app_state.read().await.load_event_record(p.event_id).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(res.to_rpcvalue())
                            })
                        },
                        METH_UPDATE_EVENT_RECORD => {
                            let p = match UpdateEventRecordParams::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), Some(p.event_id), method.to_owned(), EVENTCTL_ROOT_METHODS).await, async move || {
                                let res = app_state.write().await.update_event_record(p.event_id, p.change, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(res)
                            })
                        },
                        METH_SET_EVENT_DB_MODE => {
                            let p = match SetEventDbModeParams::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), Some(p.event_id), method.to_owned(), EVENTCTL_ROOT_METHODS).await, async move || {
                                let res = app_state.write().await.set_event_db_mode(p.event_id, p.db_mode, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(res)
                            })
                        }
                        METH_DELETE_EVENT => {
                            let p = DeleteEventParams::try_from_rpcvalue(rq.param().unwrap_or_default())
                                .map_err(|_| UnresolvedRequest)?;
                            m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                                let event_id = app_state.read().await.api_token_to_event_id(&p.api_token).await
                                    .map_err(anyhow_to_rpc_error)?;
                                let was_deleted = app_state.write().await.delete_event(event_id, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(RpcValue::from(was_deleted))
                            })
                        },
                        METH_PUSH_EVENT_DB => {
                            let p = match PushEventDbParams::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), Some(p.event_id), method.to_owned(), EVENTCTL_ROOT_METHODS).await, async move || {
                                State::push_event_db(app_state, p.event_id, &p.compressed_data, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(RpcValue::null())
                            })
                        }
                        METH_PULL_EVENT_DB => {
                            let p = match JustEventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(escalate_event_owner_rights(&rq, app_state.clone(), Some(p.event_id), method.to_owned(), EVENTCTL_ROOT_METHODS).await, async move || {
                                let (len, zdata) = State::pull_event_db(app_state, p.event_id).await
                                    .map_err(anyhow_to_rpc_error)?;
                                let ret = vec![RpcValue::from(len as i64), RpcValue::from(zdata)];
                                Ok(RpcValue::from(ret))
                            })
                        },
                        METH_LIST_EVENTS => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let events = app_state.write().await.list_events().await
                                .map_err(anyhow_to_rpc_error)?;
                            to_rpcvalue(&events).map_err(|e| string_to_rpc_error(e.to_string()))
                        }),
                        METH_EVENT_DATA => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let p = JustEventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default())
                                .map_err(anyhow_to_rpc_error)?;
                            let res = app_state.read().await.load_event_record(p.event_id).await;
                            res.map(|mut rec| {
                                rec.api_token = "".into();
                                rec.to_rpcvalue()
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
                            let res = app_state.read().await.opened_event_status(event_id).await;
                            res.map(|status| status.to_rpcvalue()).map_err(anyhow_to_rpc_error)
                        }),
                        METH_EVENT_UPDATE_LATE_ENTRY => {
                            let params = match LateEntryParams::try_from(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_NODE_METHODS, anyhow::anyhow!(err)),
                            };
                            let methods = escalate_event_owner_rights(&rq, app_state.clone(), Some(event_id), method.to_owned(), EVENTCTL_NODE_METHODS).await;
                            m.resolve(methods, async move || {
                                let Some(user_id) = sanitize_user_id(&rq).map(str::to_string) else {
                                    return Err::<RpcValue, _>(str_to_rpc_error("user id is required"));
                                };
                                app_state.read().await.opened_event_status(event_id).await.map_err(anyhow_to_rpc_error)?;
                                let qxsql = RemoteEventSqlApi::new(event_id, client_cmd_tx.clone());
                                if let Some(qxchange_id) = params.change_id {
                                    if params.late_entry.is_none() {
                                        // delete existing record with no changes
                                        let ok = qxsql.delete_record_event("qxchanges", qxchange_id, None).await.map_err(anyhow_to_rpc_error)?;
                                        return Ok(RpcValue::from(ok))
                                    }
                                    let qxchange = QxChangeRecord {
                                        data: params.late_entry.map(|le| Data::LateEntry(le)),
                                        ..Default::default()
                                    };
                                    let ok = qxsql.update_record_event("qxchanges", qxchange_id, &qxchange.to_record(), None).await.map_err(anyhow_to_rpc_error)?;
                                    Ok(RpcValue::from(ok))
                                } else {
                                    let (foreign_table, foreign_id) = if let Some(le) = &params.late_entry {
                                        match le.id {
                                            qxchange::LateEntryId::RunId(id) => ("runs".to_string(), id),
                                            qxchange::LateEntryId::ClassId(id) => ("classes".to_string(), id),
                                        }
                                    } else {
                                        return Err(str_to_rpc_error("Run or Class ID is required"));
                                    };
                                    let qxchange = QxChangeRecord {
                                        stage_id: Some(params.stage_id),
                                        foreign_table: Some(foreign_table),
                                        foreign_id: Some(foreign_id),
                                        data_type: Some("LateEntry".to_string()),
                                        data: params.late_entry.map(|le| Data::LateEntry(le)),
                                        user_id: Some(user_id),
                                        status: Some(qxchange::Status::Pending),
                                        status_message: None,
                                    };
                                    let id = qxsql.create_record_event("qxchanges", &qxchange.to_record(), None).await.map_err(anyhow_to_rpc_error)?;
                                    Ok(RpcValue::from(id))
                                }
                            })
                        },
                        METH_EVENT_CLOSE => m.resolve(EVENTCTL_NODE_METHODS, async move || {
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


#[derive(Debug, Serialize, Deserialize)]
struct LateEntryParams {
    stage_id: i64,
    change_id: Option<i64>,
    late_entry: Option<LateEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    issuer: Option<String>,
}
impl TryFrom<&RpcValue> for LateEntryParams {
    type Error = String;
    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_event_record_params_try_from_rpcvalue() {
        let value = RpcValue::from_cpon(r#"{"event_id":2,"change":{"api_token":"gubosinori","config":{"members":{"member@example.com":"Organizer"}},"stage_count":3}}"#).unwrap();
        let params = UpdateEventRecordParams::try_from_rpcvalue(&value).unwrap();

        assert_eq!(params.event_id, 2);
        assert_eq!(params.change.stage_count, Some(3));
        assert_eq!(params.change.api_token.as_deref(), Some("gubosinori"));
        let config = params.change.config.unwrap();
        assert!(matches!(config.members.get("member@example.com"), Some(Role::Organizer)));
    }

    #[test]
    fn set_event_db_mode_params_try_from_rpcvalue() {
        let value = RpcValue::from_cpon(r#"{"event_id":2,"db_mode":"Replica"}"#).unwrap();
        let params = SetEventDbModeParams::try_from_rpcvalue(&value).unwrap();

        assert_eq!(params.event_id, 2);
        assert_eq!(params.db_mode, crate::state::EventDbMode::Replica);
        assert_eq!(params.to_rpcvalue().to_cpon(), r#"{"db_mode":"Replica","event_id":2}"#);
    }

    #[test]
    fn params_reject_missing_field() {
        assert!(SetEventDbModeParams::try_from_rpcvalue(&RpcValue::from_cpon(r#"{"event_id":2}"#).unwrap()).is_err());
    }

    #[test]
    fn push_event_db_params_try_from_rpcvalue() {
        let value = RpcValue::from_cpon(r#"{"event_id":2,"compressed_data":b"abc"}"#).unwrap();
        let params = PushEventDbParams::try_from_rpcvalue(&value).unwrap();

        assert_eq!(params.event_id, 2);
        assert_eq!(params.compressed_data, b"abc");
    }

    #[test]
    fn push_event_db_params_rpc_type() {
        assert_eq!(PushEventDbParams::RPC_TYPE, "{i:event_id,x:compressed_data}");
    }

    #[test]
    fn single_field_param_is_transparent() {
        let params = JustEventIdParam::try_from_rpcvalue(&RpcValue::from_cpon("42").unwrap()).unwrap();
        assert_eq!(params.event_id, 42);
        assert_eq!(params.to_rpcvalue().to_cpon(), "42");

        let params = DeleteEventParams::try_from_rpcvalue(&RpcValue::from_cpon(r#""tok""#).unwrap()).unwrap();
        assert_eq!(params.api_token, "tok");
        assert_eq!(params.to_rpcvalue().to_cpon(), r#""tok""#);
    }
}
