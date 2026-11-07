
use log::warn;
use qxsql::sql::{CREATE_PARAMS, CREATE_RESULT, DELETE_PARAMS, DELETE_RESULT, EXEC_PARAMS, EXEC_RESULT, QUERY_PARAMS, QUERY_RESULT, READ_PARAMS, READ_RESULT, UPDATE_PARAMS, UPDATE_RESULT};
use qxsql::{QueryAndParams, QxSqlApi, QxSqlApiRecChng, RecDeleteParam, RecInsertParam, RecReadParam, RecUpdateParam};
use serde::{Serialize, Deserialize};
use crate::eventsqlapi::LocalEventSqlApi;
use crate::filenode::{META_FILE_APPEND, META_FILE_CRC, META_FILE_READ, META_FILE_SHA1, META_FILE_SIZE, META_FILE_STAT, META_FILE_TRUNCATE, META_FILE_WRITE};
use crate::rpcvalueext::RpcValueExt;
use crate::rpctype::RpcTypeStr;
use shvclient::ClientCommandSender;
use shvclient::clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request};
use shvproto::{RpcValue, from_rpcvalue};
use shvrpc::metamethod::{AccessLevel, MetaMethod, Flags};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use crate::qxchange::{self, Data, LateEntry, QxChangeRecord, QxChangeRecordChange};
use crate::qxeventconfig::Role;
use crate::remotedbsqlapi::RemoteEventSqlApi;
use crate::{anyhow_to_rpc_error, filenode, resolve_error, split_first_fragment, str_to_rpc_error};
use crate::state::{open_event, setup_remote_db_recchng_monitoring, pulled_event_db_file, pushed_event_db_file, EventDbMode, EventId, EventListItem, EventRecord, EventRecordChange, SharedAppState, State};


#[derive(Debug)]
enum EventCtlNode {
    Root,
    Event(EventId),
    EventSqlApi(EventId),
    PushEventDbFileNode(EventId),
    PullEventDbFileNode(EventId),
}

impl EventCtlNode {
    fn from_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::Root);
        }
        let (event_id_str, dir) = split_first_fragment(path, '/');
        let event_id = event_id_str.parse::<i64>()?;
        match dir {
            "" => Ok(Self::Event(event_id)),
            "sql" => Ok(Self::EventSqlApi(event_id)),
            "pushDb" => Ok(Self::PushEventDbFileNode(event_id)),
            "pullDb" => Ok(Self::PullEventDbFileNode(event_id)),
            _ => Err(anyhow::anyhow!("invalid path: {}", path))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
struct EventIdParam {
    event_id: EventId,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
struct ApiTokenParam {
    api_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
struct CreateEventParam {
    owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
struct CreateEventResult {
    event_id: EventId,
    api_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
struct OpenEventResult {
    event_id: EventId,
    mount_point: String,
}

fn sanitize_user_id(rq: &RpcMessage) -> Option<&str> {
    rq.user_id().map(|user_id| user_id.split(':').next().unwrap_or(user_id))
}

pub const QX_API_TOKEN: &str = "qx_api_token";

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
pub struct UpdateEventRecordParams {
    event_id: EventId,
    change: EventRecordChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
pub struct SetEventDbModeParams {
    event_id: EventId,
    db_mode: crate::state::EventDbMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
#[serde(transparent)]
struct ListEventsResult {
    events: Vec<EventListItem>,
}

async fn escalate_event_owner_rights(rq: &RpcMessage, app_state: SharedAppState, event_id: Option<EventId>, method_name: String, methods: &'static [MetaMethod]) -> Vec<MetaMethod> {
    // log::info!("escalate_event_owner_rights, method_name: {method_name}, event id: {event_id:?}");
    let Some(mm) = methods.iter().find(|&m| m.name == method_name) else {
        warn!("Unknown method: {method_name}");
        return vec![];
    };

    if !mm.flags.contains(Flags::UserIDRequired) {
        return vec![ mm.clone() ];
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
            const METH_CREATE_EVENT: &str = "createEvent";
            const METH_OPEN_EVENT: &str = "openEvent";
            const METH_OPEN_EVENT_API_KEY: &str = "openEventApiKey";
            const METH_READ_EVENT_RECORD: &str = "readEventRecord";
            const METH_UPDATE_EVENT_RECORD: &str = "updateEventRecord";
            const METH_SET_EVENT_DB_MODE: &str = "setEventDbMode";
            const METH_DELETE_EVENT: &str = "deleteEvent";
            const METH_LIST_EVENTS: &str = "listEvents";
            const METH_EVENT_DATA: &str = "eventData";
            const EVENTCTL_ROOT_METHODS: &[MetaMethod] = &[
                META_METHOD_DIR,
                META_METHOD_LS,
                MetaMethod::new_static(
                    METH_CREATE_EVENT, Flags::None, AccessLevel::Write, CreateEventParam::RPC_TYPE_STR, CreateEventResult::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    METH_OPEN_EVENT, Flags::None, AccessLevel::Read, EventIdParam::RPC_TYPE_STR, OpenEventResult::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    METH_OPEN_EVENT_API_KEY, Flags::None, AccessLevel::Read, ApiTokenParam::RPC_TYPE_STR, OpenEventResult::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    // user with Service access or user_id == event_owner can read event records
                    // read event record is privileged operation, because it exposes api_key
                    METH_READ_EVENT_RECORD, Flags::UserIDRequired, AccessLevel::Service, EventIdParam::RPC_TYPE_STR, EventRecord::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    // user with Service access or user_id == event_owner can update event records
                    METH_UPDATE_EVENT_RECORD, Flags::UserIDRequired, AccessLevel::Service, UpdateEventRecordParams::RPC_TYPE_STR, bool::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    // user with Service access or user_id == event_owner can change the database mode
                    METH_SET_EVENT_DB_MODE, Flags::UserIDRequired, AccessLevel::Service, SetEventDbModeParams::RPC_TYPE_STR, bool::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    METH_DELETE_EVENT, Flags::UserIDRequired, AccessLevel::Read, ApiTokenParam::RPC_TYPE_STR, bool::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    METH_LIST_EVENTS, Flags::None, AccessLevel::Read, "n", ListEventsResult::RPC_TYPE_STR, &[], "",
                ),
                MetaMethod::new_static(
                    METH_EVENT_DATA, Flags::None, AccessLevel::Read, EventIdParam::RPC_TYPE_STR, EventRecord::RPC_TYPE_STR, &[], "",
                ),
            ];

            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENTCTL_ROOT_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_ROOT_METHODS, async move || {
                    Ok(list_events(app_state).await)
                }),
                Method::Other(m) => {
                    let method = m.method();

                    match method {
                        METH_CREATE_EVENT => {
                            let p = match CreateEventParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                                let (event_id, api_token) = app_state.write().await.create_event(p.owner, client_cmd_tx.clone()).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(CreateEventResult { event_id, api_token }.to_rpcvalue())
                            })
                        },
                        METH_OPEN_EVENT => {
                            let p = match EventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                                let mount_point = open_event(app_state, p.event_id, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(OpenEventResult { event_id: p.event_id, mount_point }.to_rpcvalue())
                            })
                        },
                        METH_OPEN_EVENT_API_KEY => {
                            let p = match ApiTokenParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                                let event_id = app_state.read().await.api_token_to_event_id(&p.api_token).await
                                    .map_err(anyhow_to_rpc_error)?;
                                let mount_point = open_event(app_state, event_id, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(OpenEventResult { event_id, mount_point }.to_rpcvalue())
                            })
                        },
                        METH_READ_EVENT_RECORD => {
                            let p = match EventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
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
                                let res = app_state.write().await.set_event_db_mode(p.event_id, p.db_mode, client_cmd_tx.clone()).await
                                    .map_err(anyhow_to_rpc_error)?;
                                if matches!(p.db_mode, crate::state::EventDbMode::Replica) {
                                    setup_remote_db_recchng_monitoring(app_state, p.event_id, client_cmd_tx).await;
                                }
                                Ok(res)
                            })
                        }
                        METH_DELETE_EVENT => {
                            let p = match ApiTokenParam::try_from_rpcvalue(rq.param().unwrap_or_default()) {
                                Ok(params) => params,
                                Err(err) => return resolve_error(&m, EVENTCTL_ROOT_METHODS, err),
                            };
                            m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                                let event_id = app_state.read().await.api_token_to_event_id(&p.api_token).await
                                    .map_err(anyhow_to_rpc_error)?;
                                let was_deleted = app_state.write().await.delete_event(event_id, client_cmd_tx).await
                                    .map_err(anyhow_to_rpc_error)?;
                                Ok(RpcValue::from(was_deleted))
                            })
                        },
                        METH_LIST_EVENTS => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let events = app_state.write().await.list_events().await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(ListEventsResult { events }.to_rpcvalue())
                        }),
                        METH_EVENT_DATA => m.resolve(EVENTCTL_ROOT_METHODS, async move || {
                            let p = EventIdParam::try_from_rpcvalue(rq.param().unwrap_or_default())
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
            const METH_EVENT_STATUS: &str = "status";
            const METH_EVENT_UPDATE_LATE_ENTRY: &str = "updateLateEntry";
            const METH_EVENT_CLOSE: &str = "close";
            const EVENTCTL_NODE_METHODS: &[MetaMethod] = &[
                META_METHOD_DIR,
                META_METHOD_LS,
                MetaMethod::new_static(
                    METH_EVENT_UPDATE_LATE_ENTRY, Flags::UserIDRequired, AccessLevel::Read, "{i:run_id,{?}:record}", "b|i", &[], "",
                ),
                MetaMethod::new_static(
                    METH_EVENT_STATUS, Flags::None, AccessLevel::Read, "", "{?}", &[], "",
                ),
                MetaMethod::new_static(
                    METH_EVENT_CLOSE, Flags::None, AccessLevel::Read, "",  "", &[], "",
                ),
            ];

            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(EVENTCTL_NODE_METHODS),
                Method::Ls(ls) => ls.resolve(EVENTCTL_NODE_METHODS, async move || {
                    Ok(vec!["sql".into(), "pushDb".into(), "pullDb".into()])
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
                            m.resolve(EVENTCTL_NODE_METHODS, async move || {
                                let Some(user_id) = sanitize_user_id(&rq).map(str::to_string) else {
                                    return Err::<RpcValue, _>(str_to_rpc_error("user id is required"));
                                };
                                app_state.read().await.opened_event_status(event_id).await.map_err(anyhow_to_rpc_error)?;
                                let db_mode = app_state.write().await.event_db_mode(event_id).await.map_err(anyhow_to_rpc_error)?;
                                // Replica writes go to the remote master DB; Master writes go to the local event DB.
                                let qxsql: Box<dyn QxSqlApiRecChng> = match db_mode {
                                    EventDbMode::Replica => Box::new(RemoteEventSqlApi::new(event_id, client_cmd_tx.clone())),
                                    EventDbMode::Master => Box::new(LocalEventSqlApi::new(event_id, app_state.clone(), client_cmd_tx.clone())),
                                    _ => return Err(str_to_rpc_error(&format!("Late entries cannot be updated in {db_mode:?} DB mode"))),
                                };
                                if let Some(qxchange_id) = params.change_id {
                                    if params.late_entry.is_none() {
                                        // delete existing record with no changes
                                        let ok = qxsql.delete_record_with_recchng("qxchanges", qxchange_id, None).await.map_err(anyhow_to_rpc_error)?;
                                        return Ok(RpcValue::from(ok))
                                    }
                                    let qxchange = QxChangeRecordChange {
                                        data: params.late_entry.map(|le| Data::LateEntry(le)),
                                        ..Default::default()
                                    };
                                    let ok = qxsql.update_record_with_recchng("qxchanges", qxchange_id, &qxchange.to_record(), None).await.map_err(anyhow_to_rpc_error)?;
                                    Ok(RpcValue::from(ok))
                                } else {
                                    let (foreign_table, foreign_id) = if let Some(le) = &params.late_entry {
                                        match le.kind {
                                            qxchange::LateEntryKind::Update { run_id: id } => ("runs".to_string(), id),
                                            qxchange::LateEntryKind::NewEntry { class_id: id } => ("classes".to_string(), id),
                                            qxchange::LateEntryKind::DeleteEntry { run_id: id } => ("runs".to_string(), id),
                                        }
                                    } else {
                                        return Err(str_to_rpc_error("Run or Class ID is required"));
                                    };
                                    let qxchange = QxChangeRecord {
                                        stage_id: params.stage_id,
                                        foreign_table: Some(foreign_table),
                                        foreign_id: Some(foreign_id),
                                        data_type: "LateEntry".to_string(),
                                        data: params.late_entry.map(|le| Data::LateEntry(le)),
                                        user_id: Some(user_id),
                                        status: qxchange::Status::default(),
                                        status_message: None,
                                        created: chrono::Utc::now().fixed_offset(),
                                    };

                                    let id = qxsql.create_record_with_recchng("qxchanges", &qxchange.to_record(), None).await.map_err(anyhow_to_rpc_error)?;
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
        EventCtlNode::EventSqlApi(event_id) => {
            const METH_SQL_QUERY: &str = "query";
            const METH_SQL_EXEC: &str = "exec";
            const METH_SQL_CREATE: &str = "create";
            const METH_SQL_READ: &str = "read";
            const METH_SQL_UPDATE: &str = "update";
            const METH_SQL_DELETE: &str = "delete";
            const EVENTCTL_SQL_NODE_METHODS: &[MetaMethod] = &[
                META_METHOD_DIR, META_METHOD_LS,
                MetaMethod::new_static(
                    METH_SQL_QUERY, Flags::None, AccessLevel::Read, QUERY_PARAMS, QUERY_RESULT, &[], "",
                ),
                MetaMethod::new_static(
                    METH_SQL_EXEC, Flags::UserIDRequired, AccessLevel::Write, EXEC_PARAMS, EXEC_RESULT, &[], "",
                ),
                MetaMethod::new_static(
                    METH_SQL_CREATE, Flags::UserIDRequired, AccessLevel::Write, CREATE_PARAMS, CREATE_RESULT, &[], "",
                ),
                MetaMethod::new_static(
                    METH_SQL_READ, Flags::None, AccessLevel::Read, READ_PARAMS, READ_RESULT, &[], "",
                ),
                MetaMethod::new_static(
                    METH_SQL_UPDATE, Flags::UserIDRequired, AccessLevel::Write, UPDATE_PARAMS, UPDATE_RESULT, &[], "",
                ),
                MetaMethod::new_static(
                    METH_SQL_DELETE, Flags::UserIDRequired, AccessLevel::Write, DELETE_PARAMS, DELETE_RESULT, &[], "",
                ),
            ];

            let methods = EVENTCTL_SQL_NODE_METHODS;
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => ls.resolve(methods, async move || Ok(vec![])),
                Method::Other(m) => {
                    let method = m.method();
                    let rights = escalate_event_owner_rights(&rq, app_state.clone(), Some(event_id), method.to_owned(), methods);
                    match method {
                        METH_SQL_QUERY => m.resolve(rights.await, async move || {
                            let query = QueryAndParams::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.query(query.query(), query.params()).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_EXEC => m.resolve(rights.await, async move || {
                            let query = QueryAndParams::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.exec(query.query(), query.params()).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_CREATE => m.resolve(rights.await, async move || {
                            let param = RecInsertParam::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.create_record_with_recchng(&param.table, &param.record, param.issuer).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_READ => m.resolve(rights.await, async move || {
                            let param = RecReadParam::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let fields = qxsql::string_list_to_ref_vec(&param.fields);
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.read_record(&param.table, param.id, fields).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_UPDATE => m.resolve(rights.await, async move || {
                            let param = RecUpdateParam::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.update_record_with_recchng(&param.table, param.id, &param.record, param.issuer).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        METH_SQL_DELETE => m.resolve(rights.await, async move || {
                            let param = RecDeleteParam::try_from(rq.param().unwrap_or_default())
                                .map_err(|err| str_to_rpc_error(&err))?;
                            let sql_api = LocalEventSqlApi::new(event_id, app_state, client_cmd_tx);
                            sql_api.delete_record_with_recchng(&param.table, param.id, param.issuer).await
                                .map(|res| res.to_rpcvalue())
                                .map_err(anyhow_to_rpc_error)
                        }),
                        _ => err_unresolved_request(),
                    }
                }
            }
        }
        EventCtlNode::PushEventDbFileNode(event_id) => {
            const METH_PUSH_DB_COMMIT: &str = "commit";
            const PUSH_EVENT_DB_FILE_NODE_METHODS: &[MetaMethod] = &[
                META_METHOD_DIR, META_METHOD_LS, META_FILE_STAT, META_FILE_SIZE, META_FILE_CRC, META_FILE_SHA1, META_FILE_READ, META_FILE_WRITE, META_FILE_TRUNCATE, META_FILE_APPEND,
                MetaMethod::new_static(
                    // param is SHA-1 of the uncompressed database
                    METH_PUSH_DB_COMMIT, Flags::UserIDRequired, AccessLevel::Service, "b(20)", "n", &[], "",
                ),
            ];

            let methods = PUSH_EVENT_DB_FILE_NODE_METHODS;
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => ls.resolve(methods, async move || Ok(vec![])),
                Method::Other(m) => {
                    let method = m.method();
                    let rights = escalate_event_owner_rights(&rq, app_state.clone(), Some(event_id), method.to_owned(), methods);
                    if method == METH_PUSH_DB_COMMIT {
                        let expected_sha1 = match parse_sha1_param(rq.param().unwrap_or_default()) {
                            Ok(expected_sha1) => expected_sha1,
                            Err(err) => return resolve_error(&m, methods, err),
                        };
                        return m.resolve(rights.await, async move || {
                            State::commit_pushed_event_db(app_state, event_id, expected_sha1, client_cmd_tx).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::null())
                        });
                    }
                    filenode::resolve_op(&m, &rq, methods, rights, async move |op| {
                        // commit holds the write lock, so it cannot move the file under a running modification
                        let _commit_guard = if op.modifies_file() { Some(app_state.read().await) } else { None };
                        let file = pushed_event_db_file(event_id);
                        smol::unblock(move || op.execute(std::path::Path::new(&file))).await
                    }).await
                }
            }
        }
        EventCtlNode::PullEventDbFileNode(event_id) => {
            const METH_PULL_DB_SNAPSHOT: &str = "snapshot";
            const PULL_EVENT_DB_FILE_NODE_METHODS: &[MetaMethod] = &[
                META_METHOD_DIR, META_METHOD_LS, META_FILE_STAT, META_FILE_SIZE, META_FILE_CRC, META_FILE_SHA1, META_FILE_READ,
                MetaMethod::new_static(
                    METH_PULL_DB_SNAPSHOT, Flags::UserIDRequired, AccessLevel::Service, "n", "n", &[], "",
                ),
            ];
            let methods = PULL_EVENT_DB_FILE_NODE_METHODS;
            match Method::from_request(&rq) {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => ls.resolve(methods, async move || Ok(vec![])),
                Method::Other(m) => {
                    let method = m.method();
                    let rights = escalate_event_owner_rights(&rq, app_state.clone(), Some(event_id), method.to_owned(), methods);
                    if method == METH_PULL_DB_SNAPSHOT {
                        return m.resolve(rights.await, async move || {
                            State::snapshot_event_db(app_state, event_id).await
                                .map_err(anyhow_to_rpc_error)?;
                            Ok(RpcValue::null())
                        });
                    }
                    filenode::resolve_op(&m, &rq, methods, rights, async move |op| {
                        let file = pulled_event_db_file(event_id);
                        smol::unblock(move || {
                            let path = std::path::Path::new(&file);
                            // an empty file would look like a successfully downloaded empty database
                            if !path.exists() {
                                anyhow::bail!("Event {event_id} DB snapshot does not exist, call {METH_PULL_DB_SNAPSHOT} first");
                            }
                            op.execute(path)
                        }).await
                    }).await
                }
            }
        }
    }
}

fn parse_sha1_param(param: &RpcValue) -> anyhow::Result<Vec<u8>> {
    let sha1 = <&[u8]>::try_from(param).map_err(|e| anyhow::anyhow!("Invalid SHA-1 param: {e}"))?;
    if sha1.len() != 20 {
        anyhow::bail!("Invalid SHA-1 param, 20 bytes expected, got: {}", sha1.len());
    }
    Ok(sha1.to_vec())
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
    late_entry: Option<LateEntry>, // drop change if None
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
    fn event_record_rpc_types() {
        assert_eq!(
            EventRecord::RPC_TYPE_STR,
            "{i|n:id,s:name,t:date,i:stage_count,s:owner,s:api_token,{{s}:members}:config,s:db_mode}"
        );
        assert_eq!(
            UpdateEventRecordParams::RPC_TYPE_STR,
            "{i:event_id,{s|n:name,t|n:date,i|n:stage_count,s|n:owner,s|n:api_token,{{s}:members}|n:config,s|n:db_mode}:change}"
        );
        assert_eq!(SetEventDbModeParams::RPC_TYPE_STR, "{i:event_id,s:db_mode}");
        assert_eq!(ListEventsResult::RPC_TYPE_STR, "[{i:id,s:name,i:stage_count,t:date,s:owner,s:db_mode}]");

        let record = EventRecord {
            id: Some(1),
            name: "ev".into(),
            date: chrono::DateTime::parse_from_rfc3339("2026-10-31T10:00:00+01:00").unwrap(),
            stage_count: 2,
            owner: "owner".into(),
            api_token: "tok".into(),
            config: crate::qxeventconfig::QxEventConfig {
                members: [("member".to_string(), Role::Organizer)].into(),
            },
            db_mode: crate::state::EventDbMode::Master,
        };
        let value = record.to_rpcvalue();
        assert_eq!(value.get("date").unwrap().type_name(), "DateTime");
        assert_eq!(value.get("db_mode").unwrap().as_str(), "Master");
        assert_eq!(value.get("config").unwrap().get("members").unwrap().get("member").unwrap().as_str(), "Organizer");
    }

    #[test]
    fn single_field_param_is_map() {
        let params = EventIdParam::try_from_rpcvalue(&RpcValue::from_cpon(r#"{"event_id":42}"#).unwrap()).unwrap();
        assert_eq!(params.event_id, 42);
        assert_eq!(params.to_rpcvalue().to_cpon(), r#"{"event_id":42}"#);
        assert!(EventIdParam::try_from_rpcvalue(&RpcValue::from_cpon("42").unwrap()).is_err());
        assert_eq!(EventIdParam::RPC_TYPE_STR, "{i:event_id}");

        let params = ApiTokenParam::try_from_rpcvalue(&RpcValue::from_cpon(r#"{"api_token":"tok"}"#).unwrap()).unwrap();
        assert_eq!(params.api_token, "tok");
        assert_eq!(params.to_rpcvalue().to_cpon(), r#"{"api_token":"tok"}"#);
    }
}
