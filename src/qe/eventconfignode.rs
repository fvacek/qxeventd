use chrono::{DateTime, FixedOffset};
use qxsql::{QxSqlApi, QxSqlApiRecChng, sql::record_from_slice};
use shvclient::{ClientCommandSender, clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request}};
use shvproto::{RpcValue, Value};
use shvrpc::{RpcMessage, metamethod::{AccessLevel, Flags, MetaMethod}};

use super::{EventConfig, KEY_COUNTRY, KEY_CURRENT_STAGE, KEY_DATE, KEY_DIRECTOR, KEY_MAIN_REFEREE, KEY_NAME, KEY_PLACE, KEY_STAGE_COUNT, KEY_TIME, normalize_date, normalize_time, stage_start_date_time_key};
use crate::eventctlnode::{escalate_event_owner_rights, sanitize_user_id};
use crate::eventsqlapi::LocalEventSqlApi;
use shvproto::RpcTypeStr;
use crate::rpcvalueext::RpcValueExt;
use crate::{anyhow_to_rpc_error, split_first_fragment, str_to_rpc_error};
use crate::state::{EventDbMode, EventId, SharedAppState, State};

const METH_GET: &str = "get";
const METH_SET: &str = "set";
const SIG_CHNG: &str = "chng";

const EVENT_FIELDS: &[&str] = &["name", "place", "country", "stageCount", "currentStage", "date", "time", "director", "mainReferee", "stages"];
const STAGE_FIELDS: &[&str] = &["startDateTime"];

const DIR_NODE_METHODS: &[MetaMethod] = &[META_METHOD_DIR, META_METHOD_LS];
const ROOT_NODE_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(METH_GET, Flags::IsGetter, AccessLevel::Read, "", EventConfig::RPC_TYPE_STR, &[], ""),
];

macro_rules! value_node_methods {
    ($type:literal) => {
        &[
            META_METHOD_DIR,
            META_METHOD_LS,
            MetaMethod::new_static(
                METH_GET, Flags::IsGetter, AccessLevel::Read, "", $type, &[(SIG_CHNG, Some($type))], "",
            ),
            MetaMethod::new_static(
                // user with Write access or event organizer can change the event config
                METH_SET, Flags::IsSetter.union(Flags::UserIDRequired), AccessLevel::Write, $type, "", &[], "",
            ),
        ]
    };
}
const STRING_NODE_METHODS: &[MetaMethod] = value_node_methods!("s");
const INT_NODE_METHODS: &[MetaMethod] = value_node_methods!("i");
const DATETIME_NODE_METHODS: &[MetaMethod] = value_node_methods!("t");

#[derive(Debug, Clone, Copy)]
enum ValueType {
    String,
    Int,
    DateTime,
}

/// Node of the [`EventConfig`] tree.
#[derive(Debug, Clone, Copy)]
enum ConfigNode {
    Root,
    Name,
    Place,
    Country,
    StageCount,
    CurrentStage,
    Date,
    Time,
    Director,
    MainReferee,
    Stages,
    Stage,
    StageStartDateTime(usize),
}

impl ConfigNode {
    /// `None` if the path does not exist, stages are numbered from 1 to `stage_count`.
    fn from_path(path: &str, config: &EventConfig) -> Option<Self> {
        let (field, rest) = split_first_fragment(path, '/');
        let node = match field {
            "" => Self::Root,
            "name" => Self::Name,
            "place" => Self::Place,
            "country" => Self::Country,
            "stageCount" => Self::StageCount,
            "currentStage" => Self::CurrentStage,
            "date" => Self::Date,
            "time" => Self::Time,
            "director" => Self::Director,
            "mainReferee" => Self::MainReferee,
            "stages" => {
                let (stage, rest) = split_first_fragment(rest, '/');
                if stage.is_empty() {
                    Self::Stages
                } else {
                    let stage = stage.parse::<usize>().ok().filter(|&stage| config.stage(stage).is_some())?;
                    return match rest {
                        "" => Some(Self::Stage),
                        "startDateTime" => Some(Self::StageStartDateTime(stage)),
                        _ => None,
                    };
                }
            }
            _ => return None,
        };
        rest.is_empty().then_some(node)
    }

    fn value_type(self) -> Option<ValueType> {
        match self {
            Self::Root | Self::Stages | Self::Stage => None,
            Self::Name | Self::Place | Self::Country | Self::Date | Self::Time | Self::Director | Self::MainReferee => Some(ValueType::String),
            Self::StageCount | Self::CurrentStage => Some(ValueType::Int),
            Self::StageStartDateTime(_) => Some(ValueType::DateTime),
        }
    }

    fn methods(self) -> &'static [MetaMethod] {
        if matches!(self, Self::Root) {
            ROOT_NODE_METHODS
        } else {
            match self.value_type() {
                None => DIR_NODE_METHODS,
                Some(ValueType::String) => STRING_NODE_METHODS,
                Some(ValueType::Int) => INT_NODE_METHODS,
                Some(ValueType::DateTime) => DATETIME_NODE_METHODS,
            }
        }
    }

    fn children(self, config: &EventConfig) -> Vec<String> {
        match self {
            Self::Root => EVENT_FIELDS.iter().map(|f| f.to_string()).collect(),
            Self::Stages => (1..=config.stages.len()).map(|stage| stage.to_string()).collect(),
            Self::Stage => STAGE_FIELDS.iter().map(|f| f.to_string()).collect(),
            _ => vec![],
        }
    }

    fn get(self, config: &EventConfig) -> RpcValue {
        let datetime = |dt: &DateTime<FixedOffset>| RpcValue::from(shvproto::DateTime::from_datetime(dt));
        match self {
            Self::Root => config.to_rpcvalue(),
            Self::Stages | Self::Stage => RpcValue::null(),
            Self::Name => config.name.as_str().into(),
            Self::Place => config.place.as_str().into(),
            Self::Country => config.country.as_str().into(),
            Self::StageCount => (config.stage_count as i64).into(),
            Self::CurrentStage => (config.current_stage as i64).into(),
            Self::Date => config.date.as_str().into(),
            Self::Time => config.time.as_str().into(),
            Self::Director => config.director.as_str().into(),
            Self::MainReferee => config.main_referee.as_str().into(),
            Self::StageStartDateTime(stage) => config.stage(stage).map_or_else(RpcValue::null, |s| datetime(&s.start_date_time)),
        }
    }

    /// Config table `(key, value)` to be written for `value`, it must be of the node value type.
    fn config_value(self, value: &RpcValue) -> Result<(String, String), String> {
        let string = || match &value.value {
            Value::String(s) => Ok(s.to_string()),
            _ => Err(format!("String expected, got: {}", value.type_name())),
        };
        let int = || match &value.value {
            Value::Int(i) if *i >= 0 => Ok(i.to_string()),
            Value::UInt(i) => Ok(i.to_string()),
            _ => Err(format!("Non-negative Int expected, got: {}", value.to_cpon())),
        };
        let datetime = || match &value.value {
            Value::DateTime(dt) => Ok(dt.to_chrono_datetime()),
            _ => Err(format!("DateTime expected, got: {}", value.type_name())),
        };
        let one = |key: &str, value: String| (key.to_string(), value);
        Ok(match self {
            Self::Root | Self::Stages | Self::Stage => return Err("Node has no value".to_string()),
            Self::Name => one(KEY_NAME, string()?),
            Self::Place => one(KEY_PLACE, string()?),
            Self::Country => one(KEY_COUNTRY, string()?),
            Self::StageCount => {
                let count = int()?;
                if count == "0" {
                    return Err("Stage count must be at least 1".to_string());
                }
                one(KEY_STAGE_COUNT, count)
            }
            Self::CurrentStage => one(KEY_CURRENT_STAGE, int()?),
            Self::Date => one(KEY_DATE, normalize_date(&string()?).ok_or("Date YYYY-MM-DD expected")?),
            Self::Time => one(KEY_TIME, normalize_time(&string()?).ok_or("Time HH:MM[:SS] expected")?),
            Self::Director => one(KEY_DIRECTOR, string()?),
            Self::MainReferee => one(KEY_MAIN_REFEREE, string()?),
            Self::StageStartDateTime(stage) => one(&stage_start_date_time_key(stage), datetime()?.to_rfc3339()),
        })
    }
}

/// Inserts or updates the event `config` table value of `key` and emits `recchng`.
async fn set_config_value(sql_api: &LocalEventSqlApi, key: &str, value: &str, issuer: Option<String>) -> anyhow::Result<()> {
    let result = sql_api.query("SELECT id FROM config WHERE id = :id", Some(&record_from_slice(&[("id", key.into())]))).await?;
    if result.rows.is_empty() {
        let record = record_from_slice(&[("id", key.into()), ("cvalue", value.into())]);
        sql_api.create_record_with_recchng("config", &record, issuer).await?;
    } else {
        let record = record_from_slice(&[("cvalue", value.into())]);
        sql_api.update_record_with_recchng("config", key.into(), &record, issuer).await?;
    }
    Ok(())
}

/// [`EventConfig`] exposed as a node tree, `stages/<n>` is the stage `n` config.
/// Values can be set only in [`EventDbMode::Master`] DB mode.
pub(crate) async fn resolve_event_config_request(rq: RpcMessage, event_id: EventId, path: String, app_state: SharedAppState, client_cmd_tx: ClientCommandSender) -> RequestHandlerResult {
    let sql_api = LocalEventSqlApi::new(event_id, app_state.clone(), client_cmd_tx.clone());
    let config = match EventConfig::load(&sql_api).await {
        Ok(config) => config,
        Err(err) => {
            log::warn!("Cannot load event {event_id} config: {err}");
            return err_unresolved_request();
        }
    };
    let Some(node) = ConfigNode::from_path(&path, &config) else {
        return err_unresolved_request();
    };
    let methods = node.methods();

    match Method::from_request(&rq) {
        Method::Dir(dir) => dir.resolve(methods),
        Method::Ls(ls) => ls.resolve(methods, async move || Ok(node.children(&config))),
        Method::Other(m) => {
            let method = m.method();
            if node.value_type().is_none()
                && !(matches!(node, ConfigNode::Root) && method == METH_GET)
            {
                return err_unresolved_request();
            }
            match method {
                METH_GET => m.resolve(methods, async move || Ok(node.get(&config))),
                METH_SET => {
                    let rights = escalate_event_owner_rights(&rq, app_state.clone(), Some(event_id), method.to_owned(), methods).await;
                    let issuer = sanitize_user_id(&rq).map(str::to_owned);
                    m.resolve(rights, async move || {
                        let db_mode = State::event_db_mode(&app_state, event_id).await.map_err(anyhow_to_rpc_error)?;
                        if db_mode != EventDbMode::Master {
                            return Err(str_to_rpc_error(&format!("Event config can be set only in Master DB mode, current mode: {db_mode:?}")));
                        }
                        let (key, value) = node.config_value(rq.param().unwrap_or_default())
                            .map_err(|err| str_to_rpc_error(&err))?;
                        set_config_value(&sql_api, &key, &value, issuer).await.map_err(anyhow_to_rpc_error)?;
                        // signal the value as stored, date time loses its sub-second part for example
                        let config = EventConfig::load(&sql_api).await.map_err(anyhow_to_rpc_error)?;
                        let shv_path = format!("eventctl/{event_id}/config/{path}");
                        client_cmd_tx.send_message(RpcMessage::new_signal(&shv_path, SIG_CHNG).with_param(node.get(&config)))
                            .map_err(|err| anyhow_to_rpc_error(err.into()))?;
                        Ok(RpcValue::null())
                    })
                }
                _ => err_unresolved_request(),
            }
        }
    }
}
