use std::collections::BTreeSet;

use qxsql::{QxSqlApi, sql::{DbValue, record_from_slice}};
use shvclient::{ClientCommandSender, clientnode::{META_METHOD_DIR, META_METHOD_LS, Method, RequestHandlerResult, err_unresolved_request}};
use shvproto::RpcValue;
use shvrpc::{RpcMessage, metamethod::{AccessLevel, Flags, MetaMethod}};

use crate::eventctlnode::escalate_event_owner_rights;
use crate::eventsqlapi::LocalEventSqlApi;
use crate::{anyhow_to_rpc_error, str_to_rpc_error};
use crate::state::{EventId, SharedAppState};

const METH_GET: &str = "get";
const METH_SET: &str = "set";
const SIG_CHNG: &str = "chng";

const DIR_NODE_METHODS: &[MetaMethod] = &[META_METHOD_DIR, META_METHOD_LS];
const VALUE_NODE_METHODS: &[MetaMethod] = &[
    META_METHOD_DIR,
    META_METHOD_LS,
    MetaMethod::new_static(
        METH_GET, Flags::IsGetter, AccessLevel::Read, "", "s|n", &[(SIG_CHNG, Some("s|n"))], "",
    ),
    MetaMethod::new_static(
        // user with Write access or event organizer can change the event config
        METH_SET, Flags::IsSetter.union(Flags::UserIDRequired), AccessLevel::Write, "s|i|d|b|n", "b", &[], "",
    ),
];

/// Event database `config` table exposed as a node tree, config key `event.name`
/// is accessible on path `event/name` relative to the event `config` node.
pub(crate) async fn resolve_event_config_request(rq: RpcMessage, event_id: EventId, path: String, app_state: SharedAppState, client_cmd_tx: ClientCommandSender) -> RequestHandlerResult {
    let sql_api = LocalEventSqlApi::new(event_id, app_state.clone(), client_cmd_tx.clone());
    let keys = match config_keys(&sql_api).await {
        Ok(keys) => keys,
        Err(err) => {
            log::warn!("Cannot read event {event_id} config keys: {err}");
            return err_unresolved_request();
        }
    };
    let prefix = if path.is_empty() { String::new() } else { format!("{}.", path.replace('/', ".")) };
    let ckey = path.replace('/', ".");
    let is_value_node = !path.is_empty() && keys.contains(&ckey);
    let children = keys.iter()
        .filter_map(|key| key.strip_prefix(&prefix))
        .map(|rest| rest.split('.').next().unwrap_or(rest).to_string())
        .filter(|child| !child.is_empty())
        .collect::<BTreeSet<_>>();
    if !is_value_node && children.is_empty() && !path.is_empty() {
        return err_unresolved_request();
    }
    let methods = if is_value_node { VALUE_NODE_METHODS } else { DIR_NODE_METHODS };

    match Method::from_request(&rq) {
        Method::Dir(dir) => dir.resolve(methods),
        Method::Ls(ls) => ls.resolve(methods, async move || Ok(children.into_iter().collect())),
        Method::Other(m) => {
            if !is_value_node {
                return err_unresolved_request();
            }
            let method = m.method();
            match method {
                METH_GET => m.resolve(methods, async move || {
                    read_config_value(&sql_api, &ckey).await
                        .map(|value| value.map_or_else(RpcValue::null, RpcValue::from))
                        .map_err(anyhow_to_rpc_error)
                }),
                METH_SET => {
                    let rights = escalate_event_owner_rights(&rq, app_state, Some(event_id), method.to_owned(), methods).await;
                    m.resolve(rights, async move || {
                        let value = rpcvalue_to_config_value(rq.param().unwrap_or_default())
                            .map_err(|err| str_to_rpc_error(&err))?;
                        let shv_path = format!("eventctl/{event_id}/config/{path}");
                        write_config_value(&sql_api, &ckey, value, &shv_path, &client_cmd_tx).await
                            .map(RpcValue::from)
                            .map_err(anyhow_to_rpc_error)
                    })
                }
                _ => err_unresolved_request(),
            }
        }
    }
}

async fn config_keys(sql_api: &LocalEventSqlApi) -> anyhow::Result<BTreeSet<String>> {
    let result = sql_api.query("SELECT ckey FROM config WHERE ckey IS NOT NULL", None).await?;
    Ok(result.rows.iter()
        .filter_map(|row| row.first().and_then(DbValue::as_str).map(str::to_string))
        .filter(|key| !key.is_empty())
        .collect())
}

async fn read_config_value(sql_api: &LocalEventSqlApi, ckey: &str) -> anyhow::Result<Option<String>> {
    let result = sql_api.query(
        "SELECT CAST(cvalue AS TEXT) AS cvalue FROM config WHERE ckey = :ckey",
        Some(&record_from_slice(&[("ckey", ckey.into())])),
    ).await?;
    Ok(result.value(0, 0).and_then(DbValue::as_str).map(str::to_string))
}

/// Config values are stored as text, `null` clears the value.
fn rpcvalue_to_config_value(value: &RpcValue) -> Result<Option<String>, String> {
    use shvproto::Value;
    match &value.value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.to_string())),
        Value::Int(i) => Ok(Some(i.to_string())),
        Value::UInt(i) => Ok(Some(i.to_string())),
        Value::Double(d) => Ok(Some(d.to_string())),
        Value::Bool(b) => Ok(Some(b.to_string())),
        _ => Err(format!("Unsupported config value type: {}", value.type_name())),
    }
}

/// Writes the value and notifies about the change by the `chng` signal on the config node.
async fn write_config_value(
    sql_api: &LocalEventSqlApi,
    ckey: &str,
    value: Option<String>,
    shv_path: &str,
    client_cmd_tx: &ClientCommandSender,
) -> anyhow::Result<bool> {
    let cvalue = value.clone().map_or(DbValue::Null, DbValue::from);
    let result = sql_api.exec(
        "UPDATE config SET cvalue = :cvalue WHERE ckey = :ckey",
        Some(&record_from_slice(&[("ckey", ckey.into()), ("cvalue", cvalue)])),
    ).await?;
    if result.rows_affected == 0 {
        return Ok(false);
    }
    let chng = value.map_or_else(RpcValue::null, RpcValue::from);
    client_cmd_tx.send_message(RpcMessage::new_signal(shv_path, SIG_CHNG).with_param(chng))?;
    Ok(true)
}
