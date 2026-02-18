use anyhow::anyhow;
use async_trait::async_trait;
use qxsql::sql::{ExecResult, QueryResult};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use shvproto::{make_list, to_rpcvalue, from_rpcvalue};

use crate::{call_rpc_error_to_anyhow, state::{EventId, SharedAppState, event_mount_point}};

pub struct EventSqlApi {
    event_id: EventId,
    app_state: SharedAppState,
    rpc_client: ClientCommandSender,
}

impl EventSqlApi {
    pub fn new(event_id: EventId, app_state: SharedAppState, rpc_client: ClientCommandSender) -> Self {
        Self {
            event_id,
            app_state,
            rpc_client,
        }
    }
    async fn is_local_event(&self) -> anyhow::Result<bool> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .map(|e| e.data.is_local)
            .ok_or_else(|| anyhow!("Event id: {} is not open.", self.event_id))
    }
    fn event_sql_path(&self) -> String {
        format!("{}/sql", event_mount_point(self.event_id))
    }
}

#[async_trait]
impl qxsql::QxSqlApi for EventSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> {
        if self.is_local_event().await? {
            return Err(anyhow!("Local event database is not implemented yet."));
        }
        let params = to_rpcvalue(&params)?;
        let rpc_value = self.rpc_client.call_rpc_method(self.event_sql_path(), "query", Some(make_list![query, params].into()), None, None::<fn(f64)>).await
            .map_err(call_rpc_error_to_anyhow)?;
        let res: QueryResult = from_rpcvalue(&rpc_value)?;
        Ok(res)
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        if self.is_local_event().await? {
            return Err(anyhow!("Local event database is not implemented yet."));
            // let empty_params = Record::default();
            // let params = params.unwrap_or(&empty_params);
            // sql_exec(&self.0, query, params).await
        }
        let params = to_rpcvalue(&params)?;
        let rpc_value = self.rpc_client.call_rpc_method(self.event_sql_path(), "exec", Some(make_list![query, params].into()), None, None::<fn(f64)>).await
            .map_err(call_rpc_error_to_anyhow)?;
        let res: ExecResult = from_rpcvalue(&rpc_value)?;
        Ok(res)
    }
}

impl qxsql::QxSqlApiRecChng for EventSqlApi {}
