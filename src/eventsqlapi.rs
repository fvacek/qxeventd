use anyhow::anyhow;
use async_sqlite::Pool;
use async_trait::async_trait;
use qxsql::{RecChng};
use qxsql::sql::{ExecResult, QueryResult};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use shvproto::{make_list, to_rpcvalue, from_rpcvalue};

use crate::appsqlapi::AppSqlApi;
use crate::state::{remote_event_mount_point};
use crate::{call_rpc_error_to_anyhow, state::{EventId, SharedAppState}};

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
    async fn local_event_db(&self) -> anyhow::Result<Option<Pool>> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .map(|e| e.local_db.clone())
            .ok_or_else(|| anyhow!("Event id: {} is not open.", self.event_id))
    }
    async fn is_local_event_db(&self) -> anyhow::Result<bool> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .map(|e| e.local_db.is_some())
            .ok_or_else(|| anyhow!("Event id: {} is not open.", self.event_id))
    }
    fn remote_event_sql_path(&self) -> String {
        format!("{}/sql", remote_event_mount_point(self.event_id))
    }
}

#[async_trait]
impl qxsql::QxSqlApi for EventSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> {
        if let Some(db) = self.local_event_db().await? {
            let qxsql = AppSqlApi::new(db, self.rpc_client.clone());
            qxsql.query(query, params).await
        } else {
            let params = to_rpcvalue(&params)?;
            let rpc_value = self.rpc_client.call_rpc_method(self.remote_event_sql_path(), "query", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
                .map_err(call_rpc_error_to_anyhow)?;
            let res: QueryResult = from_rpcvalue(&rpc_value)?;
            Ok(res)
        }
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        if let Some(db) = self.local_event_db().await? {
            let qxsql = AppSqlApi::new(db, self.rpc_client.clone());
            qxsql.exec(query, params).await
        } else {
            let params = to_rpcvalue(&params)?;
            let rpc_value = self.rpc_client.call_rpc_method(self.remote_event_sql_path(), "exec", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
                .map_err(call_rpc_error_to_anyhow)?;
            let res: ExecResult = from_rpcvalue(&rpc_value)?;
            Ok(res)
        }
    }
}

#[async_trait]
impl qxsql::QxSqlApiRecChng for EventSqlApi {
    fn filter_recchng(&self, recchng: RecChng) -> Option<RecChng>  {
        Some(recchng)
    }

    async fn client_command_sender(&self) -> Option<ClientCommandSender>  {
        if self.is_local_event_db().await.unwrap_or(true) {
            None
        } else {
            Some(self.rpc_client.clone())
        }
    }
}
