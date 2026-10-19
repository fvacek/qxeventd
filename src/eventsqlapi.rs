use anyhow::anyhow;
use async_sqlite::Pool;
use async_trait::async_trait;
use qxsql::sql::{ExecResult, QueryResult, RecChng};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use crate::localdbsqlapi::LocalDbSqlApi;
use crate::state::{EventId, SharedAppState};

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
    async fn event_db(&self) -> anyhow::Result<Pool> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .map(|e| e.db.clone())
            .ok_or_else(|| anyhow!("Event id: {} is not open.", self.event_id))
    }
    // pub async fn create_record_event(&self, table: &str, record: &Record, issuer: Option<String>) -> anyhow::Result<i64> {
    //         return self.create_record_with_recchng(table, record, issuer).await;
    // }
    // pub async fn update_record_event(&self, table: &str, id: i64, record: &Record, issuer: Option<String>) -> anyhow::Result<bool> {
    //     return self.update_record_with_recchng(table, id, record, issuer).await;
    // }
    // pub async fn delete_record_event(&self, table: &str, id: i64, issuer: Option<String>) -> anyhow::Result<bool> {
    //     return self.delete_record_with_recchng(table, id, issuer).await;
    // }
}

#[async_trait]
impl qxsql::QxSqlApi for EventSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> {
        let qxsql = LocalDbSqlApi::new(self.event_db().await?, self.rpc_client.clone());
        qxsql.query(query, params).await
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let qxsql = LocalDbSqlApi::new(self.event_db().await?, self.rpc_client.clone());
        qxsql.exec(query, params).await
    }
}

#[async_trait]
impl qxsql::QxSqlApiRecChng for EventSqlApi {
    fn filter_recchng(&self, recchng: RecChng) -> Option<RecChng>  {
        Some(recchng)
    }

    async fn client_command_sender(&self) -> Option<ClientCommandSender>  {
            Some(self.rpc_client.clone())
    }
}
