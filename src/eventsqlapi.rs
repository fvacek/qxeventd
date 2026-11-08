use std::sync::Arc;

use anyhow::anyhow;
use async_sqlite::Pool;
use async_trait::async_trait;
use qxsql::sql::{ExecResult, QueryResult, RecChng};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use crate::localdbsqlapi::LocalDbSqlApi;
use crate::state::{EventId, OpenEventCtl, SharedAppState};

pub struct LocalEventSqlApi {
    event_id: EventId,
    app_state: SharedAppState,
    rpc_client: ClientCommandSender,
}

impl LocalEventSqlApi {
    pub fn new(event_id: EventId, app_state: SharedAppState, rpc_client: ClientCommandSender) -> Self {
        Self {
            event_id,
            app_state,
            rpc_client,
        }
    }
    /// The app state lock is released when this returns, it must not be held during
    /// [`OpenEventCtl::use_db`] usage, see [`OpenEventCtl`].
    async fn opened_event(&self) -> anyhow::Result<Arc<OpenEventCtl>> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .cloned()
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
impl qxsql::QxSqlApi for LocalEventSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> {
        let event = self.opened_event().await?;
        let db = event.use_db().await?;
        let qxsql = LocalDbSqlApi::new(Pool::clone(&db), self.rpc_client.clone());
        qxsql.query(query, params).await
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let event = self.opened_event().await?;
        let db = event.use_db().await?;
        let qxsql = LocalDbSqlApi::new(Pool::clone(&db), self.rpc_client.clone());
        qxsql.exec(query, params).await
    }
}

#[async_trait]
impl qxsql::QxSqlApiRecChng for LocalEventSqlApi {
    fn filter_recchng(&self, recchng: RecChng) -> Option<RecChng>  {
        Some(recchng)
    }

    async fn client_command_sender(&self) -> Option<ClientCommandSender>  {
            Some(self.rpc_client.clone())
    }

    fn recchng_shv_path(&self) -> String {
        format!("eventctl/{}/sql", self.event_id)
    }
}
