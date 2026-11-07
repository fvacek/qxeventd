use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::anyhow;
use async_sqlite::Pool;
use async_trait::async_trait;
use qxsql::sql::{ExecResult, QueryResult, RecChng};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use crate::localdbsqlapi::LocalDbSqlApi;
use crate::state::{EventId, OpenEventCtl, SharedAppState};

/// Generated column names per table. All event DBs share the same schema,
/// so the cache is process-wide and filled lazily on first use of a table.
static GENERATED_COLUMNS: LazyLock<Mutex<HashMap<String, Arc<Vec<String>>>>> = LazyLock::new(Default::default);

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
    async fn opened_event(&self) -> anyhow::Result<Arc<smol::lock::RwLock<OpenEventCtl>>> {
        self.app_state.read().await.open_events.get(&self.event_id)
            .cloned()
            .ok_or_else(|| anyhow!("Event id: {} is not open.", self.event_id))
    }
    async fn event_db(&self) -> anyhow::Result<Pool> {
        Ok(self.opened_event().await?.read().await.db.clone())
    }
    /// Names of generated (computed) columns of `table`, see [`GENERATED_COLUMNS`].
    pub async fn generated_columns(&self, table: &str) -> anyhow::Result<Arc<Vec<String>>> {
        if let Some(columns) = GENERATED_COLUMNS.lock().unwrap_or_else(|e| e.into_inner()).get(table) {
            return Ok(columns.clone());
        }
        let db = self.event_db().await?;
        let table_name = table.to_string();
        // pragma_table_xinfo.hidden: 2 = VIRTUAL generated column, 3 = STORED generated column
        let columns = db
            .conn(move |conn| {
                let mut stmt = conn.prepare("SELECT name FROM pragma_table_xinfo(?1) WHERE hidden IN (2, 3)")?;
                stmt.query_map([&table_name], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        let columns = Arc::new(columns);
        GENERATED_COLUMNS.lock().unwrap_or_else(|e| e.into_inner()).insert(table.to_string(), columns.clone());
        Ok(columns)
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
        let qxsql = LocalDbSqlApi::new(self.event_db().await?, self.rpc_client.clone());
        qxsql.query(query, params).await
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let qxsql = LocalDbSqlApi::new(self.event_db().await?, self.rpc_client.clone());
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
