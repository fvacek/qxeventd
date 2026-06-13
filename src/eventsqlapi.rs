use anyhow::anyhow;
use async_sqlite::Pool;
use async_trait::async_trait;
use qxsql::{QxSqlApiRecChng, RecDeleteParam, RecUpdateParam};
use qxsql::sql::{ExecResult, QueryResult, RecChng, RecInsertParam};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use shvproto::{make_list, to_rpcvalue, from_rpcvalue};
use crate::appsqlapi::AppSqlApi;
use crate::state::remote_event_sql_path;
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
    pub async fn create_record_event(&self, table: &str, record: &Record, issuer: Option<String>) -> anyhow::Result<i64> {
        if self.is_local_event_db().await? {
            return self.create_record_with_recchng(table, record, issuer).await;
        } else {
            let param = RecInsertParam {
                table: table.to_string(),
                record: record.clone(),
                issuer,
            };
            let id: i64 = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "create", Some(to_rpcvalue(&param)?), None, None, None::<fn(f64)>).await
                .map_err(call_rpc_error_to_anyhow)?;
            Ok(id)
        }
    }
    pub async fn update_record_event(&self, table: &str, id: i64, record: &Record, issuer: Option<String>) -> anyhow::Result<bool> {
        if self.is_local_event_db().await? {
            return self.update_record_with_recchng(table, id, record, issuer).await;
        } else {
            let param = RecUpdateParam {
                table: table.to_string(),
                id,
                record: record.clone(),
                issuer,
            };
            let updated: bool = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "update", Some(to_rpcvalue(&param)?), None, None, None::<fn(f64)>).await
                .map_err(call_rpc_error_to_anyhow)?;
            Ok(updated)
        }
    }
    #[allow(dead_code)]
    pub async fn delete_record_event(&self, table: &str, id: i64, issuer: Option<String>) -> anyhow::Result<bool> {
        if self.is_local_event_db().await? {
            return self.delete_record_with_recchng(table, id, issuer).await;
        } else {
            let param = RecDeleteParam {
                table: table.to_string(),
                id,
                issuer,
            };
            let deleted: bool = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "delete", Some(to_rpcvalue(&param)?), None, None, None::<fn(f64)>).await
                .map_err(call_rpc_error_to_anyhow)?;
            Ok(deleted)
        }
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
            let rpc_value = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "query", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
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
            let rpc_value = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "exec", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
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
