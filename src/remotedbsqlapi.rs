use async_trait::async_trait;
use qxsql::{RecDeleteParam, RecUpdateParam};
use qxsql::sql::{ExecResult, QueryResult, RecChng, RecInsertParam};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;
use shvproto::{make_list, to_rpcvalue, from_rpcvalue};
use crate::state::remote_event_sql_path;
use crate::{call_rpc_error_to_anyhow, state::EventId};

pub struct RemoteEventSqlApi {
    event_id: EventId,
    rpc_client: ClientCommandSender,
}

impl RemoteEventSqlApi {
    pub fn new(event_id: EventId, rpc_client: ClientCommandSender) -> Self {
        Self {
            event_id,
            rpc_client,
        }
    }
    pub async fn create_record_event(&self, table: &str, record: &Record, issuer: Option<String>) -> anyhow::Result<i64> {
        let param = RecInsertParam {
            table: table.to_string(),
            record: record.clone(),
            issuer,
        };
        let id: i64 = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "create", Some(to_rpcvalue(&param)?), None, None, None::<fn(f64)>).await
            .map_err(call_rpc_error_to_anyhow)?;
        Ok(id)
    }
    pub async fn update_record_event(&self, table: &str, id: i64, record: &Record, issuer: Option<String>) -> anyhow::Result<bool> {
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
    #[allow(dead_code)]
    pub async fn delete_record_event(&self, table: &str, id: i64, issuer: Option<String>) -> anyhow::Result<bool> {
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

#[async_trait]
impl qxsql::QxSqlApi for RemoteEventSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> {
        let params = to_rpcvalue(&params)?;
        let rpc_value = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "query", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
            .map_err(call_rpc_error_to_anyhow)?;
        let res: QueryResult = from_rpcvalue(&rpc_value)?;
        Ok(res)
    }

    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let params = to_rpcvalue(&params)?;
        let rpc_value = self.rpc_client.call_rpc_method(remote_event_sql_path(self.event_id), "exec", Some(make_list![query, params].into()), None, None, None::<fn(f64)>).await
            .map_err(call_rpc_error_to_anyhow)?;
        let res: ExecResult = from_rpcvalue(&rpc_value)?;
        Ok(res)
    }
}

#[async_trait]
impl qxsql::QxSqlApiRecChng for RemoteEventSqlApi {
    fn filter_recchng(&self, recchng: RecChng) -> Option<RecChng>  {
        Some(recchng)
    }

    async fn client_command_sender(&self) -> Option<ClientCommandSender>  {
            None
    }
}
