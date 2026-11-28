use async_trait::async_trait;
use qxsql::{DbValue, RecChng, sql::{DbField, ExecResult, QueryResult}};
use qxsql::sql::Record;
use shvclient::ClientCommandSender;

use crate::db::{Db, Value};

pub struct LocalDbSqlApi(Db, Option<ClientCommandSender>);

impl LocalDbSqlApi {
    pub fn new(db: Db, client_command_sender: ClientCommandSender) -> Self {
        Self(db, Some(client_command_sender))
    }
    pub fn new_without_recchng(db: Db) -> Self {
        Self(db, None)
    }
}

#[async_trait]
impl qxsql::QxSqlApi for LocalDbSqlApi {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<QueryResult> { let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_query(&self.0, query, params).await
    }
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_exec(&self.0, query, params).await
    }
}

#[async_trait]
impl qxsql::QxSqlApiRecChng for LocalDbSqlApi {
    fn filter_recchng(&self, mut recchng: RecChng) -> Option<RecChng> {
        if let Some(mut record) = recchng.record.take() {
            record.remove("api_token");
            recchng.record = Some(record);
        }
        Some(recchng)
    }

    async fn client_command_sender(&self) -> Option<ClientCommandSender> {
        self.1.clone()
    }

}

fn convert_dbvalue_to_sql(key: &str, value: &DbValue) -> anyhow::Result<Value> {
    match value {
        DbValue::String(s) => Ok(Value::Text(s.clone())),
        DbValue::Int(i) => Ok(Value::Integer(*i)),
        DbValue::DateTime(dt) => Ok(Value::Text(dt.to_rfc3339())),
        DbValue::Null => Ok(Value::Null),
        DbValue::Blob(b) => Ok(Value::Blob(b.clone())),
        DbValue::Bool(b) => Ok(Value::Integer(*b as i64)),
        _ => anyhow::bail!("Unsupported value type for field {key}"),
    }
}

/// Named SQL parameters of `record` fields, `:field_name` = value.
pub(crate) fn process_record_params(record: &Record) -> anyhow::Result<Vec<(String, Value)>> {
    record.iter()
        .map(|(key, value)| Ok((format!(":{key}"), convert_dbvalue_to_sql(key, value)?)))
        .collect()
}

fn convert_sql_to_dbvalue(value: Value) -> DbValue {
    match value {
        Value::Null => DbValue::Null,
        Value::Integer(i) => i.into(),
        Value::Real(r) => r.into(),
        Value::Text(t) => t.into(),
        Value::Blob(b) => b.as_slice().into(),
    }
}

async fn sql_query(
    db: &Db,
    query: &str,
    params: &Record
) -> anyhow::Result<QueryResult> {
    let params = process_record_params(params)?;
    let conn = db.conn().await?;
    let mut rows = conn.query(query, params).await?;
    let fields: Vec<DbField> = rows.column_names().into_iter().map(|name| DbField { name }).collect();
    let mut table = Vec::new();
    while let Some(row) = rows.next().await? {
        let rec = (0..fields.len())
            .map(|i| row.get_value(i).map(convert_sql_to_dbvalue))
            .collect::<Result<Vec<_>, _>>()?;
        table.push(rec);
    }
    Ok(QueryResult { fields, rows: table })
}

async fn sql_exec(
    db: &Db,
    query: &str,
    params: &Record
) -> anyhow::Result<ExecResult> {
    let params = process_record_params(params)?;
    let conn = db.conn().await?;
    let rows_affected = conn.execute(query, params).await?;
    Ok(ExecResult { rows_affected: rows_affected as i64, insert_id: None })
}
