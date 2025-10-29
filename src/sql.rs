use crate::appstate::{QxSharedAppState};
use async_sqlite::rusqlite::types::ValueRef;
use async_trait::async_trait;
use qxsql::{sql::{DbField, ExecResult, Record, SelectResult}, DbValue};

pub struct QxSql(pub QxSharedAppState);

#[async_trait]
impl qxsql::sql::SqlProvider for QxSql {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<SelectResult> {
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_query(&self.0, query, params).await
    }
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_exec(&self.0, query, params).await
    }
}

fn convert_dbvalue_to_sql(key: &str, value: &DbValue) -> Result<async_sqlite::rusqlite::types::Value, async_sqlite::rusqlite::Error> {
    match value {
        DbValue::String(s) => Ok(s.as_str().to_string().into()),
        DbValue::Int(i) => Ok((*i).into()),
        DbValue::DateTime(dt) => Ok(dt.to_rfc3339().into()),
        DbValue::Null => Ok(async_sqlite::rusqlite::types::Value::Null),
        _ => Err(async_sqlite::rusqlite::Error::ToSqlConversionFailure(
            format!("Unsupported value type for field {}", key).into(),
        )),
    }
}

fn process_record_params(record: &Record) -> Result<Vec<(String, async_sqlite::rusqlite::types::Value)>, async_sqlite::rusqlite::Error> {
    let mut params: Vec<(String, async_sqlite::rusqlite::types::Value)> = Vec::new();

    for (key, value) in record.iter() {
        if key == "id" {
            continue;
        }
        let param_name = format!(":{}", key);
        let sql_value = convert_dbvalue_to_sql(key, value)?;
        params.push((param_name, sql_value));
    }

    if params.is_empty() {
        return Err(async_sqlite::rusqlite::Error::InvalidPath(
            "No fields to process".into(),
        ));
    }

    Ok(params)
}

fn create_param_refs(params: &[(String, async_sqlite::rusqlite::types::Value)]) -> Vec<(&str, &dyn async_sqlite::rusqlite::ToSql)> {
    params
        .iter()
        .map(|(name, val)| (name.as_str(), val as &dyn async_sqlite::rusqlite::ToSql))
        .collect()
}

async fn sql_query(
    app_state: &QxSharedAppState,
    query: &str,
    params: &Record
) -> anyhow::Result<SelectResult> {
    let query = query.to_string();
    let params = process_record_params(params)?;
    let state = app_state.read().await;
    let table = state.db_pool
        .conn(move |conn| {
            let param_refs = create_param_refs(&params);
            let mut stmt = conn.prepare(&query)?;
            let fields: Vec<DbField> = stmt.column_names().iter().map(|s| DbField { name: s.to_string() }).collect();
            let rows = stmt
                .query_map(&param_refs[..], |row| {
                    let mut rec: Vec<DbValue> = Vec::new();
                    for i in 0..fields.len() {
                        let value = row.get_ref(i)?; // <-- get a ValueRef (borrowed SQL value)
                        match value {
                            ValueRef::Null => rec.push(DbValue::Null),
                            ValueRef::Integer(i) => rec.push(i.into()),
                            ValueRef::Real(r) => rec.push(r.into()),
                            ValueRef::Text(t) => rec.push(String::from_utf8_lossy(t).to_string().into()),
                            ValueRef::Blob(b) => rec.push(b.into()),
                        };
                    }
                    Ok(rec)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SelectResult { fields, rows })
        })
        .await?;
    Ok(table)
}

async fn sql_exec(
    app_state: &QxSharedAppState,
    query: &str,
    params: &Record
) -> anyhow::Result<ExecResult> {
    let query = query.to_string();
    let params = process_record_params(params)?;
    let state = app_state.read().await;
    let result = state.db_pool
        .conn(move |conn| {
            let param_refs = create_param_refs(&params);
            let mut stmt = conn.prepare(&query)?;
            let rows_affected = stmt.execute(&param_refs[..])?;
            Ok(ExecResult { rows_affected: rows_affected as i64, insert_id: None })
        })
        .await?;
    Ok(result)
}
