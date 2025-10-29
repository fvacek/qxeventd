use crate::appstate::{QxSharedAppState};
use async_sqlite::rusqlite::types::ValueRef;
use async_trait::async_trait;
use qxsql::{sql::{DbField, ExecResult, Record, SelectResult}, DbValue};
use shvproto::{RpcValue, rpcvalue};

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

fn convert_rpc_value_to_sql(key: &str, value: &RpcValue) -> Result<async_sqlite::rusqlite::types::Value, async_sqlite::rusqlite::Error> {
    match value {
        RpcValue { value: rpcvalue::Value::String(s), .. } => Ok(s.as_str().to_string().into()),
        RpcValue { value: rpcvalue::Value::Int(i), .. } => Ok((*i).into()),
        RpcValue { value: rpcvalue::Value::DateTime(dt), .. } => Ok(dt.to_chrono_datetime().to_rfc3339().into()),
        RpcValue { value: rpcvalue::Value::Null, .. } => Ok(async_sqlite::rusqlite::types::Value::Null),
        _ => Err(async_sqlite::rusqlite::Error::ToSqlConversionFailure(
            format!("Unsupported value type for field {}", key).into(),
        )),
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

fn process_record_params(record: &rpcvalue::Map) -> Result<Vec<(String, async_sqlite::rusqlite::types::Value)>, async_sqlite::rusqlite::Error> {
    let mut params: Vec<(String, async_sqlite::rusqlite::types::Value)> = Vec::new();

    for (key, value) in record.iter() {
        if key == "id" {
            continue;
        }
        let param_name = format!(":{}", key);
        let sql_value = convert_rpc_value_to_sql(key, value)?;
        params.push((param_name, sql_value));
    }

    if params.is_empty() {
        return Err(async_sqlite::rusqlite::Error::InvalidPath(
            "No fields to process".into(),
        ));
    }

    Ok(params)
}

fn process_record_params2(record: &Record) -> Result<Vec<(String, async_sqlite::rusqlite::types::Value)>, async_sqlite::rusqlite::Error> {
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

pub async fn list_records(
    app_state: QxSharedAppState,
    table: &str,
    field_list: Option<&[&str]>,
) -> anyhow::Result<Vec<rpcvalue::Map>> {
    let fields = field_list
        .unwrap_or(&["*"])
        .iter()
        .map(|field| field.to_string())
        .collect::<Vec<String>>();
    let table = table.to_string();
    let state = app_state.read().await;
    let events = state.db_pool
        .conn(move |conn| {
            let mut stmt = conn.prepare(&format!("SELECT {} FROM {table}", fields.join(", ")))?;
            let column_names: Vec<String> =
                stmt.column_names().iter().map(|s| s.to_string()).collect();
            let rows = stmt
                .query_map([], |row| {
                    let mut rec = rpcvalue::Map::new();
                    for (i, col_name) in column_names.iter().enumerate() {
                        let value = row.get_ref(i)?; // <-- get a ValueRef (borrowed SQL value)
                        match value {
                            ValueRef::Null => rec.insert(col_name.to_string(), RpcValue::from(())),
                            ValueRef::Integer(i) => rec.insert(col_name.to_string(), RpcValue::from(i)),
                            ValueRef::Real(f) => rec.insert(col_name.to_string(), RpcValue::from(f)),
                            ValueRef::Text(t) => rec.insert(col_name.to_string(), RpcValue::from(String::from_utf8_lossy(t).to_string())),
                            ValueRef::Blob(b) => rec.insert(col_name.to_string(), RpcValue::from(b.to_vec())),
                        };
                    }
                    Ok(rec)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await?;
    Ok(events)
}

pub async fn create_record(
    state: QxSharedAppState,
    table: &str,
    record: rpcvalue::Map,
) -> anyhow::Result<i64> {
    let state = state.read().await;

    let table_name = table.to_string();
    let record2 = record.clone();

    let insert_id = state
        .db_pool
        .conn(move |conn| {
            let params = process_record_params(&record2)?;

            let fields: Vec<String> = params.iter()
                .map(|(name, _)| name.trim_start_matches(':').to_string())
                .collect();
            let values: Vec<String> = params.iter()
                .map(|(name, _)| name.clone())
                .collect();

            let sql = format!("INSERT INTO {table_name} ({}) VALUES ({}) RETURNING id", fields.join(", "), values.join(", "));

            let param_refs = create_param_refs(&params);
            let insert_id: i64 = conn.query_row(&sql, &param_refs[..], |row| row.get(0))?;
            Ok(insert_id)
        })
        .await?;
    Ok(insert_id)
}

pub async fn update_record(
    state: QxSharedAppState,
    table: &str,
    id: i64,
    record: rpcvalue::Map,
) -> anyhow::Result<i64> {
    let state = state.read().await;

    let table_name = table.to_string();
    let record2 = record.clone();

    let rows_inserted = state
        .db_pool
        .conn(move |conn| {
            let mut params = process_record_params(&record2)?;

            let fields: Vec<String> = params.iter()
                .map(|(name, _)| format!("{} = {}", name.trim_start_matches(':'), name))
                .collect();

            params.push((":id".into(), id.into()));

            let sql = format!("UPDATE {table_name} SET {} WHERE id = :id", fields.join(", "));

            let param_refs = create_param_refs(&params);
            let rows_inserted = conn.execute(&sql, &param_refs[..])?;
            Ok(rows_inserted)
        })
        .await?;
    Ok(rows_inserted as i64)
}

async fn sql_query(
    app_state: &QxSharedAppState,
    query: &str,
    params: &Record
) -> anyhow::Result<SelectResult> {
    let query = query.to_string();
    let params = process_record_params2(params)?;
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
    let params = process_record_params2(params)?;
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
