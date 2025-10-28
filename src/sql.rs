use crate::appstate::{QxLockedAppState, QxSharedAppState};
use async_sqlite::rusqlite::types::ValueRef;
use shvclient::ClientCommandSender;
use shvproto::{RpcValue, rpcvalue};

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
    let events = state
        .db_pool
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

pub async fn create_record(state: QxSharedAppState, _shv_sender: ClientCommandSender<QxLockedAppState>, table: &str, record: rpcvalue::Map, _issuer: &str) -> anyhow::Result<i64> {
    let state = state.read().await;
    let table = table.to_string();

    let updated = state
        .db_pool
        .conn(move |conn| {
            let mut fields = Vec::new();
            let mut values = Vec::new();
            let mut params: Vec<(String, async_sqlite::rusqlite::types::Value)> = Vec::new();

            // Process each field in the map
            for (key, value) in record.iter() {
                if key == "id" {
                    // Skip the id field for SET clause
                    continue;
                }
                let param_name = format!(":{}", key);
                fields.push(key.to_string());
                values.push(param_name.clone());

                let sql_value = match value {
                    RpcValue {
                        value: rpcvalue::Value::String(s),
                        ..
                    } => s.as_str().to_string().into(),
                    RpcValue {
                        value: rpcvalue::Value::Int(i),
                        ..
                    } => (*i).into(),
                    RpcValue {
                        value: rpcvalue::Value::DateTime(dt),
                        ..
                    } => dt.to_chrono_datetime().to_rfc3339().into(),
                    RpcValue {
                        value: rpcvalue::Value::Null,
                        ..
                    } => async_sqlite::rusqlite::types::Value::Null,
                    _ => {
                        return Err(async_sqlite::rusqlite::Error::ToSqlConversionFailure(
                            format!("Unsupported value type for field {}", key).into(),
                        ));
                    }
                };

                params.push((param_name, sql_value));
            }

            if fields.is_empty() {
                return Err(async_sqlite::rusqlite::Error::InvalidPath(
                    "No fields to update".into(),
                ));
            }

            let sql = format!("INSERT INTO {table} ({}) VALUES ({}) RETURNING id", fields.join(", "), values.join(", "));

            let mut stmt = conn.prepare(&sql)?;
            let param_refs: Vec<(&str, &dyn async_sqlite::rusqlite::ToSql)> = params
                .iter()
                .map(|(name, val)| (name.as_str(), val as &dyn async_sqlite::rusqlite::ToSql))
                .collect();
            let rows_affected = stmt.execute(&param_refs[..])?;

            Ok(rows_affected as i64)
        })
        .await?;

    Ok(updated)
}
