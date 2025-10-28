use async_sqlite::rusqlite::named_params;
use chrono::FixedOffset;
use serde::{Deserialize, Serialize};
use shvproto::{FromRpcValue, RpcValue, rpcvalue};

use crate::{appstate::{QxLockedAppState, QxSharedAppState}, sql::list_records};

#[derive(Debug, Serialize, Deserialize, FromRpcValue, Default)]
pub struct EventRecord {
    pub id: Option<i64>,
    pub name: Option<String>,
    pub date: Option<chrono::DateTime<FixedOffset>>,
    pub api_token: Option<String>,
    pub owner: Option<String>,
}

pub async fn list_events(app_state: QxSharedAppState) -> anyhow::Result<Vec<rpcvalue::Map>> {
    list_records(app_state, "events", Some(&["id", "name", "date", "owner"])).await
}

pub async fn create_event(app_state: &QxLockedAppState, rec: EventRecord) -> anyhow::Result<i64> {
    let state = app_state.read().await;
    let events = state.db_pool.conn(move |conn| {
        conn.query_row(
                "INSERT INTO events (name, date, api_token, owner) VALUES (:name, :date, :api_token, :owner) RETURNING id",
                named_params! {
                    ":name": rec.name,
                    ":date": rec.date,
                    ":api_token": rec.api_token,
                    ":owner": rec.owner
                },
                |row| row.get::<_, i64>(0)
            )
    }).await?;
    Ok(events)
}

pub async fn read_event(app_state: &QxLockedAppState, id: i64) -> anyhow::Result<EventRecord> {
    let state = app_state.read().await;
    let event = state
        .db_pool
        .conn(move |conn| {
            let event = conn.query_row(
                "SELECT name, date, api_token, owner FROM events WHERE id = :id",
                named_params! { ":id": id, },
                |row| {
                    Ok(EventRecord {
                        id: Some(id),
                        name: row.get("name")?,
                        date: row.get("date")?,
                        api_token: row.get("api_token")?,
                        owner: row.get("owner")?,
                    })
                },
            );
            event
        })
        .await?;
    Ok(event)
}
pub async fn update_event(
    app_state: &QxLockedAppState,
    id: i64,
    rec: rpcvalue::Map,
) -> anyhow::Result<i64> {
    update_table(app_state, "events".to_string(), id, rec).await
}

pub async fn delete_event(app_state: &QxLockedAppState, id: i64) -> anyhow::Result<()> {
    let state = app_state.read().await;
    state
        .db_pool
        .conn(move |conn| {
            conn.execute(
                "DELETE FROM events WHERE id = :id",
                named_params! { ":id": id, },
            )
        })
        .await?;
    Ok(())
}

async fn update_table(
    app_state: &QxLockedAppState,
    table_name: String,
    id: i64,
    rec: rpcvalue::Map,
) -> anyhow::Result<i64> {
    let state = app_state.read().await;

    let updated = state
        .db_pool
        .conn(move |conn| {
            let mut sql = format!("UPDATE {} SET ", table_name);
            let mut updates = Vec::new();
            let mut params: Vec<(String, async_sqlite::rusqlite::types::Value)> = Vec::new();

            // Process each field in the map
            for (key, value) in rec.iter() {
                if key == "id" {
                    // Skip the id field for SET clause
                    continue;
                }
                let param_name = format!(":{}", key);
                updates.push(format!("{} = {}", key, param_name));

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

            if updates.is_empty() {
                return Err(async_sqlite::rusqlite::Error::InvalidPath(
                    "No fields to update".into(),
                ));
            }

            sql.push_str(&updates.join(", "));
            sql.push_str(" WHERE id = :id");
            params.push((":id".to_string(), id.into()));

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
