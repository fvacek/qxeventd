use std::backtrace::Backtrace;

use async_sqlite::rusqlite::named_params;
use chrono::{FixedOffset};
use log::error;
use serde::{Deserialize, Serialize};
use shvproto::{rpcvalue, FromRpcValue, RpcValue};
use anyhow::anyhow;

use crate::appstate::QxLockedAppState;

#[derive(Debug,Serialize,Deserialize,FromRpcValue,Default)]
pub struct EventRecord {
    pub id: Option<i32>,
    pub name: Option<String>,
    pub date: Option<chrono::DateTime<FixedOffset>>,
    pub api_token: Option<String>,
    pub owner: Option<String>,
}

pub async fn list_events(app_state: &QxLockedAppState) -> anyhow::Result<Vec<EventRecord>> {
    let state = app_state.read().await;
    let events = state.db_pool.conn(|conn| {
        let mut stmt = conn.prepare("SELECT id, name, date FROM events")?;
        stmt.query_map([], |row| {
                Ok(EventRecord {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    date: row.get(2)?,
                    ..Default::default()
                })
            })?.collect()
    }).await?;
    Ok(events)
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

pub async fn update_event(app_state: &QxLockedAppState, rec: rpcvalue::Map) -> anyhow::Result<i64> {
    let state = app_state.read().await;

    let updated = state.db_pool.conn(move |conn| {
        let sql = String::from("UPDATE events SET ");
        let (sql2, params) = to_sql_params(rec)?;

        let mut stmt = conn.prepare(&format!("{sql} {sql2}"))?;
        let rows_affected = stmt.execute(&params[..])?;

        Ok(rows_affected as i64)
    }).await?;

    Ok(updated)
}

fn rpcvalue_to_ruslite(value: RpcValue) -> Result<async_sqlite::rusqlite::types::Value, async_sqlite::rusqlite::Error> {
    let val = match value.value {
        rpcvalue::Value::Null => async_sqlite::rusqlite::types::Value::Null,
        rpcvalue::Value::Bool(b) => b.into(),
        rpcvalue::Value::String(s) => { let s2: String = *s; s2.into()},
        shvproto::Value::Int(i) => i.into(),
        shvproto::Value::UInt(u) => (u as i64).into(),
        shvproto::Value::Double(d) => d.into(),
        shvproto::Value::DateTime(dt) => (&dt.to_chrono_datetime() as &dyn async_sqlite::rusqlite::ToSql).into(),
        _ => return Err(async_sqlite::rusqlite::Error::ToSqlConversionFailure(format!("Unsupported type {}", value.type_name()).into())),
    };
    Ok(val)
}

fn to_sql_params(rec: rpcvalue::Map) -> Result<(String, Vec<(&'static str, async_sqlite::rusqlite::types::Value)>), async_sqlite::rusqlite::Error> {
    let id = rec.get("id").ok_or_else(|| create_rusqlite_error_invalid_column_name("Missing id"))?.as_int();
    let mut sql = String::new();
    let mut params: Vec<(&'static str, async_sqlite::rusqlite::types::Value)> = Vec::new();
    for (key, value) in rec.into_iter() {
        sql.push_str(&format!("{} = :{}, ", key, key));
        params.push((key.as_str(), rpcvalue_to_ruslite(value)?));
    }
    if sql.ends_with(", ") {
        sql.truncate(sql.len() - 2);
    }
    sql.push_str(" WHERE id = :id");
    params.push((":id", id.into()));

    Ok((sql, params))
}

fn create_rusqlite_error_invalid_column_name(err: &str) -> async_sqlite::rusqlite::Error {
    error!("Error: {err}\nbacktrace: {}", Backtrace::capture());
    async_sqlite::rusqlite::Error::InvalidColumnName(err.into())
}
