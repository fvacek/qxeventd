use async_sqlite::rusqlite::named_params;
use chrono::{FixedOffset};
use serde::{Deserialize, Serialize};
use shvproto::FromRpcValue;

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

pub async fn update_event(app_state: &QxLockedAppState, rec: EventRecord) -> anyhow::Result<i64> {
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
