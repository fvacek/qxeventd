use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::appstate::QxLockedAppState;

#[derive(Debug,Serialize,Deserialize,Default)]
pub struct EventRecord {
    pub id: Option<i32>,
    pub name: Option<String>,
    pub date: Option<chrono::DateTime<Utc>>,
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
