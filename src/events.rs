use async_sqlite::rusqlite::named_params;
use chrono::FixedOffset;
use serde::{Deserialize, Serialize};
use shvproto::FromRpcValue;

use crate::appstate::QxLockedAppState;

#[derive(Debug, Serialize, Deserialize, FromRpcValue, Default)]
pub struct EventRecord {
    pub id: Option<i64>,
    pub name: Option<String>,
    pub date: Option<chrono::DateTime<FixedOffset>>,
    pub api_token: Option<String>,
    pub owner: Option<String>,
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
