use qxsql::{Record, sql::{QxSqlApi}};
use serde::{Deserialize, Serialize};
use shvproto::{RpcValue};

use crate::{generate_api_token, qxappsql::QxAppSql};

pub type EventId = i64;

pub(crate) struct State {
    pub db_pool: async_sqlite::Pool,
}

impl State {
    pub fn event_mount_point(&self, event_id: EventId) -> String {
        format!("test/hsh{}", event_id)
    }

    pub async fn create_event(&self, owner: String) -> anyhow::Result<EventId> {
        if owner.is_empty() {
            return Err(anyhow::anyhow!("Owner cannot be empty"));
        }
        let event_data = EventData {
            name: String::new(),
            date: chrono::Utc::now(),
            owner,
            api_token: generate_api_token(),
        };
        let rec: Record = shvproto::from_rpcvalue(&RpcValue::from(&event_data))?;
        let qxsql = QxAppSql(self.db_pool.clone());
        let event_id = qxsql.create_record("events", &rec).await?;
        Ok(event_id)
    }

    pub async fn event_data(&self, event_id: EventId) -> anyhow::Result<EventData> {
        let qxsql = QxAppSql(self.db_pool.clone());
        let data = qxsql
            .read_record("events", event_id, None)
            .await?;
        if let Some(rec) = data {
            if let Some(json) = rec.get("data") {
                let data: EventData = serde_json::from_str(json.as_str().unwrap_or_default())?;
                return Ok(data)
            }
        }
        Err(anyhow::anyhow!("Event id: {} not found", event_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventData {
    pub name: String,
    pub date: chrono::DateTime<chrono::Utc>,
    pub owner: String,
    pub api_token: String,
}

impl From<&EventData> for RpcValue {
    fn from(value: &EventData) -> Self {
        shvproto::to_rpcvalue(value).expect("Failed to convert EventData to RpcValue")
    }
}
impl From<&RpcValue> for EventData {
    fn from(value: &RpcValue) -> Self {
        shvproto::from_rpcvalue(value).expect("Failed to convert RpcValue to EventData")
    }
}

// impl From<&EventData> for Record {
//     fn from(value: &EventData) -> Self {
//         let v = to_rpcvalue(value).expect("Failed to convert EventData to RpcValue");
//         record_from_rpcvalue(&v).expect("Failed to convert RpcValue to qxsql::Record")
//     }
// }
