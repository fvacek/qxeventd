use std::{collections::BTreeMap, path::Path};

use log::info;
use qxsql::{Record, sql::{QxSqlApi}};
use serde::{Deserialize, Serialize};
use shvproto::{RpcValue};
use async_process::{Child, Command};
use crate::{eventdb::migrate_db, generate_api_token, global_config, qxappsql::QxAppSql};

pub type EventId = i64;

pub(crate) struct State {
    pub db_pool: async_sqlite::Pool,
    pub open_events: BTreeMap<EventId, OpenEvent>,
}

impl State {

    pub async fn create_event(&self, owner: String) -> anyhow::Result<(EventId, String)> {
        if owner.is_empty() {
            return Err(anyhow::anyhow!("Owner cannot be empty"));
        }
        let event_data = EventData {
            name: String::new(),
            date: chrono::Utc::now(),
            owner,
            api_token: generate_api_token(),
            is_local: false,
        };
        let rec: Record = shvproto::from_rpcvalue(&RpcValue::from(&event_data))?;
        let qxsql = QxAppSql(self.db_pool.clone());
        let event_id = qxsql.create_record("events", &rec).await?;
        Ok((event_id, event_data.api_token))
    }

    pub async fn open_event(&mut self, event_id: EventId) -> anyhow::Result<()> {
        if self.open_events.contains_key(&event_id) {
            return Ok(());
        }
        let event_data = self.event_data_from_sql(event_id).await?;
        let api_token = event_data.api_token.clone();
        let qxsql_process = if event_data.is_local {
            let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
            if !check_file_exists(&db_file) {
                create_file_path(&db_file)?;
            }
            migrate_db(&db_file).await?;
            let child = Command::new("qxsqld")
                .args(&["--device-id", &api_token])
                .args(&["--database", &db_file])
                .spawn()?; // Don't await, just start it
            info!("Child process qxsqld started OK");
            Some(child)
        } else {
            None
        };
        self.open_events.insert(event_id, OpenEvent { qxsql_process, data: event_data });
        Ok(())
    }
    pub async fn close_event(&mut self, event_id: EventId) -> anyhow::Result<()> {
        if let Some(event) = self.open_events.remove(&event_id) {
            if let Some(mut child) = event.qxsql_process {
                child.kill()?;
                let status = child.status().await?;
                info!("qxsql process killed with status: {:?}", status);
            }
        }
        Ok(())
    }

    pub async fn event_data_from_sql(&self, event_id: EventId) -> anyhow::Result<EventData> {
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
    pub is_local: bool,
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

pub(crate) struct OpenEvent {
    pub data: EventData,
    pub qxsql_process: Option<Child>,
}

impl OpenEvent {
}

fn check_file_exists(path: &str) -> bool {
    std::fs::metadata(path).is_ok()
}
fn create_file_path(db_file: &str) -> anyhow::Result<()> {
    let dir = Path::new(db_file).parent().unwrap();
    std::fs::create_dir_all(dir)?;
    Ok(())
}
