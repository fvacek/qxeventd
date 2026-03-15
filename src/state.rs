use std::{collections::BTreeMap, sync::Arc};

use anyhow::bail;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Local;
use log::info;
use qxsql::QxSqlApiRecChng;
use qxsql::{Record};
use qxsql::{sql::{QxSqlApi, record_from_slice}};
use serde::{Deserialize, Serialize};
use shvclient::ClientCommandSender;
use shvproto::RpcValue;
use shvrpc::RpcMessage;
use shvrpc::util::join_path;
use smol::{lock::RwLock, channel};

use crate::appsqlapi::AppSqlApi;
use crate::eventdb::migrate_db;
use crate::generate_api_token;
use crate::global_config;

pub type EventId = i64;

pub type SharedAppState = Arc<RwLock<State>>;

pub(crate) struct State {
    pub db_pool: async_sqlite::Pool,
    pub open_events: BTreeMap<EventId, OpenEventCtl>,
    pub shutdown_sender: Option<channel::Sender<()>>,
}

impl State {
    pub async fn quit_app(&mut self, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let event_ids: Vec<_> = self.open_events.keys().copied().collect();
        for event_id in event_ids {
            if let Err(e) = self.close_event(event_id, client_command_sender.clone()).await {
                bail!("Failed to close event {}: {}", event_id, e);
            }
        }
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.try_send(());
        }
        Ok(())
    }

    pub async fn list_events(&self) -> anyhow::Result<Vec<Record>> {
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let records = qxsql.list_records("events", Some(vec!["id", "name", "date", "owner"]), None, None).await?;
        Ok(records)
    }

    pub async fn create_event(&self, owner: String, client_cmd_tx: ClientCommandSender) -> anyhow::Result<(EventId, String)> {
        if owner.is_empty() {
            return Err(anyhow::anyhow!("Owner cannot be empty"));
        }
        let api_token = generate_api_token();
        let event_data = EventData {
            name: String::new(),
            date: chrono::Local::now().fixed_offset(),
            owner: owner.clone(),
            is_local: false,
            api_token: api_token.clone(),
        };
        let rec = event_data.to_record()?;
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let event_id = qxsql.create_record_with_recchng("events", &rec, client_cmd_tx, Some(owner)).await?;
        info!("Created event {event_id}");
        Ok((event_id, api_token))
    }

    pub async fn open_event(&mut self, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        if let Some(event) = self.open_events.get_mut(&event_id) {
            event.touched_at = chrono::Utc::now();
            return Ok(());
        }
        let event_data = self.load_event_data_from_sql(event_id).await?;

        let local_db = if event_data.is_local {
            let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
            let pool = migrate_db(&db_file, &event_data).await?;
            Some(pool)
        } else {
            None
        };

        // ping child
        let mount_point = event_mount_point(event_id);
        let _: () = client_command_sender.call_rpc_method(join_path(mount_point, ".app"), "ping", None, None, None::<fn(_)>).await
            .map_err(|_| anyhow!("Failed to ping DB service."))?;

        let now = chrono::Utc::now();
        self.open_events.insert(event_id, OpenEventCtl { data: event_data, local_db, open_at: now, touched_at: now });

        let message = RpcMessage::new_signal("event", "lsmod").with_param(true);
        client_command_sender.send_message(message)
            .map_err(|e| anyhow!("Failed to send message {}", e))?;

        Ok(())
    }
    pub async fn event_status(&mut self, event_id: EventId) -> anyhow::Result<EventStatus> {
        self.open_events.get(&event_id).ok_or_else(|| anyhow!("Invalid event id: {event_id}"))
            .map(|ectl| {
                EventStatus {
                    is_local: ectl.local_db.is_some(),
                    open_at: ectl.open_at.with_timezone(&Local).fixed_offset(),
                    expires_at: ectl.expires_at().with_timezone(&Local).fixed_offset()
                }
            })
    }
    pub async fn close_event(&mut self, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<bool> {
        if let Some(_event) = self.open_events.remove(&event_id) {
            // let mount_point = event_mount_point(event_id);

            let message = RpcMessage::new_signal("event", "lsmod").with_param(false);
            client_command_sender.send_message(message)
            .map_err(|e| anyhow!("Failed to send message {}", e))?;

            // log::info!("Sending quit RPC message to qxsql of event {event_id} mounted at {mount_point}");
            // let _: () = client_command_sender.call_rpc_method(format!("{mount_point}/.app"), "quit", None, None, None::<fn(_)>).await
            //     .map_err(|err| anyhow::anyhow!("Failed to shut down event: {}", err))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn delete_event(&mut self, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<bool> {
        self.close_event(event_id, client_command_sender.clone()).await?;
        log::info!("Deleting event {}", event_id);
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let was_deleted = qxsql.delete_record_with_recchng("events", event_id, client_command_sender, None).await?;
        Ok(was_deleted)
    }

    pub async fn gc_expired_events(&mut self, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let event_age_list = self.open_events.iter()
            .map(|(id, event)| (*id, event.expires_at())).collect::<Vec<_>>();
        let now = chrono::Utc::now();
        for (event_id, expires_at) in event_age_list {
            if expires_at < now {
                info!("Closing event: {event_id} as expired.");
                self.close_event(event_id, client_command_sender.clone()).await?;
            }
        }
        Ok(())
    }

    pub async fn api_token_to_event_id(&self, api_token: &str) -> anyhow::Result<EventId> {
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let result = qxsql
            .query("SELECT id FROM events WHERE api_token = :api_token", Some(&record_from_slice(&[("api_token", api_token.into())])))
            .await?;
        let event_id = result.rows.first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.to_int());
        event_id.ok_or_else(|| anyhow::anyhow!("API token not found"))
    }

    pub async fn load_event_data_from_sql(&self, event_id: EventId) -> anyhow::Result<EventData> {
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let data = qxsql
            .read_record("events", event_id, None)
            .await?;
        if let Some(rec) = data {
            let data = EventData::from_record(&rec)?;
            return Ok(data)
        }
        Err(anyhow::anyhow!("Event id: {} not found", event_id))
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventStatus {
    pub is_local: bool,
    pub open_at: DateTime<chrono::FixedOffset>,
    pub expires_at: DateTime<chrono::FixedOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventData {
    pub name: String,
    pub date: DateTime<chrono::FixedOffset>,
    pub owner: String,
    pub api_token: String,
    pub is_local: bool,
}

impl EventData {
    fn from_record(record: &Record) -> anyhow::Result<Self> {
        let get_field = |name| record.get(name).ok_or_else(||anyhow!("Cannot get field '{}'.", name));
        Ok(Self {
            name: get_field("name")?.as_str().unwrap_or_default().to_string(),
            date: get_field("date")?.to_datetime().unwrap_or_else(|| chrono::Local::now().fixed_offset()),
            owner: get_field("owner")?.as_str().unwrap_or_default().to_string(),
            is_local: get_field("is_local")?.to_bool(),
            api_token: get_field("api_token")?.as_str().unwrap_or_default().to_string(),
        })
    }
    fn to_record(&self) -> anyhow::Result<Record> {
        let mut record = Record::new();
        record.insert("name".to_string(), self.name.clone().into());
        record.insert("date".to_string(), self.date.into());
        record.insert("owner".to_string(), self.owner.clone().into());
        record.insert("api_token".to_string(), self.api_token.clone().into());
        record.insert("is_local".to_string(), self.is_local.into());
        Ok(record)
    }
}

impl From<&EventStatus> for RpcValue {
    fn from(value: &EventStatus) -> Self {
        shvproto::to_rpcvalue(value).expect("Failed to convert EventStatus to RpcValue")
    }
}

impl From<EventStatus> for RpcValue {
    fn from(value: EventStatus) -> Self {
        shvproto::to_rpcvalue(&value).expect("Failed to convert EventStatus to RpcValue")
    }
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

pub(crate) struct OpenEventCtl {
    pub data: EventData,
    pub local_db: Option<async_sqlite::Pool>,
    pub open_at: DateTime<chrono::Utc>,
    pub touched_at: DateTime<chrono::Utc>,
}

impl OpenEventCtl {
    pub fn expires_at(&self) -> DateTime<chrono::Utc> {
        self.open_at + global_config().event_expire_duration
    }
}

pub fn event_mount_point(event_id: EventId) -> String {
    format!("{}/{event_id}", global_config().events_mount_point)
}
