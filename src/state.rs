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
use shvproto::make_map;
use shvrpc::RpcMessage;
use shvrpc::util::join_path;
use smol::{lock::RwLock, channel};

use crate::appsqlapi::AppSqlApi;
use crate::eventdb::migrate_db;
use crate::eventsqlapi::EventSqlApi;
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
        let records = qxsql.list_records("events", Some(vec!["id", "name", "date", "owner", "is_local"]), None, None).await?;
        Ok(records)
    }

    pub async fn create_event(&self, owner: String, client_cmd_tx: ClientCommandSender) -> anyhow::Result<(EventId, String)> {
        if owner.is_empty() {
            return Err(anyhow::anyhow!("Owner cannot be empty"));
        }
        let api_token = generate_api_token();
        let event_data = EventRecord {
            is_local: false,
            name: String::new(),
            date: chrono::Local::now().fixed_offset(),
            owner: owner.clone(),
            api_token: api_token.clone(),
            id: None,
            stage: default_stage(),
        };
        let rec = event_data.to_record()?;
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let event_id = qxsql.create_record_with_recchng("events", &rec, client_cmd_tx.clone(), Some(owner)).await?;
        info!("Created event {event_id}");
        if !event_data.is_local {
            Self::register_event_mount_point(event_id, &api_token, client_cmd_tx).await?;
        }
        Ok((event_id, api_token))
    }

    async fn register_event_mount_point(event_id: EventId, api_token: &str, client_cmd_tx: ClientCommandSender) -> anyhow::Result<()> {
        let remote_mount_point = format!("{}/{}", global_config().remote_events_mount_point, event_id);
        let param: Vec<RpcValue> = vec![
            api_token.into(),
            make_map!( "mountPoint".to_string() => RpcValue::from(remote_mount_point),).into(),
        ];
        let _res: RpcValue = client_cmd_tx.call_rpc_method(".broker/access/mounts", "setValue", Some(param.into()), None, None, None::<fn(f64)>)
            .await.map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    pub async fn update_event_record(&self, event_id: EventId, record: EventRecordChange, client_cmd_tx: ClientCommandSender) -> anyhow::Result<bool> {
        // always update the mount point, for case than shvbroker config is reloaded from file
        let event_data = self.event_record(event_id).await?;
        if !event_data.is_local {
            Self::register_event_mount_point(event_id, &event_data.api_token, client_cmd_tx.clone()).await?;
        }
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        qxsql.update_record_with_recchng("events", event_id, &record.to_record(), client_cmd_tx, None).await
    }

    pub fn open_event_status(&self, event_id: EventId) -> anyhow::Result<EventStatus> {
        self.open_events.get(&event_id).ok_or_else(|| anyhow!("Invalid event id: {event_id}"))
            .map(|ectl| {
                EventStatus {
                    is_local: ectl.local_db.is_some(),
                    open_at: ectl.open_at.with_timezone(&Local).fixed_offset(),
                    expires_at: ectl.expires_at().with_timezone(&Local).fixed_offset(),
                    current_stage: ectl.current_stage,
                }
            })
    }

    pub async fn event_record(&self, event_id: EventId) -> anyhow::Result<EventRecord> {
        let qxsql = AppSqlApi::new(self.db_pool.clone());
        let record = qxsql.read_record("events", event_id, None).await?
            .ok_or_else(||anyhow!("Event id: {event_id} not found"))?;
        EventRecord::from_record(&record)
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

}

pub(crate) async fn open_event(app_state: SharedAppState, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<String> {
    let event_shv_path = event_api_shv_path(event_id);
    {
        let mut state = app_state.write().await;
        if let Some(event) = state.open_events.get_mut(&event_id) {
            event.touched_at = chrono::Utc::now();
            return Ok(event_shv_path);
        }
    }

    let event_record = app_state.read().await.event_record(event_id).await?;
    let local_db = if event_record.is_local {
        let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
        let pool = migrate_db(&db_file, &event_record).await?;
        Some(pool)
    } else {
        // ping child
        let _: () = client_command_sender.call_rpc_method(join_path(remote_event_mount_point(event_id), ".app"), "ping", None, None, None, None::<fn(_)>).await
            .map_err(|_| anyhow!("Failed to ping DB service."))?;
        None
    };

    let current_stage = update_event_record_from_event_config(app_state.clone(), client_command_sender.clone(), event_id, &event_record).await?;
    let now = chrono::Utc::now();
    app_state.write().await.open_events.insert(event_id, OpenEventCtl {
        current_stage,
        local_db,
        open_at: now,
        touched_at: now
    });

    let message = RpcMessage::new_signal("event", "lsmod").with_param(true);
    client_command_sender.send_message(message)
        .map_err(|e| anyhow!("Failed to send message {}", e))?;

    Ok(event_shv_path)
}

async fn update_event_record_from_event_config(app_state: SharedAppState, client_command_sender: ClientCommandSender, event_id: i64, event_record: &EventRecord) -> anyhow::Result<i64> {
    let event_sql = EventSqlApi::new(event_id, app_state.clone(), client_command_sender.clone());
    let result = event_sql.query("SELECT ckey, cvalue FROM config WHERE ckey IN ('event.currentStageId', 'event.name')", None).await?;
    let mut changes = EventRecordChange::default();
    let mut current_stage = 1;
    for row in 0..result.row_count() {
        if let Some(key) = result.value(row, 0) {
            if let Some(key) = key.as_str() {
                match key {
                    "event.currentStageId" => {
                        let stage = result.value(row, 1).and_then(|v| v.as_str())
                            .and_then(|v| v.parse::<i64>().ok());
                        if let Some(stage) = stage {
                            current_stage = stage;
                            if event_record.stage != stage {
                                changes.stage = Some(stage);
                            }
                        }
                    }
                    "event.name" => {
                        let event_name = result.value(row, 1).and_then(|v| v.as_str());
                        if let Some(event_name) = event_name && event_record.name != event_name {
                            changes.name = Some(event_name.to_string());
                        }
                    }
                    _ => {
                        // skip other keys
                    }
                }

            }
        }
    }
    if !changes.is_empty() {
        app_state.read().await.update_event_record(event_id, changes, client_command_sender).await?;
        info!("Updated event record for event {event_id}");
    }
    Ok(current_stage)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventStatus {
    pub current_stage: i64,
    pub is_local: bool,
    pub open_at: DateTime<chrono::FixedOffset>,
    pub expires_at: DateTime<chrono::FixedOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub id: Option<i64>,
    pub name: String,
    pub date: DateTime<chrono::FixedOffset>,
    #[serde(default = "default_stage")]
    pub stage: i64,
    pub owner: String,
    pub api_token: String,
    pub is_local: bool,
}

fn default_stage() -> i64 { 1 }

impl EventRecord {
    fn from_record(record: &Record) -> anyhow::Result<Self> {
        let get_field = |name| record.get(name).ok_or_else(||anyhow!("Cannot get field '{}'.", name));
        Ok(Self {
            id: get_field("id")?.to_int(),
            name: get_field("name")?.as_str().unwrap_or_default().to_string(),
            date: get_field("date")?.to_datetime().unwrap_or_else(|| chrono::Local::now().fixed_offset()),
            stage: get_field("stage")?.to_int().unwrap_or(default_stage()),
            owner: get_field("owner")?.as_str().unwrap_or_default().to_string(),
            is_local: get_field("is_local")?.to_bool(),
            api_token: get_field("api_token")?.as_str().unwrap_or_default().to_string(),
        })
    }
    fn to_record(&self) -> anyhow::Result<Record> {
        let mut record = Record::new();
        record.insert("name".to_string(), self.name.clone().into());
        record.insert("date".to_string(), self.date.into());
        record.insert("stage".to_string(), self.stage.into());
        record.insert("owner".to_string(), self.owner.clone().into());
        record.insert("api_token".to_string(), self.api_token.clone().into());
        record.insert("is_local".to_string(), self.is_local.into());
        Ok(record)
    }
}

impl_rpcvalue_conversions!(EventStatus);
impl_rpcvalue_conversions!(EventRecord);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct EventRecordChange {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub date: Option<DateTime<chrono::FixedOffset>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub is_local: Option<bool>,
}

impl EventRecordChange {
    fn to_record(&self) -> Record {
        let mut record = Record::new();
        if let Some(name) = &self.name {
            record.insert("name".to_string(), name.clone().into());
        }
        if let Some(date) = &self.date {
            record.insert("date".to_string(), (*date).into());
        }
        if let Some(stage) = &self.stage {
            record.insert("stage".to_string(), (*stage).into());
        }
        if let Some(owner) = &self.owner {
            record.insert("owner".to_string(), owner.clone().into());
        }
        if let Some(api_token) = &self.api_token {
            record.insert("api_token".to_string(), api_token.clone().into());
        }
        if let Some(is_local) = &self.is_local {
            record.insert("is_local".to_string(), (*is_local).into());
        }
        record
    }
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.date.is_none()
            && self.stage.is_none()
            && self.owner.is_none()
            && self.api_token.is_none()
            && self.is_local.is_none()
    }
}

pub(crate) struct OpenEventCtl {
    pub current_stage: i64,
    pub local_db: Option<async_sqlite::Pool>,
    pub open_at: DateTime<chrono::Utc>,
    pub touched_at: DateTime<chrono::Utc>,
}

impl OpenEventCtl {
    pub fn expires_at(&self) -> DateTime<chrono::Utc> {
        self.open_at + global_config().event_expire_duration
    }
}

pub fn event_api_shv_path(event_id: EventId) -> String {
    format!("{}/eventctl/{event_id}", global_config().client.mount.as_deref().unwrap_or_default())
}

pub fn remote_event_mount_point(event_id: EventId) -> String {
    format!("{}/{event_id}", global_config().remote_events_mount_point)
}
