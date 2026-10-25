use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};
use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::{Compression, write::ZlibEncoder};
use futures::StreamExt;
use anyhow::bail;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Local;
use log::*;
use qxsql::{QxSqlApiRecChng, RecChng, RecOp};
use qxsql::{DbValue, Record};
use qxsql::{sql::{QxSqlApi, record_from_slice}};
use serde::{Deserialize, Serialize};
use shvclient::ClientCommandSender;

use shvproto::RpcValue;
use shvproto::make_map;
use shvrpc::rpc::ShvRI;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use smol::{lock::RwLock, channel};
use shvrpc::util::join_path;

use crate::localdbsqlapi::LocalDbSqlApi;
use crate::eventdb::migrate_db;
use crate::eventsqlapi::EventSqlApi;
use crate::generate_api_token;
use crate::global_config;
use crate::qxeventconfig::QxEventConfig;


pub type EventId = i64;

pub type SharedAppState = Arc<RwLock<State>>;

pub(crate) struct State {
    pub db_pool: async_sqlite::Pool,
    pub open_events: BTreeMap<EventId, Arc<RwLock<OpenEventCtl>>>,
    pub event_records: HashMap<EventId, Arc<RwLock<EventRecord>>>,
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
        let qxsql = LocalDbSqlApi::new_without_recchng(self.db_pool.clone());
        let records = qxsql.list_records("events", Some(vec!["id", "name", "stage_count", "date", "owner", "db_mode"]), None, None).await?;
        Ok(records)
    }

    pub async fn create_event(&self, owner: String, rpc_client: ClientCommandSender) -> anyhow::Result<(EventId, String)> {
        if owner.is_empty() {
            return Err(anyhow::anyhow!("Owner cannot be empty"));
        }
        let api_token = generate_api_token();
        let event_data = EventRecord {
            db_mode: EventDbMode::Replica,
            name: String::new(),
            date: chrono::Local::now().fixed_offset(),
            owner: owner.clone(),
            api_token: api_token.clone(),
            id: None,
            stage_count: default_stage_count(),
            config: QxEventConfig::default(),
        };
        let rec = event_data.to_record()?;
        let qxsql = LocalDbSqlApi::new(self.db_pool.clone(), rpc_client.clone());
        let event_id = qxsql.create_record_with_recchng("events", &rec, Some(owner)).await?;
        info!("Created event {event_id}");
        if matches!(event_data.db_mode, EventDbMode::Replica) {
            Self::register_event_mount_point(event_id, &api_token, rpc_client).await?;
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

    pub async fn update_event_record(&mut self, event_id: EventId, record: EventRecordChange, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        // always update the mount point, for case than shvbroker config is reloaded from file
        let event_record = self.load_event_record(event_id).await?;
        if matches!(event_record.db_mode, EventDbMode::Replica) {
            Self::register_event_mount_point(event_id, &event_record.api_token, rpc_client.clone()).await?;
        }
        let qxsql = LocalDbSqlApi::new(self.db_pool.clone(), rpc_client.clone());
        let ret = qxsql.update_record_with_recchng("events", event_id, &record.to_record(), None).await;
        self.forget_event_record(event_id).await?;
        ret
    }

    pub async fn set_event_db_mode(&mut self, event_id: EventId, db_mode: EventDbMode, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        let event_record = self.load_event_record(event_id).await?;
        if matches!(db_mode, EventDbMode::Replica) {
            Self::register_event_mount_point(event_id, &event_record.api_token, rpc_client.clone()).await?;
        }
        let mut record = Record::new();
        record.insert("db_mode".to_string(), db_mode.as_record_value().into());
        let qxsql = LocalDbSqlApi::new(self.db_pool.clone(), rpc_client);
        let result = qxsql.update_record_with_recchng("events", event_id, &record, None).await;
        self.forget_event_record(event_id).await?;
        result
    }

    pub async fn opened_event_status(&self, event_id: EventId) -> anyhow::Result<EventStatus> {
        let event = self.open_events.get(&event_id)
            .ok_or_else(|| anyhow!("Invalid event id: {event_id}"))?
            .clone();
        let event = event.read().await;
        Ok(EventStatus {
            master_db_mode: EventDbMode::ReadOnly,
            open_at: event.open_at.with_timezone(&Local).fixed_offset(),
            expires_at: event.expires_at().with_timezone(&Local).fixed_offset(),
        })
    }

    pub async fn event_record(&mut self, event_id: EventId) -> anyhow::Result<Arc<RwLock<EventRecord>>> {
        if let Some(record) = self.event_records.get(&event_id) {
            return Ok(record.clone());
        }
        let record = Arc::new(RwLock::new(self.load_event_record(event_id).await?));
        self.event_records.insert(event_id, record.clone());
        Ok(record)
    }

    async fn forget_event_record(&mut self, event_id: EventId) -> anyhow::Result<()> {
        self.event_records.remove(&event_id);
        Ok(())
    }

    pub async fn load_event_record(&self, event_id: EventId) -> anyhow::Result<EventRecord> {
        let qxsql = LocalDbSqlApi::new_without_recchng(self.db_pool.clone());
        let record = qxsql.read_record("events", event_id, None).await?
            .ok_or_else(||anyhow!("Event id: {event_id} not found"))?;
        EventRecord::from_record(&record)
    }

    pub async fn push_event_db(app_state: SharedAppState, event_id: EventId, zdata: &[u8], client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        // Decode before closing the event so an invalid upload does not interrupt a
        // currently running event.
        let mut decoder = ZlibDecoder::new(zdata);
        let mut db_data = Vec::new();
        decoder.read_to_end(&mut db_data)
            .map_err(|err| anyhow!("Failed to decompress event database: {err}"))?;

        // An open SQLite pool would continue using the old database after the file
        // is replaced. Close it first, then install the new file atomically.
        let was_open = {
            let was_open = {
                let mut state = app_state.write().await;
                let was_open = state.open_events.contains_key(&event_id);
                state.close_event(event_id, client_command_sender.clone()).await?;
                was_open
            };

            let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
            let db_path = std::path::Path::new(&db_file);
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp_file = format!("{db_file}.upload-tmp");
            std::fs::write(&tmp_file, &db_data)?;
            if let Err(err) = std::fs::rename(&tmp_file, db_path) {
                let _ = std::fs::remove_file(&tmp_file);
                return Err(err.into());
            }
            was_open
        };

        if was_open {
            open_event(app_state, event_id, client_command_sender).await?;
        }
        Ok(())
    }

    pub async fn pull_event_db(app_state: SharedAppState, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<(usize, Vec<u8>)> {
        let was_open = {
            let mut state = app_state.write().await;
            let was_open = state.open_events.contains_key(&event_id);
            if was_open {
                state.close_event(event_id, client_command_sender.clone()).await?;
            }
            was_open
        };

        let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
        let db_data = std::fs::read(&db_file)?;
        let db_data_size = db_data.len();
        if was_open {
            open_event(app_state, event_id, client_command_sender).await?;
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&db_data)?;
        Ok((db_data_size, encoder.finish()?))
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
    pub async fn delete_event(&mut self, event_id: EventId, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        log::info!("Deleting event {}", event_id);
        self.close_event(event_id, rpc_client.clone()).await?;
        let qxsql = LocalDbSqlApi::new(self.db_pool.clone(), rpc_client);
        let was_deleted = qxsql.delete_record_with_recchng("events", event_id, None).await?;
        Ok(was_deleted)
    }

    pub async fn gc_expired_events(&mut self, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let event_age_list = self.open_events.iter()
            .map(|(id, event)| (*id, event.clone())).collect::<Vec<_>>();
        let now = chrono::Utc::now();
        for (event_id, event) in event_age_list {
            if event.read().await.expires_at() < now {
                info!("Closing event: {event_id} as expired.");
                self.close_event(event_id, client_command_sender.clone()).await?;
            }
        }
        Ok(())
    }

    pub async fn api_token_to_event_id(&self, api_token: &str) -> anyhow::Result<EventId> {
        let qxsql = LocalDbSqlApi::new_without_recchng(self.db_pool.clone());
        let result = qxsql
            .query("SELECT id FROM events WHERE api_token = :api_token", Some(&record_from_slice(&[("api_token", api_token.into())])))
            .await?;
        let event_id = result.rows.first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.to_int());
        event_id.ok_or_else(|| anyhow::anyhow!("API token not found"))
    }

}

pub(crate) async fn open_event(app_state: SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) -> anyhow::Result<String> {
    let event_shv_path = event_api_shv_path(event_id);
    {
        let state = app_state.write().await;
        if let Some(event) = state.open_events.get(&event_id).cloned() {
            event.write().await.touched_at = chrono::Utc::now();
            return Ok(event_shv_path);
        }
    }

    let event_record = app_state.read().await.load_event_record(event_id).await?;
    let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
    let pool = migrate_db(&db_file, &event_record, rpc_client.clone()).await?;
    let now = chrono::Utc::now();
    app_state.write().await.open_events.insert(event_id, Arc::new(RwLock::new(OpenEventCtl {
        db: pool,
        open_at: now,
        touched_at: now,
    })));

    let message = RpcMessage::new_signal("event", "lsmod").with_param(true);
    rpc_client.send_message(message)
        .map_err(|e| anyhow!("Failed to send message {}", e))?;

    smol::spawn(setup_remote_db_connection(app_state.clone(), event_id, rpc_client.clone())).detach();

    Ok(event_shv_path)
}

async fn setup_remote_db_connection(app_state: SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) {
    let ping_result: anyhow::Result<()> = rpc_client
        .call_rpc_method(join_path(remote_event_mount_point(event_id), ".app"), "ping", None, None, None, None::<fn(_)>)
        .await
        .map_err(|_| anyhow!("Failed to ping DB service."));
    if let Err(err) = ping_result {
        error!("Failed to set up remote DB connection for event {event_id}: {err}");
        if let Err(close_err) = app_state.write().await.close_event(event_id, rpc_client.clone()).await {
            error!("Failed to close event {event_id} after remote DB setup failure: {close_err}");
        }
        return;
    }

    let ri = match ShvRI::from_path_method_signal(&remote_event_sql_path(event_id), "", Some("recchng")) {
        Ok(ri) => ri,
        Err(err) => {
            error!("Failed to create subscription request for event {event_id}: {err}");
            if let Err(close_err) = app_state.write().await.close_event(event_id, rpc_client.clone()).await {
                error!("Failed to close event {event_id} after remote DB setup failure: {close_err}");
            }
            return;
        }
    };
    info!("Subscribing SQL recchng: {ri:?}");
    let mut subscriber = match rpc_client.subscribe(ri).await {
        Ok(subscriber) => subscriber,
        Err(err) => {
            error!("Failed to subscribe to remote event {event_id}: {err}");
            if let Err(close_err) = app_state.write().await.close_event(event_id, rpc_client.clone()).await {
                error!("Failed to close event {event_id} after remote DB setup failure: {close_err}");
            }
            return;
        }
    };

    while let Some(frame) = subscriber.next().await {
        match frame.to_rpcmesage() {
            Ok(message) => {
                if message.is_signal() && message.method() == Some("recchng") {
                    let remote_sql_path = remote_event_sql_path(event_id);
                    if let Some(path) = message.shv_path() && path == &remote_sql_path {
                        info!("Received event {event_id} subscription message: {:?}", message.shv_path());
                        match rechng_from_rpcvalue(message.param()) {
                            Ok(recchng) => match reply_remote_db_recchng(app_state.clone(), event_id, &recchng, rpc_client.clone()).await {
                                Ok(()) => {}
                                Err(err) => error!("Failed to reply remote db recchng for event {event_id}: {err}"),
                            },
                            Err(err) => {
                                error!("Failed to decode event {event_id} recchng signal: {err}");
                                continue;
                            }
                        }
                    }
                }
            },
            Err(err) => error!("Failed to decode event {event_id} subscription message: {err}"),
        }
    }
    info!("SQL recchng {event_id} subscription stream closed");
    if let Err(err) = app_state.write().await.close_event(event_id, rpc_client).await {
        error!("Failed to close event {event_id} after SQL recchng subscription closed: {err}");
    }
}

async fn reply_remote_db_recchng(app_state: SharedAppState, event_id: EventId, recchng: &RecChng, rpc_client: ClientCommandSender) -> anyhow::Result<()> {
    let sql_api = EventSqlApi::new(event_id, app_state, rpc_client);

    match &recchng.op {
        RecOp::Insert => match recchng.record.as_ref() {
            Some(record) => {
                sql_api.create_record_with_recchng(&recchng.table, &record, recchng.issuer.clone()).await?;
                Ok(())
            }
            None => Err(anyhow!("insert recchng has no record")),
        },
        RecOp::Update => match recchng.record.as_ref() {
            Some(record) => {
                sql_api
                    .update_record_with_recchng(&recchng.table, recchng.id, &record, recchng.issuer.clone())
                    .await?;
                Ok(())
            }
            None => Err(anyhow!("update recchng has no record")),
        },
        RecOp::Delete => {
            sql_api
                .delete_record_with_recchng(&recchng.table, recchng.id, recchng.issuer.clone())
                .await?;
            Ok(())
        }
    }
}

fn rechng_from_rpcvalue(value: Option<&RpcValue>) -> anyhow::Result<RecChng> {
    let value = value.ok_or_else(|| anyhow!("recchng signal has no parameter"))?;
    shvproto::from_rpcvalue(value)
        .map_err(|err| anyhow!("invalid recchng value: {err}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EventDbMode {
    ReadOnly,
    Master,
    Replica,
}

impl EventDbMode {
    fn from_record_value(value: &DbValue) -> Self {
        let mode = value.as_str();
        match mode {
            Some("ReadOnly") => Self::ReadOnly,
            Some("Master") => Self::Master,
            Some("Replica") => Self::Replica,
            _ => {
                // Rows created before EventDbMode was introduced stored this as a bool.
                if value.to_bool() {
                    Self::Master
                } else {
                    Self::Replica
                }
            }
        }
    }

    fn as_record_value(self) -> String {
        match self {
            Self::ReadOnly => "ReadOnly",
            Self::Master => "Master",
            Self::Replica => "Replica",
        }.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventStatus {
    pub master_db_mode: EventDbMode,
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
    #[serde(default = "default_stage_count")]
    pub stage_count: i64,
    pub owner: String,
    pub api_token: String,
    pub config: QxEventConfig,
    pub db_mode: EventDbMode,
}

fn default_stage_count() -> i64 { 1 }

impl EventRecord {
    fn from_record(record: &Record) -> anyhow::Result<Self> {
        let get_field = |name| record.get(name).ok_or_else(||anyhow!("Cannot get field '{}'.", name));
        Ok(Self {
            id: get_field("id")?.to_int(),
            name: get_field("name")?.as_str().unwrap_or_default().to_string(),
            date: get_field("date")?.to_datetime().unwrap_or_else(|| chrono::Local::now().fixed_offset()),
            stage_count: get_field("stage_count")?.to_int().unwrap_or(default_stage_count()),
            owner: get_field("owner")?.as_str().unwrap_or_default().to_string(),
            db_mode: EventDbMode::from_record_value(get_field("db_mode")?),
            api_token: get_field("api_token")?.as_str().unwrap_or_default().to_string(),
            config: get_field("config")?.as_str()
                .and_then(|config| serde_json::from_str(config).ok())
                .unwrap_or_default(),
        })
    }
    fn to_record(&self) -> anyhow::Result<Record> {
        let mut record = Record::new();
        record.insert("name".to_string(), self.name.clone().into());
        record.insert("date".to_string(), self.date.into());
        record.insert("stage_count".to_string(), self.stage_count.into());
        record.insert("owner".to_string(), self.owner.clone().into());
        record.insert("api_token".to_string(), self.api_token.clone().into());
        record.insert("config".to_string(), serde_json::to_string(&self.config)?.into());
        record.insert("db_mode".to_string(), self.db_mode.as_record_value().into());
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
    pub stage_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub master_db_mode: Option<EventDbMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<QxEventConfig>,
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
        if let Some(stage) = &self.stage_count {
            record.insert("stage_count".to_string(), (*stage).into());
        }
        if let Some(owner) = &self.owner {
            record.insert("owner".to_string(), owner.clone().into());
        }
        if let Some(api_token) = &self.api_token {
            record.insert("api_token".to_string(), api_token.clone().into());
        }
        if let Some(master_db_mode) = self.master_db_mode {
            record.insert("db_mode".to_string(), master_db_mode.as_record_value().into());
        }
        if let Some(config) = &self.config {
            record.insert("config".to_string(), serde_json::to_string(config).expect("config to json should not fail").into());
        }
        record
    }
    // pub fn is_empty(&self) -> bool {
    //     self.name.is_none()
    //         && self.date.is_none()
    //         && self.stage_count.is_none()
    //         && self.owner.is_none()
    //         && self.api_token.is_none()
    //         && self.master_db_mode.is_none()
    //         && self.config.is_none()
    // }
}

pub(crate) struct OpenEventCtl {
    pub db: async_sqlite::Pool,
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

pub fn remote_event_sql_path(event_id: EventId) -> String {
    format!("{}/sql", remote_event_mount_point(event_id))
}
