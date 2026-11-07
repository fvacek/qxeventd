use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
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
use crate::eventdb::{migrate_db, migrate_db_file};
use crate::eventsqlapi::LocalEventSqlApi;
use crate::generate_api_token;
use crate::global_config;
use crate::qxeventconfig::QxEventConfig;
use crate::rpctype::{RpcType, RpcTypeStr};


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

    pub async fn list_events(&self) -> anyhow::Result<Vec<EventListItem>> {
        let qxsql = LocalDbSqlApi::new_without_recchng(self.db_pool.clone());
        let records = qxsql.list_records("events", Some(vec!["id", "name", "stage_count", "date", "owner", "db_mode"]), None, None).await?;
        records.iter().map(EventListItem::from_record).collect()
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

    pub async fn event_db_mode(&mut self, event_id: EventId) -> anyhow::Result<EventDbMode> {
        let record = self.event_record(event_id).await?;
        Ok(record.read().await.db_mode.clone())
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

    /// Install the zlib compressed database uploaded using the pushDb file node. The uploaded
    /// file is kept if it cannot be installed, so the upload does not have to be repeated.
    /// `expected_sha1` is the SHA-1 of the uncompressed database.
    pub async fn commit_pushed_event_db(app_state: SharedAppState, event_id: EventId, expected_sha1: Vec<u8>, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let pushed_file = pushed_event_db_file(event_id);
        let tmp_file = format!("{pushed_file}.inflate-tmp-{:016x}", rand::random::<u64>());

        // Decode and verify before closing the event so an invalid upload does not interrupt
        // a currently running event.
        let inflate_result = {
            let pushed_file = pushed_file.clone();
            let tmp_file = tmp_file.clone();
            smol::unblock(move || {
                inflate_file(&pushed_file, &tmp_file)
                    .map_err(|err| anyhow!("Failed to decompress event {event_id} database: {err}"))?;
                if crate::filenode::sha1(std::path::Path::new(&tmp_file), 0, None)? != expected_sha1 {
                    bail!("Event {event_id} database does not match the expected SHA-1");
                }
                migrate_db_file(&tmp_file)
                    .map_err(|err| anyhow!("Invalid event {event_id} database: {err}"))
            }).await
        };
        let install_result = match inflate_result {
            Ok(()) => Self::install_event_db(app_state, event_id, &tmp_file, client_command_sender).await,
            Err(err) => Err(err),
        };
        if install_result.is_err() {
            let _ = std::fs::remove_file(&tmp_file);
        }
        install_result?;

        if let Err(err) = std::fs::remove_file(&pushed_file) {
            warn!("Failed to remove pushed event DB file {pushed_file}: {err}");
        }
        Ok(())
    }

    /// Replace the event database with `new_db_file`, the file is moved.
    /// It must be located in the same file system as the event database.
    async fn install_event_db(app_state: SharedAppState, event_id: EventId, new_db_file: &str, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let db_file = event_db_file(event_id);
        let was_open = {
            // An open SQLite pool would continue using the old database after the file
            // is replaced. Close it first, then install the new file atomically. The lock
            // is held until the file is replaced, so the event cannot be opened meanwhile.
            let mut state = app_state.write().await;
            // Checked before closing, so missing file does not interrupt a running event.
            std::fs::metadata(new_db_file)
                .map_err(|err| anyhow!("Cannot install event {event_id} database {new_db_file}: {err}"))?;
            let was_open = state.open_events.contains_key(&event_id);
            state.close_event(event_id, client_command_sender.clone()).await?;
            // SQLite would replay WAL left over from the old database over the new one.
            for stale_file in [format!("{db_file}-wal"), format!("{db_file}-shm")] {
                if let Err(err) = std::fs::remove_file(&stale_file) && err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
            std::fs::rename(new_db_file, &db_file)?;
            was_open
        };

        if was_open {
            open_event(app_state, event_id, client_command_sender).await?;
        }
        Ok(())
    }

    /// Create the zlib compressed file served by the pullDb file node, it replaces the previous
    /// one atomically.
    pub async fn snapshot_event_db(app_state: SharedAppState, event_id: EventId) -> anyhow::Result<()> {
        let snapshot_file = pulled_event_db_file(event_id);
        let tmp_file = format!("{snapshot_file}.tmp-{:016x}", rand::random::<u64>());
        let create_result: anyhow::Result<()> = {
            // The lock is held until the copy is done, so the event database cannot be
            // replaced, nor the event opened or closed meanwhile.
            let state = app_state.read().await;
            let open_event_db = match state.open_events.get(&event_id).cloned() {
                Some(event) => Some(event.read().await.db.clone()),
                None => None,
            };
            match open_event_db {
                Some(db) => backup_open_event_db(&db, event_id, &tmp_file).await,
                None => {
                    let db_file = event_db_file(event_id);
                    let tmp_file = tmp_file.clone();
                    smol::unblock(move || std::fs::copy(db_file, tmp_file)).await
                        .map(|_| ())
                        .map_err(|e| anyhow!("Failed to copy event {event_id} DB: {e}"))
                }
            }
        };
        // The copy is not affected by database changes, the lock is not needed for compression.
        let compressed_tmp_file = format!("{tmp_file}.zlib");
        let result = match create_result {
            Ok(()) => {
                let tmp_file = tmp_file.clone();
                let compressed_tmp_file = compressed_tmp_file.clone();
                smol::unblock(move || {
                    deflate_file(&tmp_file, &compressed_tmp_file)
                        .map_err(|e| anyhow!("Failed to compress event {event_id} DB: {e}"))?;
                    std::fs::rename(&compressed_tmp_file, &snapshot_file)?;
                    Ok(())
                }).await
            }
            Err(err) => Err(err),
        };
        let _ = std::fs::remove_file(&tmp_file);
        if result.is_err() {
            let _ = std::fs::remove_file(&compressed_tmp_file);
        }
        result
    }

    pub async fn close_event(&mut self, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<bool> {
        if let Some(event) = self.open_events.remove(&event_id) {
            // let mount_point = event_mount_point(event_id);

            let close_result = close_event_db(&event.read().await.db).await;

            let message = RpcMessage::new_signal("event", "lsmod").with_param(false);
            client_command_sender.send_message(message)
            .map_err(|e| anyhow!("Failed to send message {}", e))?;
            close_result.map_err(|e| e.context(format!("Failed to close event {event_id} DB")))?;

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

/// Make consistent snapshot of an open event DB using SQLite online backup API
async fn backup_open_event_db(db: &async_sqlite::Pool, event_id: EventId, backup_file: &str) -> anyhow::Result<()> {
    let backup_file = backup_file.to_string();
    db.conn(move |conn| conn.backup(async_sqlite::rusqlite::MAIN_DB, &backup_file, None)).await
        .map_err(|e| anyhow!("Failed to backup event {event_id} DB: {e}"))
}

/// Checkpoint WAL and close connections, so the whole database is stored in the main DB file.
/// The checkpoint has to be explicit, the pool closes its connections concurrently and
/// SQLite does not checkpoint on close in that case, leaving the data in the WAL file.
async fn close_event_db(db: &async_sqlite::Pool) -> anyhow::Result<()> {
    let checkpoint_busy = db.conn(|conn| conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get::<_, i64>(0))).await;
    db.close().await?;
    if checkpoint_busy? != 0 {
        bail!("WAL checkpoint was blocked");
    }
    Ok(())
}

pub(crate) async fn open_event(app_state: SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) -> anyhow::Result<String> {
    let event_shv_path = event_api_shv_path(event_id);
    // The lock is held until the event is registered, so the DB file cannot be replaced by
    // commit_pushed_event_db after the pool is opened, nor can the event be opened twice.
    let mut state = app_state.write().await;
    if let Some(event) = state.open_events.get(&event_id).cloned() {
        event.write().await.touched_at = chrono::Utc::now();
        return Ok(event_shv_path);
    }

    let event_record = state.load_event_record(event_id).await?;
    let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
    let pool = migrate_db(&db_file, &event_record, rpc_client.clone()).await?;
    let now = chrono::Utc::now();
    let event = Arc::new(RwLock::new(OpenEventCtl {
        db: pool,
        open_at: now,
        touched_at: now,
    }));
    state.open_events.insert(event_id, event.clone());
    drop(state);

    let message = RpcMessage::new_signal("event", "lsmod").with_param(true);
    rpc_client.send_message(message)
        .map_err(|e| anyhow!("Failed to send message {}", e))?;

    if matches!(event_record.db_mode, EventDbMode::Replica) {
        setup_remote_db_recchng_monitoring(app_state.clone(), event_id, rpc_client.clone()).await;
    }

    Ok(event_shv_path)
}

/// Starts remote DB recchng monitoring for an open event.
/// Has to be called whenever event DB mode changes to Replica.
pub(crate) async fn setup_remote_db_recchng_monitoring(app_state: SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) {
    let Some(event) = app_state.read().await.open_events.get(&event_id).cloned() else {
        // event is not open, monitoring will be started when it is opened
        return;
    };
    smol::spawn(remote_db_recchng_monitoring_task(app_state, event_id, event, rpc_client)).detach();
}

async fn remote_db_recchng_monitoring_task(app_state: SharedAppState, event_id: EventId, event: Arc<RwLock<OpenEventCtl>>, rpc_client: ClientCommandSender) {
    let ping_result: anyhow::Result<()> = rpc_client
        .call_rpc_method(join_path(remote_event_mount_point(event_id), ".app"), "ping", None, None, None, None::<fn(_)>)
        .await
        .map_err(|_| anyhow!("Failed to ping DB service."));
    if let Err(err) = ping_result {
        warn!("Remote DB connection for event {event_id} not available, error: {err}");
        return;
    }

    let ri = match ShvRI::from_path_method_signal(&remote_event_sql_path(event_id), "", Some("recchng")) {
        Ok(ri) => ri,
        Err(err) => {
            error!("Failed to create subscription request for event {event_id}: {err}");
            return;
        }
    };
    info!("Subscribing SQL recchng: {ri:?}");
    let mut subscriber = match rpc_client.subscribe(ri).await {
        Ok(subscriber) => subscriber,
        Err(err) => {
            warn!("Failed to subscribe to remote event {event_id}, using local DB only: {err}");
            return;
        }
    };

    while let Some(frame) = subscriber.next().await {
        let is_same_event_open = app_state.read().await.open_events.get(&event_id)
            .is_some_and(|open_event| Arc::ptr_eq(open_event, &event));
        if !is_same_event_open {
            info!("Event {event_id} was closed, stopping remote DB synchronization");
            break;
        }
        match frame.to_rpcmesage() {
            Ok(message) => {
                if message.is_signal() && message.method() == Some("recchng") {
                    let remote_sql_path = remote_event_sql_path(event_id);
                    if let Some(path) = message.shv_path() && path == &remote_sql_path {
                        info!("Received event {event_id} subscription message: {:?}", message.shv_path());
                        let db_mode = match app_state.write().await.event_db_mode(event_id).await {
                            Ok(db_mode) => db_mode,
                            Err(err) => {
                                error!("Failed to load event {event_id} record: {err}");
                                continue;
                            }
                        };
                        if !matches!(db_mode, EventDbMode::Replica) {
                            info!("Event {event_id} DB mode is {db_mode:?}, stopping remote DB synchronization");
                            break;
                        }
                        match rechng_from_rpcvalue(message.param()) {
                            Ok(recchng) => match apply_remote_db_recchng(app_state.clone(), event_id, &recchng, rpc_client.clone()).await {
                                Ok(()) => {}
                                Err(err) => error!("Failed to apply remote db recchng for event {event_id}: {err}"),
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
    info!("SQL recchng {event_id} subscription ended");
}

async fn apply_remote_db_recchng(app_state: SharedAppState, event_id: EventId, recchng: &RecChng, rpc_client: ClientCommandSender) -> anyhow::Result<()> {
    let sql_api = LocalEventSqlApi::new(event_id, app_state, rpc_client);
    info!("Applying recchng {event_id}");

    match &recchng.op {
        RecOp::Insert => match recchng.record.as_ref() {
            Some(record) => {
                let record = sanitize_recchng_record(&sql_api, &recchng.table, record).await?;
                sql_api.create_record_with_recchng(&recchng.table, &record, recchng.issuer.clone()).await?;
                Ok(())
            }
            None => Err(anyhow!("insert recchng has no record")),
        },
        RecOp::Update => match recchng.record.as_ref() {
            Some(record) => {
                info!("Update recchng {recchng:?}");
                let record = sanitize_recchng_record(&sql_api, &recchng.table, record).await?;
                if record.is_empty() {
                    return Ok(());
                }
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

/// Remote clients may publish records taken from joined views, with keys like
/// `runs.leg`, `competitors.id` or computed columns like `runflags`.
/// Keep only fields that are real columns of `table`, stripping the `table.` prefix.
async fn sanitize_recchng_record(sql_api: &impl QxSqlApi, table: &str, record: &Record) -> anyhow::Result<Record> {
    let params: Record = [("table".to_string(), DbValue::from(table.to_string()))].into_iter().collect();
    let result = sql_api.query("SELECT name FROM pragma_table_info(:table)", Some(&params)).await?;
    let columns: std::collections::HashSet<String> = result.rows.iter()
        .filter_map(|row| match row.first() {
            Some(DbValue::String(s)) => Some(s.to_lowercase()),
            _ => None,
        })
        .collect();
    if columns.is_empty() {
        bail!("table '{table}' does not exist");
    }
    let prefix = format!("{}.", table.to_lowercase());
    let mut sanitized = Record::new();
    let mut dropped = Vec::new();
    for (key, value) in record {
        let lkey = key.to_lowercase();
        let col = lkey.strip_prefix(&prefix).unwrap_or(&lkey);
        if columns.contains(col) {
            sanitized.insert(col.to_string(), value.clone());
        } else {
            dropped.push(key.as_str());
        }
    }
    if !dropped.is_empty() {
        debug!("recchng for table '{table}': dropped non-column fields: {dropped:?}");
    }
    Ok(sanitized)
}

fn rechng_from_rpcvalue(value: Option<&RpcValue>) -> anyhow::Result<RecChng> {
    let value = value.ok_or_else(|| anyhow!("recchng signal has no parameter"))?;
    shvproto::from_rpcvalue(value)
        .map_err(|err| anyhow!("invalid recchng value: {err}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EventDbMode {
    Archived,
    ReadOnly,
    Master,
    Replica,
}

impl RpcType for EventDbMode {
    const RPC_TYPE: &'static str = "s";
}

impl EventDbMode {
    fn from_record_value(value: &DbValue) -> Self {
        let mode = value.as_str();
        match mode {
            Some("Archived") => Self::Archived,
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
            Self::Archived => "Archived",
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

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
pub(crate) struct EventRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub id: Option<i64>,
    pub name: String,
    #[serde(with = "crate::rpctype::chrono_datetime")]
    pub date: DateTime<chrono::FixedOffset>,
    #[serde(default = "default_stage_count")]
    pub stage_count: i64,
    pub owner: String,
    pub api_token: String,
    pub config: QxEventConfig,
    pub db_mode: EventDbMode,
}

fn default_stage_count() -> i64 { 1 }

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
pub(crate) struct EventListItem {
    pub id: EventId,
    pub name: String,
    pub stage_count: i64,
    #[serde(with = "crate::rpctype::chrono_datetime")]
    pub date: DateTime<chrono::FixedOffset>,
    pub owner: String,
    pub db_mode: EventDbMode,
}

impl EventListItem {
    fn from_record(record: &Record) -> anyhow::Result<Self> {
        let get_field = |name| record.get(name).ok_or_else(||anyhow!("Cannot get field '{}'.", name));
        Ok(Self {
            id: get_field("id")?.to_int().ok_or_else(|| anyhow!("Event id is not an integer"))?,
            name: get_field("name")?.as_str().unwrap_or_default().to_string(),
            stage_count: get_field("stage_count")?.to_int().unwrap_or(default_stage_count()),
            date: get_field("date")?.to_datetime().unwrap_or_else(|| chrono::Local::now().fixed_offset()),
            owner: get_field("owner")?.as_str().unwrap_or_default().to_string(),
            db_mode: EventDbMode::from_record_value(get_field("db_mode")?),
        })
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize, Default, RpcTypeStr)]
pub(crate) struct EventRecordChange {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::rpctype::chrono_datetime_opt")]
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

fn inflate_file(zlib_file: &str, out_file: &str) -> std::io::Result<()> {
    let mut decoder = ZlibDecoder::new(std::io::BufReader::new(std::fs::File::open(zlib_file)?));
    let mut out = std::fs::File::create(out_file)?;
    std::io::copy(&mut decoder, &mut out)?;
    Ok(())
}

fn deflate_file(in_file: &str, zlib_file: &str) -> std::io::Result<()> {
    let mut encoder = ZlibEncoder::new(std::fs::File::create(zlib_file)?, Compression::default());
    std::io::copy(&mut std::fs::File::open(in_file)?, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

fn event_db_file(event_id: EventId) -> String {
    format!("{}/{event_id}/event.qbe", global_config().data_dir)
}

/// File backing the pushDb file node, it is located next to the event database,
/// so it can be installed atomically.
pub fn pushed_event_db_file(event_id: EventId) -> String {
    format!("{}.pushdb", event_db_file(event_id))
}

/// Zlib compressed snapshot served by the pullDb file node
pub fn pulled_event_db_file(event_id: EventId) -> String {
    format!("{}.pulldb", event_db_file(event_id))
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
