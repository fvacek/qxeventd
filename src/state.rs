use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};
use std::sync::atomic::{AtomicI64, Ordering};

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
use qxsql::{DbValue, Record, ToRecord};
use qxsql::{sql::{QxSqlApi, record_from_slice}};
use serde::{Deserialize, Serialize};
use shvclient::ClientCommandSender;

use shvproto::RpcValue;
use shvproto::make_map;
use shvrpc::rpc::ShvRI;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use smol::{lock::RwLock, channel};
use shvrpc::util::join_path;

use crate::eventctlnode::METH_EVENT_DB_MODE;
use crate::localdbsqlapi::LocalDbSqlApi;
use crate::eventdb::{migrate_event_db, migrate_event_db_file};
use crate::eventsqlapi::LocalEventSqlApi;
use crate::generate_api_token;
use crate::global_config;
use crate::qxeventconfig::QxEventConfig;
use crate::rpctype::{RecordChange, RpcTypeStr};
use crate::rpcvalueext::RpcValueExt;


pub type EventId = i64;

pub type SharedAppState = Arc<RwLock<State>>;

pub(crate) struct State {
    pub db_pool: async_sqlite::Pool,
    pub open_events: BTreeMap<EventId, Arc<OpenEventCtl>>,
    /// Cache of event records, it has its own lock, so it can be used without
    /// the app state write lock, see [`State::event_record`].
    pub event_records: Arc<smol::lock::Mutex<HashMap<EventId, Arc<RwLock<EventRecord>>>>>,
    pub shutdown_sender: Option<channel::Sender<()>>,
}

impl State {
    pub async fn quit_app(&mut self, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let event_ids: Vec<_> = self.open_events.keys().copied().collect();
        for event_id in event_ids {
            if let Err(e) = self.close_event_locked(event_id, client_command_sender.clone()).await {
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

    /// The app state lock is not held during the broker call.
    pub async fn create_event(app_state: &SharedAppState, owner: String, rpc_client: ClientCommandSender) -> anyhow::Result<(EventId, String)> {
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
        let rec = event_data.to_record();
        let db_pool = app_state.read().await.db_pool.clone();
        let qxsql = LocalDbSqlApi::new(db_pool, rpc_client.clone());
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

    /// The app state lock is not held during the broker call.
    pub async fn update_event_record(app_state: &SharedAppState, event_id: EventId, record: EventRecordChange, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        let db_pool = app_state.read().await.db_pool.clone();
        // always update the mount point, for case than shvbroker config is reloaded from file
        let event_record = load_event_record(&db_pool, event_id).await?;
        let new_db_mode = record.db_mode;
        if matches!(new_db_mode.unwrap_or(event_record.db_mode), EventDbMode::Replica) {
            let api_token = record.api_token.as_deref().unwrap_or(&event_record.api_token);
            Self::register_event_mount_point(event_id, api_token, rpc_client.clone()).await?;
        }
        let qxsql = LocalDbSqlApi::new(db_pool, rpc_client.clone());
        let ret = qxsql.update_record_with_recchng("events", event_id, &record.to_record(), None).await;
        Self::forget_event_record(app_state, event_id).await;
        if let (Ok(true), Some(new_db_mode)) = (&ret, new_db_mode) && new_db_mode != event_record.db_mode {
            emit_event_db_mode_chng(&rpc_client, event_id, new_db_mode);
        }
        if ret.is_ok() && matches!(new_db_mode, Some(EventDbMode::Replica)) {
            setup_remote_db_recchng_monitoring(app_state.clone(), event_id, rpc_client).await;
        }
        ret
    }

    /// The app state lock is not held during the broker call.
    pub async fn set_event_db_mode(app_state: &SharedAppState, event_id: EventId, db_mode: EventDbMode, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        let db_pool = app_state.read().await.db_pool.clone();
        let event_record = load_event_record(&db_pool, event_id).await?;
        if matches!(db_mode, EventDbMode::Replica) {
            Self::register_event_mount_point(event_id, &event_record.api_token, rpc_client.clone()).await?;
        }
        let mut record = Record::new();
        record.insert("db_mode".to_string(), db_mode.as_record_value().into());
        let qxsql = LocalDbSqlApi::new(db_pool, rpc_client.clone());
        let result = qxsql.update_record_with_recchng("events", event_id, &record, None).await;
        Self::forget_event_record(app_state, event_id).await;
        if matches!(result, Ok(true)) && db_mode != event_record.db_mode {
            emit_event_db_mode_chng(&rpc_client, event_id, db_mode);
        }
        if result.is_ok() && matches!(db_mode, EventDbMode::Replica) {
            setup_remote_db_recchng_monitoring(app_state.clone(), event_id, rpc_client).await;
        }
        result
    }

    pub async fn opened_event_status(&self, event_id: EventId) -> anyhow::Result<EventStatus> {
        let event = self.open_events.get(&event_id)
            .ok_or_else(|| anyhow!("Invalid event id: {event_id}"))?
            .clone();
        Ok(EventStatus {
            master_db_mode: EventDbMode::ReadOnly,
            open_at: event.open_at.with_timezone(&Local).fixed_offset(),
            expires_at: event.expires_at().with_timezone(&Local).fixed_offset(),
        })
    }

    /// Cached event record, only the cache lock is held during the DB read on cache miss.
    pub async fn event_record(app_state: &SharedAppState, event_id: EventId) -> anyhow::Result<Arc<RwLock<EventRecord>>> {
        let (event_records, db_pool) = {
            let state = app_state.read().await;
            (state.event_records.clone(), state.db_pool.clone())
        };
        // Held across the DB read, so forget_event_record() called after a record update
        // cannot interleave with a load of the old record and leave it cached.
        let mut event_records = event_records.lock().await;
        if let Some(record) = event_records.get(&event_id) {
            return Ok(record.clone());
        }
        let record = Arc::new(RwLock::new(load_event_record(&db_pool, event_id).await?));
        event_records.insert(event_id, record.clone());
        Ok(record)
    }

    pub async fn event_db_mode(app_state: &SharedAppState, event_id: EventId) -> anyhow::Result<EventDbMode> {
        let record = Self::event_record(app_state, event_id).await?;
        Ok(record.read().await.db_mode)
    }

    async fn forget_event_record(app_state: &SharedAppState, event_id: EventId) {
        let event_records = app_state.read().await.event_records.clone();
        event_records.lock().await.remove(&event_id);
    }

    pub async fn load_event_record(&self, event_id: EventId) -> anyhow::Result<EventRecord> {
        load_event_record(&self.db_pool, event_id).await
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
                migrate_event_db_file(&tmp_file)
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
            state.close_event_locked(event_id, client_command_sender.clone()).await?;
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
            match state.open_events.get(&event_id).cloned() {
                Some(event) => match event.use_db().await {
                    Ok(db) => backup_open_event_db(&db, event_id, &tmp_file).await,
                    Err(err) => Err(err),
                },
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

    /// Close the event while the app state write lock is held by the caller,
    /// use it when the event must not be reopened before the lock is released.
    pub async fn close_event_locked(&mut self, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<bool> {
        match self.open_events.remove(&event_id) {
            Some(event) => {
                close_removed_event(event_id, &event, &client_command_sender).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The app state write lock is held only to remove the event from open events,
    /// the DB is checkpointed and closed without it.
    pub async fn close_event(app_state: &SharedAppState, event_id: EventId, client_command_sender: ClientCommandSender) -> anyhow::Result<bool> {
        let event = app_state.write().await.open_events.remove(&event_id);
        match event {
            Some(event) => {
                close_removed_event(event_id, &event, &client_command_sender).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub async fn delete_event(app_state: &SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) -> anyhow::Result<bool> {
        log::info!("Deleting event {}", event_id);
        Self::close_event(app_state, event_id, rpc_client.clone()).await?;
        let db_pool = app_state.read().await.db_pool.clone();
        let qxsql = LocalDbSqlApi::new(db_pool, rpc_client);
        let was_deleted = qxsql.delete_record_with_recchng("events", event_id, None).await?;
        Self::forget_event_record(app_state, event_id).await;
        Ok(was_deleted)
    }

    /// The app state write lock is held only to remove expired events from open events,
    /// their DBs are checkpointed and closed without it.
    pub async fn gc_expired_events(app_state: &SharedAppState, client_command_sender: ClientCommandSender) -> anyhow::Result<()> {
        let expired_events = {
            let mut state = app_state.write().await;
            let now = chrono::Utc::now();
            let expired_ids = state.open_events.iter()
                .filter(|(_, event)| event.expires_at() < now)
                .map(|(event_id, _)| *event_id)
                .collect::<Vec<_>>();
            expired_ids.into_iter()
                .filter_map(|event_id| state.open_events.remove(&event_id).map(|event| (event_id, event)))
                .collect::<Vec<_>>()
        };
        let mut result = Ok(());
        for (event_id, event) in expired_events {
            info!("Closing event: {event_id} as expired.");
            if let Err(err) = close_removed_event(event_id, &event, &client_command_sender).await {
                error!("Failed to close expired event {event_id}: {err}");
                result = Err(err);
            }
        }
        result
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

async fn load_event_record(db_pool: &async_sqlite::Pool, event_id: EventId) -> anyhow::Result<EventRecord> {
    let qxsql = LocalDbSqlApi::new_without_recchng(db_pool.clone());
    let record = qxsql.read_record("events", event_id, None).await?
        .ok_or_else(||anyhow!("Event id: {event_id} not found"))?;
    EventRecord::from_record(&record)
}

/// Close DB of an event already removed from open events and notify clients.
async fn close_removed_event(event_id: EventId, event: &OpenEventCtl, client_command_sender: &ClientCommandSender) -> anyhow::Result<()> {
    let close_result = event.close().await;

    let message = RpcMessage::new_signal("event", "lsmod").with_param(false);
    client_command_sender.send_message(message)
        .map_err(|e| anyhow!("Failed to send message {}", e))?;
    close_result.map_err(|e| e.context(format!("Failed to close event {event_id} DB")))
}

pub(crate) async fn open_event(app_state: SharedAppState, event_id: EventId, rpc_client: ClientCommandSender) -> anyhow::Result<String> {
    let event_shv_path = event_api_shv_path(event_id);
    // The lock is held until the event is registered, so the DB file cannot be replaced by
    // commit_pushed_event_db after the pool is opened, nor can the event be opened twice.
    let mut state = app_state.write().await;
    if let Some(event) = state.open_events.get(&event_id).cloned() {
        event.touch();
        return Ok(event_shv_path);
    }

    let event_record = state.load_event_record(event_id).await?;
    let db_file = format!("{}/{event_id}/event.qbe", global_config().data_dir);
    let pool = migrate_event_db(&db_file, &event_record, rpc_client.clone()).await?;
    let event = Arc::new(OpenEventCtl::new(pool));
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

async fn remote_db_recchng_monitoring_task(app_state: SharedAppState, event_id: EventId, event: Arc<OpenEventCtl>, rpc_client: ClientCommandSender) {
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
                    if let Some(path) = message.shv_path() && path == remote_sql_path {
                        info!(target: "ApplyRecChng", "Received event {event_id} subscription message: {:?}", message.shv_path());
                        let db_mode = State::event_db_mode(&app_state, event_id).await;
                        let db_mode = match db_mode {
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
    info!(target: "ApplyRecChng", "Applying recchng {recchng:?}");

    match &recchng.op {
        RecOp::Insert => match recchng.record.as_ref() {
            Some(record) => {
                if recchng.id <= 0 {
                    return Err(anyhow!("Insert recchng to table '{table}' has no id, replica mode requires id even for inserts", table = recchng.table));
                }
                let mut record = remove_default_and_generated_columns(&recchng.table, record);
                record.insert("id".to_owned(), recchng.id.into());
                info!(target: "ApplyRecChng", "Insert recchng: table='{}', id={}", recchng.table, recchng.id);
                sql_api.create_record_with_recchng(&recchng.table, &record, recchng.issuer.clone()).await?;
                Ok(())
            }
            None => Err(anyhow!("Insert recchng to table '{table}' has no record", table = recchng.table)),
        },
        RecOp::Update => match recchng.record.as_ref() {
            Some(record) => {
                info!(target: "ApplyRecChng", "Update recchng: table='{}', id={}", recchng.table, recchng.id);
                let mut record = record.clone();
                remove_generated_columns(&recchng.table, &mut record);
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
            info!(target: "ApplyRecChng", "Delete recchng: table='{}', id={}", recchng.table, recchng.id);
            sql_api
                .delete_record_with_recchng(&recchng.table, recchng.id, recchng.issuer.clone())
                .await?;
            Ok(())
        }
    }
}

/// Remote recchngs carry whole rows, including generated (computed) columns,
/// which SQLite refuses to INSERT/UPDATE explicitly.
fn remove_generated_columns(table: &str, record: &mut Record) {
    let generated_columns = crate::eventdb::generated_columns(table);
    if !generated_columns.is_empty() {
        record.retain(|key, _| !generated_columns.iter().any(|name| name.eq_ignore_ascii_case(key)));
    }
}

/// Removes generated columns, see [`remove_generated_columns`], and also columns having their
/// default value, these can be omitted on INSERT, the DB fills them in itself.
/// Not usable for UPDATE, where omitting a column would keep its old value.
fn remove_default_and_generated_columns(table: &str, record: &Record) -> Record {
    let generated_columns = crate::eventdb::generated_columns(table);
    record
        .iter()
        .filter(|(key, _)| !generated_columns.iter().any(|name| name.eq_ignore_ascii_case(key)))
        .filter(|(key, value)| {
            crate::eventdb::column_default(table, key)
                .is_none_or(|default| !crate::eventdb::is_default_value(value, default))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn rechng_from_rpcvalue(value: Option<&RpcValue>) -> anyhow::Result<RecChng> {
    let value = value.ok_or_else(|| anyhow!("recchng signal has no parameter"))?;
    shvproto::from_rpcvalue(value)
        .map_err(|err| anyhow!("invalid recchng value: {err}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, RpcTypeStr)]
pub(crate) enum EventDbMode {
    Archived,
    ReadOnly,
    Master,
    Replica,
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

impl From<EventDbMode> for DbValue {
    fn from(value: EventDbMode) -> Self {
        value.as_record_value().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventStatus {
    pub master_db_mode: EventDbMode,
    pub open_at: DateTime<chrono::FixedOffset>,
    pub expires_at: DateTime<chrono::FixedOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr, ToRecord, RecordChange)]
#[record_change(rpc_type_str)]
pub(crate) struct EventRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[to_record(skip)]
    #[record_change(skip)]
    pub id: Option<i64>,
    pub name: String,
    #[serde(with = "crate::rpctype::chrono_datetime")]
    #[record_change(with = "crate::rpctype::chrono_datetime_opt")]
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
}

pub(crate) struct OpenEventCtl {
    /// Use it via [`OpenEventCtl::use_db`] only, so the pool cannot be closed under a running statement.
    db: async_sqlite::Pool,
    pub open_at: DateTime<chrono::Utc>,
    /// Time of last DB use in Unix msec, the event expires after a period of inactivity.
    touched_at_ms: AtomicI64,
    /// Held shared for each DB use and exclusive while closing; the value is `true` once closed.
    /// Do not take the app state lock while holding it: `install_event_db()` closes the event
    /// under the app state write lock, so it would deadlock.
    usage: RwLock<bool>,
}

/// Shared usage of an open event DB, it keeps the DB pool open, see [`OpenEventCtl::use_db`].
pub(crate) struct EventDbUsage<'a> {
    _usage: smol::lock::RwLockReadGuard<'a, bool>,
    db: &'a async_sqlite::Pool,
}

impl std::ops::Deref for EventDbUsage<'_> {
    type Target = async_sqlite::Pool;
    fn deref(&self) -> &Self::Target {
        self.db
    }
}

impl OpenEventCtl {
    fn new(db: async_sqlite::Pool) -> Self {
        let now = chrono::Utc::now();
        Self {
            db,
            open_at: now,
            touched_at_ms: AtomicI64::new(now.timestamp_millis()),
            usage: RwLock::new(false),
        }
    }
    pub fn touch(&self) {
        self.touched_at_ms.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
    }
    pub fn touched_at(&self) -> DateTime<chrono::Utc> {
        DateTime::from_timestamp_millis(self.touched_at_ms.load(Ordering::Relaxed)).unwrap_or(self.open_at)
    }
    pub fn expires_at(&self) -> DateTime<chrono::Utc> {
        self.touched_at() + global_config().event_expire_duration
    }
    /// Marks the event as used and returns the DB pool, which is kept open until the usage
    /// is dropped. Fails if the event has been closed meanwhile.
    pub async fn use_db(&self) -> anyhow::Result<EventDbUsage<'_>> {
        let usage = self.usage.read().await;
        if *usage {
            bail!("Event is closed");
        }
        self.touch();
        Ok(EventDbUsage { _usage: usage, db: &self.db })
    }
    /// Waits for running DB usages, then checkpoints WAL and closes the DB pool.
    async fn close(&self) -> anyhow::Result<()> {
        let mut closed = self.usage.write().await;
        if *closed {
            return Ok(());
        }
        *closed = true;
        close_event_db(&self.db).await
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

pub(crate) const SIG_EVENT_DB_MODE_CHNG: &str = "eventDbModeChng";

fn emit_event_db_mode_chng(rpc_client: &ClientCommandSender, event_id: EventId, db_mode: EventDbMode) {
    let message = RpcMessage::new_signal_with_source(format!("eventctl/{event_id}"), SIG_EVENT_DB_MODE_CHNG, METH_EVENT_DB_MODE)
        .with_param(db_mode.to_rpcvalue());
    if let Err(err) = rpc_client.send_message(message) {
        error!("Failed to send {SIG_EVENT_DB_MODE_CHNG} signal for event {event_id}: {err}");
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn memory_event() -> OpenEventCtl {
        let pool = async_sqlite::PoolBuilder::new().path(":memory:").open().await.unwrap();
        OpenEventCtl::new(pool)
    }

    #[test]
    fn close_waits_for_running_db_usage() {
        smol::block_on(async {
            let event = Arc::new(memory_event().await);
            let usage = event.use_db().await.unwrap();
            let closing = smol::spawn({
                let event = event.clone();
                async move { event.close().await }
            });
            // give the close task a chance to run, it must wait for the usage
            for _ in 0..10 {
                smol::future::yield_now().await;
            }
            assert!(!closing.is_finished());
            let n: i64 = usage.conn(|conn| conn.query_row("SELECT 1", [], |row| row.get(0))).await.unwrap();
            assert_eq!(n, 1);
            drop(usage);
            closing.await.unwrap();
            assert!(event.use_db().await.is_err());
            // closing twice is harmless
            event.close().await.unwrap();
        });
    }

    #[test]
    fn db_usage_touches_event() {
        smol::block_on(async {
            let event = memory_event().await;
            event.touched_at_ms.store(0, Ordering::Relaxed);
            assert_eq!(event.touched_at().timestamp_millis(), 0);
            let before = chrono::Utc::now();
            drop(event.use_db().await.unwrap());
            assert!(event.touched_at() >= before - chrono::Duration::milliseconds(1));
        });
    }
}
