use std::path::Path;

use async_sqlite::rusqlite::Connection;
use async_sqlite::{JournalMode, Pool, PoolBuilder};
use log::info;
use qxsql::sql::{QxSqlApi, record_from_slice};
use rusqlite_migration::{M, Migrations};
use shvclient::ClientCommandSender;

use crate::{localdbsqlapi::LocalDbSqlApi, state::EventRecord};

fn check_file_exists(path: &str) -> bool {
    std::fs::metadata(path).is_ok()
}
fn create_file_path(db_file: &str) -> anyhow::Result<()> {
    let dir = Path::new(db_file).parent().unwrap();
    std::fs::create_dir_all(dir)?;
    Ok(())
}

pub async fn migrate_db(db_file: &str, event_data: &EventRecord, client_command_sender: ClientCommandSender) -> anyhow::Result<Pool> {
    let db_file_exists = check_file_exists(db_file);
    if !db_file_exists {
        info!("Creating event database file {}", db_file);
        create_file_path(db_file)?;
    }
    info!("Opening db {db_file}");

    let pool = PoolBuilder::new()
                    .path(db_file)
                    .journal_mode(JournalMode::Wal);
    let pool = pool.open()
                    .await?;

    // Update the database schema, atomically
    pool.conn_mut(|conn| Ok(migrate_connection(conn))).await??;
    let qxsql = LocalDbSqlApi::new(pool.clone(), client_command_sender);
    if !db_file_exists {
        let config_entries = [
            ("event.name", event_data.name.clone()),
            ("event.stage_count", event_data.stage_count.to_string()),
            ("event.date", event_data.date.format("%Y-%m-%d").to_string()),
            ("event.time", event_data.date.format("%H:%M:%S").to_string()),
        ];

        for (key, value) in config_entries {
            qxsql.exec("INSERT INTO config (ckey, cvalue) VALUES (:ckey, :cvalue)", Some(&record_from_slice(&[
                ("ckey", key.into()),
                ("cvalue", value.into()),
            ]))).await?;
        }
    }
    info!("Migration of: {db_file} OK");

    Ok(pool)
}

/// Checks that the file can be opened as an event database and migrates it, so it can be
/// installed without a risk of the event failing to open afterwards. Blocking.
pub fn migrate_db_file(db_file: &str) -> anyhow::Result<()> {
    migrate_connection(&mut Connection::open(db_file)?)
}

fn migrate_connection(conn: &mut Connection) -> anyhow::Result<()> {
    // A database exported by QuickEvent has the tables, but it does not have the version set.
    // Such a database is considered to be migrated already.
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let table_count: i64 = conn.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |row| row.get(0))?;
    if user_version == 0 && table_count > 0 {
        conn.pragma_update(None, "user_version", MIGRATION_ARRAY.len() as i64)?;
    }
    MIGRATIONS.to_latest(conn)?;
    Ok(())
}

const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

const MIGRATION_ARRAY: &[M] = &[
    M::up(
        include_str!("create_event_db.sql"),
    ),
];
