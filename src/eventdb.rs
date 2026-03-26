use std::path::Path;

use async_sqlite::{JournalMode, Pool, PoolBuilder};
use log::info;
use qxsql::sql::{QxSqlApi, record_from_slice};
use rusqlite_migration::{M, Migrations};

use crate::{appsqlapi::AppSqlApi, state::EventRecord};

fn check_file_exists(path: &str) -> bool {
    std::fs::metadata(path).is_ok()
}
fn create_file_path(db_file: &str) -> anyhow::Result<()> {
    let dir = Path::new(db_file).parent().unwrap();
    std::fs::create_dir_all(dir)?;
    Ok(())
}

pub async fn migrate_db(db_file: &str, event_data: &EventRecord) -> anyhow::Result<Pool> {
    let db_file_exists = check_file_exists(db_file);
    if !db_file_exists {
        info!("Creating event database file {}", db_file);
        create_file_path(db_file)?;
    }
    info!("Opening db {db_file} in journal mode: Wal");

    let pool = PoolBuilder::new()
                    .path(db_file)
                    .journal_mode(JournalMode::Wal);
    let pool = pool.open()
                    .await?;

    // Update the database schema, atomically
    pool.conn_mut(|conn| {
        match MIGRATIONS.to_latest(conn) {
            Ok(_) => Ok(()),
            Err(e) => panic!("{}", e),
        }
    }).await?;
    let qxsql = AppSqlApi::new(pool.clone());
    if !db_file_exists {
        let config_entries = [
            ("event.name", event_data.name.clone()),
            ("event.date", event_data.date.format("%Y-%m-%d").to_string()),
            ("event.time", event_data.date.format("%H:%M:%S").to_string()),
        ];

        for (key, value) in config_entries {
            qxsql.exec("INSERT INTO config (ckey, cvalue) VALUES (:ckey, :cvalue)", Some(&record_from_slice(&[
                ("ckey", key.into()),
                ("cvalue", value.into()),
            ]))).await?;
        }
        qxsql.create_record("stages", &record_from_slice(&[
            ("startdatetime", event_data.date.into()),
        ])).await?;
    }
    info!("Migration of: {db_file} OK");

    Ok(pool)
}

const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

const MIGRATION_ARRAY: &[M] = &[
    M::up(
        include_str!("create_event_db.sql"),
    ),
];
