use log::info;
use anyhow::{Context, Result};

use crate::GLOBAL_CONFIG;
use crate::db::{Connection, Db, TransactionBehavior, query_value};

// App DB migrations, applied atomically. The number of applied migrations is stored
// in `PRAGMA user_version`, like rusqlite_migration did, so existing app DBs are compatible.
const MIGRATION_ARRAY: &[&str] = &[
    include_str!("create_app_db.sql"),
    "ALTER TABLE events ADD COLUMN stage INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE events ADD COLUMN config TEXT",
    "ALTER TABLE events RENAME COLUMN stage TO stage_count",
    "ALTER TABLE events RENAME COLUMN is_local TO master_db_mode",
    "ALTER TABLE events ADD COLUMN db_mode TEXT NOT NULL DEFAULT 'ReadOnly';
         ALTER TABLE events DROP COLUMN master_db_mode;",
    // Split the RFC 3339 `date` to the local `date` (`YYYY-MM-DD`) and `time` (`HH:MM:SS`),
    // the local date and time of the stored offset are kept.
    "ALTER TABLE events ADD COLUMN time TEXT;
         UPDATE events SET date = substr(date, 1, 10), time = substr(date, 12, 8) WHERE length(date) >= 19;",
];

pub async fn create_app_db_connection() -> Result<Db> {
    let config = GLOBAL_CONFIG.get().expect("Global config should be initialized");
    open_app_db(&config.data_dir).await
}

/// Open app DB in `data_dir`, in-memory DB is used if `data_dir` is empty
pub async fn open_app_db(data_dir: &str) -> Result<Db> {
    let db_file = if data_dir.is_empty() {
        ":memory:".to_string()
    } else {
        std::fs::create_dir_all(data_dir)?;
        const DB_FILE: &str = "qxevent.sqlite";
        format!("{data_dir}/{DB_FILE}")
    };
    info!("Opening db {db_file}");
    let db = Db::open(&db_file).await?;
    migrate_app_db(&mut *db.conn().await?, MIGRATION_ARRAY.len()).await
        .with_context(|| format!("App DB {db_file} migration failed"))?;
    Ok(db)
}

/// Migrate app DB to `version` (number of applied migrations), atomically.
async fn migrate_app_db(conn: &mut Connection, version: usize) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    let current: i64 = query_value(&tx, "PRAGMA user_version", ()).await?;
    let current = usize::try_from(current).context("Invalid app DB user_version")?;
    if current > MIGRATION_ARRAY.len() {
        anyhow::bail!("App DB version {current} is newer than the latest supported version {}", MIGRATION_ARRAY.len());
    }
    if current >= version {
        tx.commit().await?;
        return Ok(());
    }
    for (i, sql) in MIGRATION_ARRAY.iter().enumerate().take(version).skip(current) {
        info!("Migrating app DB to version {}", i + 1);
        tx.execute_batch(sql).await?;
    }
    tx.execute_batch(format!("PRAGMA user_version = {version}")).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query_all;

    #[test]
    fn event_date_is_split_to_date_and_time() {
        smol::block_on(async {
            let db = Db::open(":memory:").await.unwrap();
            let mut conn = db.conn().await.unwrap();
            migrate_app_db(&mut conn, MIGRATION_ARRAY.len() - 1).await.unwrap();
            conn.execute_batch("
                INSERT INTO events (api_token, name, date, owner) VALUES
                    ('t1', 'a', '2026-10-31T10:05:07.123+01:00', 'o'),
                    ('t2', 'b', NULL, 'o');
            ").await.unwrap();
            migrate_app_db(&mut conn, MIGRATION_ARRAY.len()).await.unwrap();
            let rows: Vec<(Option<String>, Option<String>)> = query_all(&conn, "SELECT date, time FROM events ORDER BY id", ()).await.unwrap()
                .into_iter()
                .map(|row| (row.get(0).unwrap(), row.get(1).unwrap()))
                .collect();
            assert_eq!(rows, [
                (Some("2026-10-31".to_string()), Some("10:05:07".to_string())),
                (None, None),
            ]);
            assert_eq!(query_value::<i64>(&conn, "PRAGMA user_version", ()).await.unwrap(), MIGRATION_ARRAY.len() as i64);
        });
    }

    #[test]
    fn app_db_migration_is_idempotent() {
        smol::block_on(async {
            let db = Db::open(":memory:").await.unwrap();
            let mut conn = db.conn().await.unwrap();
            migrate_app_db(&mut conn, MIGRATION_ARRAY.len()).await.unwrap();
            migrate_app_db(&mut conn, MIGRATION_ARRAY.len()).await.unwrap();
            assert_eq!(query_value::<i64>(&conn, "PRAGMA user_version", ()).await.unwrap(), MIGRATION_ARRAY.len() as i64);
        });
    }
}
