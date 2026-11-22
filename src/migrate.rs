use async_sqlite::{JournalMode, Pool, PoolBuilder};
use log::info;
use rusqlite_migration::{Migrations, M};
use anyhow::Result;

use crate::GLOBAL_CONFIG;

// Define migrations. These are applied atomically.
const MIGRATION_ARRAY: &[M] = &[
    M::up(
        include_str!("create_app_db.sql"),
    ),
    M::up(
        "ALTER TABLE events ADD COLUMN stage INTEGER NOT NULL DEFAULT 1",
    ),
    M::up(
        "ALTER TABLE events ADD COLUMN config TEXT",
    ),
    M::up(
        "ALTER TABLE events RENAME COLUMN stage TO stage_count",
    ),
    M::up(
        "ALTER TABLE events RENAME COLUMN is_local TO master_db_mode",
    ),
    M::up(
        "ALTER TABLE events ADD COLUMN db_mode TEXT NOT NULL DEFAULT 'ReadOnly';
         ALTER TABLE events DROP COLUMN master_db_mode;",
    ),
    // Split the RFC 3339 `date` to the local `date` (`YYYY-MM-DD`) and `time` (`HH:MM:SS`),
    // the local date and time of the stored offset are kept.
    M::up(
        "ALTER TABLE events ADD COLUMN time TEXT;
         UPDATE events SET date = substr(date, 1, 10), time = substr(date, 12, 8) WHERE length(date) >= 19;",
    ),
];
const APP_DB_MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

pub async fn create_app_db_connection() -> Result<Pool> {
    let config = GLOBAL_CONFIG.get().expect("Global config should be initialized");
    open_app_db(&config.data_dir).await
}

/// Open app DB in `data_dir`, in-memory DB is used if `data_dir` is empty
pub async fn open_app_db(data_dir: &str) -> Result<Pool> {
    let (db_file, journal_mode) = if data_dir.is_empty() {
        (":memory:".to_string(), JournalMode::Memory)
    } else {
        std::fs::create_dir_all(data_dir)?;
        const DB_FILE: &str = "qxevent.sqlite";
        (format!("{data_dir}/{DB_FILE}"), JournalMode::Wal)
    };
    info!("Opening db {db_file} in journal mode: {journal_mode:?}");
    let pool = PoolBuilder::new()
                    .path(db_file)
                    .journal_mode(journal_mode);
    let pool = match journal_mode {
        JournalMode::Memory => pool.num_conns(1),
        _ => pool,
    };
    let pool = pool.open()
                    .await?;

    // Update the database schema, atomically
    pool.conn_mut(|conn| {
        match APP_DB_MIGRATIONS.to_latest(conn) {
            Ok(_) => Ok(()),
            Err(e) => panic!("{}", e),
        }
    }).await?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_sqlite::rusqlite::Connection;

    #[test]
    fn event_date_is_split_to_date_and_time() {
        let mut conn = Connection::open_in_memory().unwrap();
        APP_DB_MIGRATIONS.to_version(&mut conn, MIGRATION_ARRAY.len() - 1).unwrap();
        conn.execute_batch("
            INSERT INTO events (api_token, name, date, owner) VALUES
                ('t1', 'a', '2026-10-31T10:05:07.123+01:00', 'o'),
                ('t2', 'b', NULL, 'o');
        ").unwrap();
        APP_DB_MIGRATIONS.to_latest(&mut conn).unwrap();
        let rows: Vec<(Option<String>, Option<String>)> = conn
            .prepare("SELECT date, time FROM events ORDER BY id").unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap()
            .collect::<Result<_, _>>().unwrap();
        assert_eq!(rows, [
            (Some("2026-10-31".to_string()), Some("10:05:07".to_string())),
            (None, None),
        ]);
    }
}
