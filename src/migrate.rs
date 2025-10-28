use async_sqlite::{JournalMode, Pool, PoolBuilder};
use rusqlite_migration::{Migrations, M};
use anyhow::Result;

use crate::GLOBAL_CONFIG;

// Define migrations. These are applied atomically.
const MIGRATION_ARRAY: &[M] = &[
    M::up(
        r#"
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            date TEXT,
            owner TEXT,
            api_token TEXT
        );
        "#,
    ),
    // This migration can be reverted
    // M::up("CREATE TABLE animal(name TEXT);").down("DROP TABLE animal;"),
    // In the future, if the need to change the schema arises, put
    // migrations here, like so:
    // M::up("CREATE INDEX UX_friend_email ON friend(email);"),
    // M::up("CREATE INDEX UX_friend_name ON friend(name);"),
];
const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

pub async fn create_db_connection() -> Result<Pool> {
    let config = GLOBAL_CONFIG.get().expect("Global config should be initialized");
    let (db_file, journal_mode) = if config.data_dir.is_empty() {
        (":memory:".to_string(), JournalMode::Memory)
    } else {
        std::fs::create_dir_all(&config.data_dir)?;
        const DB_FILE: &str = "qxevent.sqlite";
        (format!("{}/{DB_FILE}", config.data_dir), JournalMode::Wal)
    };

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
        match MIGRATIONS.to_latest(conn) {
            Ok(_) => Ok(()),
            Err(e) => panic!("{}", e),
        }
    }).await?;

    Ok(pool)
}
