use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

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

/// Generated (computed) column names per table of the event DB schema, all names lower case.
/// Every event DB is migrated to the same schema, so it is read once from an in-memory DB
/// migrated the same way, at application start, see [`init_generated_columns`].
/// The bundled schema validity is checked by tests.
static GENERATED_COLUMNS: LazyLock<HashMap<String, Vec<String>>> = LazyLock::new(|| {
    let generated_columns = load_generated_columns().expect("Bundled event DB schema should be valid");
    info!("Event DB generated columns: {generated_columns:?}");
    generated_columns
});

/// Load [`GENERATED_COLUMNS`] eagerly, so schema problems show up at application start.
pub fn init_generated_columns() {
    LazyLock::force(&GENERATED_COLUMNS);
}

/// Generated columns of event DB `table`, they cannot be written explicitly.
pub fn generated_columns(table: &str) -> &'static [String] {
    GENERATED_COLUMNS.get(&table.to_ascii_lowercase()).map(Vec::as_slice).unwrap_or_default()
}

fn load_generated_columns() -> anyhow::Result<HashMap<String, Vec<String>>> {
    let mut conn = Connection::open_in_memory()?;
    migrate_connection(&mut conn)?;
    // pragma_table_xinfo.hidden: 2 = VIRTUAL generated column, 3 = STORED generated column
    let mut stmt = conn.prepare(
        "SELECT m.name, x.name FROM sqlite_master m, pragma_table_xinfo(m.name) x
         WHERE m.type = 'table' AND x.hidden IN (2, 3)")?;
    let mut generated_columns: HashMap<String, Vec<String>> = HashMap::new();
    let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    for row in rows {
        let (table, column) = row?;
        generated_columns.entry(table.to_ascii_lowercase()).or_default().push(column.to_ascii_lowercase());
    }
    Ok(generated_columns)
}

const MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

const MIGRATION_ARRAY: &[M] = &[
    M::up(
        include_str!("create_event_db.sql"),
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_columns_of_bundled_schema() {
        assert_eq!(generated_columns("runs"), ["disqualified"]);
        assert_eq!(generated_columns("RUNS"), ["disqualified"]);
        assert!(generated_columns("competitors").is_empty());
        assert!(generated_columns("no_such_table").is_empty());
    }
}
