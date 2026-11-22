use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use anyhow::Context;
use async_sqlite::rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use async_sqlite::{JournalMode, Pool, PoolBuilder};
use log::*;
use qxsql::DbValue;
use qxsql::sql::{QxSqlApi, record_from_slice};
use shvclient::ClientCommandSender;

use crate::{localdbsqlapi::LocalDbSqlApi, qe::{normalize_date, normalize_time}, state::{EventRecord, EventRecordChange}};

fn check_file_exists(path: &str) -> bool {
    std::fs::metadata(path).is_ok()
}
fn create_file_path(db_file: &str) -> anyhow::Result<()> {
    let dir = Path::new(db_file).parent().unwrap();
    std::fs::create_dir_all(dir)?;
    Ok(())
}

pub async fn migrate_event_db(db_file: &str, event_data: &EventRecord, client_command_sender: ClientCommandSender) -> anyhow::Result<Pool> {
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
    pool.conn_mut(|conn| Ok(migrate_event_db_connection(conn))).await??;
    let qxsql = LocalDbSqlApi::new(pool.clone(), client_command_sender);
    if !db_file_exists {
        let config_entries = [
            ("event.name", event_data.name.clone()),
            ("event.stageCount", event_data.stage_count.to_string()),
            ("event.date", event_data.date.clone()),
            ("event.time", event_data.time.clone()),
        ];

        for (key, value) in config_entries {
            qxsql.exec("INSERT INTO config (id, cvalue) VALUES (:id, :cvalue)", Some(&record_from_slice(&[
                ("id", key.into()),
                ("cvalue", value.into()),
            ]))).await?;
        }
    }
    info!("Migration of: {db_file} OK");

    Ok(pool)
}

/// Checks that the file can be opened as an event database, migrates it, so it can be
/// installed without a risk of the event failing to open afterwards, and reads the event
/// record values from its config. Failure to read them is not an error, empty change is
/// returned then. Blocking.
///
/// The database is switched to the rollback journal mode, so all changes are in the main file
/// once the connection is closed, and no `-wal` or `-shm` file is left next to it, the file
/// can be moved then. The event pool switches it back to WAL mode when it is opened.
/// The connection is closed explicitly, so a failure to close it is reported.
pub fn import_event_db_file(db_file: &str) -> anyhow::Result<EventRecordChange> {
    let mut conn = Connection::open(db_file)?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("delete") {
        anyhow::bail!("Cannot switch {db_file} to rollback journal mode, journal mode is {journal_mode}");
    }
    migrate_event_db_connection(&mut conn)?;
    let change = read_event_record_change(&conn).unwrap_or_else(|err| {
        warn!("Failed to read event record change from {db_file}: {err}");
        EventRecordChange::default()
    });
    conn.close().map_err(|(_, err)| err)?;
    Ok(change)
}

fn read_event_record_change(conn: &Connection) -> anyhow::Result<EventRecordChange> {
    let mut change = EventRecordChange::default();
    let mut stmt = conn.prepare("SELECT id, CAST(cvalue AS TEXT) FROM config WHERE id LIKE 'event.%' AND cvalue IS NOT NULL")?;
    let config = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?.trim().to_string())))?
        .filter(|row| row.as_ref().map_or(true, |(_, value)| !value.is_empty()))
        .collect::<Result<HashMap<_, _>, _>>()?;

    change.name = config.get("event.name").cloned();
    change.stage_count = config.get("event.stageCount").and_then(|v| v.parse::<i64>().ok());
    change.date = config.get("event.date").and_then(|v| normalize_date(v));
    change.time = config.get("event.time").and_then(|v| normalize_time(v));
    Ok(change)
}

/// Migrates the event DB to the latest version, atomically. The version is stored
/// in the `db.version` config value, a database exported by QuickEvent has it set too.
/// An empty database gets the [`CREATE_EVENT_DB_SQL`] schema first.
fn migrate_event_db_connection(conn: &mut Connection) -> anyhow::Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let table_count: i64 = tx.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |row| row.get(0))?;
    if table_count == 0 {
        info!("Creating event DB schema, version {CREATE_EVENT_DB_VERSION}");
        tx.execute_batch(CREATE_EVENT_DB_SQL)?;
    }
    let mut version = read_db_version(&tx)?;
    let latest = latest_db_version();
    if version > latest {
        anyhow::bail!("Event DB version {version} is newer than the latest supported version {latest}");
    }
    let current = version;
    for migration in MIGRATIONS.iter().filter(|m| m.version > current) {
        info!("Migrating event DB from version {version} to {}", migration.version);
        (migration.up)(&tx)
            .with_context(|| format!("Event DB migration to version {} failed", migration.version))?;

        let updated = tx.execute(&format!("UPDATE config SET cvalue = ?1 WHERE {} = 'db.version'", config_key_column(&tx)?), [migration.version.to_string()])?;
        if updated != 1 {
            anyhow::bail!("Event DB migration to version {} lost the 'db.version' config value", migration.version);
        }
        version = migration.version;
    }
    tx.commit()?;
    Ok(())
}

/// Name of the `config` table key column, `ckey` before version 31000, `id` since then.
fn config_key_column(conn: &Connection) -> anyhow::Result<&'static str> {
    let has_ckey: bool = conn.query_row("SELECT count(*) > 0 FROM pragma_table_info('config') WHERE name = 'ckey'", [], |row| row.get(0))?;
    Ok(if has_ckey { "ckey" } else { "id" })
}

fn read_db_version(conn: &Connection) -> anyhow::Result<i64> {
    let version: Option<String> = conn
        .query_row(&format!("SELECT CAST(cvalue AS TEXT) FROM config WHERE {} = 'db.version'", config_key_column(conn)?), [], |row| row.get(0))
        .optional()
        .context("Cannot read event DB config, it is not an event database")?
        .flatten();
    let version = version.context("Event DB has no 'db.version' config value")?;
    version.trim().parse().with_context(|| format!("Invalid event DB 'db.version' config value: {version:?}"))
}

/// Special columns of one event DB table, all names lower case.
#[derive(Debug, Default)]
struct TableColumns {
    /// Generated (computed) columns, they cannot be written explicitly.
    generated: Vec<String>,
    /// Literal default values of columns, columns with non-literal defaults
    /// (like `CURRENT_TIMESTAMP`) are not included.
    defaults: HashMap<String, DbValue>,
}

/// Special columns per table of the event DB schema, table names lower case.
/// Every event DB is migrated to the same schema, so it is read once from an in-memory DB
/// migrated the same way, at application start, see [`init_schema_columns`].
/// The bundled schema validity is checked by tests.
static SCHEMA_COLUMNS: LazyLock<HashMap<String, TableColumns>> = LazyLock::new(|| {
    let schema_columns = load_schema_columns().expect("Bundled event DB schema should be valid");
    info!("Event DB schema columns: {schema_columns:?}");
    schema_columns
});

/// Load [`SCHEMA_COLUMNS`] eagerly, so schema problems show up at application start.
pub fn init_schema_columns() {
    LazyLock::force(&SCHEMA_COLUMNS);
}

fn table_columns(table: &str) -> Option<&'static TableColumns> {
    SCHEMA_COLUMNS.get(&table.to_ascii_lowercase())
}

/// Generated columns of event DB `table`, they cannot be written explicitly.
pub fn generated_columns(table: &str) -> &'static [String] {
    table_columns(table).map(|columns| columns.generated.as_slice()).unwrap_or_default()
}

/// Literal default value of `column` of event DB `table`, if any.
pub fn column_default(table: &str, column: &str) -> Option<&'static DbValue> {
    table_columns(table)?.defaults.get(&column.to_ascii_lowercase())
}

fn load_schema_columns() -> anyhow::Result<HashMap<String, TableColumns>> {
    let mut conn = Connection::open_in_memory()?;
    migrate_event_db_connection(&mut conn)?;
    // pragma_table_xinfo.hidden: 2 = VIRTUAL generated column, 3 = STORED generated column
    let mut stmt = conn.prepare(
        "SELECT m.name, x.name, x.hidden IN (2, 3), x.dflt_value FROM sqlite_master m, pragma_table_xinfo(m.name) x
         WHERE m.type = 'table' AND (x.hidden IN (2, 3) OR x.dflt_value IS NOT NULL)")?;
    let mut schema_columns: HashMap<String, TableColumns> = HashMap::new();
    let rows = stmt.query_map([], |row| Ok((
        row.get::<_, String>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, bool>(2)?,
        row.get::<_, Option<String>>(3)?,
    )))?;
    for row in rows {
        let (table, column, generated, default) = row?;
        let table_columns = schema_columns.entry(table.to_ascii_lowercase()).or_default();
        let column = column.to_ascii_lowercase();
        if let Some(value) = default.as_deref().and_then(parse_default_literal) {
            table_columns.defaults.insert(column.clone(), value);
        }
        if generated {
            table_columns.generated.push(column);
        }
    }
    Ok(schema_columns)
}

/// Parse SQL default value literal as reported by `pragma_table_xinfo.dflt_value`.
fn parse_default_literal(default: &str) -> Option<DbValue> {
    let default = default.trim();
    if default.eq_ignore_ascii_case("NULL") {
        return Some(DbValue::Null);
    }
    if default.eq_ignore_ascii_case("TRUE") {
        return Some(DbValue::Int(1));
    }
    if default.eq_ignore_ascii_case("FALSE") {
        return Some(DbValue::Int(0));
    }
    if let Some(s) = default.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        return Some(DbValue::String(s.replace("''", "'")));
    }
    if let Ok(n) = default.parse::<i64>() {
        return Some(DbValue::Int(n));
    }
    if let Ok(d) = default.parse::<f64>() {
        return Some(DbValue::Double(d));
    }
    // CURRENT_TIMESTAMP, expressions, ...
    None
}

/// Whether `value` equals column default `default`, booleans and numbers are compared by value.
pub fn is_default_value(value: &DbValue, default: &DbValue) -> bool {
    fn as_number(value: &DbValue) -> Option<f64> {
        match value {
            DbValue::Int(n) => Some(*n as f64),
            DbValue::Double(d) => Some(*d),
            DbValue::Bool(b) => Some(if *b { 1. } else { 0. }),
            _ => None,
        }
    }
    match (as_number(value), as_number(default)) {
        (Some(a), Some(b)) => a == b,
        _ => value == default,
    }
}

/// Event DB schema, QuickEvent exports have this version or later.
const CREATE_EVENT_DB_SQL: &str = include_str!("create_event_db.sql");
/// `db.version` set by [`CREATE_EVENT_DB_SQL`].
const CREATE_EVENT_DB_VERSION: i64 = 30900;

/// Event DB migration, `version` is the `db.version` config value once `up` is applied.
/// The `db.version` is updated by the migration runner, not by the `up`.
struct Migration {
    version: i64,
    up: fn(&Connection) -> anyhow::Result<()>,
}

/// Migrations after [`CREATE_EVENT_DB_VERSION`], ordered by version.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 31000,
        up: |conn| {
            add_column_if_missing(conn, "qxchanges", "paid", "boolean NOT NULL DEFAULT 0")?;
            conn.execute_batch("ALTER TABLE config RENAME COLUMN ckey TO id")?;
            Ok(())
        }
    },
];

fn add_column_if_missing(conn: &Connection, table: &str, column: &str, definition: &str) -> anyhow::Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2 COLLATE NOCASE)",
        [table, column], |row| row.get(0))?;
    if !exists {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"))?;
    }
    Ok(())
}

fn latest_db_version() -> i64 {
    MIGRATIONS.last().map_or(CREATE_EVENT_DB_VERSION, |m| m.version)
}

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

    #[test]
    fn column_defaults_of_bundled_schema() {
        assert_eq!(column_default("runs", "isRunning"), Some(&DbValue::Int(1)));
        assert_eq!(column_default("RUNS", "misPunch"), Some(&DbValue::Int(0)));
        assert_eq!(column_default("classdefs", "startSlotIndex"), Some(&DbValue::Int(-1)));
        assert_eq!(column_default("runs", "id"), None);
        assert_eq!(column_default("no_such_table", "id"), None);
    }

    #[test]
    fn event_record_change_from_config() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("
            CREATE TABLE config (id TEXT PRIMARY KEY, cvalue TEXT);
            INSERT INTO config (id, cvalue) VALUES
                ('event.name', 'Foo'), ('event.stageCount', '2'), ('event.date', '2024-05-01');
        ").unwrap();
        let change = read_event_record_change(&conn).unwrap();
        assert_eq!(change.name.as_deref(), Some("Foo"));
        assert_eq!(change.stage_count, Some(2));
        // date and time are independent, a missing time does not drop the date
        assert_eq!(change.date.as_deref(), Some("2024-05-01"));
        assert!(change.time.is_none());
    }

    #[test]
    fn default_value_comparison() {
        assert!(is_default_value(&DbValue::Bool(false), &DbValue::Int(0)));
        assert!(is_default_value(&DbValue::Bool(true), &DbValue::Int(1)));
        assert!(is_default_value(&DbValue::Int(-1), &DbValue::Int(-1)));
        assert!(!is_default_value(&DbValue::Bool(true), &DbValue::Int(0)));
        assert!(!is_default_value(&DbValue::Null, &DbValue::Int(0)));
        assert!(is_default_value(&DbValue::String("a".into()), &DbValue::String("a".into())));
    }

    #[test]
    fn migration_versions_are_increasing() {
        let mut prev = CREATE_EVENT_DB_VERSION;
        for migration in MIGRATIONS {
            assert!(migration.version > prev, "migration {} is not after {prev}", migration.version);
            prev = migration.version;
        }
    }

    #[test]
    fn new_db_is_migrated_to_latest_version() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate_event_db_connection(&mut conn).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), latest_db_version());
        // migrating again is a no-op
        migrate_event_db_connection(&mut conn).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), latest_db_version());
    }

    #[test]
    fn quickevent_db_is_migrated_from_its_version() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(CREATE_EVENT_DB_SQL).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), CREATE_EVENT_DB_VERSION);
        migrate_event_db_connection(&mut conn).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), latest_db_version());
        let paid: i64 = conn.query_row("SELECT count(*) FROM pragma_table_info('qxchanges') WHERE name = 'paid'", [], |row| row.get(0)).unwrap();
        assert_eq!(paid, 1);
        let id: i64 = conn.query_row("SELECT count(*) FROM pragma_table_info('config') WHERE name = 'id'", [], |row| row.get(0)).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn db_with_paid_column_at_30800_is_migrated() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(CREATE_EVENT_DB_SQL).unwrap();
        conn.execute_batch("ALTER TABLE qxchanges ADD COLUMN paid boolean NOT NULL DEFAULT 0").unwrap();
        migrate_event_db_connection(&mut conn).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), latest_db_version());
    }

    #[test]
    fn newer_db_is_rejected() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate_event_db_connection(&mut conn).unwrap();
        conn.execute("UPDATE config SET cvalue = ?1 WHERE id = 'db.version'", [(latest_db_version() + 1).to_string()]).unwrap();
        assert!(migrate_event_db_connection(&mut conn).is_err());
    }

    #[test]
    fn non_event_db_is_rejected() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE foo (id integer)").unwrap();
        assert!(migrate_event_db_connection(&mut conn).is_err());
    }
}
