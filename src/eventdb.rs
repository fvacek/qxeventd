use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use async_sqlite::rusqlite::Connection;
use async_sqlite::{JournalMode, Pool, PoolBuilder};
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveTime, TimeZone};
use log::*;
use qxsql::DbValue;
use qxsql::sql::{QxSqlApi, record_from_slice};
use rusqlite_migration::{M, Migrations};
use shvclient::ClientCommandSender;

use crate::{localdbsqlapi::LocalDbSqlApi, state::{EventRecord, EventRecordChange}};

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

/// Checks that the file can be opened as an event database, migrates it, so it can be
/// installed without a risk of the event failing to open afterwards, and reads the event
/// record values from its config. Failure to read them is not an error, empty change is
/// returned then. Blocking.
///
/// A single read-write connection is used: closing a read-only connection to a WAL mode
/// database would leave `-wal` and `-shm` files behind, a read-write one checkpoints and
/// removes them.
pub fn import_event_db_file(db_file: &str) -> anyhow::Result<EventRecordChange> {
    let mut conn = Connection::open(db_file)?;
    migrate_event_db_connection(&mut conn)?;
    Ok(read_event_record_change(&conn).unwrap_or_else(|err| {
        warn!("Failed to read event record change from {db_file}: {err}");
        EventRecordChange::default()
    }))
}

fn read_event_record_change(conn: &Connection) -> anyhow::Result<EventRecordChange> {
    let mut change = EventRecordChange::default();
    let mut stmt = conn.prepare("SELECT ckey, CAST(cvalue AS TEXT) FROM config WHERE ckey LIKE 'event.%' AND cvalue IS NOT NULL")?;
    let config = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?.trim().to_string())))?
        .filter(|row| row.as_ref().map_or(true, |(_, value)| !value.is_empty()))
        .collect::<Result<HashMap<_, _>, _>>()?;

    change.name = config.get("event.name").cloned();
    change.stage_count = config.get("event.stageCount").and_then(|v| v.parse::<i64>().ok());
    let event_date = config.get("event.date").cloned();
    let event_time = config.get("event.time").cloned();
    change.date = if let Some(event_date) = event_date && let Some(event_time) = event_time {
        parse_local_date_time(&event_date, &event_time)
    } else {
        None
    };
    Ok(change)
}

/// Local date time from `event.date` (`YYYY-MM-DD`) and optional `event.time` (`HH:MM[:SS]`)
/// config values, midnight if time is missing or empty.
fn parse_local_date_time(date: &str, time: &str) -> Option<DateTime<FixedOffset>> {
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    let time = NaiveTime::parse_from_str(time, "%H:%M:%S")
            .or_else(|_| NaiveTime::parse_from_str(time, "%H:%M"))
            .ok()?;
    let local = Local.from_local_datetime(&date.and_time(time)).earliest()?;
    Some(local.fixed_offset())
}

fn migrate_event_db_connection(conn: &mut Connection) -> anyhow::Result<()> {
    // A database exported by QuickEvent has the tables, but it does not have the version set.
    // Such a database is considered to be migrated already.
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let table_count: i64 = conn.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |row| row.get(0))?;
    if user_version == 0 && table_count > 0 {
        conn.pragma_update(None, "user_version", MIGRATION_ARRAY.len() as i64)?;
    }
    EVENT_DB_MIGRATIONS.to_latest(conn)?;
    Ok(())
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

const EVENT_DB_MIGRATIONS: Migrations = Migrations::from_slice(MIGRATION_ARRAY);

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

    #[test]
    fn column_defaults_of_bundled_schema() {
        assert_eq!(column_default("runs", "isRunning"), Some(&DbValue::Int(1)));
        assert_eq!(column_default("RUNS", "misPunch"), Some(&DbValue::Int(0)));
        assert_eq!(column_default("classdefs", "startSlotIndex"), Some(&DbValue::Int(-1)));
        assert_eq!(column_default("runs", "id"), None);
        assert_eq!(column_default("no_such_table", "id"), None);
    }

    #[test]
    fn event_date_parsing() {
        let dt = parse_local_date_time("2024-05-01", "10:30:15").unwrap();
        assert_eq!(dt.format("%Y-%m-%d %H:%M:%S").to_string(), "2024-05-01 10:30:15");
        let dt = parse_local_date_time("2024-05-01", "10:30").unwrap();
        assert_eq!(dt.format("%H:%M:%S").to_string(), "10:30:00");
        let dt = parse_local_date_time("2024-05-01", "00:00").unwrap();
        assert_eq!(dt.format("%H:%M:%S").to_string(), "00:00:00");
        assert!(parse_local_date_time("2024-05-01", "xx").is_none());
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
}
