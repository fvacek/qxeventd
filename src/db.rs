//! SQLite compatible database access using Turso.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::{anyhow, bail};
use smol::lock::{Mutex, MutexGuard};
use turso::{Builder, Database, IntoParams, Row};

pub use turso::transaction::TransactionBehavior;
pub use turso::{Connection, Value};

/// Shared handle of a database with a single connection, the connection is used exclusively,
/// see [`Db::conn`]. Cloned handles refer to the same database.
#[derive(Clone)]
pub struct Db(Arc<DbInner>);

struct DbInner {
    path: String,
    /// `None` once the database is closed.
    state: Mutex<Option<OpenDb>>,
}

struct OpenDb {
    // the connection must be dropped before the database
    conn: Connection,
    _db: Database,
}

/// Exclusive use of the database connection, see [`Db::conn`].
pub struct DbConn<'a>(MutexGuard<'a, Option<OpenDb>>);

impl Deref for DbConn<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.0.as_ref().expect("Open database").conn
    }
}

impl DerefMut for DbConn<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.0.as_mut().expect("Open database").conn
    }
}

impl Db {
    /// Open database file `path`, `:memory:` opens a new in-memory database.
    /// File databases are always in WAL journal mode.
    pub async fn open(path: &str) -> anyhow::Result<Self> {
        let db = Builder::new_local(path)
            .experimental_vacuum(true)
            .build().await
            .map_err(|err| anyhow!("Cannot open database {path}: {err}"))?;
        let conn = db.connect()?;
        Ok(Self(Arc::new(DbInner {
            path: path.to_string(),
            state: Mutex::new(Some(OpenDb { conn, _db: db })),
        })))
    }

    pub fn path(&self) -> &str {
        &self.0.path
    }

    /// Exclusive use of the connection, other users wait until the returned guard is dropped.
    /// Fails if the database is closed.
    pub async fn conn(&self) -> anyhow::Result<DbConn<'_>> {
        let state = self.0.state.lock().await;
        if state.is_none() {
            bail!("Database {} is closed", self.0.path);
        }
        Ok(DbConn(state))
    }

    /// Checkpoint the WAL, so the whole database is stored in the main DB file, and close it.
    /// The empty WAL file is removed, so the main file can be moved or replaced then.
    /// All the database handles are released, so opening the same path again opens
    /// the file found there then. Closing a closed database does nothing.
    pub async fn close(&self) -> anyhow::Result<()> {
        let mut state = self.0.state.lock().await;
        let Some(open_db) = state.take() else {
            return Ok(());
        };
        let checkpoint = query_one(&open_db.conn, "PRAGMA wal_checkpoint(TRUNCATE)", ()).await
            .and_then(|row| row.ok_or_else(|| anyhow!("WAL checkpoint returned no result")))
            .and_then(|row| Ok(row.get::<i64>(0)?));
        drop(open_db);
        if checkpoint? != 0 {
            bail!("WAL checkpoint of {} was blocked", self.0.path);
        }
        remove_empty_wal_file(&self.0.path)
    }

    /// Write a consistent snapshot of the database to a new file `dest_file`.
    pub async fn backup_to(&self, dest_file: &str) -> anyhow::Result<()> {
        let conn = self.conn().await?;
        conn.execute(format!("VACUUM INTO '{}'", dest_file.replace('\'', "''")), ()).await?;
        drop(conn);
        // The snapshot has no WAL content, the WAL file can be left by the VACUUM.
        remove_empty_wal_file(dest_file)
    }
}

/// Remove the empty WAL file of database `db_file`, if any.
pub fn remove_empty_wal_file(db_file: &str) -> anyhow::Result<()> {
    let wal_file = format!("{db_file}-wal");
    match std::fs::metadata(&wal_file) {
        Ok(metadata) if metadata.len() == 0 => Ok(std::fs::remove_file(&wal_file)?),
        Ok(_) => bail!("WAL file {wal_file} is not empty"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// All rows of the query result.
pub async fn query_all(conn: &Connection, sql: &str, params: impl IntoParams) -> anyhow::Result<Vec<Row>> {
    let mut rows = conn.query(sql, params).await?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await? {
        result.push(row);
    }
    Ok(result)
}

/// The first row of the query result, if any.
pub async fn query_one(conn: &Connection, sql: &str, params: impl IntoParams) -> anyhow::Result<Option<Row>> {
    let mut rows = conn.query(sql, params).await?;
    Ok(rows.next().await?)
}

/// Single value of the first row of the query result, it fails if there is no row.
pub async fn query_value<T: turso::core::types::FromValue>(conn: &Connection, sql: &str, params: impl IntoParams) -> anyhow::Result<T> {
    let row = query_one(conn, sql, params).await?
        .ok_or_else(|| anyhow!("Query returned no rows: {sql}"))?;
    Ok(row.get::<T>(0)?)
}
