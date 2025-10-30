use crate::appstate::{QxSharedAppState};
use async_sqlite::rusqlite::types::ValueRef;
use async_trait::async_trait;
use qxsql::{sql::{DbField, ExecResult, Record, SelectResult}, DbValue};

pub struct QxSql(pub QxSharedAppState);

#[async_trait]
impl qxsql::sql::SqlProvider for QxSql {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<SelectResult> {
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_query(&self.0, query, params).await
    }
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        sql_exec(&self.0, query, params).await
    }
}

fn convert_dbvalue_to_sql(key: &str, value: &DbValue) -> Result<async_sqlite::rusqlite::types::Value, async_sqlite::rusqlite::Error> {
    match value {
        DbValue::String(s) => Ok(s.as_str().to_string().into()),
        DbValue::Int(i) => Ok((*i).into()),
        DbValue::DateTime(dt) => Ok(dt.to_rfc3339().into()),
        DbValue::Null => Ok(async_sqlite::rusqlite::types::Value::Null),
        DbValue::Blob(b) => Ok(b.clone().into()),
        _ => Err(async_sqlite::rusqlite::Error::ToSqlConversionFailure(
            format!("Unsupported value type for field {}", key).into(),
        )),
    }
}

fn process_record_params(record: &Record) -> Result<Vec<(String, async_sqlite::rusqlite::types::Value)>, async_sqlite::rusqlite::Error> {
    let mut params: Vec<(String, async_sqlite::rusqlite::types::Value)> = Vec::new();

    for (key, value) in record.iter() {
        let param_name = format!(":{}", key);
        let sql_value = convert_dbvalue_to_sql(key, value)?;
        params.push((param_name, sql_value));
    }
    Ok(params)
}

fn create_param_refs(params: &[(String, async_sqlite::rusqlite::types::Value)]) -> Vec<(&str, &dyn async_sqlite::rusqlite::ToSql)> {
    params
        .iter()
        .map(|(name, val)| (name.as_str(), val as &dyn async_sqlite::rusqlite::ToSql))
        .collect()
}

async fn sql_query(
    app_state: &QxSharedAppState,
    query: &str,
    params: &Record
) -> anyhow::Result<SelectResult> {
    let query = query.to_string();
    let params = process_record_params(params)?;
    let state = app_state.read().await;
    let table = state.db_pool
        .conn(move |conn| {
            let param_refs = create_param_refs(&params);
            let mut stmt = conn.prepare(&query)?;
            let fields: Vec<DbField> = stmt.column_names().iter().map(|s| DbField { name: s.to_string() }).collect();
            let rows = stmt
                .query_map(&param_refs[..], |row| {
                    let mut rec: Vec<DbValue> = Vec::new();
                    for i in 0..fields.len() {
                        let value = row.get_ref(i)?; // <-- get a ValueRef (borrowed SQL value)
                        match value {
                            ValueRef::Null => rec.push(DbValue::Null),
                            ValueRef::Integer(i) => rec.push(i.into()),
                            ValueRef::Real(r) => rec.push(r.into()),
                            ValueRef::Text(t) => rec.push(String::from_utf8_lossy(t).to_string().into()),
                            ValueRef::Blob(b) => rec.push(b.into()),
                        };
                    }
                    Ok(rec)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SelectResult { fields, rows })
        })
        .await?;
    Ok(table)
}

async fn sql_exec(
    app_state: &QxSharedAppState,
    query: &str,
    params: &Record
) -> anyhow::Result<ExecResult> {
    let query = query.to_string();
    let params = process_record_params(params)?;
    let state = app_state.read().await;
    let result = state.db_pool
        .conn(move |conn| {
            let param_refs = create_param_refs(&params);
            let mut stmt = conn.prepare(&query)?;
            let rows_affected = stmt.execute(&param_refs[..])?;
            Ok(ExecResult { rows_affected: rows_affected as i64, insert_id: None })
        })
        .await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appstate::QxAppState;
    use async_sqlite::PoolBuilder;
    use chrono::Utc;
    use qxsql::sql::{record_from_slice, SqlProvider};
    use smol::lock::RwLock;



    async fn create_test_app_state() -> QxSharedAppState {
        let pool = PoolBuilder::new()
            .path(":memory:")
            .num_conns(1)
            .open()
            .await
            .expect("Failed to create database pool");

        // Create a test table
        pool.conn(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER,
                    created_at TEXT
                )",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("Failed to create test table");

        let app_state = QxAppState { db_pool: pool };
        let locked_state = RwLock::new(app_state);
        shvclient::AppState::new(locked_state)
    }

    #[test]
    fn test_convert_dbvalue_to_sql() {
        // Test String conversion
        let result = convert_dbvalue_to_sql("test", &DbValue::String("hello".to_string()));
        assert!(result.is_ok());

        // Test Int conversion
        let result = convert_dbvalue_to_sql("test", &DbValue::Int(42));
        assert!(result.is_ok());

        // Test DateTime conversion
        let dt = Utc::now();
        let result = convert_dbvalue_to_sql("test", &DbValue::DateTime(dt.into()));
        assert!(result.is_ok());

        // Test Null conversion
        let result = convert_dbvalue_to_sql("test", &DbValue::Null);
        assert!(result.is_ok());

        // Test Blob conversion
        let result = convert_dbvalue_to_sql("test", &DbValue::Blob(vec![1, 2, 3]));
        assert!(result.is_ok());

        // Test unsupported type - use a pattern that matches the catch-all case
        // Since all basic types are now supported, this test verifies the error handling
        // We can't easily test this without adding a new unsupported variant
    }

    #[test]
    fn test_process_record_params() {
        let record = record_from_slice(&[
            ("name", DbValue::String("test".to_string())),
            ("id", DbValue::Int(1)),
            ("null_field", DbValue::Null),
        ]);

        let result = process_record_params(&record);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(params.len(), 3);

        // Check that parameter names are prefixed with ':'
        let param_names: Vec<String> = params.iter().map(|(name, _)| name.clone()).collect();
        assert!(param_names.contains(&":name".to_string()));
        assert!(param_names.contains(&":id".to_string()));
        assert!(param_names.contains(&":null_field".to_string()));
    }

    #[test]
    fn test_create_param_refs() {
        let params = vec![
            (":name".to_string(), async_sqlite::rusqlite::types::Value::Text("test".to_string())),
            (":id".to_string(), async_sqlite::rusqlite::types::Value::Integer(1)),
        ];

        let refs = create_param_refs(&params);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].0, ":name");
        assert_eq!(refs[1].0, ":id");
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_insert() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let params = record_from_slice(&[
            ("name", DbValue::String("test_user".to_string())),
            ("value", DbValue::Int(100)),
            ("created_at", DbValue::DateTime(Utc::now().into())),
        ]);

        let result = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&params),
            )
            .await;

        assert!(result.is_ok());
        let exec_result = result.unwrap();
        assert_eq!(exec_result.rows_affected, 1);
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_without_params() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let result = qx_sql
            .exec("CREATE TABLE IF NOT EXISTS test_table2 (id INTEGER PRIMARY KEY)", None)
            .await;

        assert!(result.is_ok());
    }

    #[smol_potat::test]
    async fn test_qxsql_query_select() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // First insert some test data
        let insert_params = record_from_slice(&[
            ("name", DbValue::String("query_test".to_string())),
            ("value", DbValue::Int(200)),
            ("created_at", DbValue::DateTime(Utc::now().into())),
        ]);

        let _ = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&insert_params),
            )
            .await
            .expect("Failed to insert test data");

        // Now query the data
        let query_params = record_from_slice(&[
            ("name", DbValue::String("query_test".to_string())),
        ]);

        let result = qx_sql
            .query("SELECT * FROM test_table WHERE name = :name", Some(&query_params))
            .await;

        assert!(result.is_ok());
        let select_result = result.unwrap();

        // Check fields
        assert!(select_result.fields.len() >= 4); // id, name, value, created_at
        let field_names: Vec<&str> = select_result.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"value"));

        // Check rows
        assert!(!select_result.rows.is_empty());
        let first_row = &select_result.rows[0];
        assert_eq!(first_row.len(), select_result.fields.len());
    }

    #[smol_potat::test]
    async fn test_qxsql_query_without_params() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let result = qx_sql.query("SELECT COUNT(*) as count FROM test_table", None).await;

        assert!(result.is_ok());
        let select_result = result.unwrap();
        assert_eq!(select_result.fields.len(), 1);
        assert_eq!(select_result.fields[0].name, "count");
        assert!(!select_result.rows.is_empty());
    }

    #[smol_potat::test]
    async fn test_qxsql_query_with_different_value_types() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // Insert data with different types
        let dt = Utc::now();
        let params = record_from_slice(&[
            ("name", DbValue::String("type_test".to_string())),
            ("value", DbValue::Int(42)),
            ("created_at", DbValue::DateTime(dt.into())),
        ]);

        let _ = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&params),
            )
            .await
            .expect("Failed to insert test data");

        // Query the data back
        let query_params = record_from_slice(&[
            ("name", DbValue::String("type_test".to_string())),
        ]);

        let result = qx_sql
            .query("SELECT name, value, created_at FROM test_table WHERE name = :name", Some(&query_params))
            .await;

        assert!(result.is_ok());
        let select_result = result.unwrap();
        assert!(!select_result.rows.is_empty());

        let row = &select_result.rows[0];
        // Verify that we can read different value types from the database
        assert_eq!(row.len(), 3);

        // The exact DbValue types depend on how SQLite returns them,
        // but we should at least get some values back
        match &row[0] {
            DbValue::String(_) => (),
            _ => panic!("Expected string value for name field"),
        }
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_update() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // Insert initial data
        let insert_params = record_from_slice(&[
            ("name", "update_test".into()),
            ("value", 100.into()),
            ("created_at", DbValue::DateTime(Utc::now().into())),
        ]);

        let _ = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&insert_params),
            )
            .await
            .expect("Failed to insert test data");

        // Update the data
        let update_params = record_from_slice(&[
            ("new_value", DbValue::Int(200)),
            ("name", DbValue::String("update_test".to_string())),
        ]);

        let result = qx_sql
            .exec(
                "UPDATE test_table SET value = :new_value WHERE name = :name",
                Some(&update_params),
            )
            .await;

        assert!(result.is_ok());
        let exec_result = result.unwrap();
        assert_eq!(exec_result.rows_affected, 1);
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_delete() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // Insert data to delete
        let insert_params = record_from_slice(&[
            ("name", DbValue::String("delete_test".to_string())),
            ("value", DbValue::Int(300)),
            ("created_at", DbValue::DateTime(Utc::now().into())),
        ]);

        let _ = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&insert_params),
            )
            .await
            .expect("Failed to insert test data");

        // Delete the data
        let delete_params = record_from_slice(&[
            ("name", DbValue::String("delete_test".to_string())),
        ]);

        let result = qx_sql
            .exec("DELETE FROM test_table WHERE name = :name", Some(&delete_params))
            .await;

        assert!(result.is_ok());
        let exec_result = result.unwrap();
        assert_eq!(exec_result.rows_affected, 1);
    }

    #[smol_potat::test]
    async fn test_qxsql_query_empty_result() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let params = record_from_slice(&[
            ("name", DbValue::String("nonexistent".to_string())),
        ]);

        let result = qx_sql
            .query("SELECT * FROM test_table WHERE name = :name", Some(&params))
            .await;

        assert!(result.is_ok());
        let select_result = result.unwrap();
        assert!(select_result.rows.is_empty());
        assert!(!select_result.fields.is_empty()); // Fields should still be present
    }

    #[smol_potat::test]
    async fn test_qxsql_invalid_sql() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let result = qx_sql.query("SELECT * FROM nonexistent_table", None).await;
        assert!(result.is_err());

        let result = qx_sql.exec("INSERT INTO nonexistent_table VALUES (1)", None).await;
        assert!(result.is_err());
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_with_null_values() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        let params = record_from_slice(&[
            ("name", DbValue::String("null_test".to_string())),
            ("value", DbValue::Null),
            ("created_at", DbValue::Null),
        ]);

        let result = qx_sql
            .exec(
                "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                Some(&params),
            )
            .await;

        assert!(result.is_ok());
        let exec_result = result.unwrap();
        assert_eq!(exec_result.rows_affected, 1);

        // Verify we can query the null values back
        let query_params = record_from_slice(&[
            ("name", DbValue::String("null_test".to_string())),
        ]);

        let query_result = qx_sql
            .query("SELECT * FROM test_table WHERE name = :name", Some(&query_params))
            .await;

        assert!(query_result.is_ok());
        let select_result = query_result.unwrap();
        assert!(!select_result.rows.is_empty());

        let row = &select_result.rows[0];
        // Check that null values are properly handled
        match &row[2] { // value column should be null
            DbValue::Null => (),
            _ => panic!("Expected null value"),
        }
    }

    #[smol_potat::test]
    async fn test_qxsql_exec_multiple_inserts() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // Insert multiple records
        for i in 1..=5 {
            let params = record_from_slice(&[
                ("name", DbValue::String(format!("user_{}", i))),
                ("value", DbValue::Int(i * 10)),
                ("created_at", DbValue::DateTime(Utc::now().into())),
            ]);

            let result = qx_sql
                .exec(
                    "INSERT INTO test_table (name, value, created_at) VALUES (:name, :value, :created_at)",
                    Some(&params),
                )
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().rows_affected, 1);
        }

        // Verify all records were inserted
        let result = qx_sql.query("SELECT COUNT(*) as count FROM test_table", None).await;
        assert!(result.is_ok());
        let select_result = result.unwrap();

        // Check that we have at least 5 records (from this test plus any from other tests)
        assert!(!select_result.rows.is_empty());
        if let DbValue::Int(count) = &select_result.rows[0][0] {
            assert!(*count >= 5);
        }
    }

    #[test]
    fn test_process_record_params_empty() {
        let record = record_from_slice(&[]);
        let result = process_record_params(&record);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert!(params.is_empty());
    }

    #[test]
    fn test_create_param_refs_empty() {
        let params = vec![];
        let refs = create_param_refs(&params);
        assert!(refs.is_empty());
    }

    #[smol_potat::test]
    async fn test_qxsql_query_with_blob_data() {
        let app_state = create_test_app_state().await;
        let qx_sql = QxSql(app_state);

        // Create a separate table for blob testing to avoid ALTER TABLE issues
        let create_result = qx_sql
            .exec("CREATE TABLE blob_test_table (id INTEGER PRIMARY KEY, name TEXT, blob_data BLOB)", None)
            .await;

        assert!(create_result.is_ok(), "Failed to create blob test table: {:?}", create_result.err());

        let blob_data = vec![1, 2, 3, 4, 5];
        let params = record_from_slice(&[
            ("name", DbValue::String("blob_test".to_string())),
            ("blob_data", DbValue::Blob(blob_data.clone())),
        ]);

        // Insert data with blob
        let result = qx_sql
            .exec(
                "INSERT INTO blob_test_table (name, blob_data) VALUES (:name, :blob_data)",
                Some(&params),
            )
            .await;

        assert!(result.is_ok(), "Failed to insert blob data: {:?}", result.err());

        // Query back the blob data
        let query_params = record_from_slice(&[
            ("name", DbValue::String("blob_test".to_string())),
        ]);

        let query_result = qx_sql
            .query("SELECT blob_data FROM blob_test_table WHERE name = :name", Some(&query_params))
            .await;

        assert!(query_result.is_ok(), "Failed to query blob data: {:?}", query_result.err());
        let select_result = query_result.unwrap();
        assert!(!select_result.rows.is_empty());

        // Verify blob data
        match &select_result.rows[0][0] {
            DbValue::Blob(returned_blob) => {
                assert_eq!(*returned_blob, blob_data);
            },
            _ => panic!("Expected blob value, got: {:?}", &select_result.rows[0][0]),
        }
    }
}
