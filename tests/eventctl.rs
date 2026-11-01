//! eventctl node endpoint tests

mod common;

use std::io::{Read, Write};

use async_sqlite::rusqlite::Connection;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use shvproto::{RpcValue, make_map};
use shvrpc::RpcMessage;
use shvrpc::metamethod::AccessLevel;

use common::{TestClient, assert_permission_denied, create_event, create_event_with_own_db, data_dir, request};
use qxeventd::QX_API_TOKEN;

fn compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn push_event_db_request(event_id: i64, compressed_data: Vec<u8>, user: &str, access: AccessLevel) -> RpcMessage {
    let param = make_map!(
        "event_id".to_string() => RpcValue::from(event_id),
        "compressed_data".to_string() => RpcValue::from(compressed_data),
    );
    request("eventctl", "pushEventDb", "", user, access).with_param(RpcValue::from(param))
}

fn pull_event_db_request(event_id: i64, user: &str, access: AccessLevel) -> RpcMessage {
    request("eventctl", "pullEventDb", &format!(r#"{{"event_id":{event_id}}}"#), user, access)
}

fn with_api_token(rq: RpcMessage, api_token: &str) -> RpcMessage {
    let mut meta = rq.meta().clone();
    meta.insert(QX_API_TOKEN, api_token.into());
    RpcMessage::from_meta(meta).with_param(rq.param().cloned())
}

async fn push_event_db(client: &mut TestClient, event_id: i64, data: &[u8]) {
    client.call(push_event_db_request(event_id, compress(data), "fanda", AccessLevel::Read)).await.expect("pushEventDb");
}

async fn pull_event_db(client: &mut TestClient, event_id: i64) -> Vec<u8> {
    let res = client.call(pull_event_db_request(event_id, "fanda", AccessLevel::Read)).await.expect("pullEventDb");
    let res = res.as_map();
    let mut data = Vec::new();
    ZlibDecoder::new(res["compressed_data"].as_blob()).read_to_end(&mut data).expect("valid zlib data");
    assert_eq!(res["uncompressed_data_size"].as_int(), data.len() as i64);
    data
}

/// Open event DB image in a temporary file, to check its content or to modify it
fn with_event_db_image<T>(data: &[u8], f: impl FnOnce(&Connection) -> T) -> (T, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("event.qbe");
    std::fs::write(&path, data).unwrap();
    let conn = Connection::open(&path).unwrap();
    let result = f(&conn);
    conn.pragma_update(None, "journal_mode", "DELETE").unwrap();
    drop(conn);
    (result, std::fs::read(&path).unwrap())
}

fn read_config(conn: &Connection, key: &str) -> String {
    conn.query_row("SELECT cvalue FROM config WHERE ckey = ?1", [key], |row| row.get(0)).unwrap()
}

#[test]
fn eventctl_ls_and_dir() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let res = client.call(request("eventctl", "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), "[]");
        let res = client.call(request("eventctl", "dir", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert!(res.to_cpon().contains(r#""createEvent""#), "{}", res.to_cpon());
    });
}

#[test]
fn create_event_and_read_record_by_owner_only() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event(&mut client, "fanda").await;
        assert_eq!(event_id, 1);
        assert_eq!(api_token.len(), 10);

        // owner with Read access is escalated to Service
        let res = client.call(request("eventctl", "readEventRecord", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await
            .expect("owner can read event record");
        assert_eq!(res.as_map()["api_token"].as_str(), api_token);

        // stranger with Read access is not
        assert_permission_denied(client.call(request("eventctl", "readEventRecord", &format!(r#"{{"event_id":{event_id}}}"#), "eve", AccessLevel::Read)).await);

        // stranger with valid api token is
        let rq = request("eventctl", "readEventRecord", &format!(r#"{{"event_id":{event_id}}}"#), "eve", AccessLevel::Read);
        let mut meta = rq.meta().clone();
        meta.insert(QX_API_TOKEN, api_token.as_str().into());
        let rq = RpcMessage::from_meta(meta).with_param(rq.param().cloned());
        client.call(rq).await.expect("api token holder can read event record");
    });
}

#[test]
fn open_event_returns_event_id_and_mount_point() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event(&mut client, "fanda").await;
        let expected = format!(r#"{{"event_id":{event_id},"mount_point":"test/qx/qxeventd/eventctl/{event_id}"}}"#);

        let res = client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();
        assert_eq!(res.to_cpon(), expected);

        let res = client.call(request("eventctl", "openEventApiKey", &format!(r#"{{"api_token":"{api_token}"}}"#), "eve", AccessLevel::Read)).await.unwrap();
        assert_eq!(res.to_cpon(), expected);
    });
}

#[test]
fn delete_event() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (_event_id, api_token) = create_event(&mut client, "fanda").await;
        let res = client.call(request("eventctl", "deleteEvent", &format!(r#"{{"api_token":"{api_token}"}}"#), "fanda", AccessLevel::Read)).await.unwrap();
        assert_eq!(res, RpcValue::from(true));
        let res = client.call(request("eventctl", "listEvents", "", "fanda", AccessLevel::Read)).await.unwrap();
        assert_eq!(res.to_cpon(), "[]");
    });
}

#[test]
fn list_events_returns_typed_items() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event(&mut client, "fanda").await;
        let res = client.call(request("eventctl", "listEvents", "", "fanda", AccessLevel::Read)).await.unwrap();
        let events = res.as_list();
        assert_eq!(events.len(), 1, "{}", res.to_cpon());
        let event = events[0].as_map();
        assert_eq!(event["id"].as_int(), event_id);
        assert_eq!(event["owner"].as_str(), "fanda");
        assert_eq!(event["stage_count"].as_int(), 1);
        assert_eq!(event["db_mode"].as_str(), "Replica");
        assert_eq!(event["date"].type_name(), "DateTime", "{}", res.to_cpon());
        assert!(!event.contains_key("api_token"));
    });
}

#[test]
fn push_and_pull_event_db() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;

        push_event_db(&mut client, event_id, b"first database").await;
        assert_eq!(pull_event_db(&mut client, event_id).await, b"first database");

        // push replaces existing database
        push_event_db(&mut client, event_id, b"second database").await;
        assert_eq!(pull_event_db(&mut client, event_id).await, b"second database");
    });
}

#[test]
fn pull_event_db_returns_compressed_data_as_blob() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;
        let res = client.call(pull_event_db_request(event_id, "fanda", AccessLevel::Read)).await.unwrap();
        assert_eq!(res.as_map()["compressed_data"].type_name(), "Blob");
    });
}

#[test]
fn pull_event_db_fails_if_there_is_no_db() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(pull_event_db_request(event_id, "fanda", AccessLevel::Read)).await.expect_err("event DB does not exist");
    });
}

#[test]
fn push_invalid_event_db_keeps_existing_one() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;

        client.call(push_event_db_request(event_id, b"not zlib data".to_vec(), "fanda", AccessLevel::Read)).await
            .expect_err("invalid data should be rejected");

        assert_eq!(pull_event_db(&mut client, event_id).await, b"database");
    });
}

#[test]
fn push_event_db_removes_stale_wal_files() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;

        // left over e.g. after crash, event is not opened
        let stale_files = ["wal", "shm"].map(|suffix| format!("{}/{event_id}/event.qbe-{suffix}", data_dir()));
        for file in &stale_files {
            std::fs::write(file, b"stale").unwrap();
        }

        push_event_db(&mut client, event_id, b"database").await;
        for file in &stale_files {
            assert!(!std::path::Path::new(file).exists(), "{file}");
        }
    });
}

#[test]
fn push_and_pull_event_db_access_rights() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;

        // stranger with Read access is not allowed
        assert_permission_denied(client.call(push_event_db_request(event_id, compress(b"eve"), "eve", AccessLevel::Read)).await);
        assert_permission_denied(client.call(pull_event_db_request(event_id, "eve", AccessLevel::Read)).await);

        // neither is one with invalid api token, the method is not even resolved then
        client.call(with_api_token(push_event_db_request(event_id, compress(b"eve"), "eve", AccessLevel::Read), "invalid")).await
            .expect_err("invalid api token cannot push event DB");
        client.call(with_api_token(pull_event_db_request(event_id, "eve", AccessLevel::Read), "invalid")).await
            .expect_err("invalid api token cannot pull event DB");
        assert_eq!(pull_event_db(&mut client, event_id).await, b"database");

        // stranger with valid api token is
        client.call(with_api_token(push_event_db_request(event_id, compress(b"token"), "eve", AccessLevel::Read), &api_token)).await
            .expect("api token holder can push event DB");
        client.call(with_api_token(pull_event_db_request(event_id, "eve", AccessLevel::Read), &api_token)).await
            .expect("api token holder can pull event DB");
        assert_eq!(pull_event_db(&mut client, event_id).await, b"token");

        // user with Service access is
        client.call(push_event_db_request(event_id, compress(b"service"), "admin", AccessLevel::Service)).await
            .expect("service user can push event DB");
        client.call(pull_event_db_request(event_id, "admin", AccessLevel::Service)).await
            .expect("service user can pull event DB");
        assert_eq!(pull_event_db(&mut client, event_id).await, b"service");
    });
}

#[test]
fn pull_closed_event_db_contains_data_written_while_opened() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();
        client.call(request(&format!("eventctl/{event_id}"), "close", "", "fanda", AccessLevel::Read)).await.unwrap();

        let data = pull_event_db(&mut client, event_id).await;
        let (stage_count, _) = with_event_db_image(&data, |conn| read_config(conn, "event.stage_count"));
        assert_eq!(stage_count, "1");
    });
}

#[test]
fn pull_and_push_opened_event_db() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();

        // pull makes snapshot of the opened DB
        let snapshot = pull_event_db(&mut client, event_id).await;
        let ((), modified) = with_event_db_image(&snapshot, |conn| {
            assert_eq!(read_config(conn, "event.stage_count"), "1");
            conn.execute("UPDATE config SET cvalue = 'pushed' WHERE ckey = 'event.name'", []).unwrap();
        });

        // push replaces DB of the opened event, which stays opened and uses the new DB
        push_event_db(&mut client, event_id, &modified).await;
        let res = client.call(request("eventctl", "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), format!(r#"["{event_id}"]"#));
        let snapshot = pull_event_db(&mut client, event_id).await;
        let (event_name, _) = with_event_db_image(&snapshot, |conn| read_config(conn, "event.name"));
        assert_eq!(event_name, "pushed");
    });
}
