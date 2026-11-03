//! eventctl node endpoint tests

mod common;

use std::io::{Read, Write};
use qxeventd::rpcvalueext::RpcValueExt;
use async_sqlite::rusqlite::Connection;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use shvproto::RpcValue;
use shvrpc::RpcMessage;
use shvrpc::metamethod::AccessLevel;

use common::{TestClient, assert_permission_denied, create_event, create_event_with_own_db, data_dir, request};
use qxeventd::{PushEventDbParam, QX_API_TOKEN};

fn compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn push_event_db_request(event_id: i64, compressed_data: Vec<u8>, user: &str, access: AccessLevel) -> RpcMessage {
    let param = PushEventDbParam { event_id: event_id, zdata: compressed_data };
    request("eventctl", "pushEventDb", "", user, access).with_param(param.to_rpcvalue())
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
    ZlibDecoder::new(res["zdata"].as_blob()).read_to_end(&mut data).expect("valid zlib data");
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
        assert_eq!(res.as_map()["zdata"].type_name(), "Blob");
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

fn push_db_request(event_id: i64, method: &str, param: RpcValue, user: &str, access: AccessLevel) -> RpcMessage {
    let param = if param.is_null() { None } else { Some(param) };
    request(&format!("eventctl/{event_id}/pushDb"), method, "", user, access).with_param(param)
}

async fn push_db_call(client: &mut TestClient, event_id: i64, method: &str, param: RpcValue) -> Result<RpcValue, shvrpc::rpcmessage::RpcError> {
    client.call(push_db_request(event_id, method, param, "fanda", AccessLevel::Read)).await
}

fn chunk(offset: i64, data: &[u8]) -> RpcValue {
    vec![RpcValue::from(offset), RpcValue::from(data)].into()
}

fn cpon(cpon: &str) -> RpcValue {
    RpcValue::from_cpon(cpon).unwrap()
}

#[test]
fn push_db_node_ls_and_dir() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;

        let res = client.call(request(&format!("eventctl/{event_id}"), "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert!(res.to_cpon().contains(r#""pushDb""#), "{}", res.to_cpon());

        let path = format!("eventctl/{event_id}/pushDb");
        let res = client.call(request(&path, "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), "[]");
        let res = client.call(request(&path, "dir", "", "fanda", AccessLevel::Browse)).await.unwrap().to_cpon();
        for method in ["stat", "size", "crc", "read", "write", "truncate", "append", "commit"] {
            assert!(res.contains(&format!(r#""{method}""#)), "{method}: {res}");
        }
    });
}

#[test]
fn push_db_upload_in_chunks_and_commit() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"old").await;

        // nothing uploaded yet
        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 0);

        push_db_call(&mut client, event_id, "write", chunk(0, b"1234")).await.unwrap();
        push_db_call(&mut client, event_id, "append", RpcValue::from(&b"56789"[..])).await.unwrap();
        // overwrite does not change the rest of the file
        push_db_call(&mut client, event_id, "write", chunk(1, b"2")).await.unwrap();

        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 9);
        let stat = push_db_call(&mut client, event_id, "stat", RpcValue::null()).await.unwrap();
        let stat = stat.as_imap();
        assert_eq!(stat[&0].as_int(), 0);
        assert_eq!(stat[&1].as_int(), 9);
        assert_eq!(stat[&4].type_name(), "DateTime");
        assert_eq!(push_db_call(&mut client, event_id, "crc", RpcValue::null()).await.unwrap().to_cpon(), "3421780262u");
        assert_eq!(push_db_call(&mut client, event_id, "read", cpon("[7,100]")).await.unwrap().as_blob(), b"89");
        assert_eq!(push_db_call(&mut client, event_id, "read", cpon("[100,1]")).await.unwrap().as_blob(), b"");

        // upload does not affect the database until commit
        assert_eq!(pull_event_db(&mut client, event_id).await, b"old");

        push_db_call(&mut client, event_id, "commit", RpcValue::null()).await.unwrap();
        assert_eq!(pull_event_db(&mut client, event_id).await, b"123456789");
        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 0);
    });
}

#[test]
fn push_db_truncate_restarts_upload() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;

        push_db_call(&mut client, event_id, "append", RpcValue::from(&b"abandoned upload"[..])).await.unwrap();
        push_db_call(&mut client, event_id, "truncate", cpon("0")).await.unwrap();
        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 0);
        push_db_call(&mut client, event_id, "truncate", cpon("3")).await.unwrap();
        assert_eq!(push_db_call(&mut client, event_id, "read", cpon("[0,10]")).await.unwrap().as_blob(), b"\0\0\0");
    });
}

#[test]
fn push_db_invalid_param_is_rejected() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_db_call(&mut client, event_id, "write", cpon("[-1,b\"a\"]")).await.expect_err("negative offset");
        push_db_call(&mut client, event_id, "unknown", RpcValue::null()).await.expect_err("unknown method");
    });
}

#[test]
fn push_db_commit_without_upload_keeps_event_opened() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();

        push_db_call(&mut client, event_id, "commit", RpcValue::null()).await.expect_err("nothing uploaded");

        let res = client.call(request("eventctl", "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), format!(r#"["{event_id}"]"#));
    });
}

#[test]
fn push_db_commit_replaces_opened_event_db() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();

        let snapshot = pull_event_db(&mut client, event_id).await;
        let ((), modified) = with_event_db_image(&snapshot, |conn| {
            conn.execute("UPDATE config SET cvalue = 'pushed' WHERE ckey = 'event.name'", []).unwrap();
        });

        for (ix, data) in modified.chunks(4096).enumerate() {
            push_db_call(&mut client, event_id, "write", chunk(ix as i64 * 4096, data)).await.unwrap();
        }
        push_db_call(&mut client, event_id, "commit", RpcValue::null()).await.unwrap();

        let res = client.call(request("eventctl", "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), format!(r#"["{event_id}"]"#));
        let snapshot = pull_event_db(&mut client, event_id).await;
        let (event_name, _) = with_event_db_image(&snapshot, |conn| read_config(conn, "event.name"));
        assert_eq!(event_name, "pushed");
    });
}

#[test]
fn push_db_access_rights() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event_with_own_db(&mut client, "fanda").await;
        let data = || RpcValue::from(&b"data"[..]);

        // stranger with Read access is not allowed
        for (method, param) in [
            ("stat", RpcValue::null()),
            ("size", RpcValue::null()),
            ("crc", RpcValue::null()),
            ("read", cpon("[0,1]")),
            ("write", chunk(0, b"eve")),
            ("truncate", cpon("0")),
            ("append", data()),
            ("commit", RpcValue::null()),
        ] {
            assert_permission_denied(client.call(push_db_request(event_id, method, param, "eve", AccessLevel::Read)).await);
        }

        // neither is one with invalid api token, the method is not even resolved then
        client.call(with_api_token(push_db_request(event_id, "append", data(), "eve", AccessLevel::Read), "invalid")).await
            .expect_err("invalid api token cannot upload");
        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 0);

        // stranger with valid api token is
        client.call(with_api_token(push_db_request(event_id, "append", data(), "eve", AccessLevel::Read), &api_token)).await
            .expect("api token holder can upload");
        // user with Service access is
        client.call(push_db_request(event_id, "append", data(), "admin", AccessLevel::Service)).await
            .expect("service user can upload");
        assert_eq!(push_db_call(&mut client, event_id, "size", RpcValue::null()).await.unwrap().as_int(), 8);
    });
}

fn pull_db_request(event_id: i64, method: &str, param: &str, user: &str, access: AccessLevel) -> RpcMessage {
    request(&format!("eventctl/{event_id}/pullDb"), method, param, user, access)
}

async fn pull_db_call(client: &mut TestClient, event_id: i64, method: &str, param: &str) -> Result<RpcValue, shvrpc::rpcmessage::RpcError> {
    client.call(pull_db_request(event_id, method, param, "fanda", AccessLevel::Read)).await
}

/// Download the snapshot in chunks
async fn pull_db_read_all(client: &mut TestClient, event_id: i64) -> Vec<u8> {
    let mut data = Vec::new();
    loop {
        let param = format!("[{},1000]", data.len());
        let chunk = pull_db_call(client, event_id, "read", &param).await.unwrap();
        if chunk.as_blob().is_empty() {
            return data;
        }
        data.extend_from_slice(chunk.as_blob());
    }
}

#[test]
fn pull_db_node_ls_and_dir() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;

        let res = client.call(request(&format!("eventctl/{event_id}"), "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert!(res.to_cpon().contains(r#""pullDb""#), "{}", res.to_cpon());

        let res = pull_db_call(&mut client, event_id, "ls", "").await.unwrap();
        assert_eq!(res.to_cpon(), "[]");
        let res = pull_db_call(&mut client, event_id, "dir", "").await.unwrap().to_cpon();
        for method in ["stat", "size", "crc", "read", "snapshot"] {
            assert!(res.contains(&format!(r#""{method}""#)), "{method}: {res}");
        }
        for method in ["write", "truncate", "append"] {
            assert!(!res.contains(&format!(r#""{method}""#)), "{method}: {res}");
        }
    });
}

#[test]
fn pull_db_is_read_only() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;
        pull_db_call(&mut client, event_id, "snapshot", "").await.unwrap();

        for (method, param) in [("write", r#"[0,b"x"]"#), ("truncate", "0"), ("append", r#"b"x""#), ("commit", "")] {
            pull_db_call(&mut client, event_id, method, param).await.expect_err(method);
        }
        assert_eq!(pull_db_read_all(&mut client, event_id).await, b"database");
    });
}

#[test]
fn pull_db_requires_snapshot() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;

        for (method, param) in [("stat", ""), ("size", ""), ("crc", ""), ("read", "[0,10]")] {
            pull_db_call(&mut client, event_id, method, param).await.expect_err(method);
        }
    });
}

#[test]
fn pull_db_snapshot_fails_if_there_is_no_db() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        pull_db_call(&mut client, event_id, "snapshot", "").await.expect_err("event DB does not exist");
    });
}

#[test]
fn pull_db_snapshot_is_stable_until_refreshed() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"123456789").await;

        pull_db_call(&mut client, event_id, "snapshot", "").await.unwrap();
        assert_eq!(pull_db_call(&mut client, event_id, "size", "").await.unwrap().as_int(), 9);
        assert_eq!(pull_db_call(&mut client, event_id, "stat", "").await.unwrap().as_imap()[&1].as_int(), 9);
        assert_eq!(pull_db_call(&mut client, event_id, "crc", "").await.unwrap().to_cpon(), "3421780262u");

        // change of the database does not affect the running download
        push_event_db(&mut client, event_id, b"new").await;
        assert_eq!(pull_db_read_all(&mut client, event_id).await, b"123456789");

        pull_db_call(&mut client, event_id, "snapshot", "").await.unwrap();
        assert_eq!(pull_db_read_all(&mut client, event_id).await, b"new");
    });
}

#[test]
fn pull_db_snapshot_of_opened_event() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, _api_token) = create_event_with_own_db(&mut client, "fanda").await;
        client.call(request("eventctl", "openEvent", &format!(r#"{{"event_id":{event_id}}}"#), "fanda", AccessLevel::Read)).await.unwrap();

        pull_db_call(&mut client, event_id, "snapshot", "").await.unwrap();
        let data = pull_db_read_all(&mut client, event_id).await;
        let (stage_count, _) = with_event_db_image(&data, |conn| read_config(conn, "event.stage_count"));
        assert_eq!(stage_count, "1");

        // event stays opened
        let res = client.call(request("eventctl", "ls", "", "fanda", AccessLevel::Browse)).await.unwrap();
        assert_eq!(res.to_cpon(), format!(r#"["{event_id}"]"#));
    });
}

#[test]
fn pull_db_access_rights() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event_with_own_db(&mut client, "fanda").await;
        push_event_db(&mut client, event_id, b"database").await;
        pull_db_call(&mut client, event_id, "snapshot", "").await.unwrap();

        // stranger with Read access is not allowed
        for (method, param) in [("stat", ""), ("size", ""), ("crc", ""), ("read", "[0,1]"), ("snapshot", "")] {
            assert_permission_denied(client.call(pull_db_request(event_id, method, param, "eve", AccessLevel::Read)).await);
        }

        // neither is one with invalid api token, the method is not even resolved then
        client.call(with_api_token(pull_db_request(event_id, "read", "[0,1]", "eve", AccessLevel::Read), "invalid")).await
            .expect_err("invalid api token cannot download");

        // stranger with valid api token is
        client.call(with_api_token(pull_db_request(event_id, "read", "[0,1]", "eve", AccessLevel::Read), &api_token)).await
            .expect("api token holder can download");
        // user with Service access is
        client.call(pull_db_request(event_id, "read", "[0,1]", "admin", AccessLevel::Service)).await
            .expect("service user can download");
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
