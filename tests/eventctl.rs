//! eventctl node endpoint tests

mod common;

use shvproto::RpcValue;
use shvrpc::RpcMessage;
use shvrpc::metamethod::AccessLevel;

use common::{TestClient, assert_permission_denied, create_event, request};

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
        meta.insert("qx_api_token", api_token.as_str().into());
        let rq = RpcMessage::from_meta(meta).with_param(rq.param().cloned());
        client.call(rq).await.expect("api token holder can read event record");
    });
}

#[test]
fn open_event_returns_event_id_and_mount_point() {
    smol::block_on(async {
        let mut client = TestClient::new().await;
        let (event_id, api_token) = create_event(&mut client, "fanda").await;
        let expected =format!(r#"{{"event_id":{event_id},"mount_point":"test/qx/qxeventd/eventctl/{event_id}"}}"#);

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
