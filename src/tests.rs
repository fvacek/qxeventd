//! In-process tests of qxeventd SHV endpoints.
//!
//! The real `shvclient::Client` with all qxeventd mounts is run against a mocked
//! broker connection (shvclient `mocking` feature), so requests go through
//! the same dispatch and access checking as in production, no shvbroker needed.

use std::sync::OnceLock;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::StreamExt;
use shvclient::{ConnectionCommand, ConnectionEvent};
use shvproto::RpcValue;
use shvrpc::metamethod::AccessLevel;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use smol::lock::RwLock;

use crate::config::Config;
use crate::state::{SharedAppState, State};
use crate::{GLOBAL_CONFIG, build_client};

const TIMEOUT: Duration = Duration::from_secs(5);

fn init_config() {
    GLOBAL_CONFIG.get_or_init(|| {
        static DATA_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        let data_dir = DATA_DIR.get_or_init(|| tempfile::tempdir().expect("tempdir"));
        let mut config = Config::default();
        config.data_dir = data_dir.path().to_string_lossy().into_owned();
        config.client.mount = Some("test/qx/qxeventd".into());
        config
    });
}

/// qxeventd client connected to a mocked broker.
struct TestClient {
    conn_evt_tx: UnboundedSender<ConnectionEvent>,
    conn_cmd_rx: UnboundedReceiver<ConnectionCommand>,
    /// Signals sent by qxeventd, collected while waiting for responses
    signals: Vec<RpcMessage>,
    _app_state: SharedAppState,
    _client_task: smol::Task<()>,
}

impl TestClient {
    async fn new() -> Self {
        init_config();
        let db_pool = crate::migrate::open_app_db("").await.expect("open app db");
        let app_state = SharedAppState::new(RwLock::new(State {
            db_pool,
            open_events: Default::default(),
            shutdown_sender: None,
            event_records: Default::default(),
        }));
        let (conn_evt_tx, conn_evt_rx) = unbounded();
        let client = build_client(app_state.clone());
        let client_task = smol::spawn(async move {
            client.mock_run_with_init(|_, _| {}, conn_evt_rx).await.expect("client loop");
        });
        let (conn_cmd_tx, conn_cmd_rx) = unbounded();
        conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).unwrap();
        Self { conn_evt_tx, conn_cmd_rx, signals: vec![], _app_state: app_state, _client_task: client_task }
    }

    fn send(&self, msg: &RpcMessage) {
        self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(msg.to_frame().unwrap())).unwrap();
    }

    async fn next_message(&mut self) -> RpcMessage {
        let cmd = futures_lite::future::or(
            self.conn_cmd_rx.next(),
            async { smol::Timer::after(TIMEOUT).await; None },
        ).await;
        match cmd {
            Some(ConnectionCommand::SendMessage(msg)) => msg,
            None => panic!("Timeout waiting for message from qxeventd"),
        }
    }

    /// Mocked responses of broker to requests sent by qxeventd
    fn mock_broker_response(rq: &RpcMessage) -> Result<RpcValue, RpcError> {
        match (rq.shv_path().unwrap_or_default(), rq.method().unwrap_or_default()) {
            (".broker/access/mounts", "setValue") => Ok(true.into()),
            (path, method) => Err(RpcError::new(RpcErrorCode::MethodNotFound, format!("mock broker: {path}:{method}"))),
        }
    }

    /// Send request and wait for its response, broker requests are answered meanwhile.
    async fn call(&mut self, rq: RpcMessage) -> Result<RpcValue, RpcError> {
        let rq_id = rq.request_id().expect("request id");
        self.send(&rq);
        loop {
            let msg = self.next_message().await;
            if msg.is_response() && msg.request_id() == Some(rq_id) {
                match msg.response() {
                    Ok(resp) => if let Some(result) = resp.success() { return Ok(result.clone()) },
                    Err(err) => return Err(err),
                }
                // delay response, wait for the next one
            } else if msg.is_request() {
                let mut resp = msg.prepare_response().unwrap();
                match Self::mock_broker_response(&msg) {
                    Ok(result) => resp.set_result(result),
                    Err(err) => resp.set_error(err),
                };
                self.send(&resp);
            } else if msg.is_signal() {
                self.signals.push(msg);
            }
        }
    }
}

/// Request builder with explicit caller identity
fn request(path: &str, method: &str, param: &str, user: &str, access: AccessLevel) -> RpcMessage {
    let param = if param.is_empty() { None } else { Some(RpcValue::from_cpon(param).expect("valid cpon")) };
    let mut rq = RpcMessage::new_request(path, method).with_param(param);
    rq.set_user_id(user);
    rq.set_access_level(access);
    rq
}

fn assert_permission_denied(res: Result<RpcValue, RpcError>) {
    let err = res.expect_err("call should fail");
    assert_eq!(err.code, RpcErrorCode::PermissionDenied.into(), "{err}");
}

async fn create_event(client: &mut TestClient, owner: &str) -> (i64, String) {
    let res = client.call(request("eventctl", "createEvent", &format!(r#"{{"owner":"{owner}"}}"#), owner, AccessLevel::Write)).await
        .expect("createEvent");
    let map = res.as_map();
    (map["event_id"].as_int(), map["api_token"].as_str().to_string())
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
