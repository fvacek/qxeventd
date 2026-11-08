//! Harness for in-process tests of qxeventd SHV endpoints.
//!
//! The real `shvclient::Client` with all qxeventd mounts is run against a mocked
//! broker connection (shvclient `mocking` feature), so requests go through
//! the same dispatch and access checking as in production, no shvbroker needed.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::StreamExt;
use shvclient::{ConnectionCommand, ConnectionEvent};
use shvproto::RpcValue;
use shvrpc::metamethod::AccessLevel;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};

use qxeventd::config::Config;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Data directory of event DBs, shared by all tests in a test binary
pub fn data_dir() -> &'static str {
    static DATA_DIR: OnceLock<String> = OnceLock::new();
    DATA_DIR.get_or_init(|| {
        let data_dir = tempfile::tempdir().expect("tempdir").keep().to_string_lossy().into_owned();
        let mut config = Config { data_dir: data_dir.clone(), ..Default::default() };
        config.client.mount = Some("test/qx/qxeventd".into());
        qxeventd::set_global_config(config);
        data_dir
    })
}

/// qxeventd client connected to a mocked broker.
pub struct TestClient {
    conn_evt_tx: UnboundedSender<ConnectionEvent>,
    conn_cmd_rx: UnboundedReceiver<ConnectionCommand>,
    /// Signals sent by qxeventd, collected while waiting for responses
    pub signals: Vec<RpcMessage>,
    /// Whether the mocked remote event DB service answers ping
    pub remote_db_available: bool,
    _client_task: smol::Task<()>,
}

impl TestClient {
    pub async fn new() -> Self {
        data_dir();
        let (conn_evt_tx, conn_evt_rx) = unbounded();
        let client = qxeventd::create_client("").await.expect("create client");
        let client_task = smol::spawn(async move {
            client.mock_run_with_init(|_, _| {}, conn_evt_rx).await.expect("client loop");
        });
        let (conn_cmd_tx, conn_cmd_rx) = unbounded();
        conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).unwrap();
        Self { conn_evt_tx, conn_cmd_rx, signals: vec![], remote_db_available: true, _client_task: client_task }
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
    fn mock_broker_response(&self, rq: &RpcMessage) -> Result<RpcValue, RpcError> {
        match (rq.shv_path().unwrap_or_default(), rq.method().unwrap_or_default()) {
            (".broker/access/mounts", "setValue") => Ok(true.into()),
            (".broker", "ls") => Ok(vec![RpcValue::from("client")].into()),
            (".broker/currentClient", "subscribe") => Ok(true.into()),
            (path, "ping") if path.ends_with("/.app") && self.remote_db_available => Ok(RpcValue::null()),
            (path, method) => Err(RpcError::new(RpcErrorCode::MethodNotFound, format!("mock broker: {path}:{method}"))),
        }
    }

    /// Send request and wait for its response, broker requests are answered meanwhile.
    pub async fn call(&mut self, rq: RpcMessage) -> Result<RpcValue, RpcError> {
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
                match self.mock_broker_response(&msg) {
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
pub fn request(path: &str, method: &str, param: &str, user: &str, access: AccessLevel) -> RpcMessage {
    let param = if param.is_empty() { None } else { Some(RpcValue::from_cpon(param).expect("valid cpon")) };
    let mut rq = RpcMessage::new_request(path, method).with_param(param);
    rq.set_user_id(user);
    rq.set_access_level(access);
    rq
}

pub fn assert_permission_denied(res: Result<RpcValue, RpcError>) {
    let err = res.expect_err("call should fail");
    assert_eq!(err.code, RpcErrorCode::PermissionDenied.into(), "{err}");
}

pub async fn create_event(client: &mut TestClient, owner: &str) -> (i64, String) {
    let res = client.call(request("eventctl", "createEvent", &format!(r#"{{"owner":"{owner}"}}"#), owner, AccessLevel::Write)).await
        .expect("createEvent");
    let map = res.as_map();
    (map["event_id"].as_int(), map["api_token"].as_str().to_string())
}

/// Create event with ID unique in the test binary.
/// Every `TestClient` numbers events from 1, but event DB files in the shared data dir are
/// identified by event ID only, so tests reading or writing them cannot share IDs.
/// ID 1 is left to tests calling `create_event`.
pub async fn create_event_with_own_db(client: &mut TestClient, owner: &str) -> (i64, String) {
    static NEXT_EVENT_ID: AtomicI64 = AtomicI64::new(2);
    let event_id = NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed);
    loop {
        let (id, api_token) = create_event(client, owner).await;
        if id == event_id {
            return (id, api_token);
        }
    }
}
