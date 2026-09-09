#![cfg(unix)]

use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use whale_sdk_rust::{InitializeParams, InitializeResult, PeerInfo, RuntimeOptions};

const WAIT_BOUND: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitializeBehavior {
    Current,
    Incompatible,
    NoReply,
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectionPlan {
    initialize: InitializeBehavior,
    hold_initialize: bool,
}

impl ConnectionPlan {
    pub fn current() -> Self {
        Self {
            initialize: InitializeBehavior::Current,
            hold_initialize: false,
        }
    }

    pub fn incompatible() -> Self {
        Self {
            initialize: InitializeBehavior::Incompatible,
            ..Self::current()
        }
    }

    pub fn no_reply() -> Self {
        Self {
            initialize: InitializeBehavior::NoReply,
            ..Self::current()
        }
    }

    pub fn hold_initialize(mut self) -> Self {
        self.hold_initialize = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdsEvent {
    Accepted,
    InitializeReceived,
    InitializeAcked,
    ClientEof,
    ConnectionEnded,
    BusinessRequest { model: String },
}

struct ConnectionControl {
    events: Mutex<Vec<UdsEvent>>,
    revision: watch::Sender<u64>,
    initialize_release: watch::Sender<bool>,
    next_session: AtomicUsize,
}

impl ConnectionControl {
    fn new() -> Self {
        let (revision, _) = watch::channel(0);
        let (initialize_release, _) = watch::channel(false);
        Self {
            events: Mutex::new(Vec::new()),
            revision,
            initialize_release,
            next_session: AtomicUsize::new(1),
        }
    }

    fn record(&self, event: UdsEvent) {
        let revision = {
            let mut events = self.events.lock().unwrap();
            events.push(event);
            events.len() as u64
        };
        self.revision.send_replace(revision);
    }

    fn event_count(&self, event: &UdsEvent) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|observed| *observed == event)
            .count()
    }

    async fn wait_for_event(&self, event: UdsEvent) {
        let mut revision = self.revision.subscribe();
        tokio::time::timeout(WAIT_BOUND, async {
            loop {
                if self.event_count(&event) > 0 {
                    return;
                }
                revision
                    .changed()
                    .await
                    .expect("UDS connection event publisher dropped");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for UDS event: {event:?}"));
    }

    async fn wait_for_initialize_release(&self) {
        self.initialize_release
            .subscribe()
            .wait_for(|released| *released)
            .await
            .expect("UDS initialize gate publisher dropped");
    }
}

pub struct UdsFixture {
    _directory: TempDir,
    path: PathBuf,
    controls: Vec<Arc<ConnectionControl>>,
    listener_task: JoinHandle<()>,
}

impl UdsFixture {
    pub fn new(plans: Vec<ConnectionPlan>) -> Self {
        let directory = tempfile::tempdir().expect("UDS fixture directory");
        let path = directory.path().join("runtime.sock");
        let listener = UnixListener::bind(&path).expect("bind UDS fixture");
        let controls: Vec<_> = plans
            .iter()
            .map(|_| Arc::new(ConnectionControl::new()))
            .collect();
        let listener_controls = controls.clone();
        let listener_task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let mut next_connection = 0usize;
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.expect("accept UDS fixture connection");
                        let plan = plans
                            .get(next_connection)
                            .copied()
                            .expect("UDS fixture accepted an unplanned connection");
                        let control = listener_controls[next_connection].clone();
                        let connection_id = next_connection;
                        next_connection += 1;
                        connections.spawn(handle_connection(stream, connection_id, plan, control));
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        completed.expect("UDS fixture connection task disappeared")
                            .expect("UDS fixture connection task failed");
                    }
                }
            }
        });
        Self {
            _directory: directory,
            path,
            controls,
            listener_task,
        }
    }

    pub fn options(&self, startup_timeout: Duration, shutdown_timeout: Duration) -> RuntimeOptions {
        RuntimeOptions::external_uds(self.path.clone(), startup_timeout, shutdown_timeout)
    }

    pub async fn wait_for_event(&self, connection: usize, event: UdsEvent) {
        self.controls[connection].wait_for_event(event).await;
    }

    pub fn event_count(&self, connection: usize, event: UdsEvent) -> usize {
        self.controls[connection].event_count(&event)
    }

    pub fn release_initialize(&self, connection: usize) {
        self.controls[connection]
            .initialize_release
            .send_replace(true);
    }
}

impl Drop for UdsFixture {
    fn drop(&mut self) {
        self.listener_task.abort();
    }
}

async fn handle_connection(
    stream: UnixStream,
    connection_id: usize,
    plan: ConnectionPlan,
    control: Arc<ConnectionControl>,
) {
    control.record(UdsEvent::Accepted);
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let Some(initialize_line) = lines
        .next_line()
        .await
        .expect("read UDS initialize request")
    else {
        finish_connection(&mut write, &control).await;
        return;
    };
    let initialize: Value =
        serde_json::from_str(&initialize_line).expect("parse UDS initialize request");
    assert_eq!(initialize["method"], "protocol.initialize");
    control.record(UdsEvent::InitializeReceived);

    if plan.hold_initialize {
        tokio::select! {
            _ = control.wait_for_initialize_release() => {}
            next = lines.next_line() => {
                match next.expect("read UDS client while initialize is held") {
                    None => {
                        finish_connection(&mut write, &control).await;
                        return;
                    }
                    Some(_) => panic!("business request arrived before initialize ACK"),
                }
            }
        }
    }

    if plan.initialize != InitializeBehavior::NoReply {
        let params: InitializeParams = serde_json::from_value(initialize["params"].clone())
            .expect("parse UDS initialize params");
        let mut result = InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: format!("uds-peer-{connection_id}"),
                version: "fixture".into(),
            },
        )
        .expect("negotiate UDS initialize result");
        if plan.initialize == InitializeBehavior::Incompatible {
            result.protocol_version = u32::MAX;
        }
        write_json(
            &mut write,
            &json!({"jsonrpc":"2.0","id":initialize["id"],"result":result}),
        )
        .await;
        control.record(UdsEvent::InitializeAcked);
    }

    while let Some(line) = lines.next_line().await.expect("read UDS business request") {
        let request: Value = serde_json::from_str(&line).expect("parse UDS business request");
        let result = match request["method"].as_str() {
            Some("session.start_thread") => {
                let model = request["params"]["model"]
                    .as_str()
                    .expect("UDS start-thread model")
                    .to_owned();
                control.record(UdsEvent::BusinessRequest { model });
                let session = control.next_session.fetch_add(1, Ordering::SeqCst);
                json!({
                    "thread_id": format!("uds-{connection_id}-session-{session}"),
                    "created_at": "2026-09-09T00:00:00Z"
                })
            }
            method => panic!("unsupported UDS fixture request: {method:?}"),
        };
        write_json(
            &mut write,
            &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
        )
        .await;
    }
    finish_connection(&mut write, &control).await;
}

async fn finish_connection(
    write: &mut tokio::net::unix::OwnedWriteHalf,
    control: &ConnectionControl,
) {
    control.record(UdsEvent::ClientEof);
    let _ = write.shutdown().await;
    control.record(UdsEvent::ConnectionEnded);
}

async fn write_json(write: &mut tokio::net::unix::OwnedWriteHalf, value: &Value) {
    write
        .write_all(value.to_string().as_bytes())
        .await
        .expect("write UDS fixture response");
    write
        .write_all(b"\n")
        .await
        .expect("terminate UDS fixture response");
    write.flush().await.expect("flush UDS fixture response");
}
