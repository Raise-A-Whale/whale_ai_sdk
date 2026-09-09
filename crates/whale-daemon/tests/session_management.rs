mod common;

use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Semaphore};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::rpc::JSONRPCRequest;
use whale_store::{MemoryStore, SessionRecord, SessionStore, StoreError, StoreRuntime};

struct Capture(mpsc::UnboundedSender<Value>);

#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.0
            .send(serde_json::from_str(line).expect("daemon emits JSON"))
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

fn fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| Ok(Box::pin(futures::stream::pending()))));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(Arc::new(Capture(tx))),
        rx,
    )
}

const PERSISTENT_THREAD: &str = "11111111-1111-4111-8111-111111111111";
const ATTACHED_THREAD: &str = "22222222-2222-4222-8222-222222222222";

struct ControlledStore {
    memory: MemoryStore,
    metadata_mode: AtomicUsize,
    metadata_calls: AtomicUsize,
    create_calls: AtomicUsize,
    fail_detach: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}

impl ControlledStore {
    fn new() -> Self {
        Self {
            memory: MemoryStore::new(),
            metadata_mode: AtomicUsize::new(0),
            metadata_calls: AtomicUsize::new(0),
            create_calls: AtomicUsize::new(0),
            fail_detach: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    fn block_next_metadata(&self) {
        self.metadata_mode.store(1, Ordering::SeqCst);
    }

    fn fail_next_metadata(&self) {
        self.metadata_mode.store(2, Ordering::SeqCst);
    }

    fn fail_next_detach(&self) {
        self.fail_detach.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl SessionStore for ControlledStore {
    fn durable(&self) -> bool {
        false
    }

    async fn create(&self, record: SessionRecord) -> whale_store::Result<()> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        self.memory.create(record).await
    }

    async fn load(&self, id: &str) -> whale_store::Result<Option<SessionRecord>> {
        self.memory.load(id).await
    }

    async fn compare_exchange(
        &self,
        id: &str,
        revision: u64,
        replacement: SessionRecord,
    ) -> whale_store::Result<()> {
        let metadata_write = replacement.owner.is_some()
            && replacement
                .configuration
                .get("metadata")
                .and_then(|metadata| metadata.get("phase"))
                .and_then(Value::as_str)
                .is_some_and(|phase| phase == "committed" || phase == "failed");
        if metadata_write {
            self.metadata_calls.fetch_add(1, Ordering::SeqCst);
            match self.metadata_mode.swap(0, Ordering::SeqCst) {
                1 => {
                    self.entered.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
                2 => return Err(StoreError::Io("injected metadata failure".into())),
                _ => {}
            }
        }
        if replacement.owner.is_none() && self.fail_detach.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Io("injected detach failure".into()));
        }
        self.memory
            .compare_exchange(id, revision, replacement)
            .await
    }

    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.memory.list().await
    }
}

async fn persistent_fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    Arc<ControlledStore>,
) {
    let store = Arc::new(ControlledStore::new());
    let runtime = Arc::new(StoreRuntime::open(store.clone()).await.unwrap());
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| Ok(Box::pin(futures::stream::pending()))));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(Capture(tx)));
    let server = DaemonServer::new(Arc::new(engine), gate).with_store_runtime(runtime);
    common::initialize(&server, &writer, Some(&mut rx)).await;
    (server, writer, rx, store)
}

fn recovery_key() -> Value {
    json!({
        "recovery_id": uuid::Uuid::new_v4().to_string(),
        "secret": format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
    })
}

fn persistent_config(thread_id: &str, phase: &str) -> Value {
    json!({
        "session_id": thread_id,
        "model": "test",
        "provider_config": {"api":"openai_responses","auth":{"type":"none"}},
        "tools": [],
        "metadata": {"phase": phase}
    })
}

async fn direct(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    serde_json::to_value(
        server
            .dispatch_request(
                JSONRPCRequest::new(id, method, Some(params)).unwrap(),
                writer,
            )
            .await,
    )
    .unwrap()
}

async fn send(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: u64,
    method: &str,
    params: Value,
) {
    server
        .handle_message(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
            writer,
        )
        .await;
}

async fn response(rx: &mut mpsc::UnboundedReceiver<Value>, id: u64) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.expect("response");
            if value["id"] == id {
                return value;
            }
        }
    })
    .await
    .expect("response deadline")
}

#[tokio::test]
async fn metadata_and_run_publication_share_one_lane() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"managed","model":"test","metadata":{"version":0}}),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());

    send(
        &server,
        &writer,
        2,
        "session.get.v2",
        json!({"thread_id":"managed","history_limit":8}),
    )
    .await;
    let initial = response(&mut rx, 2).await;
    assert_eq!(initial["result"]["lifecycle"], "open");
    assert_eq!(initial["result"]["cursor"]["seq"], 0);

    send(
        &server,
        &writer,
        3,
        "session.metadata.replace",
        json!({
            "thread_id":"managed",
            "expected_view_revision":0,
            "metadata":{"version":1}
        }),
    )
    .await;
    let replaced = response(&mut rx, 3).await;
    assert_eq!(replaced["result"]["changed"], true);
    assert_eq!(replaced["result"]["cursor"]["seq"], 1);

    send(
        &server,
        &writer,
        4,
        "thread.start_turn",
        json!({"thread_id":"managed","turn_id":"run","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 4).await.get("error").is_none());

    send(
        &server,
        &writer,
        5,
        "session.get",
        json!({"thread_id":"managed","history_limit":8}),
    )
    .await;
    let v1 = response(&mut rx, 5).await;
    assert_eq!(v1["result"]["cursor"]["seq"], 1);
    assert_eq!(v1["result"]["summary"]["metadata"]["version"], 0);

    send(
        &server,
        &writer,
        6,
        "session.get.v2",
        json!({"thread_id":"managed","history_limit":8}),
    )
    .await;
    let v2 = response(&mut rx, 6).await;
    assert_eq!(v2["result"]["cursor"]["seq"], 2);
    assert_eq!(v2["result"]["summary"]["metadata"]["version"], 1);
    assert_eq!(v2["result"]["active_run"]["snapshot"]["turn_id"], "run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_cas_is_single_winner_idempotent_owner_scoped_and_bounded() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"cas","model":"test","metadata":{"value":0}}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get.v2",
        json!({"thread_id":"cas","history_limit":8}),
    )
    .await;
    let initial = response(&mut rx, 2).await;
    let cursor = initial["result"]["cursor"].clone();
    send(
        &server,
        &writer,
        3,
        "session.subscribe.v2",
        json!({"thread_id":"cas","after":cursor,"limit":32}),
    )
    .await;
    assert!(response(&mut rx, 3).await.get("error").is_none());

    tokio::join!(
        send(
            &server,
            &writer,
            4,
            "session.metadata.replace",
            json!({"thread_id":"cas","expected_view_revision":0,"metadata":{"value":1}}),
        ),
        send(
            &server,
            &writer,
            5,
            "session.metadata.replace",
            json!({"thread_id":"cas","expected_view_revision":0,"metadata":{"value":2}}),
        )
    );
    let mut replies = Vec::new();
    let mut metadata_events = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while replies.len() < 2 || metadata_events.is_empty() {
            let value = rx.recv().await.expect("CAS output");
            if value["id"] == 4 || value["id"] == 5 {
                replies.push(value);
            } else if value["method"] == "session.event.v2"
                && value["params"]["type"] == "metadata_changed"
            {
                metadata_events.push(value);
            }
        }
    })
    .await
    .expect("CAS responses and notification");
    assert_eq!(
        replies
            .iter()
            .filter(|reply| reply.get("result").is_some())
            .count(),
        1
    );
    let conflict = replies
        .iter()
        .find(|reply| reply.get("error").is_some())
        .expect("one conflict");
    assert_eq!(conflict["error"]["code"], -32041);
    assert_eq!(conflict["error"]["data"]["kind"], "revision_conflict");
    assert_eq!(conflict["error"]["data"]["current_view_revision"], 1);
    assert_eq!(metadata_events.len(), 1);
    assert_eq!(metadata_events[0]["params"]["cursor"]["seq"], 1);

    let committed = replies
        .iter()
        .find_map(|reply| reply.get("result"))
        .expect("committed result");
    let metadata = committed["summary"]["metadata"].clone();
    send(
        &server,
        &writer,
        6,
        "session.metadata.replace",
        json!({"thread_id":"cas","expected_view_revision":1,"metadata":metadata}),
    )
    .await;
    let equal = response(&mut rx, 6).await;
    assert_eq!(equal["result"]["changed"], false);
    assert_eq!(equal["result"]["cursor"]["seq"], 1);

    send(
        &server,
        &writer,
        7,
        "session.metadata.replace",
        json!({
            "thread_id":"cas",
            "expected_view_revision":1,
            "metadata":{"oversized":"x".repeat(70_000)}
        }),
    )
    .await;
    assert_eq!(response(&mut rx, 7).await["error"]["code"], -32602);
    send(
        &server,
        &writer,
        8,
        "session.get.v2",
        json!({"thread_id":"cas","history_limit":8}),
    )
    .await;
    assert_eq!(response(&mut rx, 8).await["result"]["cursor"]["seq"], 1);

    let (foreign_tx, mut foreign_rx) = mpsc::unbounded_channel();
    let foreign = AnyTransportWriter::new(Arc::new(Capture(foreign_tx)));
    common::initialize(&server, &foreign, Some(&mut foreign_rx)).await;
    send(
        &server,
        &foreign,
        9,
        "session.get.v2",
        json!({"thread_id":"cas","history_limit":8}),
    )
    .await;
    let hidden = response(&mut foreign_rx, 9).await;
    assert_eq!(hidden["error"]["code"], -32040);
    assert_eq!(hidden["error"]["data"]["kind"], "unavailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_close_waiter_replays_closing_terminal_closed_and_retains_tombstone() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"closing","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get.v2",
        json!({"thread_id":"closing","history_limit":8}),
    )
    .await;
    let initial = response(&mut rx, 2).await;
    send(
        &server,
        &writer,
        3,
        "session.subscribe.v2",
        json!({"thread_id":"closing","after":initial["result"]["cursor"],"limit":32}),
    )
    .await;
    response(&mut rx, 3).await;
    send(
        &server,
        &writer,
        4,
        "thread.start_turn",
        json!({"thread_id":"closing","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 4).await;

    let first_server = server.clone();
    let first_writer = writer.clone();
    let first = tokio::spawn(async move {
        first_server
            .dispatch_request(
                JSONRPCRequest::new(5, "session.close", Some(json!({"thread_id":"closing"})))
                    .unwrap(),
                &first_writer,
            )
            .await
    });

    let mut ordered = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.expect("Closing notification");
            if value["method"] == "session.event.v2" {
                let kind = value["params"]["type"].as_str().unwrap().to_owned();
                let lifecycle = value["params"]["lifecycle"].as_str().map(str::to_owned);
                ordered.push((
                    kind,
                    lifecycle,
                    value["params"]["cursor"]["seq"].as_u64().unwrap(),
                ));
                if value["params"]["type"] == "lifecycle_changed"
                    && value["params"]["lifecycle"] == "closing"
                {
                    break;
                }
            }
        }
    })
    .await
    .expect("Closing became observable");
    first.abort();

    let joined = server
        .dispatch_request(
            JSONRPCRequest::new(6, "session.close", Some(json!({"thread_id":"closing"}))).unwrap(),
            &writer,
        )
        .await;
    assert!(joined.error.is_none());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !ordered
            .iter()
            .any(|(_, lifecycle, _)| lifecycle.as_deref() == Some("closed"))
        {
            let value = rx.recv().await.expect("terminal V2 notifications");
            if value["method"] == "session.event.v2" {
                ordered.push((
                    value["params"]["type"].as_str().unwrap().to_owned(),
                    value["params"]["lifecycle"].as_str().map(str::to_owned),
                    value["params"]["cursor"]["seq"].as_u64().unwrap(),
                ));
            }
        }
    })
    .await
    .expect("Closed became observable");
    assert!(ordered.windows(2).all(|pair| pair[0].2 < pair[1].2));
    let closing = ordered
        .iter()
        .position(|(_, lifecycle, _)| lifecycle.as_deref() == Some("closing"))
        .unwrap();
    let closed = ordered
        .iter()
        .position(|(_, lifecycle, _)| lifecycle.as_deref() == Some("closed"))
        .unwrap();
    assert!(
        closed > closing + 1,
        "terminal Run projection must precede Closed"
    );
    assert_eq!(
        closed,
        ordered.len() - 1,
        "Closed must be the last V2 event"
    );

    send(
        &server,
        &writer,
        7,
        "session.get.v2",
        json!({"thread_id":"closing","history_limit":8}),
    )
    .await;
    let tombstone = response(&mut rx, 7).await;
    assert_eq!(tombstone["result"]["lifecycle"], "closed");
    assert!(tombstone["result"]["active_run"].is_null());
    let final_cursor = tombstone["result"]["cursor"].clone();

    send(
        &server,
        &writer,
        8,
        "session.subscribe.v2",
        json!({"thread_id":"closing","after":final_cursor,"limit":32}),
    )
    .await;
    let terminal_page = response(&mut rx, 8).await;
    assert_eq!(terminal_page["result"]["events"], json!([]));
    assert_eq!(terminal_page["result"]["lifecycle"], "closed");
    assert_eq!(terminal_page["result"]["has_more"], false);

    send(
        &server,
        &writer,
        9,
        "session.metadata.replace",
        json!({"thread_id":"closing","expected_view_revision":final_cursor["seq"],"metadata":{}}),
    )
    .await;
    let rejected = response(&mut rx, 9).await;
    assert_eq!(rejected["error"]["code"], -32040);
    assert_eq!(rejected["error"]["data"]["kind"], "session_not_open");
    assert_eq!(rejected["error"]["data"]["lifecycle"], "closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_persistent_metadata_waiter_commits_once_and_attach_reads_stored_value() {
    let (server, writer, _rx, backend) = persistent_fixture().await;
    let key = recovery_key();
    let created = direct(
        &server,
        &writer,
        1,
        "session.create_persistent",
        json!({"key":key,"session":persistent_config(PERSISTENT_THREAD,"initial")}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");

    backend.block_next_metadata();
    let first_server = server.clone();
    let first_writer = writer.clone();
    let first = tokio::spawn(async move {
        direct(
            &first_server,
            &first_writer,
            2,
            "session.metadata.replace",
            json!({
                "thread_id":PERSISTENT_THREAD,
                "expected_view_revision":0,
                "metadata":{"phase":"committed"}
            }),
        )
        .await
    });
    let entered =
        tokio::time::timeout(std::time::Duration::from_secs(2), backend.entered.acquire())
            .await
            .expect("metadata Store CAS entered")
            .unwrap();
    entered.forget();

    let readable = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        direct(
            &server,
            &writer,
            3,
            "session.get.v2",
            json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
        ),
    )
    .await
    .expect("Store await holds no projection mutex");
    assert_eq!(readable["result"]["cursor"]["seq"], 0);
    assert_eq!(
        readable["result"]["summary"]["metadata"]["phase"],
        "initial"
    );

    first.abort();
    backend.release.add_permits(1);
    let committed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = direct(
                &server,
                &writer,
                4,
                "session.get.v2",
                json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
            )
            .await;
            if snapshot["result"]["cursor"]["seq"] == 1 {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned metadata transaction completes after caller cancellation");
    assert_eq!(
        committed["result"]["summary"]["metadata"]["phase"],
        "committed"
    );
    assert_eq!(backend.metadata_calls.load(Ordering::SeqCst), 1);

    let stale = direct(
        &server,
        &writer,
        5,
        "session.metadata.replace",
        json!({
            "thread_id":PERSISTENT_THREAD,
            "expected_view_revision":0,
            "metadata":{"phase":"duplicate"}
        }),
    )
    .await;
    assert_eq!(stale["error"]["code"], -32041);
    assert_eq!(stale["error"]["data"]["current_view_revision"], 1);
    assert_eq!(backend.metadata_calls.load(Ordering::SeqCst), 1);
    let stored = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.configuration["metadata"]["phase"], "committed");

    let closed = direct(
        &server,
        &writer,
        6,
        "session.close",
        json!({"thread_id":PERSISTENT_THREAD}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    let inspected = direct(
        &server,
        &writer,
        7,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    let attached = direct(
        &server,
        &writer,
        8,
        "session.recovery.attach",
        json!({
            "key":key,
            "expected_revision":inspected["result"]["revision"],
            "session":persistent_config(ATTACHED_THREAD,"stale-caller")
        }),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    let recovered = direct(
        &server,
        &writer,
        9,
        "session.get.v2",
        json!({"thread_id":ATTACHED_THREAD,"history_limit":8}),
    )
    .await;
    assert_eq!(
        recovered["result"]["summary"]["metadata"]["phase"],
        "committed"
    );
}

#[tokio::test]
async fn durable_metadata_failure_advances_no_cursor_or_event() {
    let (server, writer, mut rx, backend) = persistent_fixture().await;
    let mut oversized = persistent_config(ATTACHED_THREAD, "initial");
    oversized["metadata"] = json!({"oversized":"x".repeat(70_000)});
    let rejected = direct(
        &server,
        &writer,
        0,
        "session.create_persistent",
        json!({"key":recovery_key(),"session":oversized}),
    )
    .await;
    assert_eq!(
        rejected["error"]["code"],
        whale_protocol::retention::SESSION_LIMIT_EXCEEDED
    );
    assert_eq!(backend.create_calls.load(Ordering::SeqCst), 0);

    let key = recovery_key();
    let created = direct(
        &server,
        &writer,
        1,
        "session.create_persistent",
        json!({"key":key,"session":persistent_config(PERSISTENT_THREAD,"initial")}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    assert_eq!(backend.create_calls.load(Ordering::SeqCst), 1);
    let initial = direct(
        &server,
        &writer,
        2,
        "session.get.v2",
        json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
    )
    .await;
    let subscribed = direct(
        &server,
        &writer,
        3,
        "session.subscribe.v2",
        json!({
            "thread_id":PERSISTENT_THREAD,
            "after":initial["result"]["cursor"],
            "limit":32
        }),
    )
    .await;
    assert!(subscribed.get("error").is_none(), "{subscribed}");

    backend.fail_next_metadata();
    let failed = direct(
        &server,
        &writer,
        4,
        "session.metadata.replace",
        json!({
            "thread_id":PERSISTENT_THREAD,
            "expected_view_revision":0,
            "metadata":{"phase":"failed"}
        }),
    )
    .await;
    assert_eq!(
        failed["error"]["code"],
        whale_protocol::recovery::STORE_FAILED
    );
    assert_eq!(failed["error"]["data"]["kind"], "storage_failure");
    let snapshot = direct(
        &server,
        &writer,
        5,
        "session.get.v2",
        json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
    )
    .await;
    assert_eq!(snapshot["result"]["cursor"]["seq"], 0);
    assert_eq!(
        snapshot["result"]["summary"]["metadata"]["phase"],
        "initial"
    );
    assert!(
        rx.try_recv().is_err(),
        "failed Store CAS must publish no V2 event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_persistent_detach_never_publishes_closed_and_fails_owner_connection() {
    let (server, writer, mut rx, backend) = persistent_fixture().await;
    let key = recovery_key();
    let _created = direct(
        &server,
        &writer,
        1,
        "session.create_persistent",
        json!({"key":key,"session":persistent_config(PERSISTENT_THREAD,"initial")}),
    )
    .await;
    let initial = direct(
        &server,
        &writer,
        2,
        "session.get.v2",
        json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
    )
    .await;
    let subscribed = direct(
        &server,
        &writer,
        3,
        "session.subscribe.v2",
        json!({
            "thread_id":PERSISTENT_THREAD,
            "after":initial["result"]["cursor"],
            "limit":32
        }),
    )
    .await;
    assert!(subscribed.get("error").is_none(), "{subscribed}");
    backend.fail_next_detach();
    let failed = direct(
        &server,
        &writer,
        4,
        "session.close",
        json!({"thread_id":PERSISTENT_THREAD}),
    )
    .await;
    assert_eq!(
        failed["error"]["code"],
        whale_protocol::recovery::STORE_FAILED
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let after = direct(
                &server,
                &writer,
                5,
                "session.get.v2",
                json!({"thread_id":PERSISTENT_THREAD,"history_limit":8}),
            )
            .await;
            if after["error"]["message"] == "ConnectionClosed" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unknown detach outcome fails the owner connection");
    let mut lifecycles = Vec::new();
    while let Ok(value) = rx.try_recv() {
        if value["method"] == "session.event.v2" && value["params"]["type"] == "lifecycle_changed" {
            lifecycles.push(value["params"]["lifecycle"].clone());
        }
    }
    assert!(lifecycles.contains(&json!("closing")));
    assert!(!lifecycles.contains(&json!("closed")));
    let stored = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        stored.owner.is_some(),
        "failed detach must not forge durable detach"
    );
}
