mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_store::{MemoryStore, StoreRuntime};
struct Capture(mpsc::UnboundedSender<Value>);
#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.0
            .send(serde_json::from_str(line).unwrap())
            .map_err(|_| std::io::Error::other("closed"))
    }
}
fn key() -> Value {
    json!({"recovery_id":uuid::Uuid::new_v4().to_string(),"secret":format!("{}{}",uuid::Uuid::new_v4().simple(),uuid::Uuid::new_v4().simple())})
}
fn config(id: &str) -> Value {
    json!({"session_id":id,"model":"test","provider_config":{"api":"openai_responses","auth":{"type":"none"}},"tools":[]})
}
fn connection() -> (AnyTransportWriter, mpsc::UnboundedReceiver<Value>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (AnyTransportWriter::new(Arc::new(Capture(tx))), rx)
}
async fn fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let runtime = Arc::new(
        StoreRuntime::open(Arc::new(MemoryStore::new()))
            .await
            .unwrap(),
    );
    let server = DaemonServer::default_server().with_store_runtime(runtime);
    let (writer, mut rx) = connection();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    (server, writer, rx)
}
async fn rpc(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    method: &str,
    params: Value,
) -> Value {
    server
        .handle_message(
            &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
            writer,
        )
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let v = rx.recv().await.unwrap();
            if v["id"] == 1 {
                return v;
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn optional_capability_requires_configured_store() {
    for configured in [false, true] {
        let server = if configured {
            DaemonServer::default_server().with_store_runtime(Arc::new(
                StoreRuntime::open(Arc::new(MemoryStore::new()))
                    .await
                    .unwrap(),
            ))
        } else {
            DaemonServer::default_server()
        };
        let (writer, mut rx) = connection();
        let result=rpc(&server,&writer,&mut rx,"protocol.initialize",json!({"client":{"name":"test","version":"1"},"protocol_versions":[1],"required_capabilities":[]})).await;
        assert_eq!(
            result["result"]["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!("session_recovery.v1")),
            configured
        );
    }
}
#[tokio::test]
async fn create_close_attach_retains_data_and_uses_fresh_identity() {
    let (server, writer, mut rx) = fixture().await;
    let key = key();
    let created=rpc(&server,&writer,&mut rx,"session.create_persistent",json!({"key":key,"session":config("11111111-1111-4111-8111-111111111111"),"run_defaults":{"max_steps":3,"timeout_ms":123}})).await;
    assert!(created.get("error").is_none(), "{created}");
    assert_eq!(
        created["result"]["thread"]["thread_id"],
        "11111111-1111-4111-8111-111111111111"
    );
    let inspected = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    assert_eq!(inspected["result"]["attached"], true);
    assert!(!inspected["result"]
        .to_string()
        .contains(key["secret"].as_str().unwrap()));
    let closed = rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":"11111111-1111-4111-8111-111111111111"}),
    )
    .await;
    assert_eq!(closed["result"]["closed"], true);
    assert!(server.sessions().is_empty());
    let inspected = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    assert_eq!(inspected["result"]["attached"], false);
    let attached=rpc(&server,&writer,&mut rx,"session.recovery.attach",json!({"key":key,"expected_revision":inspected["result"]["revision"],"session":config("22222222-2222-4222-8222-222222222222"),"run_defaults":{"max_steps":3,"timeout_ms":123}})).await;
    assert!(attached.get("error").is_none(), "{attached}");
    assert_eq!(
        attached["result"]["thread"]["thread_id"],
        "22222222-2222-4222-8222-222222222222"
    );
    assert!(attached["result"]["epoch"].as_u64() > created["result"]["epoch"].as_u64());
    assert!(!server
        .sessions()
        .contains_key("11111111-1111-4111-8111-111111111111"));
    assert!(server
        .sessions()
        .contains_key("22222222-2222-4222-8222-222222222222"));
    server.disconnect_connection(&writer).await;
}
#[tokio::test]
async fn rejected_attachment_does_not_claim_lease_or_publish_session() {
    let (server, writer, mut rx) = fixture().await;
    let key = key();
    assert!(rpc(&server,&writer,&mut rx,"session.create_persistent",json!({"key":key,"session":config("11111111-1111-4111-8111-111111111111"),"run_defaults":{"max_steps":10}})).await.get("error").is_none());
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":"11111111-1111-4111-8111-111111111111"}),
    )
    .await;
    let state = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    let mut mismatch = config("22222222-2222-4222-8222-222222222222");
    mismatch["model"] = json!("different");
    let rejected=rpc(&server,&writer,&mut rx,"session.recovery.attach",json!({"key":key,"expected_revision":state["result"]["revision"],"session":mismatch,"run_defaults":{"max_steps":10}})).await;
    assert!(rejected.get("error").is_some());
    assert!(server.sessions().is_empty());
    let after = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    assert_eq!(after["result"], state["result"]);
}

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use whale_store::{SessionRecord, SessionStore, StoreError};
struct ControlledStore {
    memory: MemoryStore,
    gate: AtomicUsize,
    fail: AtomicBool,
    conflict: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl ControlledStore {
    fn new() -> Self {
        Self {
            memory: MemoryStore::new(),
            gate: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            conflict: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
    fn arm(&self, kind: usize, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
        self.gate.store(kind, Ordering::SeqCst);
    }
    async fn intercept(&self, kind: usize) -> whale_store::Result<()> {
        if self
            .gate
            .compare_exchange(kind, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            if self.conflict.load(Ordering::SeqCst) {
                return Err(StoreError::Conflict);
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(StoreError::Io("injected durable write failure".into()));
            }
        }
        Ok(())
    }
}
#[async_trait]
impl SessionStore for ControlledStore {
    fn durable(&self) -> bool {
        false
    }
    async fn create(&self, record: SessionRecord) -> whale_store::Result<()> {
        self.intercept(1).await?;
        self.memory.create(record).await
    }
    async fn load(&self, id: &str) -> whale_store::Result<Option<SessionRecord>> {
        self.memory.load(id).await
    }
    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.memory.list().await
    }
    async fn compare_exchange(
        &self,
        id: &str,
        revision: u64,
        replacement: SessionRecord,
    ) -> whale_store::Result<()> {
        let kind = if replacement
            .runs
            .values()
            .any(|r| r.snapshot.status.is_terminal())
        {
            3
        } else if !replacement.runs.is_empty() {
            2
        } else {
            4
        };
        self.intercept(kind).await?;
        self.memory
            .compare_exchange(id, revision, replacement)
            .await
    }
}
async fn controlled(
    backend: Arc<ControlledStore>,
    completed: bool,
) -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    Arc<AtomicUsize>,
) {
    use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |_, _| {
        seen.fetch_add(1, Ordering::SeqCst);
        if completed {
            Ok(Box::pin(futures::stream::iter([Ok(
                whale_protocol::AgentStreamEvent::TurnCompleted {
                    thread_id: "provider".into(),
                    turn_id: "provider".into(),
                    usage: Default::default(),
                },
            )])))
        } else {
            Ok(Box::pin(futures::stream::pending()))
        }
    }));
    let runtime = Arc::new(StoreRuntime::open(backend).await.unwrap());
    let server = DaemonServer::new(Arc::new(engine), gate).with_store_runtime(runtime);
    let (writer, mut rx) = connection();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    (server, writer, rx, calls)
}
fn spawn_rpc(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: i64,
    method: &str,
    params: Value,
) -> tokio::task::JoinHandle<()> {
    let server = server.clone();
    let writer = writer.clone();
    let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string();
    tokio::spawn(async move { server.handle_message(&request, &writer).await })
}
async fn received(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let v = rx.recv().await.unwrap();
            if predicate(&v) {
                return v;
            }
        }
    })
    .await
    .unwrap()
}
const LIVE: &str = "11111111-1111-4111-8111-111111111111";
#[tokio::test]
async fn recovered_session_view_uses_fresh_stream_safe_metadata_and_canonical_history() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
    let key = key();
    let mut original = config(LIVE);
    original["agent_name"] = json!("safe-agent");
    original["metadata"] = json!({"label":"visible"});
    let created = rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":original}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    let durable = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.configuration["version"], 3);
    assert_eq!(
        durable.configuration["runtime_features"]["interactions_enabled"],
        false
    );
    assert_eq!(durable.configuration["metadata"]["label"], "visible");
    assert!(durable.configuration["session"].get("metadata").is_none());
    let started = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({
            "thread_id":LIVE,"turn_id":"view-run",
            "input_items":[serde_json::to_value(
                whale_protocol::CanonicalItem::user_text("remember me")
            ).unwrap()]
        }),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    received(&mut rx, |value| {
        value["method"] == "turn.event"
            && value["params"]["type"] == "finished"
            && value["params"]["turn_id"] == "view-run"
    })
    .await;
    let old_view = rpc(
        &server,
        &writer,
        &mut rx,
        "session.get",
        json!({"thread_id":LIVE,"history_limit":32}),
    )
    .await["result"]
        .clone();
    assert!(old_view["history"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["type"] == "user_message"));
    let old_stream = old_view["cursor"]["stream_id"].clone();
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":LIVE}),
    )
    .await;
    let inspected = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;

    let replacement_id = "22222222-2222-4222-8222-222222222222";
    let mut replacement = config(replacement_id);
    replacement["agent_name"] = json!("safe-agent");
    replacement["metadata"] = json!({"label":"stale-caller"});
    let attached = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.attach",
        json!({
            "key":key,"expected_revision":inspected["result"]["revision"],
            "session":replacement
        }),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    let view = rpc(
        &server,
        &writer,
        &mut rx,
        "session.get",
        json!({"thread_id":replacement_id,"history_limit":32}),
    )
    .await["result"]
        .clone();
    assert_eq!(view["summary"]["agent_name"], "safe-agent");
    assert_eq!(view["summary"]["metadata"]["label"], "visible");
    assert_eq!(view["cursor"]["seq"], 0);
    assert_ne!(view["cursor"]["stream_id"], old_stream);
    assert!(view["history"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["type"] == "user_message"));
    let serialized = view.to_string();
    assert!(!serialized.contains(key["secret"].as_str().unwrap()));
    assert!(!serialized.contains(key["recovery_id"].as_str().unwrap()));
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn creation_commit_precedes_ack_and_failure_publishes_no_session() {
    for fail in [false, true] {
        let backend = Arc::new(ControlledStore::new());
        let (server, writer, mut rx, _) = controlled(backend.clone(), false).await;
        let key = key();
        backend.arm(1, fail);
        let creating = spawn_rpc(
            &server,
            &writer,
            10,
            "session.create_persistent",
            json!({"key":key,"session":config(LIVE)}),
        );
        backend.entered.notified().await;
        assert!(!creating.is_finished());
        assert!(rx.try_recv().is_err());
        assert!(server.sessions().is_empty());
        assert!(backend
            .load(key["recovery_id"].as_str().unwrap())
            .await
            .unwrap()
            .is_none());
        backend.release.add_permits(1);
        creating.await.unwrap();
        let response = received(&mut rx, |v| v["id"] == 10).await;
        if fail {
            assert_eq!(response["error"]["code"], -32022);
            assert!(server.sessions().is_empty());
        } else {
            assert!(response.get("error").is_none());
            assert!(backend
                .load(key["recovery_id"].as_str().unwrap())
                .await
                .unwrap()
                .is_some());
        }
        server.disconnect_connection(&writer).await;
    }
}
#[tokio::test]
async fn eof_during_creation_joins_commit_and_detaches_without_publication() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, _) = controlled(backend.clone(), false).await;
    let key = key();
    backend.arm(1, false);
    let creating = spawn_rpc(
        &server,
        &writer,
        10,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    );
    backend.entered.notified().await;
    let mut disconnect = tokio::spawn({
        let s = server.clone();
        let w = writer.clone();
        async move { s.disconnect_connection(&w).await }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut disconnect)
            .await
            .is_err()
    );
    backend.release.add_permits(1);
    creating.await.unwrap();
    disconnect.await.unwrap();
    assert!(received(&mut rx, |v| v["id"] == 10)
        .await
        .get("error")
        .is_some());
    assert!(server.sessions().is_empty());
    let record = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(record.owner.is_none());
}
#[tokio::test]
async fn accepted_run_is_durable_before_ack_or_provider_dispatch() {
    for fail in [false, true] {
        let backend = Arc::new(ControlledStore::new());
        let (server, writer, mut rx, calls) = controlled(backend.clone(), false).await;
        let key = key();
        assert!(rpc(
            &server,
            &writer,
            &mut rx,
            "session.create_persistent",
            json!({"key":key,"session":config(LIVE)})
        )
        .await
        .get("error")
        .is_none());
        backend.arm(2, fail);
        let starting = spawn_rpc(
            &server,
            &writer,
            10,
            "thread.start_turn",
            json!({"thread_id":LIVE,"turn_id":"run","input_items":[]}),
        );
        backend.entered.notified().await;
        assert!(!starting.is_finished());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(rx.try_recv().is_err());
        backend.release.add_permits(1);
        starting.await.unwrap();
        let response = received(&mut rx, |v| v["id"] == 10).await;
        if fail {
            assert_eq!(response["error"]["code"], -32022);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            assert!(response.get("error").is_none());
            let record = backend
                .load(key["recovery_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert!(record.runs.contains_key("run"));
        }
        server.disconnect_connection(&writer).await;
    }
}
#[tokio::test]
async fn durable_terminal_precedes_finished_and_store_failure_cannot_report_success() {
    for fail in [false, true] {
        let backend = Arc::new(ControlledStore::new());
        let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
        let key = key();
        assert!(rpc(
            &server,
            &writer,
            &mut rx,
            "session.create_persistent",
            json!({"key":key,"session":config(LIVE)})
        )
        .await
        .get("error")
        .is_none());
        backend.arm(3, fail);
        let starting = spawn_rpc(
            &server,
            &writer,
            10,
            "thread.start_turn",
            json!({"thread_id":LIVE,"turn_id":"run","input_items":[]}),
        );
        starting.await.unwrap();
        assert!(received(&mut rx, |v| v["id"] == 10)
            .await
            .get("error")
            .is_none());
        backend.entered.notified().await;
        while let Ok(value) = rx.try_recv() {
            assert_ne!(value["params"]["type"], "finished");
        }
        assert!(!backend
            .load(key["recovery_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap()
            .runs["run"]
            .snapshot
            .status
            .is_terminal());
        backend.release.add_permits(1);
        let event = received(&mut rx, |v| v["params"]["type"] == "finished").await;
        if fail {
            assert_eq!(event["params"]["snapshot"]["status"], "failed");
            assert_eq!(event["params"]["snapshot"]["error"]["code"], "STORE_FAILED");
            let rejected = rpc(
                &server,
                &writer,
                &mut rx,
                "thread.start_turn",
                json!({"thread_id":LIVE,"turn_id":"next","input_items":[]}),
            )
            .await;
            assert_eq!(rejected["error"]["code"], -32022);
        } else {
            let record = backend
                .load(key["recovery_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(&record.runs["run"].snapshot).unwrap(),
                event["params"]["snapshot"]
            );
        }
        server.disconnect_connection(&writer).await;
    }
}
fn tool() -> Value {
    json!({"name":"lookup","description":"look up","parameters":{"type":"object"},"is_host_tool":true,"binding_id":"local-only"})
}
#[tokio::test]
async fn close_during_registration_joins_commit_and_reports_uncertain_registration() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
    let key = key();
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)})
    )
    .await
    .get("error")
    .is_none());
    backend.arm(4, false);
    let registration = spawn_rpc(
        &server,
        &writer,
        10,
        "session.register_tools",
        json!({"thread_id":LIVE,"tools":[tool()]}),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        backend.entered.notified(),
    )
    .await
    .expect("registration must reach durable commit");
    assert!(!registration.is_finished());
    let closing = spawn_rpc(
        &server,
        &writer,
        11,
        "session.close",
        json!({"thread_id":LIVE}),
    );
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!closing.is_finished());
    backend.release.add_permits(1);
    registration.await.unwrap();
    closing.await.unwrap();
    let mut replies = std::collections::HashMap::new();
    for _ in 0..2 {
        let value = rx.recv().await.unwrap();
        replies.insert(value["id"].as_u64().unwrap(), value);
    }
    assert_eq!(replies[&10]["error"]["code"], -32022, "{:?}", replies);
    assert!(replies[&11].get("error").is_none());
    let record = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(record.owner.is_none());
    assert_eq!(
        record.configuration["session"]["tools"][0]["name"],
        "lookup"
    );
    assert!(record.configuration["session"]["tools"][0]
        .get("binding_id")
        .is_none());
}
#[tokio::test]
async fn registration_is_durable_before_ack_and_io_failure_preserves_old_configuration() {
    for fail in [false, true] {
        let backend = Arc::new(ControlledStore::new());
        let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
        let key = key();
        rpc(
            &server,
            &writer,
            &mut rx,
            "session.create_persistent",
            json!({"key":key,"session":config(LIVE)}),
        )
        .await;
        backend.arm(4, fail);
        let registering = spawn_rpc(
            &server,
            &writer,
            10,
            "session.register_tools",
            json!({"thread_id":LIVE,"tools":[tool()]}),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            backend.entered.notified(),
        )
        .await
        .unwrap();
        assert!(!registering.is_finished());
        assert!(rx.try_recv().is_err());
        assert_eq!(
            backend
                .load(key["recovery_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap()
                .configuration["session"]["tools"],
            json!([])
        );
        backend.release.add_permits(1);
        registering.await.unwrap();
        let response = received(&mut rx, |v| v["id"] == 10).await;
        if fail {
            assert_eq!(response["error"]["code"], -32022);
        } else {
            assert_eq!(response["result"]["registered_count"], 1);
        }
        let record = backend
            .load(key["recovery_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.configuration["session"]["tools"]
                .as_array()
                .unwrap()
                .len(),
            usize::from(!fail)
        );
        if fail {
            let closed = rpc(
                &server,
                &writer,
                &mut rx,
                "session.close",
                json!({"thread_id":LIVE}),
            )
            .await;
            assert_eq!(closed["error"]["code"], -32022);
        }
        server.disconnect_connection(&writer).await;
    }
}
#[tokio::test]
async fn recovery_requires_key_revision_detachment_and_never_reuses_live_identity() {
    let (server, writer, mut rx) = fixture().await;
    let key = key();
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    )
    .await;
    let snapshot = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await["result"]
        .clone();
    let mut wrong = key.clone();
    wrong["secret"] = json!("0".repeat(64));
    let bad = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":wrong}),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32021);
    let active = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.forget",
        json!({"key":key,"expected_revision":snapshot["revision"]}),
    )
    .await;
    assert_eq!(active["error"]["code"], -32021);
    let (foreign, mut other_rx) = connection();
    common::initialize(&server, &foreign, Some(&mut other_rx)).await;
    let foreign_ack = rpc(
        &server,
        &foreign,
        &mut other_rx,
        "session.recovery.acknowledge",
        json!({"key":key,"expected_revision":snapshot["revision"],"execution_ids":["unknown"]}),
    )
    .await;
    assert_eq!(foreign_ack["error"]["code"], -32021);
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":LIVE}),
    )
    .await;
    let detached = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await["result"]
        .clone();
    let reused = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.attach",
        json!({"key":key,"expected_revision":detached["revision"],"session":config(LIVE)}),
    )
    .await;
    assert_eq!(reused["error"]["code"], -32021);
    let stale = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.forget",
        json!({"key":key,"expected_revision":snapshot["revision"]}),
    )
    .await;
    assert_eq!(stale["error"]["code"], -32021);
    let forget = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.forget",
        json!({"key":key,"expected_revision":detached["revision"]}),
    )
    .await;
    assert_eq!(forget["result"]["forgotten"], true);
    let recreated = rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config("44444444-4444-4444-8444-444444444444")}),
    )
    .await;
    assert_eq!(recreated["error"]["code"], -32021);
    server.disconnect_connection(&writer).await;
    server.disconnect_connection(&foreign).await;
}
#[tokio::test]
async fn abandoned_creation_waiter_finishes_transaction_and_detaches_its_lease() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
    let key = key();
    backend.arm(1, false);
    let creating = spawn_rpc(
        &server,
        &writer,
        10,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    );
    backend.entered.notified().await;
    creating.abort();
    let _ = creating.await;
    // Join the cleanup independently of the abandoned transport handler.
    let cleanup = {
        let server = server.clone();
        let writer = writer.clone();
        tokio::spawn(async move { server.disconnect_connection(&writer).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!cleanup.is_finished());
    backend.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(2), cleanup)
        .await
        .unwrap()
        .unwrap();
    assert!(server.sessions().get(LIVE).is_none());
    assert!(backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap()
        .owner
        .is_none());
    assert!(rx.try_recv().is_err());
}
#[tokio::test]
async fn unknown_acknowledgment_is_owned_exact_and_revision_scoped_before_new_dispatch() {
    use whale_protocol::{
        contexts::ToolExecutionRecord,
        recovery::RecoveryKey,
        runs::{RunSnapshot, StartTurnParams},
        CanonicalItem,
    };
    use whale_store::RunMutation;
    let backend = Arc::new(MemoryStore::new());
    let runtime = Arc::new(StoreRuntime::open(backend).await.unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let invocations = calls.clone();
    let gate = Arc::new(whale_core::ApprovalGate::new());
    let engine = whale_core::AgentEngine::new(Arc::new(whale_core::ToolExecutionCoordinator::new(
        Arc::new(whale_core::ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |_, _| {
        invocations.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::iter(vec![Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                thread_id: "ignored".into(),
                turn_id: "ignored".into(),
                usage: Default::default(),
            },
        )])))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate).with_store_runtime(runtime.clone());
    let (writer, mut rx) = connection();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    let key = RecoveryKey::new();
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    )
    .await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":LIVE}),
    )
    .await;
    let detached = runtime.inspect(&key).await.unwrap();
    let journal = runtime
        .attach(
            &key,
            detached.revision,
            detached.configuration,
            "interrupted-owner".into(),
            "22222222-2222-4222-8222-222222222222".into(),
        )
        .await
        .unwrap();
    let params:StartTurnParams=serde_json::from_value(json!({"thread_id":"22222222-2222-4222-8222-222222222222","turn_id":"archived","input_items":[]})).unwrap();
    let snapshot:RunSnapshot=serde_json::from_value(json!({"thread_id":params.thread_id,"turn_id":"archived","status":"running","items":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0},"pending_approvals":[],"tool_executions":[],"last_seq":0})).unwrap();
    journal
        .begin_run(params, snapshot, json!({}))
        .await
        .unwrap();
    journal
        .commit(
            "archived",
            RunMutation::ModelInput {
                step_id: "step".into(),
                step_index: 0,
                request: json!({"projected":"saved input"}),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "archived",
            RunMutation::ModelItem {
                step_id: "step".into(),
                item: CanonicalItem::ToolCall {
                    id: "call-item".into(),
                    call_id: "call".into(),
                    name: "lookup".into(),
                    namespace: None,
                    arguments: Some(json!({})),
                    raw_arguments: "{}".into(),
                },
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "archived",
            RunMutation::ModelStepFinished {
                step_id: "step".into(),
                usage: Default::default(),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "archived",
            RunMutation::DispatchIntent {
                call_id: "call".into(),
                execution: ToolExecutionRecord {
                    call_id: "call".into(),
                    original_arguments: json!({}),
                    arguments: json!({}),
                },
            },
        )
        .await
        .unwrap();
    journal.detach().await.unwrap();
    let archived = runtime.inspect(&key).await.unwrap();
    assert_eq!(archived.unknown_executions.len(), 1);
    let fresh = "33333333-3333-4333-8333-333333333333";
    let attached = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.attach",
        json!({"key":key,"expected_revision":archived.revision,"session":config(fresh)}),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        runtime.inspect(&key).await.unwrap().runs[0].snapshot,
        archived.runs[0].snapshot
    );
    let old = rpc(
        &server,
        &writer,
        &mut rx,
        "turn.get",
        json!({"thread_id":fresh,"turn_id":"archived"}),
    )
    .await;
    assert_eq!(old["error"]["message"], "RunNotFound");
    let blocked = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":fresh,"turn_id":"new","input_items":[]}),
    )
    .await;
    assert_eq!(blocked["error"]["code"], -32021);
    let current = runtime.inspect(&key).await.unwrap();
    let execution = current.unknown_executions[0].execution_id.clone();
    for (revision, ids) in [
        (archived.revision, json!([execution])),
        (current.revision, json!(["wrong"])),
    ] {
        let rejected = rpc(
            &server,
            &writer,
            &mut rx,
            "session.recovery.acknowledge",
            json!({"key":key,"expected_revision":revision,"execution_ids":ids}),
        )
        .await;
        assert_eq!(rejected["error"]["code"], -32021);
    }
    let acknowledged = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.acknowledge",
        json!({"key":key,"expected_revision":current.revision,"execution_ids":[execution]}),
    )
    .await;
    assert_eq!(
        acknowledged["result"]["unknown_executions"][0]["acknowledged"], true,
        "{acknowledged}"
    );
    let started = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":fresh,"turn_id":"new","input_items":[]}),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let final_event = received(&mut rx, |v| v["params"]["type"] == "finished").await;
    assert_eq!(
        final_event["params"]["snapshot"]["status"], "completed",
        "{final_event}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn journal_cas_failure_is_uncertain_storage_error_not_retryable_rejection() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, calls) = controlled(backend.clone(), true).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key(),"session":config(LIVE)}),
    )
    .await;
    backend.arm(2, false);
    backend.conflict.store(true, Ordering::SeqCst);
    let starting = spawn_rpc(
        &server,
        &writer,
        10,
        "thread.start_turn",
        json!({"thread_id":LIVE,"turn_id":"run","input_items":[]}),
    );
    backend.entered.notified().await;
    backend.release.add_permits(1);
    starting.await.unwrap();
    let response = received(&mut rx, |v| v["id"] == 10).await;
    assert_eq!(response["error"]["code"], -32022);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.disconnect_connection(&writer).await;
}
#[tokio::test]
async fn invalid_registration_cannot_make_durable_configuration_unattachable() {
    let (server, writer, mut rx) = fixture().await;
    let key = key();
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    )
    .await;
    let before = rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await["result"]
        .clone();
    let mut blank = tool();
    blank["name"] = json!("  ");
    for tools in [json!([tool(), blank]), json!([tool(), tool()])] {
        let rejected = rpc(
            &server,
            &writer,
            &mut rx,
            "session.register_tools",
            json!({"thread_id":LIVE,"tools":tools}),
        )
        .await;
        assert_eq!(rejected["error"]["code"], -32602, "{rejected}");
        assert_eq!(
            rpc(
                &server,
                &writer,
                &mut rx,
                "session.recovery.inspect",
                json!({"key":key})
            )
            .await["result"],
            before
        );
    }
    server.disconnect_connection(&writer).await;
}
#[tokio::test]
async fn close_joining_failed_creation_reports_storage_failure() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, _) = controlled(backend.clone(), true).await;
    backend.arm(1, true);
    let creating = spawn_rpc(
        &server,
        &writer,
        10,
        "session.create_persistent",
        json!({"key":key(),"session":config(LIVE)}),
    );
    backend.entered.notified().await;
    let closing = spawn_rpc(
        &server,
        &writer,
        11,
        "session.close",
        json!({"thread_id":LIVE}),
    );
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!closing.is_finished());
    backend.release.add_permits(1);
    creating.await.unwrap();
    closing.await.unwrap();
    for _ in 0..2 {
        let response = rx.recv().await.unwrap();
        assert_eq!(response["error"]["code"], -32022, "{response}");
    }
}
#[tokio::test]
async fn incoming_recovery_frames_are_not_logged_with_secrets() {
    use tracing::instrument::WithSubscriber;
    #[derive(Clone)]
    struct Log(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Log {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = bytes.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .with_ansi(false)
        .with_writer(move || Log(output.clone()))
        .finish();
    let key = key();
    let frame =
        json!({"jsonrpc":"2.0","id":1,"method":"session.recovery.inspect","params":{"key":key}})
            .to_string();
    let (tx, _rx) = mpsc::unbounded_channel();
    DaemonServer::default_server()
        .run(futures::stream::iter(vec![Ok(frame)]), Capture(tx))
        .with_subscriber(subscriber)
        .await
        .unwrap();
    let log = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert!(log.contains("Received JSON-RPC frame"), "{log}");
    assert!(!log.contains(key["secret"].as_str().unwrap()), "{log}");
    assert!(!log.contains(key["recovery_id"].as_str().unwrap()), "{log}");
}
#[tokio::test]
async fn abandoned_legacy_run_during_acceptance_does_not_leave_a_live_attachment() {
    let backend = Arc::new(ControlledStore::new());
    let (server, writer, mut rx, calls) = controlled(backend.clone(), true).await;
    let key = key();
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":config(LIVE)}),
    )
    .await;
    backend.arm(2, false);
    let running = spawn_rpc(
        &server,
        &writer,
        10,
        "thread.run_turn",
        json!({"thread_id":LIVE,"input_items":[]}),
    );
    backend.entered.notified().await;
    running.abort();
    let _ = running.await;
    backend.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_millis(300), async {
        while server.sessions().contains_key(LIVE) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abandoned persistent legacy transaction must detach");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let record = backend
        .load(key["recovery_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(record.owner.is_none());
    assert_eq!(record.runs.len(), 1);
    assert!(record
        .runs
        .values()
        .all(|run| run.snapshot.status.is_terminal()));
}
