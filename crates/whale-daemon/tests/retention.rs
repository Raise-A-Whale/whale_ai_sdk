mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
struct Capture(mpsc::UnboundedSender<Value>);
#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.0
            .send(serde_json::from_str(line).unwrap())
            .map_err(|_| std::io::Error::other("closed"))
    }
}
fn server() -> (DaemonServer, Arc<AtomicUsize>) {
    use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
    let count = Arc::new(AtomicUsize::new(0));
    let invoked = count.clone();
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |_, _| {
        invoked.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                thread_id: "model".into(),
                turn_id: "model".into(),
                usage: Default::default(),
            },
        )])))
    }));
    (DaemonServer::new(Arc::new(engine), gate), count)
}
async fn connection(server: &DaemonServer) -> (AnyTransportWriter, mpsc::UnboundedReceiver<Value>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(Capture(tx)));
    common::initialize(server, &writer, Some(&mut rx)).await;
    (writer, rx)
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
    while let Some(value) = rx.recv().await {
        if value["id"] == 1 {
            return value;
        }
    }
    panic!("connection ended")
}
async fn finish(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let event = rx.recv().await.unwrap();
            if event["params"]["type"] == "finished" {
                return event["params"]["snapshot"].clone();
            }
        }
    })
    .await
    .unwrap()
}
fn config(limits: Value) -> Value {
    json!({"session_id":"session","model":"test","provider_config":{"api":"openai_responses","auth":{"type":"none"}},"limits":limits})
}
#[tokio::test]
async fn accepted_turn_limit_refuses_next_id_before_model_dispatch() {
    let (server, calls) = server();
    let (writer, mut rx) = connection(&server).await;
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_accepted_turns":1}))
    )
    .await
    .get("error")
    .is_none());
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"one","input_items":[]})
    )
    .await
    .get("error")
    .is_none());
    assert_eq!(finish(&mut rx).await["status"], "completed");
    let rejected = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"two","input_items":[]}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32031, "{rejected}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"two"})
        )
        .await["error"]["message"],
        "RunNotFound"
    );
    server.disconnect_connection(&writer).await;
}
#[tokio::test]
async fn invalid_limits_do_not_publish_or_reserve_session() {
    let (server, _) = server();
    let (writer, mut rx) = connection(&server).await;
    let rejected = rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_history_bytes":0})),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32602, "{rejected}");
    assert!(server.sessions().is_empty());
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_history_bytes":1000}))
    )
    .await
    .get("error")
    .is_none());
}
fn retained(server: DaemonServer, ttl: Option<u64>, count: Option<u64>) -> DaemonServer {
    server.with_retention_policy(serde_json::from_value(json!({"sweep_interval_ms":2,"runs":{"terminal_ttl_ms":ttl,"max_terminal_runs_per_session":count}})).unwrap()).unwrap()
}
#[tokio::test(start_paused = true)]
async fn ttl_expires_idle_payload_and_every_control_rejects_old_identity() {
    let (server, calls) = server();
    let server = retained(server, Some(10), None);
    let (writer, mut rx) = connection(&server).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({})),
    )
    .await;
    let start = json!({"thread_id":"session","turn_id":"old","input_items":[]});
    rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        start.clone(),
    )
    .await;
    let saved = finish(&mut rx).await;
    assert_eq!(saved["status"], "completed");
    tokio::time::advance(std::time::Duration::from_millis(9)).await;
    tokio::task::yield_now().await;
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "turn.get",
        json!({"thread_id":"session","turn_id":"old"})
    )
    .await
    .get("error")
    .is_none());
    tokio::time::advance(std::time::Duration::from_millis(5)).await;
    tokio::task::yield_now().await;
    for method in [
        "turn.get",
        "turn.cancel",
        "thread.start_turn",
        "turn.resolve_approval",
    ] {
        let params = if method == "thread.start_turn" {
            start.clone()
        } else if method == "turn.resolve_approval" {
            json!({"thread_id":"session","turn_id":"old","request_id":"missing","decision":"approve"})
        } else {
            json!({"thread_id":"session","turn_id":"old"})
        };
        let expired = rpc(&server, &writer, &mut rx, method, params).await;
        assert_eq!(expired["error"]["code"], -32030, "{method}: {expired}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.disconnect_connection(&writer).await;
}
#[tokio::test(start_paused = true)]
async fn terminal_count_retires_oldest_and_does_not_refund_session_quota() {
    let (server, calls) = server();
    let server = retained(server, None, Some(1));
    let (writer, mut rx) = connection(&server).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_accepted_turns":2})),
    )
    .await;
    for turn in ["first", "second"] {
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":turn,"input_items":[]}),
        )
        .await;
        finish(&mut rx).await;
        tokio::time::advance(std::time::Duration::from_millis(3)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"first"})
        )
        .await["error"]["code"],
        -32030
    );
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "turn.get",
        json!({"thread_id":"session","turn_id":"second"})
    )
    .await
    .get("error")
    .is_none());
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":"third","input_items":[]})
        )
        .await["error"]["code"],
        -32031
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.disconnect_connection(&writer).await;
}
struct Gate {
    once: std::sync::atomic::AtomicBool,
    acceptance: bool,
    fail: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
struct GatedCapture {
    tx: mpsc::UnboundedSender<Value>,
    gate: Arc<Gate>,
}
#[async_trait]
impl OutgoingTransport for GatedCapture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        let matches = if self.gate.acceptance {
            value["result"].get("turn_id").is_some() && value["result"].get("status").is_none()
        } else {
            value["params"]["type"] == "finished"
        };
        if matches && self.gate.once.swap(false, Ordering::SeqCst) {
            self.gate.entered.notify_one();
            self.gate.release.acquire().await.unwrap().forget();
            if self.gate.fail {
                return Err(std::io::Error::other("injected delivery failure"));
            }
        }
        self.tx
            .send(value)
            .map_err(|_| std::io::Error::other("closed"))
    }
}
async fn gated(
    server: &DaemonServer,
    acceptance: bool,
    fail: bool,
) -> (
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    Arc<Gate>,
) {
    let gate = Arc::new(Gate {
        once: std::sync::atomic::AtomicBool::new(true),
        acceptance,
        fail,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(GatedCapture {
        tx,
        gate: gate.clone(),
    }));
    common::initialize(server, &writer, Some(&mut rx)).await;
    (writer, rx, gate)
}
#[tokio::test(start_paused = true)]
async fn final_delivery_backpressure_and_failure_are_protected_from_expiry() {
    for fail in [false, true] {
        let (server, _) = server();
        let server = retained(server, Some(10), Some(1));
        let (writer, mut rx, gate) = gated(&server, false, fail).await;
        rpc(
            &server,
            &writer,
            &mut rx,
            "session.start_thread",
            config(json!({})),
        )
        .await;
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":"held","input_items":[]}),
        )
        .await;
        gate.entered.notified().await;
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            rpc(
                &server,
                &writer,
                &mut rx,
                "turn.get",
                json!({"thread_id":"session","turn_id":"held"})
            )
            .await["result"]["status"],
            "completed"
        );
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":"peer","input_items":[]}),
        )
        .await;
        finish(&mut rx).await;
        tokio::time::advance(std::time::Duration::from_millis(15)).await;
        tokio::task::yield_now().await;
        assert!(rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"held"})
        )
        .await
        .get("error")
        .is_none());
        gate.release.add_permits(1);
        if !fail {
            finish(&mut rx).await;
        } else {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(9)).await;
        tokio::task::yield_now().await;
        assert!(rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"held"})
        )
        .await
        .get("error")
        .is_none());
        tokio::time::advance(std::time::Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        let result = rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"held"}),
        )
        .await;
        if fail {
            assert_eq!(result["result"]["status"], "completed");
        } else {
            assert_eq!(result["error"]["code"], -32030);
        }
        let closed = rpc(
            &server,
            &writer,
            &mut rx,
            "session.close",
            json!({"thread_id":"session"}),
        )
        .await;
        if fail {
            assert_eq!(closed["error"]["code"], -32603);
        } else {
            assert_eq!(closed["result"]["closed"], true);
        }
        server.disconnect_connection(&writer).await;
    }
}
#[tokio::test]
async fn cancellation_before_core_still_consumes_accepted_turn_quota() {
    let (server, calls) = server();
    let (writer, mut rx, gate) = gated(&server, true, false).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_accepted_turns":1})),
    )
    .await;
    let start = {
        let server = server.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            server.handle_message(&json!({"jsonrpc":"2.0","id":9,"method":"thread.start_turn","params":{"thread_id":"session","turn_id":"cancelled","input_items":[]}}).to_string(),&writer).await
        })
    };
    gate.entered.notified().await;
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.cancel",
            json!({"thread_id":"session","turn_id":"cancelled"})
        )
        .await["result"]["status"],
        "cancelling"
    );
    gate.release.add_permits(1);
    start.await.unwrap();
    assert_eq!(finish(&mut rx).await["status"], "cancelled");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let rejected = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"new","input_items":[]}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32031);
}
#[tokio::test]
async fn history_budget_rejection_preserves_history_and_id_for_corrected_retry() {
    let (server, calls) = server();
    let (writer, mut rx) = connection(&server).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_history_bytes":100})),
    )
    .await;
    let item = whale_protocol::CanonicalItem::user_text("too big".repeat(100));
    let rejected = rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"same","input_items":[item]}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32031);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(server
        .sessions()
        .get("session")
        .unwrap()
        .clone()
        .lock()
        .await
        .history()
        .is_empty());
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"same","input_items":[]})
    )
    .await
    .get("error")
    .is_none());
    assert_eq!(finish(&mut rx).await["status"], "completed");
    server.disconnect_connection(&writer).await;
}
#[tokio::test(start_paused = true)]
async fn durable_archives_outlive_live_expiry_and_quota_survives_attachment() {
    let runtime = Arc::new(
        whale_store::StoreRuntime::open(Arc::new(whale_store::MemoryStore::new()))
            .await
            .unwrap(),
    );
    let (server, calls) = server();
    let server = retained(server.with_store_runtime(runtime.clone()), Some(10), None);
    let (writer, mut rx) = connection(&server).await;
    let key = whale_protocol::recovery::RecoveryKey::new();
    let first = "11111111-1111-4111-8111-111111111111";
    let second = "22222222-2222-4222-8222-222222222222";
    let mut session = config(json!({"max_accepted_turns":2}));
    session["session_id"] = json!(first);
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.create_persistent",
        json!({"key":key,"session":session})
    )
    .await
    .get("error")
    .is_none());
    rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":first,"turn_id":"one","input_items":[]}),
    )
    .await;
    let original = finish(&mut rx).await;
    tokio::time::advance(std::time::Duration::from_millis(15)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":first,"turn_id":"one"})
        )
        .await["error"]["code"],
        -32030
    );
    assert_eq!(
        serde_json::to_value(&runtime.inspect(&key).await.unwrap().runs[0].snapshot).unwrap(),
        original
    );
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.close",
        json!({"thread_id":first}),
    )
    .await;
    let saved = runtime.inspect(&key).await.unwrap();
    session["session_id"] = json!(second);
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.recovery.attach",
        json!({"key":key,"expected_revision":saved.revision,"session":session})
    )
    .await
    .get("error")
    .is_none());
    rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":second,"turn_id":"two","input_items":[]}),
    )
    .await;
    assert_eq!(finish(&mut rx).await["status"], "completed");
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":second,"turn_id":"three","input_items":[]})
        )
        .await["error"]["code"],
        -32031
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.inspect(&key).await.unwrap().runs.len(), 2);
    server.disconnect_connection(&writer).await;
}
#[tokio::test(start_paused = true)]
async fn store_retention_sweeps_without_a_connection_or_a_followup_rpc() {
    let backend = Arc::new(whale_store::MemoryStore::new());
    let runtime = Arc::new(
        whale_store::StoreRuntime::open(backend.clone())
            .await
            .unwrap(),
    );
    let key = whale_protocol::recovery::RecoveryKey::new();
    runtime
        .create(key.clone(), json!({}), "owner".into(), "old".into())
        .await
        .unwrap()
        .detach()
        .await
        .unwrap();
    runtime
        .create(
            whale_protocol::recovery::RecoveryKey::new(),
            json!({}),
            "owner".into(),
            "new".into(),
        )
        .await
        .unwrap()
        .detach()
        .await
        .unwrap();
    // Configured before Store attachment also works; the worker only holds a weak Store reference.
    let server = DaemonServer::default_server()
        .with_retention_policy(
            serde_json::from_value(
                json!({"sweep_interval_ms":2,"store":{"max_retained_sessions":1}}),
            )
            .unwrap(),
        )
        .unwrap()
        .with_store_runtime(runtime.clone());
    tokio::time::advance(std::time::Duration::from_millis(3)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    use whale_store::SessionStore;
    let records = backend.list().await.unwrap();
    assert_eq!(records.iter().filter(|record| record.forgotten).count(), 1);
    assert_eq!(records.iter().filter(|record| !record.forgotten).count(), 1);
    drop(server);
}
#[tokio::test]
async fn optional_retention_capability_and_always_supported_limits_are_negotiated() {
    for enabled in [false, true] {
        let (server, _) = server();
        let server = if enabled {
            retained(server, Some(10), None)
        } else {
            server
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let writer = AnyTransportWriter::new(Arc::new(Capture(tx)));
        let response=rpc(&server,&writer,&mut rx,"protocol.initialize",json!({"client":{"name":"test","version":"1"},"protocol_versions":[1],"required_capabilities":["session_limits.v1"]})).await;
        let caps = response["result"]["capabilities"].as_array().unwrap();
        assert!(caps.contains(&json!("session_limits.v1")));
        assert_eq!(caps.contains(&json!("run_retention.v1")), enabled);
    }
}
#[test]
fn cli_rejects_invalid_retention_before_serving_requests() {
    for value in [
        json!({"sweep_interval_ms":0}),
        json!({"runs":{"unknown":1}}),
    ] {
        let path =
            std::env::temp_dir().join(format!("whale-retention-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, value.to_string()).unwrap();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_whale-daemon"))
            .arg("--retention-config")
            .arg(&path)
            .output()
            .unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("sweep_interval_ms") || error.contains("unknown"),
            "{error}"
        );
    }
}
#[tokio::test(start_paused = true)]
async fn expired_identity_remains_owner_scoped_and_close_clears_its_control_state() {
    let (server, _) = server();
    let server = retained(server, Some(10), None);
    let (writer, mut rx) = connection(&server).await;
    let (foreign, mut foreign_rx) = connection(&server).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({})),
    )
    .await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"old","input_items":[]}),
    )
    .await;
    finish(&mut rx).await;
    tokio::time::advance(std::time::Duration::from_millis(15)).await;
    tokio::task::yield_now().await;
    for method in ["turn.get", "turn.cancel", "thread.start_turn"] {
        let rejected = rpc(
            &server,
            &foreign,
            &mut foreign_rx,
            method,
            json!({"thread_id":"session","turn_id":"old","input_items":[]}),
        )
        .await;
        assert_ne!(rejected["error"]["code"], -32030);
        assert!(rejected.get("error").is_some());
    }
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "session.close",
            json!({"thread_id":"session"})
        )
        .await["result"]["closed"],
        true
    );
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":"old","input_items":[]})
        )
        .await["error"]["message"],
        "SessionClosed"
    );
    server.disconnect_connection(&writer).await;
    server.disconnect_connection(&foreign).await;
}
#[tokio::test]
async fn retention_configuration_cannot_fork_workers_for_shared_registry() {
    let (server, _) = server();
    let _other_owner = server.clone();
    let policy = serde_json::from_value(json!({"runs":{"terminal_ttl_ms":1}})).unwrap();
    assert!(
        server.with_retention_policy(policy).is_err(),
        "configure before sharing, not a second policy on a shared registry"
    );
}

#[tokio::test]
async fn model_request_budget_failure_has_stable_terminal_code_without_dispatch() {
    let (server, calls) = server();
    let (writer, mut rx) = connection(&server).await;
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_model_request_bytes":1}))
    )
    .await
    .get("error")
    .is_none());
    assert!(rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"one","input_items":[]})
    )
    .await
    .get("error")
    .is_none());
    let terminal = finish(&mut rx).await;
    assert_eq!(terminal["status"], "failed");
    assert_eq!(terminal["error"]["code"], "SESSION_LIMIT_EXCEEDED");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"one"})
        )
        .await["result"],
        terminal
    );
    server.disconnect_connection(&writer).await;
}

#[tokio::test(start_paused = true)]
async fn active_model_wait_survives_many_idle_sweeps_and_can_still_be_cancelled() {
    use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
    let gate = Arc::new(ApprovalGate::new());
    let entered = Arc::new(AtomicUsize::new(0));
    let observed = entered.clone();
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::pending()))
    }));
    let server = retained(DaemonServer::new(Arc::new(engine), gate), Some(10), Some(1));
    let (writer, mut rx) = connection(&server).await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "session.start_thread",
        config(json!({"max_accepted_turns":1})),
    )
    .await;
    rpc(
        &server,
        &writer,
        &mut rx,
        "thread.start_turn",
        json!({"thread_id":"session","turn_id":"one","input_items":[]}),
    )
    .await;
    while entered.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"one"})
        )
        .await["result"]["status"],
        "running"
    );
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.cancel",
            json!({"thread_id":"session","turn_id":"one"})
        )
        .await["result"]["status"],
        "cancelling"
    );
    assert_eq!(finish(&mut rx).await["status"], "cancelled");
    tokio::time::advance(std::time::Duration::from_millis(15)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "turn.get",
            json!({"thread_id":"session","turn_id":"one"})
        )
        .await["error"]["code"],
        -32030
    );
    assert_eq!(
        rpc(
            &server,
            &writer,
            &mut rx,
            "thread.start_turn",
            json!({"thread_id":"session","turn_id":"two","input_items":[]})
        )
        .await["error"]["code"],
        -32031
    );
    server.disconnect_connection(&writer).await;
}

#[tokio::test(start_paused = true)]
async fn maintenance_preserves_all_clone_store_bindings_after_unconfigured_clone_requests() {
    use whale_store::{MemoryStore, SessionStore, StoreRuntime};
    let mut stores = Vec::new();
    for _ in 0..2 {
        let backend = Arc::new(MemoryStore::new());
        let runtime = Arc::new(StoreRuntime::open(backend.clone()).await.unwrap());
        for _ in 0..2 {
            runtime
                .create(
                    whale_protocol::recovery::RecoveryKey::new(),
                    json!({}),
                    "owner".into(),
                    uuid::Uuid::new_v4().to_string(),
                )
                .await
                .unwrap()
                .detach()
                .await
                .unwrap();
        }
        stores.push((backend, runtime));
    }
    let unconfigured = DaemonServer::default_server()
        .with_retention_policy(
            serde_json::from_value(
                json!({"sweep_interval_ms":2,"store":{"max_retained_sessions":1}}),
            )
            .unwrap(),
        )
        .unwrap();
    let first = unconfigured.clone().with_store_runtime(stores[0].1.clone());
    let second = unconfigured.clone().with_store_runtime(stores[1].1.clone());
    // Old clones remain valid servers; their incoming messages cannot unbind either Store.
    let (writer, _) = connection(&unconfigured).await;
    tokio::time::advance(std::time::Duration::from_millis(3)).await;
    for _ in 0..40 {
        tokio::task::yield_now().await;
    }
    for (index, (backend, _)) in stores.iter().enumerate() {
        assert_eq!(
            backend
                .list()
                .await
                .unwrap()
                .iter()
                .filter(|record| record.forgotten)
                .count(),
            1,
            "Store {index} must still be maintained without a request to its server clone"
        );
    }
    unconfigured.disconnect_connection(&writer).await;
    drop((first, second));
}
