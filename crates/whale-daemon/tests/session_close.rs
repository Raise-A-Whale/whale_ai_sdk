mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify, Semaphore};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::{AgentStreamEvent, CanonicalItem};

struct Capture {
    messages: mpsc::UnboundedSender<Value>,
    terminal_entered: Arc<Notify>,
    terminal_release: Option<Arc<Semaphore>>,
}
#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "turn.event" && value["params"]["type"] == "finished" {
            self.terminal_entered.notify_one();
            if let Some(release) = &self.terminal_release {
                release.acquire().await.unwrap().forget();
            }
        }
        self.messages
            .send(value)
            .map_err(|_| std::io::Error::other("closed"))
    }
}
fn fixture(
    block_terminal: bool,
) -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    Arc<Notify>,
    Arc<Semaphore>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, step| {
        let mut events = if step == 0 {
            vec![Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "provider".into(),
                item: CanonicalItem::tool_call("call", None, "lookup", Some(json!({})), "{}"),
            })]
        } else {
            vec![]
        };
        events.push(Ok(AgentStreamEvent::TurnCompleted {
            turn_id: "provider".into(),
            thread_id: "provider".into(),
            usage: Default::default(),
        }));
        Ok(Box::pin(futures::stream::iter(events)))
    }));
    let (tx, rx) = mpsc::unbounded_channel();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let writer = AnyTransportWriter::new(Arc::new(Capture {
        messages: tx,
        terminal_entered: entered.clone(),
        terminal_release: block_terminal.then(|| release.clone()),
    }));
    (
        DaemonServer::new(Arc::new(engine), gate),
        writer,
        rx,
        entered,
        release,
    )
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
            &json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}).to_string(),
            writer,
        )
        .await;
}
fn spawn_send(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: u64,
    method: &'static str,
    params: Value,
) -> tokio::task::JoinHandle<()> {
    let server = server.clone();
    let writer = writer.clone();
    tokio::spawn(async move { send(&server, &writer, id, method, params).await })
}
async fn until(rx: &mut mpsc::UnboundedReceiver<Value>, accept: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.unwrap();
            if accept(&value) {
                return value;
            }
        }
    })
    .await
    .unwrap()
}
async fn create(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    id: u64,
    thread: &str,
    approval: bool,
    host_context: bool,
) {
    send(server,writer,id,"session.start_thread",json!({"session_id":thread,"model":"test","context_policy":{"type":if host_context{"host"}else{"full_history"}},"tools":[{"name":"lookup","description":"Lookup","parameters":{},"is_host_tool":true,"require_approval":approval}]})).await;
    assert!(until(rx, |v| v["id"] == id).await.get("error").is_none());
}
async fn start(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    id: u64,
    thread: &str,
) {
    send(
        server,
        writer,
        id,
        "thread.start_turn",
        json!({"thread_id":thread,"turn_id":"run","input_items":[]}),
    )
    .await;
    assert!(until(rx, |v| v["id"] == id).await.get("error").is_none());
}
#[tokio::test]
async fn close_is_idempotent_scoped_and_tombstones_cannot_be_reused() {
    let (s, w, mut rx, _, _) = fixture(false);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    send(&s, &w, 2, "session.close", json!({"thread_id":"s"})).await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 2).await["result"],
        json!({"thread_id":"s","closed":true})
    );
    assert!(!s.sessions().contains_key("s"));
    for (id, thread) in [(3, "s"), (4, "unknown")] {
        send(&s, &w, id, "session.close", json!({"thread_id":thread})).await;
        assert_eq!(
            until(&mut rx, |v| v["id"] == id).await["result"]["closed"],
            false
        );
    }
    let (_, foreign, mut foreign_rx, _, _) = fixture(false);
    common::initialize(&s, &foreign, Some(&mut foreign_rx)).await;
    send(&s, &foreign, 5, "session.close", json!({"thread_id":"s"})).await;
    assert!(until(&mut foreign_rx, |v| v["id"] == 5)
        .await
        .get("error")
        .is_some());
    send(
        &s,
        &w,
        6,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 6)
        .await
        .get("error")
        .is_some());
    send(
        &s,
        &w,
        7,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"new","input_items":[]}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 7)
        .await
        .get("error")
        .is_some());
}
#[tokio::test]
async fn close_cancels_host_wait_and_finished_precedes_response_without_affecting_other_session() {
    let (s, w, mut rx, _, _) = fixture(false);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    create(&s, &w, &mut rx, 2, "other", false, false).await;
    start(&s, &w, &mut rx, 3, "s").await;
    let call = until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    let closing = spawn_send(&s, &w, 4, "session.close", json!({"thread_id":"s"}));
    let mut terminal = false;
    let mut cancel = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.unwrap();
            if value["method"] == "tool.cancel_host" {
                assert_eq!(value["params"]["call_id"], call["params"]["call_id"]);
                cancel = true;
            }
            if value["params"]["type"] == "finished" {
                assert_eq!(value["params"]["snapshot"]["status"], "cancelled");
                terminal = true;
            }
            if value["id"] == 4 {
                assert_eq!(value["result"]["closed"], true);
                assert!(terminal, "close response overtook finished");
                break;
            }
        }
    })
    .await
    .unwrap();
    closing.await.unwrap();
    if !cancel {
        until(&mut rx, |v| v["method"] == "tool.cancel_host").await;
    }
    assert!(s.pending_host_tool_calls().is_empty());
    assert!(s.sessions().contains_key("other"));
    send(
        &s,
        &w,
        5,
        "turn.get",
        json!({"thread_id":"s","turn_id":"run"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 5).await["error"]["message"],
        "RunNotFound"
    );
    start(&s, &w, &mut rx, 6, "other").await;
    until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    s.disconnect_connection(&w).await;
}
#[tokio::test]
async fn close_drains_approval_and_context_waits() {
    for context in [false, true] {
        let (s, w, mut rx, _, _) = fixture(false);
        common::initialize(&s, &w, Some(&mut rx)).await;
        create(&s, &w, &mut rx, 1, "s", !context, context).await;
        start(&s, &w, &mut rx, 2, "s").await;
        let pending = until(&mut rx, |v| {
            if context {
                v["method"] == "context.build_host"
            } else {
                v["params"]["event"]["type"] == "approval_requested"
            }
        })
        .await;
        send(&s, &w, 3, "session.close", json!({"thread_id":"s"})).await;
        assert_eq!(
            until(&mut rx, |v| v["id"] == 3).await["result"]["closed"],
            true
        );
        assert_eq!(s.approval_gate().pending_count(), 0);
        assert!(s.sessions().is_empty());
        if context {
            s.handle_message(&json!({"jsonrpc":"2.0","id":pending["id"],"result":{"system_prompt":null,"items":[]}}).to_string(),&w).await;
        }
        send(
            &s,
            &w,
            4,
            "turn.get",
            json!({"thread_id":"s","turn_id":"run"}),
        )
        .await;
        assert_eq!(
            until(&mut rx, |v| v["id"] == 4).await["error"]["message"],
            "RunNotFound"
        );
    }
}
#[tokio::test]
async fn close_waits_for_terminal_delivery_and_concurrent_close_joins_same_cleanup() {
    let (s, w, mut rx, entered, release) = fixture(true);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    start(&s, &w, &mut rx, 2, "s").await;
    until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    let first = spawn_send(&s, &w, 3, "session.close", json!({"thread_id":"s"}));
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("close never cancelled running tool");
    let second = spawn_send(&s, &w, 4, "session.close", json!({"thread_id":"s"}));
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    send(
        &s,
        &w,
        5,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"new","input_items":[]}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 5)
        .await
        .get("error")
        .is_some());
    send(
        &s,
        &w,
        6,
        "session.register_tools",
        json!({"thread_id":"s","tools":[]}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 6)
        .await
        .get("error")
        .is_some());
    send(
        &s,
        &w,
        7,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 7)
        .await
        .get("error")
        .is_some());
    release.add_permits(1);
    let mut finished = false;
    let mut responses = vec![];
    tokio::time::timeout(Duration::from_secs(2), async {
        while responses.len() < 2 {
            let v = rx.recv().await.unwrap();
            if v["params"]["type"] == "finished" {
                finished = true;
            }
            if v["id"] == 3 || v["id"] == 4 {
                assert!(finished);
                responses.push(v["result"]["closed"].as_bool().unwrap());
            }
        }
    })
    .await
    .unwrap();
    first.await.unwrap();
    second.await.unwrap();
    responses.sort();
    assert_eq!(responses, vec![false, true]);
    assert!(s.sessions().is_empty());
}

struct HoldStartAck {
    inner: AnyTransportWriter,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl OutgoingTransport for HoldStartAck {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["id"] == 2 && value["result"]["turn_id"] == "run" {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.send_line(line).await
    }
}
#[tokio::test]
async fn close_cannot_publish_finished_before_a_held_start_acceptance() {
    let (s, inner, mut rx, _, _) = fixture(false);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let w = AnyTransportWriter::new(Arc::new(HoldStartAck {
        inner,
        entered: entered.clone(),
        release: release.clone(),
    }));
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    let starting = spawn_send(
        &s,
        &w,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"run","input_items":[]}),
    );
    entered.notified().await;
    let closing = spawn_send(&s, &w, 3, "session.close", json!({"thread_id":"s"}));
    assert!(
        tokio::time::timeout(Duration::from_millis(60), rx.recv())
            .await
            .is_err(),
        "close published output before start acceptance"
    );
    assert!(!closing.is_finished());
    release.add_permits(1);
    let mut accepted = false;
    let mut finished = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.unwrap();
            if value["id"] == 2 {
                accepted = true;
            }
            if value["params"]["type"] == "finished" {
                assert!(accepted);
                finished = true;
            }
            if value["id"] == 3 {
                assert!(accepted && finished);
                assert_eq!(value["result"]["closed"], true);
                break;
            }
        }
    })
    .await
    .unwrap();
    starting.await.unwrap();
    closing.await.unwrap();
}
#[tokio::test]
async fn queued_registration_is_rejected_after_close_and_close_future_abort_does_not_leak() {
    let (s, w, mut rx, entered, release) = fixture(true);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    start(&s, &w, &mut rx, 2, "s").await;
    until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    let queued = spawn_send(
        &s,
        &w,
        3,
        "session.register_tools",
        json!({"thread_id":"s","tools":[{"name":"later","description":"Later","parameters":{},"is_host_tool":true}]}),
    );
    tokio::task::yield_now().await;
    assert!(!queued.is_finished());
    let closing = spawn_send(&s, &w, 4, "session.close", json!({"thread_id":"s"}));
    entered.notified().await;
    closing.abort();
    let _ = closing.await;
    let rejected = until(&mut rx, |v| v["id"] == 3).await;
    assert_eq!(rejected["error"]["message"], "SessionClosed");
    queued.await.unwrap();
    let repeated = spawn_send(&s, &w, 5, "session.close", json!({"thread_id":"s"}));
    release.add_permits(1);
    assert_eq!(
        until(&mut rx, |v| v["id"] == 5).await["result"]["closed"],
        false
    );
    repeated.await.unwrap();
    assert!(s.sessions().is_empty());
}

#[tokio::test]
async fn disconnect_unblocks_close_that_is_waiting_for_start_acceptance() {
    let (s, inner, mut rx, _, _) = fixture(false);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let w = AnyTransportWriter::new(Arc::new(HoldStartAck {
        inner,
        entered: entered.clone(),
        release: release.clone(),
    }));
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    let starting = spawn_send(
        &s,
        &w,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"run","input_items":[]}),
    );
    entered.notified().await;
    let closing = spawn_send(&s, &w, 3, "session.close", json!({"thread_id":"s"}));
    // The close job must be running before disconnect takes ownership of cleanup.
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::time::timeout(Duration::from_millis(200), s.disconnect_connection(&w))
        .await
        .expect("disconnect left pending acceptance asleep");
    assert!(s.sessions().is_empty());
    starting.abort();
    let _ = starting.await;
    closing.await.unwrap();
}

struct FailTerminal {
    inner: AnyTransportWriter,
}
#[async_trait]
impl OutgoingTransport for FailTerminal {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "turn.event" && value["params"]["type"] == "finished" {
            return Err(std::io::Error::other("terminal transport failed"));
        }
        self.inner.send_line(line).await
    }
}
#[tokio::test]
async fn failed_terminal_delivery_is_an_internal_error_after_cleanup_not_success() {
    let (s, inner, mut rx, _, _) = fixture(false);
    let w = AnyTransportWriter::new(Arc::new(FailTerminal { inner }));
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    start(&s, &w, &mut rx, 2, "s").await;
    until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    send(&s, &w, 3, "session.close", json!({"thread_id":"s"})).await;
    let response = until(&mut rx, |v| v["id"] == 3).await;
    assert_eq!(response["error"]["code"], -32603);
    assert!(response.get("result").is_none());
    assert!(s.sessions().is_empty());
    assert!(s.pending_host_tool_calls().is_empty());
    send(
        &s,
        &w,
        4,
        "turn.get",
        json!({"thread_id":"s","turn_id":"run"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 4).await["error"]["message"],
        "RunNotFound"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_and_concurrent_creates_leave_no_sessions_or_reusable_connection() {
    let (s, w, mut rx, _, _) = fixture(false);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "existing", false, false).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(33));
    let mut tasks = vec![];
    for index in 0..32 {
        let s = s.clone();
        let w = w.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            send(
                &s,
                &w,
                100 + index,
                "session.start_thread",
                json!({"session_id":format!("racing-{index}"),"model":"test"}),
            )
            .await;
        }));
    }
    barrier.wait().await;
    s.disconnect_connection(&w).await;
    for task in tasks {
        task.await.unwrap();
    }
    assert!(s.sessions().is_empty());
    send(
        &s,
        &w,
        500,
        "session.start_thread",
        json!({"session_id":"late","model":"test"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 500).await["error"]["message"],
        "ConnectionClosed"
    );
    assert!(s.sessions().is_empty());
}
#[tokio::test]
async fn blank_close_ids_and_foreign_active_sessions_are_rejected() {
    let (s, w, mut rx, _, _) = fixture(false);
    common::initialize(&s, &w, Some(&mut rx)).await;
    create(&s, &w, &mut rx, 1, "s", false, false).await;
    for (id, thread) in [(2, ""), (3, "   ")] {
        send(&s, &w, id, "session.close", json!({"thread_id":thread})).await;
        assert_eq!(
            until(&mut rx, |v| v["id"] == id).await["error"]["code"],
            -32602
        );
    }
    let (_, other, mut other_rx, _, _) = fixture(false);
    common::initialize(&s, &other, Some(&mut other_rx)).await;
    send(&s, &other, 4, "session.close", json!({"thread_id":"s"})).await;
    assert_eq!(
        until(&mut other_rx, |v| v["id"] == 4).await["error"]["code"],
        -32602
    );
    assert!(s.sessions().contains_key("s"));
}
