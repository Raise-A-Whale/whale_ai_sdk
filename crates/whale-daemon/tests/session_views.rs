mod common;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};

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

fn partial_fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| {
        let events = futures::stream::iter([
            Ok(whale_protocol::AgentStreamEvent::ItemStarted {
                turn_id: "provider".into(),
                item_id: "partial".into(),
                item_type: "assistant_message".into(),
                phase: Some(whale_protocol::MessagePhase::FinalAnswer),
            }),
            Ok(whale_protocol::AgentStreamEvent::TextDelta {
                turn_id: "provider".into(),
                item_id: "partial".into(),
                delta: "working".into(),
            }),
        ])
        .chain(futures::stream::pending());
        Ok(Box::pin(events))
    }));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(Arc::new(Capture(tx))),
        rx,
    )
}

fn completed_fixture_with(
    transport: Arc<dyn OutgoingTransport>,
) -> (DaemonServer, AnyTransportWriter) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| {
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                thread_id: "provider".into(),
                turn_id: "provider".into(),
                usage: Default::default(),
            },
        )])))
    }));
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(transport),
    )
}

fn pending_fixture_with(
    transport: Arc<dyn OutgoingTransport>,
) -> (DaemonServer, AnyTransportWriter) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| Ok(Box::pin(futures::stream::pending()))));
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(transport),
    )
}

fn approval_fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, step| {
        let item = if step == 0 {
            whale_protocol::CanonicalItem::tool_call(
                "provider-call",
                None,
                "search",
                Some(json!({"query":"original"})),
                "{\"query\":\"original\"}",
            )
        } else {
            whale_protocol::CanonicalItem::assistant_text(
                "done",
                whale_protocol::MessagePhase::FinalAnswer,
            )
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(whale_protocol::AgentStreamEvent::ItemCompleted {
                turn_id: "provider".into(),
                item,
            }),
            Ok(whale_protocol::AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            }),
        ])))
    }));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(Arc::new(Capture(tx))),
        rx,
    )
}

struct FailSessionEvents(mpsc::UnboundedSender<Value>);

#[async_trait]
impl OutgoingTransport for FailSessionEvents {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "session.event" {
            return Err(std::io::Error::other(
                "injected Session notification failure",
            ));
        }
        self.0
            .send(value)
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

struct HoldStartAck {
    output: mpsc::UnboundedSender<Value>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

struct BlockSessionEvent {
    output: mpsc::UnboundedSender<Value>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[async_trait]
impl OutgoingTransport for BlockSessionEvent {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "session.event" {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.output
            .send(value)
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

struct FailFirstSessionEvent {
    output: mpsc::UnboundedSender<Value>,
    attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl OutgoingTransport for FailFirstSessionEvent {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "session.event"
            && self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
        {
            return Err(std::io::Error::other("transient Session event failure"));
        }
        self.output
            .send(value)
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

#[async_trait]
impl OutgoingTransport for HoldStartAck {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["id"] == 2 {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.output
            .send(value)
            .map_err(|_| std::io::Error::other("capture closed"))
    }
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
async fn fresh_session_get_preserves_safe_metadata_and_cursor_zero() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({
            "session_id":"view-session",
            "agent_name":"researcher",
            "model":"test",
            "metadata":{"workspace":"alpha"}
        }),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());

    send(
        &server,
        &writer,
        2,
        "session.get",
        json!({"thread_id":"view-session","history_limit":8}),
    )
    .await;
    let snapshot = response(&mut rx, 2).await;
    assert_eq!(snapshot["result"]["summary"]["thread_id"], "view-session");
    assert_eq!(snapshot["result"]["summary"]["agent_name"], "researcher");
    assert_eq!(
        snapshot["result"]["summary"]["metadata"]["workspace"],
        "alpha"
    );
    assert_eq!(snapshot["result"]["cursor"]["seq"], 0);
    assert_eq!(snapshot["result"]["history"]["capacity"], 8);
}

#[tokio::test]
async fn accepted_run_is_committed_before_execution_but_published_after_start_ack() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"ordered","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;

    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"ordered","turn_id":"run","input_items":[]}),
    )
    .await;
    let ack = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ack["id"], 2,
        "the start response must be the first visible frame: {ack}"
    );
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let event = rx.recv().await.unwrap();
            if event["method"] == "session.event" {
                return event;
            }
        }
    })
    .await
    .expect("RunChanged notification after start ACK");
    assert_eq!(event["method"], "session.event", "{event}");
    assert_eq!(event["params"]["cursor"]["seq"], 1);
    assert_eq!(event["params"]["type"], "run_changed");

    send(
        &server,
        &writer,
        3,
        "session.get",
        json!({"thread_id":"ordered","history_limit":8}),
    )
    .await;
    let snapshot = response(&mut rx, 3).await;
    assert_eq!(
        snapshot["result"]["active_run"]["snapshot"]["turn_id"],
        "run"
    );
    assert!(snapshot["result"]["cursor"]["seq"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn get_during_blocked_run_returns_partial_draft_without_session_lock() {
    let (server, writer, mut rx) = partial_fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"partial-session","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"partial-session","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 2).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = rx.recv().await.unwrap();
            if frame["method"] == "session.event"
                && frame["params"]["event"]["event"]["type"] == "text_delta"
            {
                break;
            }
        }
    })
    .await
    .expect("partial delta publication");

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        send(
            &server,
            &writer,
            3,
            "session.get",
            json!({"thread_id":"partial-session","history_limit":8}),
        ),
    )
    .await
    .expect("session.get must not await the executing ThreadSession lock");
    let snapshot = response(&mut rx, 3).await;
    assert_eq!(
        snapshot["result"]["active_run"]["in_progress_items"][0]["text"],
        "working"
    );
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn subscribe_replays_offline_events_in_a_fixed_window() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"replay","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get",
        json!({"thread_id":"replay","history_limit":8}),
    )
    .await;
    let cursor = response(&mut rx, 2).await["result"]["cursor"].clone();
    send(
        &server,
        &writer,
        3,
        "thread.start_turn",
        json!({"thread_id":"replay","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 3).await;

    send(
        &server,
        &writer,
        4,
        "session.subscribe",
        json!({"thread_id":"replay","after":cursor,"limit":1}),
    )
    .await;
    let first = response(&mut rx, 4).await;
    assert_eq!(first["result"]["events"][0]["cursor"]["seq"], 1);
    let through = first["result"]["through"].clone();
    if first["result"]["has_more"] == true {
        send(
            &server,
            &writer,
            5,
            "session.subscribe",
            json!({
                "thread_id":"replay",
                "after":first["result"]["resume_after"],
                "through":through,
                "limit":256
            }),
        )
        .await;
        let second = response(&mut rx, 5).await;
        assert_eq!(second["result"]["through"], through);
        assert_eq!(second["result"]["has_more"], false);
    }
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn cancel_advances_cursor_and_foreign_views_are_indistinguishable_from_unknown() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"cancel-view","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"cancel-view","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 2).await;
    send(
        &server,
        &writer,
        3,
        "session.get",
        json!({"thread_id":"cancel-view","history_limit":8}),
    )
    .await;
    let before = response(&mut rx, 3).await["result"]["cursor"]["seq"]
        .as_u64()
        .unwrap();
    send(
        &server,
        &writer,
        4,
        "turn.cancel",
        json!({"thread_id":"cancel-view","turn_id":"run"}),
    )
    .await;
    response(&mut rx, 4).await;
    send(
        &server,
        &writer,
        5,
        "session.get",
        json!({"thread_id":"cancel-view","history_limit":8}),
    )
    .await;
    let after = response(&mut rx, 5).await;
    assert!(after["result"]["cursor"]["seq"].as_u64().unwrap() > before);

    let (other_tx, mut other_rx) = mpsc::unbounded_channel();
    let other = AnyTransportWriter::new(Arc::new(Capture(other_tx)));
    common::initialize(&server, &other, Some(&mut other_rx)).await;
    send(
        &server,
        &other,
        6,
        "session.get",
        json!({"thread_id":"cancel-view","history_limit":8}),
    )
    .await;
    let foreign = response(&mut other_rx, 6).await["error"].clone();
    send(
        &server,
        &other,
        7,
        "session.get",
        json!({"thread_id":"unknown","history_limit":8}),
    )
    .await;
    let unknown = response(&mut other_rx, 7).await["error"].clone();
    assert_eq!(foreign["code"], unknown["code"]);
    assert_eq!(foreign["message"], unknown["message"]);
    server.disconnect_connection(&writer).await;
    server.disconnect_connection(&other).await;
}

#[tokio::test]
async fn close_and_eof_remove_view_and_recreated_identity_gets_fresh_stream() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"reuse","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get",
        json!({"thread_id":"reuse","history_limit":8}),
    )
    .await;
    let old_stream = response(&mut rx, 2).await["result"]["cursor"]["stream_id"].clone();
    server.disconnect_connection(&writer).await;

    let (next_tx, mut next_rx) = mpsc::unbounded_channel();
    let next = AnyTransportWriter::new(Arc::new(Capture(next_tx)));
    common::initialize(&server, &next, Some(&mut next_rx)).await;
    send(
        &server,
        &next,
        3,
        "session.start_thread",
        json!({"session_id":"reuse","model":"test"}),
    )
    .await;
    assert!(response(&mut next_rx, 3).await.get("error").is_none());
    send(
        &server,
        &next,
        4,
        "session.get",
        json!({"thread_id":"reuse","history_limit":8}),
    )
    .await;
    let new_stream = response(&mut next_rx, 4).await["result"]["cursor"]["stream_id"].clone();
    assert_ne!(old_stream, new_stream);
    send(
        &server,
        &next,
        5,
        "session.close",
        json!({"thread_id":"reuse"}),
    )
    .await;
    response(&mut next_rx, 5).await;
    send(
        &server,
        &next,
        6,
        "session.get",
        json!({"thread_id":"reuse","history_limit":8}),
    )
    .await;
    assert!(response(&mut next_rx, 6).await.get("error").is_some());
}

#[tokio::test]
async fn failed_session_notification_does_not_change_terminal_delivery_or_replay() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, writer) = completed_fixture_with(Arc::new(FailSessionEvents(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"send-failure","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get",
        json!({"thread_id":"send-failure","history_limit":8}),
    )
    .await;
    let cursor = response(&mut rx, 2).await["result"]["cursor"].clone();
    send(
        &server,
        &writer,
        3,
        "thread.start_turn",
        json!({"thread_id":"send-failure","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 3).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = rx.recv().await.unwrap();
            if frame["method"] == "turn.event" && frame["params"]["type"] == "finished" {
                break;
            }
        }
    })
    .await
    .expect("legacy terminal event remains deliverable");

    send(
        &server,
        &writer,
        4,
        "session.subscribe",
        json!({"thread_id":"send-failure","after":cursor,"limit":256}),
    )
    .await;
    let replay = response(&mut rx, 4).await;
    assert!(replay["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["type"] == "run_event" && event["event"]["type"] == "finished"));
    send(
        &server,
        &writer,
        5,
        "session.close",
        json!({"thread_id":"send-failure"}),
    )
    .await;
    assert!(response(&mut rx, 5).await.get("error").is_none());
}

#[tokio::test]
async fn two_runs_restart_run_seq_but_share_contiguous_session_order() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, writer) = completed_fixture_with(Arc::new(Capture(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"two-runs","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "session.get",
        json!({"thread_id":"two-runs","history_limit":8}),
    )
    .await;
    let cursor = response(&mut rx, 2).await["result"]["cursor"].clone();
    for (id, turn) in [(3, "first"), (4, "second")] {
        send(
            &server,
            &writer,
            id,
            "thread.start_turn",
            json!({"thread_id":"two-runs","turn_id":turn,"input_items":[]}),
        )
        .await;
        response(&mut rx, id).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let frame = rx.recv().await.unwrap();
                if frame["method"] == "turn.event"
                    && frame["params"]["type"] == "finished"
                    && frame["params"]["turn_id"] == turn
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
    send(
        &server,
        &writer,
        5,
        "session.subscribe",
        json!({"thread_id":"two-runs","after":cursor,"limit":256}),
    )
    .await;
    let replay = response(&mut rx, 5).await;
    let events = replay["result"]["events"].as_array().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event["cursor"]["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    for turn in ["first", "second"] {
        assert!(events.iter().any(|event| {
            event["type"] == "run_changed"
                && event["run"]["snapshot"]["turn_id"] == turn
                && event["run"]["snapshot"]["last_seq"] == 0
        }));
        assert!(events.iter().any(|event| {
            event["type"] == "run_event"
                && event["event"]["turn_id"] == turn
                && event["event"]["seq"] == 1
        }));
    }
}

#[tokio::test]
async fn cancel_while_start_ack_is_blocked_is_journaled_but_live_order_starts_at_one() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let held = Arc::new(HoldStartAck {
        output: tx,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let (server, writer) = completed_fixture_with(held.clone());
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"ack-gate","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        10,
        "session.get",
        json!({"thread_id":"ack-gate","history_limit":8}),
    )
    .await;
    let cursor = response(&mut rx, 10).await["result"]["cursor"].clone();

    let start = tokio::spawn({
        let server = server.clone();
        let writer = writer.clone();
        async move {
            send(
                &server,
                &writer,
                2,
                "thread.start_turn",
                json!({"thread_id":"ack-gate","turn_id":"run","input_items":[]}),
            )
            .await;
        }
    });
    held.entered.notified().await;
    send(
        &server,
        &writer,
        3,
        "turn.cancel",
        json!({"thread_id":"ack-gate","turn_id":"run"}),
    )
    .await;
    loop {
        let frame = rx.recv().await.unwrap();
        assert_ne!(frame["method"], "session.event", "{frame}");
        if frame["id"] == 3 {
            break;
        }
    }
    if let Ok(Some(frame)) =
        tokio::time::timeout(std::time::Duration::from_millis(20), rx.recv()).await
    {
        assert_ne!(frame["method"], "session.event", "{frame}");
    }

    send(
        &server,
        &writer,
        4,
        "session.subscribe",
        json!({"thread_id":"ack-gate","after":cursor,"limit":2}),
    )
    .await;
    let replay = response(&mut rx, 4).await;
    assert_eq!(replay["result"]["events"][0]["cursor"]["seq"], 1);
    assert_eq!(replay["result"]["events"][1]["cursor"]["seq"], 2);
    assert_eq!(
        replay["result"]["events"][1]["run"]["snapshot"]["status"],
        "cancelling"
    );

    held.release.add_permits(1);
    start.await.unwrap();
    assert_eq!(response(&mut rx, 2).await["result"]["turn_id"], "run");
    let mut live = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while live.len() < 2 {
            let frame = rx.recv().await.unwrap();
            if frame["method"] == "session.event" {
                live.push(frame["params"]["cursor"]["seq"].as_u64().unwrap());
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(&live[..2], &[1, 2]);
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn approval_resolution_commits_run_changed_before_resumed_execution() {
    let (server, writer, mut rx) = approval_fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({
            "session_id":"approval-view",
            "model":"test",
            "tools":[{
                "name":"search","description":"search",
                "parameters":{"type":"object"},
                "is_host_tool":true,"require_approval":true
            }]
        }),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"approval-view","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut rx, 2).await;
    let request = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = rx.recv().await.unwrap();
            if frame["method"] == "session.event"
                && frame["params"]["event"]["event"]["type"] == "approval_requested"
            {
                return frame;
            }
        }
    })
    .await
    .unwrap();
    send(
        &server,
        &writer,
        3,
        "session.get",
        json!({"thread_id":"approval-view","history_limit":8}),
    )
    .await;
    let before = response(&mut rx, 3).await;
    assert_eq!(
        before["result"]["active_run"]["snapshot"]["status"],
        "waiting_approval"
    );
    let cursor = before["result"]["cursor"].clone();

    send(
        &server,
        &writer,
        4,
        "turn.resolve_approval",
        json!({
            "thread_id":"approval-view","turn_id":"run",
            "request_id":request["params"]["event"]["event"]["request_id"],
            "decision":"reject","feedback":"denied"
        }),
    )
    .await;
    assert_eq!(response(&mut rx, 4).await["result"]["resolved"], true);
    send(
        &server,
        &writer,
        5,
        "session.subscribe",
        json!({"thread_id":"approval-view","after":cursor,"limit":256}),
    )
    .await;
    let replay = response(&mut rx, 5).await;
    assert!(replay["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["type"] == "run_changed"));
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn notifier_retries_transient_failure_and_close_cancels_blocked_old_stream() {
    let (retry_tx, mut retry_rx) = mpsc::unbounded_channel();
    let retry = Arc::new(FailFirstSessionEvent {
        output: retry_tx,
        attempts: std::sync::atomic::AtomicUsize::new(0),
    });
    let (server, writer) = pending_fixture_with(retry.clone());
    common::initialize(&server, &writer, Some(&mut retry_rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"retry","model":"test"}),
    )
    .await;
    response(&mut retry_rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"retry","turn_id":"run","input_items":[]}),
    )
    .await;
    response(&mut retry_rx, 2).await;
    let retried = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = retry_rx.recv().await.unwrap();
            if frame["method"] == "session.event" {
                return frame;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(retried["params"]["cursor"]["seq"], 1);
    assert!(retry.attempts.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    server.disconnect_connection(&writer).await;

    let (blocked_tx, mut blocked_rx) = mpsc::unbounded_channel();
    let blocked = Arc::new(BlockSessionEvent {
        output: blocked_tx,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let (server, writer) = pending_fixture_with(blocked.clone());
    common::initialize(&server, &writer, Some(&mut blocked_rx)).await;
    send(
        &server,
        &writer,
        10,
        "session.start_thread",
        json!({"session_id":"reused-after-block","model":"test"}),
    )
    .await;
    response(&mut blocked_rx, 10).await;
    send(
        &server,
        &writer,
        11,
        "thread.start_turn",
        json!({"thread_id":"reused-after-block","turn_id":"old","input_items":[]}),
    )
    .await;
    response(&mut blocked_rx, 11).await;
    blocked.entered.notified().await;
    server.disconnect_connection(&writer).await;
    blocked.release.add_permits(1);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    while let Ok(frame) = blocked_rx.try_recv() {
        assert_ne!(
            frame["method"], "session.event",
            "old stream leaked after close: {frame}"
        );
    }

    let (new_tx, mut new_rx) = mpsc::unbounded_channel();
    let new_writer = AnyTransportWriter::new(Arc::new(Capture(new_tx)));
    common::initialize(&server, &new_writer, Some(&mut new_rx)).await;
    send(
        &server,
        &new_writer,
        12,
        "session.start_thread",
        json!({"session_id":"reused-after-block","model":"test"}),
    )
    .await;
    assert!(response(&mut new_rx, 12).await.get("error").is_none());
    server.disconnect_connection(&new_writer).await;
}

#[tokio::test]
async fn legacy_result_observes_terminal_session_projection() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, writer) = completed_fixture_with(Arc::new(Capture(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"legacy-view","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.run_turn",
        json!({"thread_id":"legacy-view","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 2).await.get("error").is_none());
    send(
        &server,
        &writer,
        3,
        "session.get",
        json!({"thread_id":"legacy-view","history_limit":8}),
    )
    .await;
    let view = response(&mut rx, 3).await;
    assert!(view["result"]["active_run"].is_null());
    assert_eq!(view["result"]["last_run"]["status"], "completed");
    server.disconnect_connection(&writer).await;
}
