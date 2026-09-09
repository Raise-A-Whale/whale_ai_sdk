mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
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
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let v = rx.recv().await.unwrap();
            if v["id"] == id {
                return v;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn start_is_immediate_busy_cancel_and_final_query() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    let accepted = response(&mut rx, 2).await;
    assert_eq!(accepted["result"]["turn_id"], "r", "{accepted}");
    send(
        &server,
        &writer,
        3,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r2","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 3).await["error"]["message"]
        .as_str()
        .unwrap()
        .contains("SessionBusy"));
    send(
        &server,
        &writer,
        4,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    response(&mut rx, 4).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let event = rx.recv().await.unwrap();
            if event["params"]["type"] == "finished" {
                assert_eq!(event["params"]["snapshot"]["status"], "cancelled");
                break;
            }
        }
    })
    .await
    .unwrap();
    send(
        &server,
        &writer,
        5,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(response(&mut rx, 5).await["result"]["status"], "cancelled");
    send(
        &server,
        &writer,
        6,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r3","input_items":[],"timeout_ms":10}),
    )
    .await;
    assert_eq!(response(&mut rx, 6).await["result"]["turn_id"], "r3");
}

#[tokio::test]
async fn another_connection_cannot_query_or_cancel_owned_run() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    assert_eq!(response(&mut rx, 2).await["result"]["turn_id"], "r");
    let (tx, mut other_rx) = mpsc::unbounded_channel();
    let other = AnyTransportWriter::new(Arc::new(Capture(tx)));
    common::initialize(&server, &other, None).await;
    send(
        &server,
        &other,
        3,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert!(response(&mut other_rx, 3).await.get("error").is_some());
    send(
        &server,
        &other,
        4,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert!(response(&mut other_rx, 4).await.get("error").is_some());
}

#[tokio::test]
async fn legacy_run_rejects_busy_and_is_cleaned_on_disconnect() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    let s = server.clone();
    let w = writer.clone();
    let task = tokio::spawn(async move {
        send(
            &s,
            &w,
            2,
            "thread.run_turn",
            json!({"thread_id":"s","input_items":[]}),
        )
        .await;
    });
    tokio::task::yield_now().await;
    server.disconnect_connection(&writer).await;
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("legacy execution leaked on disconnect")
        .unwrap();
    assert!(server.sessions().is_empty());
}

fn tool_fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    use whale_protocol::{
        canonical::{CanonicalItem, MessagePhase},
        events::{AgentStreamEvent, UsageMetrics},
    };
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, step| {
        let item = if step == 0 {
            CanonicalItem::tool_call(
                "provider-call",
                None,
                "search",
                Some(json!({"query":"original"})),
                "{\"query\":\"original\"}",
            )
        } else {
            CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "provider".into(),
                item,
            }),
            Ok(AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider-thread".into(),
                usage: UsageMetrics {
                    input_tokens: 3,
                    output_tokens: 1,
                    ..Default::default()
                },
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
async fn next_matching(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.unwrap();
            if predicate(&value) {
                return value;
            }
        }
    })
    .await
    .unwrap()
}
async fn tool_thread(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    thread: &str,
    approval: bool,
) {
    send(server,writer,1,"session.start_thread",json!({"session_id":thread,"model":"test","tools":[{"name":"search","description":"search","parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]},"is_host_tool":true,"require_approval":approval}]})).await;
    assert!(response(rx, 1).await.get("error").is_none());
}

#[tokio::test]
async fn cancel_approval_removes_wait_and_does_not_dispatch_host_tool() {
    let (server, writer, mut rx) = tool_fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    tool_thread(&server, &writer, &mut rx, "s", true).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    assert_eq!(
        rx.recv().await.unwrap()["id"],
        2,
        "acceptance must precede every event"
    );
    let request = next_matching(&mut rx, |v| {
        v["params"]["event"]["type"] == "approval_requested"
    })
    .await;
    assert_eq!(server.approval_gate().pending_count(), 1);
    send(
        &server,
        &writer,
        3,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    let snapshot = response(&mut rx, 3).await;
    assert_eq!(snapshot["result"]["status"], "waiting_approval");
    assert_eq!(
        snapshot["result"]["pending_approvals"][0]["request_id"],
        request["params"]["event"]["request_id"]
    );
    send(
        &server,
        &writer,
        4,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    response(&mut rx, 4).await;
    let terminal = next_matching(&mut rx, |v| {
        assert_ne!(v["method"], "tool.execute_host");
        v["params"]["type"] == "finished"
    })
    .await;
    assert_eq!(terminal["params"]["snapshot"]["status"], "cancelled");
    assert_eq!(
        terminal["params"]["snapshot"]["pending_approvals"],
        json!([])
    );
    assert_eq!(terminal["params"]["snapshot"]["usage"]["input_tokens"], 3);
    assert_eq!(server.approval_gate().pending_count(), 0);
    let items = terminal["params"]["snapshot"]["items"].as_array().unwrap();
    assert!(
        items.iter().any(|item| item["type"] == "tool_result"
            && item["call_id"] == "provider-call"
            && item["is_error"] == true),
        "cancelled approval left a dangling call in its final snapshot"
    );
    let session = server.sessions().get("s").unwrap().value().clone();
    let mut history = session.lock().await.history().to_vec();
    history.push(whale_protocol::canonical::CanonicalItem::user_text(
        "continue",
    ));
    use whale_adapters::{OpenAIAdapter, OpenAIWireApi, ProtocolAdapter, SamplingOptions};
    for api in [OpenAIWireApi::ChatCompletions, OpenAIWireApi::Responses] {
        let adapter = OpenAIAdapter::with_options("", "http://localhost/v1", api);
        let (body, _) = adapter
            .serialize_request(None, &history, &[], &SamplingOptions::new("test"))
            .unwrap();
        if api == OpenAIWireApi::Responses {
            let input = body["input"].as_array().unwrap();
            assert!(input
                .iter()
                .any(|item| item["type"] == "function_call" && item["call_id"] == "provider-call"));
            assert!(input
                .iter()
                .any(|item| item["type"] == "function_call_output"
                    && item["call_id"] == "provider-call"));
        } else {
            let messages = body["messages"].as_array().unwrap();
            let call = messages
                .iter()
                .position(|message| message["tool_calls"][0]["id"] == "provider-call")
                .unwrap();
            assert_eq!(messages[call + 1]["role"], "tool");
            assert_eq!(messages[call + 1]["tool_call_id"], "provider-call");
        }
    }
}

#[tokio::test]
async fn modified_approval_scoped_host_error_and_multistep_usage_are_retained() {
    let (server, writer, mut rx) = tool_fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    tool_thread(&server, &writer, &mut rx, "s", true).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    let mut seq = 0;
    let mut terminal_count = 0;
    let request = next_matching(&mut rx, |v| {
        v["params"]["event"]["type"] == "approval_requested"
    })
    .await;
    let request_id = request["params"]["event"]["request_id"].clone();
    send(&server,&writer,3,"turn.resolve_approval",json!({"thread_id":"s","turn_id":"r","request_id":request_id,"decision":"modify_arguments","arguments":{"query":"changed"}})).await;
    assert_eq!(response(&mut rx, 3).await["result"]["resolved"], true);
    let host = next_matching(&mut rx, |v| v["method"] == "tool.execute_host").await;
    assert_eq!(host["params"]["thread_id"], "s");
    assert_eq!(host["params"]["arguments"]["query"], "changed");
    server.handle_message(&json!({"jsonrpc":"2.0","id":host["id"],"result":{"call_id":host["params"]["call_id"],"output":{"type":"text","text":"failure detail"},"is_error":true}}).to_string(),&writer).await;
    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.unwrap();
            if value["method"] != "turn.event" {
                continue;
            }
            let next = value["params"]["seq"].as_u64().unwrap();
            assert!(next > seq);
            seq = next;
            assert_ne!(value["params"]["event"]["type"], "turn_completed");
            assert_ne!(value["params"]["event"]["type"], "turn_failed");
            if value["params"]["type"] == "finished" {
                terminal_count += 1;
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(terminal_count, 1);
    let snap = &terminal["params"]["snapshot"];
    assert_eq!(snap["status"], "completed");
    assert_eq!(snap["usage"]["input_tokens"], 6);
    assert!(snap["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["type"] == "tool_result" && v["is_error"] == true));
    assert_eq!(server.pending_host_tool_calls().len(), 0);
}

#[tokio::test]
async fn deadline_is_queryable_failure_and_restores_session_defaults() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"default"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(&server,&writer,2,"thread.start_turn",json!({"thread_id":"s","turn_id":"r","input_items":[],"timeout_ms":10,"options":{"model":"override"}})).await;
    response(&mut rx, 2).await;
    let terminal = next_matching(&mut rx, |v| v["params"]["type"] == "finished").await;
    assert_eq!(terminal["params"]["snapshot"]["status"], "failed");
    assert_eq!(
        terminal["params"]["snapshot"]["error"]["code"],
        "DEADLINE_EXCEEDED"
    );
    let session = server.sessions().get("s").unwrap().value().clone();
    assert_eq!(session.lock().await.sampling_options().model, "default");
}

#[tokio::test]
async fn eof_cleans_waiting_host_and_only_own_sessions() {
    let (server, writer, mut rx) = tool_fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    tool_thread(&server, &writer, &mut rx, "s", false).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    let host = next_matching(&mut rx, |v| v["method"] == "tool.execute_host").await;
    assert_eq!(server.pending_host_tool_calls().len(), 1);
    let (other_tx, mut other_rx) = mpsc::unbounded_channel();
    let other = AnyTransportWriter::new(Arc::new(Capture(other_tx)));
    common::initialize(&server, &other, Some(&mut other_rx)).await;
    tool_thread(&server, &other, &mut other_rx, "other", false).await;
    // A response received on another connection must not resolve this host call.
    server.handle_message(&json!({"jsonrpc":"2.0","id":host["id"],"result":{"call_id":host["params"]["call_id"],"output":{"type":"text","text":"spoof"},"is_error":false}}).to_string(),&other).await;
    assert_eq!(server.pending_host_tool_calls().len(), 1);
    server.disconnect_connection(&writer).await;
    assert_eq!(server.pending_host_tool_calls().len(), 0);
    assert!(!server.sessions().contains_key("s"));
    assert!(server.sessions().contains_key("other"));
}

#[tokio::test]
async fn actual_reader_eof_cancels_approval_and_reclaims_session() {
    let (server, _, _) = tool_fixture();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (input_tx, input_rx) = mpsc::channel(8);
    let s = server.clone();
    let task = tokio::spawn(async move {
        s.run(
            tokio_stream::wrappers::ReceiverStream::new(input_rx),
            Capture(tx),
        )
        .await
        .unwrap();
    });
    input_tx
        .send(Ok(common::initialization_request()))
        .await
        .unwrap();
    assert_eq!(rx.recv().await.unwrap()["result"]["protocol_version"], 1);
    input_tx.send(Ok(json!({"jsonrpc":"2.0","id":1,"method":"session.start_thread","params":{"session_id":"s","model":"test","tools":[{"name":"search","description":"search","parameters":{},"is_host_tool":true,"require_approval":true}]}}).to_string())).await.unwrap();
    response(&mut rx, 1).await;
    input_tx.send(Ok(json!({"jsonrpc":"2.0","id":2,"method":"thread.start_turn","params":{"thread_id":"s","turn_id":"r","input_items":[]}}).to_string())).await.unwrap();
    next_matching(&mut rx, |v| {
        v["params"]["event"]["type"] == "approval_requested"
    })
    .await;
    assert_eq!(server.approval_gate().pending_count(), 1);
    drop(input_tx);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.approval_gate().pending_count(), 0);
    assert!(server.sessions().is_empty());
}

#[tokio::test]
async fn canonical_inputs_replay_conflict_and_limit_validation() {
    use whale_protocol::canonical::{CanonicalContent, CanonicalItem};
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"default"}),
    )
    .await;
    response(&mut rx, 1).await;
    for limits in [json!({"max_steps":0}), json!({"timeout_ms":0})] {
        let mut params = json!({"thread_id":"s","turn_id":"invalid","input_items":[]});
        params
            .as_object_mut()
            .unwrap()
            .extend(limits.as_object().unwrap().clone());
        send(&server, &writer, 2, "thread.start_turn", params).await;
        assert!(response(&mut rx, 2).await.get("error").is_some());
    }
    let inputs = vec![
        CanonicalItem::UserMessage {
            id: "preserve-id".into(),
            content: vec![
                CanonicalContent::text("first"),
                CanonicalContent::text("second"),
            ],
        },
        CanonicalItem::user_text("third"),
    ];
    let params = json!({"thread_id":"s","turn_id":"r","input_items":inputs});
    send(&server, &writer, 3, "thread.start_turn", params.clone()).await;
    assert_eq!(response(&mut rx, 3).await["result"]["turn_id"], "r");
    next_matching(&mut rx, |v| {
        v["params"]["event"]["type"] == "item_completed"
            && v["params"]["event"]["item"]["id"] == inputs[1].id()
    })
    .await;
    send(&server, &writer, 4, "thread.start_turn", params).await;
    assert_eq!(response(&mut rx, 4).await["result"]["turn_id"], "r");
    send(
        &server,
        &writer,
        5,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 5).await["error"]["message"]
        .as_str()
        .unwrap()
        .contains("RunConflict"));
    send(
        &server,
        &writer,
        6,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(
        response(&mut rx, 6).await["result"]["items"],
        serde_json::to_value(inputs).unwrap()
    );
    server.disconnect_connection(&writer).await;
}

struct BlockEvents(mpsc::UnboundedSender<Value>);
#[async_trait]
impl OutgoingTransport for BlockEvents {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["method"] == "turn.event" {
            futures::future::pending::<()>().await;
        }
        self.0
            .send(value)
            .map_err(|_| std::io::Error::other("closed"))
    }
}
#[tokio::test]
async fn deadline_is_enforced_while_event_writer_is_blocked() {
    let (server, _, _) = fixture();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(BlockEvents(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[],"timeout_ms":10}),
    )
    .await;
    response(&mut rx, 2).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    send(
        &server,
        &writer,
        3,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(response(&mut rx, 3).await["result"]["status"], "failed");
}

struct DelayLegacyTerminal {
    out: mpsc::UnboundedSender<Value>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl OutgoingTransport for DelayLegacyTerminal {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["params"]["event"]["type"] == "turn_completed" {
            self.out.send(json!({"blocked":true})).unwrap();
            self.release.notified().await;
        }
        self.out
            .send(value)
            .map_err(|_| std::io::Error::other("closed"))
    }
}
#[tokio::test]
async fn legacy_response_follows_the_terminal_stream_event() {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| {
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            },
        )])))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let writer = AnyTransportWriter::new(Arc::new(DelayLegacyTerminal {
        out: tx,
        release: release.clone(),
    }));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    let s = server.clone();
    let w = writer.clone();
    let task = tokio::spawn(async move {
        send(
            &s,
            &w,
            2,
            "thread.run_turn",
            json!({"thread_id":"s","input_items":[]}),
        )
        .await;
    });
    next_matching(&mut rx, |v| v["blocked"] == true).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    while let Ok(frame) = rx.try_recv() {
        assert_ne!(
            frame["id"], 2,
            "legacy result overtook the blocked terminal event"
        );
        assert_eq!(
            frame["method"], "session.event",
            "unexpected frame: {frame}"
        );
    }
    release.notify_one();
    next_matching(&mut rx, |frame| {
        assert_ne!(frame["id"], 2, "legacy result overtook the terminal event");
        frame["params"]["event"]["type"] == "turn_completed"
    })
    .await;
    assert!(response(&mut rx, 2).await.get("result").is_some());
    task.await.unwrap();
}

#[tokio::test]
async fn cancellation_snapshot_contains_all_committed_history_items() {
    let (server, _, _) = fixture();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(BlockEvents(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    response(&mut rx, 1).await;
    let input: Vec<_> = (0..300)
        .map(|n| whale_protocol::canonical::CanonicalItem::user_text(n.to_string()))
        .collect();
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":input}),
    )
    .await;
    response(&mut rx, 2).await;
    tokio::task::yield_now().await;
    send(
        &server,
        &writer,
        3,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    response(&mut rx, 3).await;
    let session = server.sessions().get("s").unwrap().value().clone();
    let history = tokio::time::timeout(Duration::from_secs(1), async {
        session.lock().await.history().to_vec()
    })
    .await
    .unwrap();
    send(
        &server,
        &writer,
        4,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(
        response(&mut rx, 4).await["result"]["items"],
        serde_json::to_value(history).unwrap()
    );
}

/// Suspend inside execution.poll, after the outer select already polled its
/// cancellation branch. A real cancel RPC completes before that same poll returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_cancel_wins_when_core_finishes_inside_the_same_select_poll() {
    use whale_core::{context::ContextPolicy, CancellationToken};
    use whale_protocol::contexts::{ContextBuildRequest, ModelContext};
    struct CancelDuringPoll {
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    #[async_trait]
    impl ContextPolicy for CancelDuringPoll {
        async fn build(
            &self,
            _: ContextBuildRequest,
            cancellation: CancellationToken,
        ) -> Result<ModelContext, String> {
            self.entered.notify_one();
            // This test-only blocking callback fixes the interleaving within one
            // Future::poll; production extensions should yield cooperatively.
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            assert!(cancellation.is_cancelled());
            Err("context construction observed cooperative cancellation".into())
        }
    }
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, receiver) = std::sync::mpsc::channel();
    server
        .sessions()
        .get("s")
        .unwrap()
        .value()
        .clone()
        .lock()
        .await
        .set_context_policy(Arc::new(CancelDuringPoll {
            entered: entered.clone(),
            release: std::sync::Mutex::new(receiver),
        }));
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 2).await.get("error").is_none());
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    send(
        &server,
        &writer,
        3,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(response(&mut rx, 3).await["result"]["status"], "cancelling");
    release.send(()).unwrap();
    let terminal = next_matching(&mut rx, |v| v["params"]["type"] == "finished").await;
    let snapshot = &terminal["params"]["snapshot"];
    assert_eq!(snapshot["status"], "cancelled", "{snapshot}");
    assert_eq!(snapshot["result"]["status"], "interrupted");
    assert_eq!(snapshot["error"]["code"], "CANCELLED");
    send(
        &server,
        &writer,
        4,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(response(&mut rx, 4).await["result"], *snapshot);
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn genuine_core_failure_and_late_cancel_preserve_the_failed_terminal() {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| {
        Err(whale_core::CoreError::Internal(
            "controlled provider failure".into(),
        ))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let writer = AnyTransportWriter::new(Arc::new(Capture(tx)));
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[]}),
    )
    .await;
    assert!(response(&mut rx, 2).await.get("error").is_none());
    let terminal = next_matching(&mut rx, |v| v["params"]["type"] == "finished").await;
    let snapshot = &terminal["params"]["snapshot"];
    assert_eq!(snapshot["status"], "failed");
    assert_eq!(snapshot["error"]["code"], "RUN_FAILED");
    assert!(snapshot["error"]["message"]
        .as_str()
        .unwrap()
        .contains("controlled provider failure"));
    send(
        &server,
        &writer,
        3,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(response(&mut rx, 3).await["result"], *snapshot);
    server.disconnect_connection(&writer).await;
}
