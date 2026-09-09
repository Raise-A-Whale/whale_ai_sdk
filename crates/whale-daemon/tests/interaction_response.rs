use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::interactions::{CAPABILITY_INTERACTIONS, INTERACTION_CONFLICT};

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
    .with_stream_provider(Arc::new(|_, step| {
        let item = if step == 0 {
            whale_protocol::CanonicalItem::tool_call(
                "provider-call",
                None,
                "search",
                Some(json!({"query":"original"})),
                r#"{"query":"original"}"#,
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

async fn send(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    server
        .handle_message(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
            writer,
        )
        .await;
    loop {
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("response timeout")
            .expect("response channel");
        if value.get("id") == Some(&json!(id)) {
            return value;
        }
    }
}

async fn start_approval(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
) -> String {
    let initialized = send(
        server,
        writer,
        rx,
        1,
        "protocol.initialize",
        json!({
            "client":{"name":"interaction-response-test","version":"1"},
            "protocol_versions":[1],
            "required_capabilities":[CAPABILITY_INTERACTIONS]
        }),
    )
    .await;
    assert!(initialized.get("error").is_none(), "{initialized}");
    let started = send(
        server,
        writer,
        rx,
        2,
        "session.start_thread",
        json!({
            "session_id":"interaction-session",
            "model":"mock",
            "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
            "tools":[{
                "name":"search",
                "description":"search",
                "parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false},
                "require_approval":true,
                "is_host_tool":true
            }],
            "interactions_enabled":true
        }),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let run = send(
        server,
        writer,
        rx,
        3,
        "thread.start_turn",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "input_items":[],
            "max_steps":2
        }),
    )
    .await;
    assert!(run.get("error").is_none(), "{run}");
    loop {
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("Interaction Requested timeout")
            .expect("capture remains open");
        if value["method"] == "session.interaction_event" && value["params"]["type"] == "requested"
        {
            return value["params"]["interaction"]["request_id"]
                .as_str()
                .unwrap()
                .to_owned();
        }
    }
}

#[tokio::test]
async fn generic_and_typed_paths_share_one_redacted_idempotency_transaction() {
    let (server, writer, mut rx) = fixture();
    let request_id = start_approval(&server, &writer, &mut rx).await;

    let before = send(
        &server,
        &writer,
        &mut rx,
        4,
        "session.interactions.get",
        json!({"thread_id":"interaction-session"}),
    )
    .await;
    assert_eq!(before["result"]["pending"][0]["request_id"], request_id);
    let requested_cursor = before["result"]["cursor"].clone();

    let accepted = send(
        &server,
        &writer,
        &mut rx,
        5,
        "turn.respond_interaction",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"approve"}
        }),
    )
    .await;
    assert_eq!(accepted["result"]["resolved"], true, "{accepted}");

    let retry = send(
        &server,
        &writer,
        &mut rx,
        6,
        "turn.resolve_approval",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "decision":"approve"
        }),
    )
    .await;
    assert_eq!(retry["result"]["resolved"], true, "{retry}");

    let conflict = send(
        &server,
        &writer,
        &mut rx,
        7,
        "turn.respond_interaction",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"reject"}
        }),
    )
    .await;
    assert_eq!(
        conflict["error"]["code"], INTERACTION_CONFLICT,
        "{conflict}"
    );

    let after = send(
        &server,
        &writer,
        &mut rx,
        8,
        "session.interactions.get",
        json!({"thread_id":"interaction-session"}),
    )
    .await;
    assert_eq!(after["result"]["pending"], json!([]));
    let encoded = serde_json::to_string(&after).unwrap();
    assert!(!encoded.contains("fingerprint"));
    assert!(!encoded.contains("approve"));

    let replay = send(
        &server,
        &writer,
        &mut rx,
        9,
        "session.interactions.subscribe",
        json!({
            "thread_id":"interaction-session",
            "after":requested_cursor,
            "limit":16
        }),
    )
    .await;
    assert_eq!(replay["result"]["events"].as_array().unwrap().len(), 1);
    let encoded = serde_json::to_string(&replay).unwrap();
    assert!(!encoded.contains("fingerprint"));
    assert!(!encoded.contains("approve"));
}

#[tokio::test]
async fn invalid_response_stays_pending_for_a_corrected_retry() {
    let (server, writer, mut rx) = fixture();
    let request_id = start_approval(&server, &writer, &mut rx).await;
    let invalid = send(
        &server,
        &writer,
        &mut rx,
        10,
        "turn.respond_interaction",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"maybe","secret":"must-not-echo"}
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32052, "{invalid}");
    assert!(!invalid.to_string().contains("must-not-echo"));

    let pending = send(
        &server,
        &writer,
        &mut rx,
        11,
        "turn.interactions.get",
        json!({"thread_id":"interaction-session","turn_id":"turn-1"}),
    )
    .await;
    assert_eq!(pending["result"]["pending"][0]["request_id"], request_id);

    let corrected = send(
        &server,
        &writer,
        &mut rx,
        12,
        "turn.respond_interaction",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"reject"}
        }),
    )
    .await;
    assert_eq!(corrected["result"]["resolved"], true, "{corrected}");
}

#[tokio::test]
async fn core_generic_typed_and_legacy_resolvers_share_the_same_commit() {
    let (server, writer, mut rx) = fixture();
    let request_id = start_approval(&server, &writer, &mut rx).await;

    assert!(server
        .approval_gate()
        .resolve_approval(&request_id, whale_core::ApprovalDecision::Accept));
    let generic_retry = send(
        &server,
        &writer,
        &mut rx,
        20,
        "turn.respond_interaction",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"approve"}
        }),
    )
    .await;
    assert_eq!(generic_retry["result"]["resolved"], true, "{generic_retry}");
    let typed_retry = send(
        &server,
        &writer,
        &mut rx,
        21,
        "turn.resolve_approval",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "decision":"approve"
        }),
    )
    .await;
    assert_eq!(typed_retry["result"]["resolved"], true, "{typed_retry}");
    let legacy_retry = send(
        &server,
        &writer,
        &mut rx,
        22,
        "approval.resolve",
        json!({"request_id":request_id,"decision":"approve"}),
    )
    .await;
    assert_eq!(legacy_retry["result"]["resolved"], true, "{legacy_retry}");
    let legacy_conflict = send(
        &server,
        &writer,
        &mut rx,
        23,
        "approval.resolve",
        json!({"request_id":request_id,"decision":"reject"}),
    )
    .await;
    assert_eq!(legacy_conflict["error"]["message"], "ApprovalConflict");

    let mut id = 24;
    let run = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let run = send(
                &server,
                &writer,
                &mut rx,
                id,
                "turn.get",
                json!({"thread_id":"interaction-session","turn_id":"turn-1"}),
            )
            .await;
            id += 1;
            if run["result"]["pending_approvals"] == json!([]) {
                break run;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delegated Core response did not commit typed projection");
    assert_eq!(run["result"]["pending_approvals"], json!([]), "{run}");
    assert_ne!(run["result"]["status"], "waiting_approval", "{run}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_equivalent_responses_deliver_once_and_replay_one_removal() {
    let (server, writer, mut rx) = fixture();
    let request_id = start_approval(&server, &writer, &mut rx).await;
    let requested_cursor = send(
        &server,
        &writer,
        &mut rx,
        30,
        "session.interactions.get",
        json!({"thread_id":"interaction-session"}),
    )
    .await["result"]["cursor"]
        .clone();

    let mut responders = Vec::new();
    for id in [31, 32] {
        let server = server.clone();
        let writer = writer.clone();
        let request_id = request_id.clone();
        responders.push(tokio::spawn(async move {
            server
                .handle_message(
                    &json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "method":"turn.respond_interaction",
                        "params":{
                            "thread_id":"interaction-session",
                            "turn_id":"turn-1",
                            "request_id":request_id,
                            "response":{"decision":"approve"}
                        }
                    })
                    .to_string(),
                    &writer,
                )
                .await;
        }));
    }
    let mut acknowledgements = Vec::new();
    while acknowledgements.len() < 2 {
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if value["id"] == 31 || value["id"] == 32 {
            acknowledgements.push(value);
        }
    }
    for responder in responders {
        responder.await.unwrap();
    }
    assert!(acknowledgements
        .iter()
        .all(|value| value["result"]["resolved"] == true));

    let replay = send(
        &server,
        &writer,
        &mut rx,
        33,
        "session.interactions.subscribe",
        json!({
            "thread_id":"interaction-session",
            "after":requested_cursor,
            "limit":16
        }),
    )
    .await;
    let removals: Vec<_> = replay["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == "removed")
        .collect();
    assert_eq!(removals.len(), 1, "{replay}");
    assert_eq!(removals[0]["request_id"], request_id);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while server.approval_gate().pending_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resolved approval continuation was not released");
}
