use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::interactions::{CAPABILITY_INTERACTIONS, INTERACTION_UNAVAILABLE};

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
        let value = rx.recv().await.expect("response");
        if value.get("id") == Some(&json!(id)) {
            return value;
        }
    }
}

#[tokio::test]
async fn negotiated_opt_in_exposes_fresh_session_and_turn_snapshots() {
    let (server, writer, mut rx) = fixture();
    let initialized = send(
        &server,
        &writer,
        &mut rx,
        1,
        "protocol.initialize",
        json!({
            "client":{"name":"interaction-view-test","version":"1"},
            "protocol_versions":[1],
            "required_capabilities":[CAPABILITY_INTERACTIONS]
        }),
    )
    .await;
    assert!(initialized["result"]["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|capability| capability == CAPABILITY_INTERACTIONS));

    let started = send(
        &server,
        &writer,
        &mut rx,
        2,
        "session.start_thread",
        json!({
            "session_id":"interaction-session",
            "model":"mock",
            "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
            "interactions_enabled":true
        }),
    )
    .await;
    assert_eq!(started["result"]["thread_id"], "interaction-session");

    let started_run = send(
        &server,
        &writer,
        &mut rx,
        3,
        "thread.start_turn",
        json!({
            "thread_id":"interaction-session",
            "turn_id":"turn-1",
            "input_items":[],
            "max_steps":1
        }),
    )
    .await;
    assert_eq!(started_run["result"]["turn_id"], "turn-1");

    let session = send(
        &server,
        &writer,
        &mut rx,
        4,
        "session.interactions.get",
        json!({"thread_id":"interaction-session"}),
    )
    .await;
    assert_eq!(session["result"]["thread_id"], "interaction-session");
    assert_eq!(session["result"]["cursor"]["seq"], 0);
    assert_eq!(session["result"]["pending"], json!([]));

    let run = send(
        &server,
        &writer,
        &mut rx,
        5,
        "turn.interactions.get",
        json!({"thread_id":"interaction-session","turn_id":"turn-1"}),
    )
    .await;
    assert_eq!(run["result"]["thread_id"], "interaction-session");
    assert_eq!(run["result"]["turn_id"], "turn-1");
    assert_eq!(run["result"]["cursor"], session["result"]["cursor"]);
    assert_eq!(run["result"]["pending"], json!([]));

    let replay = send(
        &server,
        &writer,
        &mut rx,
        6,
        "session.interactions.subscribe",
        json!({
            "thread_id":"interaction-session",
            "after":session["result"]["cursor"],
            "limit":128
        }),
    )
    .await;
    assert_eq!(replay["result"]["events"], json!([]));
    assert_eq!(replay["result"]["through"]["seq"], 0);
    assert_eq!(replay["result"]["has_more"], false);
}

#[tokio::test]
async fn legacy_sessions_default_disabled_and_foreign_queries_are_indistinguishable() {
    let (server, writer, mut rx) = fixture();
    let initialized = send(
        &server,
        &writer,
        &mut rx,
        10,
        "protocol.initialize",
        json!({
            "client":{"name":"legacy-view-test","version":"1"},
            "protocol_versions":[1],
            "required_capabilities":[]
        }),
    )
    .await;
    assert!(initialized.get("error").is_none(), "{initialized}");
    let started = send(
        &server,
        &writer,
        &mut rx,
        11,
        "session.start_thread",
        json!({"session_id":"legacy-session","model":"mock"}),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let disabled = send(
        &server,
        &writer,
        &mut rx,
        12,
        "session.interactions.get",
        json!({"thread_id":"legacy-session"}),
    )
    .await;
    assert_eq!(disabled["error"]["code"], INTERACTION_UNAVAILABLE);

    let (foreign_tx, mut foreign_rx) = mpsc::unbounded_channel();
    let foreign = AnyTransportWriter::new(Arc::new(Capture(foreign_tx)));
    let foreign_init = send(
        &server,
        &foreign,
        &mut foreign_rx,
        13,
        "protocol.initialize",
        json!({
            "client":{"name":"foreign-view-test","version":"1"},
            "protocol_versions":[1],
            "required_capabilities":[CAPABILITY_INTERACTIONS]
        }),
    )
    .await;
    assert!(foreign_init.get("error").is_none(), "{foreign_init}");
    let hidden = send(
        &server,
        &foreign,
        &mut foreign_rx,
        14,
        "session.interactions.get",
        json!({"thread_id":"legacy-session"}),
    )
    .await;
    assert_eq!(hidden["error"]["code"], INTERACTION_UNAVAILABLE);
    let absent = send(
        &server,
        &foreign,
        &mut foreign_rx,
        15,
        "session.interactions.get",
        json!({"thread_id":"absent-session"}),
    )
    .await;
    assert_eq!(absent["error"]["code"], INTERACTION_UNAVAILABLE);
    assert_eq!(hidden["error"]["message"], absent["error"]["message"]);
}
