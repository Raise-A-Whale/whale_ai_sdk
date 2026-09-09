use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify, Semaphore};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::{
    interactions::{
        CAPABILITY_INTERACTIONS, INTERACTION_NOT_FOUND, INTERACTION_REMOVAL_CANCELLED,
        INTERACTION_REMOVAL_CONNECTION_CLOSED, INTERACTION_REMOVAL_ORIGIN_FINISHED,
        INTERACTION_REMOVAL_SESSION_CLOSED, INTERACTION_UNAVAILABLE,
    },
    recovery::RecoveryKey,
    runs::{PendingApproval, RunSnapshot, RunStatus, StartTurnParams},
    AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase,
};
use whale_store::{MemoryStore, StoreRuntime};

struct Capture {
    messages: mpsc::UnboundedSender<Value>,
    terminal_entered: Option<Arc<Notify>>,
    terminal_release: Option<Arc<Semaphore>>,
}

#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let value: Value = serde_json::from_str(line).expect("daemon emits JSON");
        if value["method"] == "turn.event" && value["params"]["type"] == "finished" {
            if let Some(entered) = &self.terminal_entered {
                entered.notify_one();
            }
            if let Some(release) = &self.terminal_release {
                release.acquire().await.unwrap().forget();
            }
        }
        self.messages
            .send(value)
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

struct Fixture {
    server: DaemonServer,
    gate: Arc<ApprovalGate>,
    writer: AnyTransportWriter,
    messages: mpsc::UnboundedReceiver<Value>,
    terminal_entered: Arc<Notify>,
    terminal_release: Arc<Semaphore>,
}

fn fixture(block_terminal: bool) -> Fixture {
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
                "lookup",
                Some(json!({"query": "original"})),
                r#"{"query":"original"}"#,
            )
        } else {
            CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "provider".into(),
                item,
            }),
            Ok(AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            }),
        ])))
    }));
    let (messages, receiver) = mpsc::unbounded_channel();
    let terminal_entered = Arc::new(Notify::new());
    let terminal_release = Arc::new(Semaphore::new(0));
    let writer = AnyTransportWriter::new(Arc::new(Capture {
        messages,
        terminal_entered: block_terminal.then(|| terminal_entered.clone()),
        terminal_release: block_terminal.then(|| terminal_release.clone()),
    }));
    Fixture {
        server: DaemonServer::new(Arc::new(engine), gate.clone()),
        gate,
        writer,
        messages: receiver,
        terminal_entered,
        terminal_release,
    }
}

async fn handle(
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

async fn next_matching(
    messages: &mut mpsc::UnboundedReceiver<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = messages.recv().await.expect("capture remains open");
            if predicate(&value) {
                return value;
            }
        }
    })
    .await
    .expect("matching daemon frame timeout")
}

async fn rpc(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    messages: &mut mpsc::UnboundedReceiver<Value>,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    handle(server, writer, id, method, params).await;
    next_matching(messages, |value| value.get("id") == Some(&json!(id))).await
}

async fn initialize(fixture: &mut Fixture) {
    let result = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        1,
        "protocol.initialize",
        json!({
            "client":{"name":"interaction-lifecycle-test","version":"1"},
            "protocol_versions":[1],
            "required_capabilities":[CAPABILITY_INTERACTIONS]
        }),
    )
    .await;
    assert!(result.get("error").is_none(), "{result}");
}

async fn create_session(fixture: &mut Fixture, approval: bool) {
    let result = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        2,
        "session.start_thread",
        json!({
            "session_id":"lifecycle-session",
            "model":"mock",
            "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
            "tools":[{
                "name":"lookup",
                "description":"lookup",
                "parameters":{
                    "type":"object",
                    "properties":{"query":{"type":"string"}},
                    "required":["query"],
                    "additionalProperties":false
                },
                "require_approval":approval,
                "is_host_tool":true
            }],
            "interactions_enabled":true
        }),
    )
    .await;
    assert!(result.get("error").is_none(), "{result}");
}

async fn start_turn(fixture: &mut Fixture, timeout_ms: Option<u64>) {
    let mut params = json!({
        "thread_id":"lifecycle-session",
        "turn_id":"turn-1",
        "input_items":[],
        "max_steps":2
    });
    if let Some(timeout_ms) = timeout_ms {
        params["timeout_ms"] = json!(timeout_ms);
    }
    let result = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        3,
        "thread.start_turn",
        params,
    )
    .await;
    assert!(result.get("error").is_none(), "{result}");
}

async fn requested(fixture: &mut Fixture) -> Value {
    next_matching(&mut fixture.messages, |value| {
        value["method"] == "session.interaction_event" && value["params"]["type"] == "requested"
    })
    .await
}

async fn assert_removal_replay(
    fixture: &mut Fixture,
    requested: &Value,
    expected_cause: &str,
    id: u64,
) {
    let replay = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        id,
        "session.interactions.subscribe",
        json!({
            "thread_id":"lifecycle-session",
            "after":requested["params"]["cursor"],
            "limit":16
        }),
    )
    .await;
    assert!(replay.get("error").is_none(), "{replay}");
    assert_eq!(replay["result"]["events"].as_array().unwrap().len(), 1);
    assert_eq!(replay["result"]["events"][0]["type"], "removed");
    assert_eq!(replay["result"]["events"][0]["cause"], expected_cause);
    assert_eq!(
        replay["result"]["events"][0]["cursor"]["seq"],
        requested["params"]["cursor"]["seq"].as_u64().unwrap() + 1
    );
}

#[tokio::test]
async fn cancel_and_deadline_remove_before_terminal_and_reject_late_response() {
    for deadline in [false, true] {
        let mut fixture = fixture(false);
        initialize(&mut fixture).await;
        create_session(&mut fixture, true).await;
        start_turn(&mut fixture, deadline.then_some(150)).await;
        let requested = requested(&mut fixture).await;
        let request_id = requested["params"]["interaction"]["request_id"]
            .as_str()
            .unwrap()
            .to_owned();

        if !deadline {
            let cancelled = rpc(
                &fixture.server,
                &fixture.writer,
                &mut fixture.messages,
                4,
                "turn.cancel",
                json!({"thread_id":"lifecycle-session","turn_id":"turn-1"}),
            )
            .await;
            assert!(cancelled.get("error").is_none(), "{cancelled}");
        }

        let terminal = next_matching(&mut fixture.messages, |value| {
            value["method"] == "turn.event" && value["params"]["type"] == "finished"
        })
        .await;
        if deadline {
            assert_eq!(terminal["params"]["snapshot"]["status"], "failed");
            assert_eq!(
                terminal["params"]["snapshot"]["error"]["code"],
                "DEADLINE_EXCEEDED"
            );
        } else {
            assert_eq!(terminal["params"]["snapshot"]["status"], "cancelled");
        }
        assert_eq!(fixture.gate.pending_count(), 0);
        assert_removal_replay(&mut fixture, &requested, INTERACTION_REMOVAL_CANCELLED, 5).await;

        let late = rpc(
            &fixture.server,
            &fixture.writer,
            &mut fixture.messages,
            6,
            "turn.respond_interaction",
            json!({
                "thread_id":"lifecycle-session",
                "turn_id":"turn-1",
                "request_id":request_id,
                "response":{"decision":"approve"}
            }),
        )
        .await;
        assert_eq!(late["error"]["code"], INTERACTION_NOT_FOUND, "{late}");
        assert!(fixture.server.pending_host_tool_calls().is_empty());
    }
}

#[tokio::test]
async fn completing_host_callback_clears_exact_nested_request_and_wakes_its_rpc() {
    let mut fixture = fixture(false);
    initialize(&mut fixture).await;
    create_session(&mut fixture, false).await;
    start_turn(&mut fixture, None).await;
    let host = next_matching(&mut fixture.messages, |value| {
        value["method"] == "tool.execute_host"
    })
    .await;
    let host_call_id = host["params"]["call_id"].as_str().unwrap().to_owned();
    let nested_request_id = uuid::Uuid::new_v4().to_string();
    let nested = {
        let server = fixture.server.clone();
        let writer = fixture.writer.clone();
        let host_call_id = host_call_id.clone();
        let nested_request_id = nested_request_id.clone();
        tokio::spawn(async move {
            handle(
                &server,
                &writer,
                20,
                "turn.request_interaction",
                json!({
                    "thread_id":"lifecycle-session",
                    "turn_id":"turn-1",
                    "host_call_id":host_call_id,
                    "request_id":nested_request_id,
                    "kind":"vendor.question",
                    "title":"Need callback input",
                    "payload":{"safe":"display"},
                    "response_schema":{
                        "$schema":"https://json-schema.org/draft/2020-12/schema",
                        "type":"object",
                        "properties":{"answer":{"type":"string"}},
                        "required":["answer"],
                        "additionalProperties":false
                    }
                }),
            )
            .await;
        })
    };
    let requested = requested(&mut fixture).await;
    assert_eq!(
        requested["params"]["interaction"]["request_id"],
        nested_request_id
    );

    fixture
        .server
        .handle_message(
            &json!({
                "jsonrpc":"2.0",
                "id":host["id"],
                "result":{
                    "call_id":host_call_id,
                    "output":CanonicalToolOutput::text("done"),
                    "is_error":false
                }
            })
            .to_string(),
            &fixture.writer,
        )
        .await;
    let nested_result = next_matching(&mut fixture.messages, |value| value["id"] == 20).await;
    assert_eq!(
        nested_result["error"]["code"], INTERACTION_NOT_FOUND,
        "{nested_result}"
    );
    nested.await.unwrap();
    let terminal = next_matching(&mut fixture.messages, |value| {
        value["method"] == "turn.event" && value["params"]["type"] == "finished"
    })
    .await;
    assert_eq!(terminal["params"]["snapshot"]["status"], "completed");
    assert_removal_replay(
        &mut fixture,
        &requested,
        INTERACTION_REMOVAL_ORIGIN_FINISHED,
        21,
    )
    .await;
}

#[tokio::test]
async fn abandoned_close_waiter_still_clears_pending_and_closes_interaction_routes() {
    let mut fixture = fixture(true);
    initialize(&mut fixture).await;
    create_session(&mut fixture, true).await;
    start_turn(&mut fixture, None).await;
    let requested = requested(&mut fixture).await;
    let request_id = requested["params"]["interaction"]["request_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let close = {
        let server = fixture.server.clone();
        let writer = fixture.writer.clone();
        tokio::spawn(async move {
            handle(
                &server,
                &writer,
                30,
                "session.close",
                json!({"thread_id":"lifecycle-session"}),
            )
            .await;
        })
    };
    tokio::time::timeout(Duration::from_secs(2), fixture.terminal_entered.notified())
        .await
        .expect("close did not reach terminal delivery");
    close.abort();
    let _ = close.await;
    assert_eq!(fixture.gate.pending_count(), 0);
    fixture.terminal_release.add_permits(1);

    let mut removed = false;
    let mut terminal = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !(removed && terminal) {
            let value = fixture.messages.recv().await.unwrap();
            if value["method"] == "session.interaction_event"
                && value["params"]["type"] == "removed"
            {
                assert_eq!(value["params"]["request_id"], request_id);
                assert_eq!(value["params"]["cause"], INTERACTION_REMOVAL_SESSION_CLOSED);
                removed = true;
            }
            if value["method"] == "turn.event" && value["params"]["type"] == "finished" {
                terminal = true;
            }
        }
    })
    .await
    .expect("daemon-owned close cleanup did not finish");

    let repeated = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        31,
        "session.close",
        json!({"thread_id":"lifecycle-session"}),
    )
    .await;
    assert_eq!(repeated["result"]["closed"], false, "{repeated}");
    let closed_route = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        32,
        "session.interactions.get",
        json!({"thread_id":"lifecycle-session"}),
    )
    .await;
    assert_eq!(
        closed_route["error"]["code"], INTERACTION_UNAVAILABLE,
        "{closed_route}"
    );
    let late = rpc(
        &fixture.server,
        &fixture.writer,
        &mut fixture.messages,
        33,
        "turn.respond_interaction",
        json!({
            "thread_id":"lifecycle-session",
            "turn_id":"turn-1",
            "request_id":request_id,
            "response":{"decision":"approve"}
        }),
    )
    .await;
    assert!(late.get("error").is_some(), "{late}");
    assert!(fixture.server.sessions().is_empty());
}

#[tokio::test]
async fn reader_eof_clears_pending_with_connection_scope_and_reclaims_session() {
    let fixture = fixture(false);
    let server = fixture.server;
    let gate = fixture.gate;
    let (messages, mut output) = mpsc::unbounded_channel();
    let (input, incoming) = mpsc::channel(8);
    let running = {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .run(
                    tokio_stream::wrappers::ReceiverStream::new(incoming),
                    Capture {
                        messages,
                        terminal_entered: None,
                        terminal_release: None,
                    },
                )
                .await
                .unwrap();
        })
    };
    input
        .send(Ok(json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"protocol.initialize",
            "params":{
                "client":{"name":"interaction-eof-test","version":"1"},
                "protocol_versions":[1],
                "required_capabilities":[CAPABILITY_INTERACTIONS]
            }
        })
        .to_string()))
        .await
        .unwrap();
    next_matching(&mut output, |value| value["id"] == 1).await;
    input
        .send(Ok(json!({
            "jsonrpc":"2.0",
            "id":2,
            "method":"session.start_thread",
            "params":{
                "session_id":"lifecycle-session",
                "model":"mock",
                "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
                "tools":[{
                    "name":"lookup",
                    "description":"lookup",
                    "parameters":{},
                    "require_approval":true,
                    "is_host_tool":true
                }],
                "interactions_enabled":true
            }
        })
        .to_string()))
        .await
        .unwrap();
    next_matching(&mut output, |value| value["id"] == 2).await;
    input
        .send(Ok(json!({
            "jsonrpc":"2.0",
            "id":3,
            "method":"thread.start_turn",
            "params":{
                "thread_id":"lifecycle-session",
                "turn_id":"turn-1",
                "input_items":[],
                "max_steps":2
            }
        })
        .to_string()))
        .await
        .unwrap();
    next_matching(&mut output, |value| value["id"] == 3).await;
    next_matching(&mut output, |value| {
        value["method"] == "session.interaction_event" && value["params"]["type"] == "requested"
    })
    .await;
    assert_eq!(gate.pending_count(), 1);

    drop(input);
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("reader EOF left daemon cleanup running")
        .unwrap();
    assert_eq!(gate.pending_count(), 0);
    assert!(server.sessions().is_empty());

    // EOF does not promise final frame delivery, but if the queued removal is
    // observed it must expose only the connection-scoped cause.
    while let Ok(value) = output.try_recv() {
        if value["method"] == "session.interaction_event" && value["params"]["type"] == "removed" {
            assert_eq!(
                value["params"]["cause"],
                INTERACTION_REMOVAL_CONNECTION_CLOSED
            );
        }
    }
}

#[tokio::test]
async fn recovered_pending_run_is_interrupted_and_attach_starts_a_fresh_empty_stream() {
    let before = "11111111-1111-4111-8111-111111111111";
    let after = "22222222-2222-4222-8222-222222222222";
    let backend = Arc::new(MemoryStore::new());
    let first_runtime = Arc::new(StoreRuntime::open(backend.clone()).await.unwrap());
    let mut first = fixture(false);
    first.server = first
        .server
        .clone()
        .with_store_runtime(first_runtime.clone());
    initialize(&mut first).await;
    let key = RecoveryKey::new();
    let created = rpc(
        &first.server,
        &first.writer,
        &mut first.messages,
        60,
        "session.create_persistent",
        json!({
            "key":key,
            "session":{
                "session_id":before,
                "model":"mock",
                "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
                "tools":[]
            },
            "run_defaults":{"max_steps":2},
            "interactions_enabled":true
        }),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    let old_snapshot = rpc(
        &first.server,
        &first.writer,
        &mut first.messages,
        61,
        "session.interactions.get",
        json!({"thread_id":before}),
    )
    .await;
    assert_eq!(old_snapshot["result"]["cursor"]["seq"], 0);
    let closed = rpc(
        &first.server,
        &first.writer,
        &mut first.messages,
        62,
        "session.close",
        json!({"thread_id":before}),
    )
    .await;
    assert_eq!(closed["result"]["closed"], true, "{closed}");

    let detached = first_runtime.inspect(&key).await.unwrap();
    let crash_journal = first_runtime
        .attach(
            &key,
            detached.revision,
            detached.configuration.clone(),
            "crash-owner".into(),
            "crash-thread".into(),
        )
        .await
        .unwrap();
    let tool_call = CanonicalItem::tool_call("pending-call", None, "lookup", Some(json!({})), "{}");
    crash_journal
        .begin_run(
            StartTurnParams {
                thread_id: "crash-thread".into(),
                turn_id: "pending-turn".into(),
                input_items: Vec::new(),
                options: None,
                max_steps: 2,
                timeout_ms: None,
            },
            RunSnapshot {
                thread_id: "crash-thread".into(),
                turn_id: "pending-turn".into(),
                status: RunStatus::WaitingApproval,
                items: Vec::new(),
                usage: Default::default(),
                tool_executions: Vec::new(),
                pending_approvals: vec![PendingApproval {
                    request_id: "pending-request".into(),
                    tool_call,
                    reason: None,
                }],
                last_seq: 1,
                result: None,
                error: None,
            },
            json!({"model":"mock"}),
        )
        .await
        .unwrap();
    drop(crash_journal);

    let reopened = Arc::new(StoreRuntime::open(backend).await.unwrap());
    let recovered = reopened.inspect(&key).await.unwrap();
    assert_eq!(recovered.runs[0].snapshot.status, RunStatus::Failed);
    assert_eq!(
        recovered.runs[0].snapshot.error.as_ref().unwrap().code,
        "RECOVERY_INTERRUPTED"
    );
    assert!(recovered.runs[0].snapshot.pending_approvals.is_empty());

    let mut second = fixture(false);
    second.server = second.server.clone().with_store_runtime(reopened);
    initialize(&mut second).await;
    let attached = rpc(
        &second.server,
        &second.writer,
        &mut second.messages,
        63,
        "session.recovery.attach",
        json!({
            "key":key,
            "expected_revision":recovered.revision,
            "session":{
                "session_id":after,
                "model":"mock",
                "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
                "tools":[]
            },
            "run_defaults":{"max_steps":2},
            "interactions_enabled":true
        }),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    let fresh = rpc(
        &second.server,
        &second.writer,
        &mut second.messages,
        64,
        "session.interactions.get",
        json!({"thread_id":after}),
    )
    .await;
    assert_eq!(fresh["result"]["cursor"]["seq"], 0, "{fresh}");
    assert_eq!(fresh["result"]["pending"], json!([]), "{fresh}");
    assert_ne!(
        fresh["result"]["cursor"]["stream_id"],
        old_snapshot["result"]["cursor"]["stream_id"]
    );
    while let Ok(value) = second.messages.try_recv() {
        assert_ne!(value["method"], "session.interaction_event", "{value}");
    }
}
