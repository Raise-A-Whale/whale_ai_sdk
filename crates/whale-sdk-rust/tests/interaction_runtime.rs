//! End-to-end Interaction proofs through the public Rust application SDK.

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex as StdMutex, OnceLock,
};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use whale_core::{
    AgentEngine, ApprovalGate, ToolContext as CoreToolContext, ToolExecutionCoordinator,
    ToolHandler, ToolRegistry,
};
use whale_daemon::{DaemonServer, OutgoingTransport};
use whale_protocol::runs::RunEventPayload;
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_sdk_rust::{
    AgentDefinition, HostTool, InteractionEventPayload, InteractionRequest,
    InteractionSubscriptionOptions, InteractionWatchOptions, RecoveryKey, RuntimeMode,
    RuntimeOptions, RuntimeSource, SdkError, ToolContext, WhaleRuntime,
};
use whale_store::{MemoryStore, SessionStore, StoreRuntime};

const TIMEOUT: Duration = Duration::from_secs(5);

fn interaction_request(kind: &str, title: &str) -> InteractionRequest {
    InteractionRequest::new(
        kind,
        title,
        json!({
            "provider": "fixture",
            "scopes": ["read"],
            "authorization_url": "https://auth.invalid/device",
            "display_code": "SAFE-CODE"
        }),
        Some(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"credential_ref": {"type": "string"}},
            "required": ["credential_ref"],
            "additionalProperties": false
        })),
    )
    .unwrap()
}

fn prompt_text(session: &whale_core::ThreadSession) -> String {
    use whale_protocol::CanonicalContent;
    session
        .history()
        .iter()
        .rev()
        .find_map(|item| match item {
            CanonicalItem::UserMessage { content, .. } => Some(
                content
                    .iter()
                    .filter_map(|block| match block {
                        CanonicalContent::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

fn engine(
    registry: Arc<ToolRegistry>,
    gate: Arc<ApprovalGate>,
    choose_tool: impl Fn(&str) -> &'static str + Send + Sync + 'static,
) -> AgentEngine {
    let coordinator = Arc::new(ToolExecutionCoordinator::new(registry, gate));
    AgentEngine::new(coordinator).with_stream_provider(Arc::new(move |session, step| {
        let item = if step == 0 {
            let tool = choose_tool(&prompt_text(session));
            CanonicalItem::tool_call(
                format!("{tool}-{}", uuid::Uuid::new_v4()),
                None,
                tool,
                Some(json!({})),
                "{}",
            )
        } else {
            CanonicalItem::assistant_text("fixture complete", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "provider-turn".into(),
                item,
            }),
            Ok(AgentStreamEvent::TurnCompleted {
                turn_id: "provider-turn".into(),
                thread_id: "provider-thread".into(),
                usage: Default::default(),
            }),
        ])))
    }))
}

struct HostInteractionTool {
    name: &'static str,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl HostTool for HostInteractionTool {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        "Requests host input without blocking the connection reader"
    }

    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Err("context-aware implementation was bypassed".into())
    }

    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<CanonicalToolOutput, String> {
        if !context
            .report_progress("waiting for host", Some(0.25))
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("progress route was not active".into());
        }
        let response = context
            .request_interaction(interaction_request(
                "example.runtime.input",
                "Choose a fixture credential",
            ))
            .await
            .map_err(|error| error.to_string())?;
        if response["credential_ref"].as_str().is_none() {
            return Err("credential_ref response is missing".into());
        }
        if !context
            .report_progress("host answered", Some(0.75))
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("progress route ended before callback completion".into());
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::text("host input accepted"))
    }
}

struct FastHostTool;

#[async_trait]
impl HostTool for FastHostTool {
    fn name(&self) -> &str {
        "fast"
    }

    fn description(&self) -> &str {
        "Completes immediately"
    }

    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("fast session complete"))
    }
}

async fn host_runtime() -> WhaleRuntime {
    let gate = Arc::new(ApprovalGate::new());
    let server = DaemonServer::new(
        Arc::new(engine(
            Arc::new(ToolRegistry::new()),
            gate.clone(),
            |prompt| {
                if prompt == "fast" {
                    "fast"
                } else {
                    "host_interaction"
                }
            },
        )),
        gate,
    );
    let options = RuntimeOptions {
        source: RuntimeSource::Embedded { server },
        startup_timeout: TIMEOUT,
        shutdown_timeout: TIMEOUT,
    };
    let runtime = WhaleRuntime::open(options).await.unwrap();
    assert_eq!(runtime.info().mode, RuntimeMode::Embedded);
    runtime
}

#[tokio::test]
async fn embedded_nested_callback_keeps_reader_live_and_sessions_isolated() {
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = host_runtime().await;
    let mut definition = AgentDefinition::new("interactive-host", "fixture");
    definition.tool_names = vec!["host_interaction".into(), "fast".into()];
    definition.timeout_ms = Some(5_000);
    let agent = runtime
        .agent(
            definition,
            vec![
                Arc::new(HostInteractionTool {
                    name: "host_interaction",
                    completed: completed.clone(),
                }),
                Arc::new(FastHostTool),
            ],
        )
        .unwrap()
        .with_interactions_enabled();
    let first = agent.create_session().await.unwrap();
    let second = agent.create_session().await.unwrap();
    let mut watch = first
        .watch_interactions(InteractionWatchOptions::default())
        .await
        .unwrap();
    assert!(watch.snapshot.pending.is_empty());

    let first_run = first.start_turn("needs input").await.unwrap();
    let mut run_events = first_run.events().unwrap();
    let requested = tokio::time::timeout(TIMEOUT, watch.events.recv())
        .await
        .expect("nested callback did not publish an Interaction")
        .expect("Interaction stream ended")
        .unwrap();
    let request_id = match &requested.payload {
        InteractionEventPayload::Requested { interaction } => {
            assert_eq!(interaction.turn_id, first_run.id());
            interaction.request_id.clone()
        }
        other => panic!("expected Requested, got {other:?}"),
    };

    // This second Session must finish while the first reverse callback is
    // suspended in a forward request on the same connection.
    let second_run = second.start_turn("fast").await.unwrap();
    let second_result = tokio::time::timeout(TIMEOUT, second_run.result())
        .await
        .expect("another Session was blocked by the nested callback")
        .unwrap();
    assert_eq!(second_result.status, whale_protocol::TurnStatus::Completed);
    assert!(second
        .interaction_snapshot()
        .await
        .unwrap()
        .pending
        .is_empty());
    assert_eq!(
        first_run
            .pending_interactions()
            .await
            .unwrap()
            .pending
            .len(),
        1
    );

    let resolved = tokio::time::timeout(
        TIMEOUT,
        first_run.respond_interaction(
            &request_id,
            json!({"credential_ref": "credential://embedded/fixture"}),
        ),
    )
    .await
    .expect("nested response deadlocked the connection reader")
    .unwrap();
    assert!(resolved.resolved);
    let removed = tokio::time::timeout(TIMEOUT, watch.events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        removed.payload,
        InteractionEventPayload::Removed { request_id: ref removed_id, .. }
            if removed_id == &request_id
    ));
    assert_eq!(
        tokio::time::timeout(TIMEOUT, first_run.result())
            .await
            .unwrap()
            .unwrap()
            .status,
        whale_protocol::TurnStatus::Completed
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    let mut progress = Vec::new();
    while let Some(event) = run_events.recv().await.unwrap() {
        if let RunEventPayload::Stream {
            event: AgentStreamEvent::ToolProgress { message, .. },
        } = event.payload
        {
            progress.push(message);
        }
    }
    assert_eq!(progress, ["waiting for host", "host answered"]);

    let closing_run = first.start_turn("needs input").await.unwrap();
    let closing_request = tokio::time::timeout(TIMEOUT, watch.events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closing_request.payload {
        InteractionEventPayload::Requested { .. } => {}
        other => panic!("expected Requested before close, got {other:?}"),
    }
    first.close().await.unwrap();
    assert_eq!(
        closing_run.result().await.unwrap().status,
        whale_protocol::TurnStatus::Interrupted
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert!(watch.events.recv().await.is_none());
    assert!(second
        .interaction_snapshot()
        .await
        .unwrap()
        .pending
        .is_empty());
    second.close().await.unwrap();
    runtime.shutdown().await.unwrap();
}

struct CoreInteractionTool {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolHandler for CoreInteractionTool {
    fn name(&self) -> &str {
        "core_interaction"
    }

    fn description(&self) -> &str {
        "Core-owned Interaction fixture"
    }

    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Err("context-aware implementation was bypassed".into())
    }

    async fn execute_with_context(
        &self,
        context: CoreToolContext,
        _: Value,
    ) -> Result<(CanonicalToolOutput, bool), String> {
        self.started.fetch_add(1, Ordering::SeqCst);
        context.report_progress("core waiting", Some(0.5)).await?;
        let response = context
            .request_interaction(interaction_request(
                "whale.auth",
                "Authorize the fixture provider",
            ))
            .await
            .map_err(|error| error.to_string())?;
        if response["credential_ref"].as_str().is_none() {
            return Err("credential_ref response is missing".into());
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok((CanonicalToolOutput::text("credential accepted"), false))
    }
}

struct TraceCapture {
    output: Arc<StdMutex<String>>,
    next_span: AtomicU64,
}

struct FieldCapture<'a>(&'a mut String);

impl tracing::field::Visit for FieldCapture<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}

impl tracing::Subscriber for TraceCapture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed).max(1))
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write;
        let mut line = String::new();
        let _ = write!(line, "{}", event.metadata().name());
        event.record(&mut FieldCapture(&mut line));
        let mut output = self.output.lock().unwrap();
        output.push_str(&line);
        output.push('\n');
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}

    fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
        Some(tracing::metadata::LevelFilter::TRACE)
    }
}

fn trace_output() -> Arc<StdMutex<String>> {
    static OUTPUT: OnceLock<Arc<StdMutex<String>>> = OnceLock::new();
    OUTPUT
        .get_or_init(|| {
            let output = Arc::new(StdMutex::new(String::new()));
            tracing::subscriber::set_global_default(TraceCapture {
                output: output.clone(),
                next_span: AtomicU64::new(1),
            })
            .expect("integration test installs one trace subscriber");
            output
        })
        .clone()
}

#[cfg(unix)]
struct WireCapture {
    attempted: Arc<StdMutex<Vec<String>>>,
    delivered: Arc<StdMutex<Vec<String>>>,
    dropped_requested: Arc<AtomicBool>,
    write: tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>,
}

#[cfg(unix)]
#[async_trait]
impl OutgoingTransport for WireCapture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.attempted.lock().unwrap().push(line.to_owned());
        let requested = serde_json::from_str::<Value>(line).is_ok_and(|value| {
            value["method"] == "session.interaction_event" && value["params"]["type"] == "requested"
        });
        if requested && self.dropped_requested.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.delivered.lock().unwrap().push(line.to_owned());
        let mut write = self.write.lock().await;
        write.write_all(line.as_bytes()).await?;
        write.write_all(b"\n").await?;
        write.flush().await
    }
}

#[cfg(unix)]
struct UdsDaemon {
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
    server: DaemonServer,
    inbound: Arc<StdMutex<Vec<String>>>,
    attempted_outbound: Arc<StdMutex<Vec<String>>>,
    delivered_outbound: Arc<StdMutex<Vec<String>>>,
    dropped_requested: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

#[cfg(unix)]
impl UdsDaemon {
    async fn start(server: DaemonServer) -> Self {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::LinesStream;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("interaction.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let inbound = Arc::new(StdMutex::new(Vec::new()));
        let attempted_outbound = Arc::new(StdMutex::new(Vec::new()));
        let delivered_outbound = Arc::new(StdMutex::new(Vec::new()));
        let dropped_requested = Arc::new(AtomicBool::new(true));
        let task = {
            let server = server.clone();
            let inbound = inbound.clone();
            let attempted = attempted_outbound.clone();
            let delivered = delivered_outbound.clone();
            let dropped = dropped_requested.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await?;
                let (read, write) = socket.into_split();
                let lines = LinesStream::new(BufReader::new(read).lines()).map(move |line| {
                    if let Ok(line) = &line {
                        inbound.lock().unwrap().push(line.clone());
                    }
                    line
                });
                server
                    .run(
                        lines,
                        WireCapture {
                            attempted,
                            delivered,
                            dropped_requested: dropped,
                            write: tokio::sync::Mutex::new(write),
                        },
                    )
                    .await
            })
        };
        Self {
            directory,
            path,
            server,
            inbound,
            attempted_outbound,
            delivered_outbound,
            dropped_requested,
            task,
        }
    }

    async fn finish(self) {
        assert!(self.directory.path().exists());
        tokio::time::timeout(TIMEOUT, self.task)
            .await
            .expect("UDS daemon did not stop after client EOF")
            .unwrap()
            .unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn external_uds_replays_a_lost_frame_resolves_once_and_scrubs_secrets() {
    const RAW_CREDENTIAL: &str = "sk-runtime-secret-never-serialized";
    const RESPONSE_VALUE: &str = "credential://external/opaque-fixture";
    let traces = trace_output();
    let trace_start = traces.lock().unwrap().len();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(ToolRegistry::new());
    registry
        .register(Arc::new(CoreInteractionTool {
            started: started.clone(),
            completed: completed.clone(),
        }))
        .unwrap();
    let gate = Arc::new(ApprovalGate::new());
    let backend = Arc::new(MemoryStore::new());
    let store_runtime = Arc::new(StoreRuntime::open(backend.clone()).await.unwrap());
    let server = DaemonServer::new(
        Arc::new(engine(registry, gate.clone(), |_| "core_interaction")),
        gate,
    )
    .with_store_runtime(store_runtime);
    let fixture = UdsDaemon::start(server).await;
    let runtime = WhaleRuntime::open(RuntimeOptions::external_uds(
        &fixture.path,
        TIMEOUT,
        TIMEOUT,
    ))
    .await
    .unwrap();
    assert_eq!(runtime.info().mode, RuntimeMode::ExternalUds);

    // The application keeps RAW_CREDENTIAL in its own credential store and
    // sends only an opaque reference in the Interaction response.
    assert!(RAW_CREDENTIAL.starts_with("sk-"));
    let recovery = RecoveryKey::new();
    let session = runtime
        .agent(
            AgentDefinition::new("external-interaction", "fixture"),
            Vec::new(),
        )
        .unwrap()
        .with_interactions_enabled()
        .create_persistent_session(&recovery)
        .await
        .unwrap();
    let initial = session.interaction_snapshot().await.unwrap();
    assert_eq!(initial.cursor.seq, 0);
    let run = session.start_turn("authorize").await.unwrap();

    let pending = tokio::time::timeout(TIMEOUT, async {
        loop {
            let snapshot = run.pending_interactions().await.unwrap();
            if let Some(pending) = snapshot.pending.first() {
                break pending.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Core tool did not suspend for Interaction");
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert!(!fixture.dropped_requested.load(Ordering::SeqCst));
    assert_eq!(
        fixture
            .delivered_outbound
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains("session.interaction_event"))
            .count(),
        0,
        "the deliberately dropped live Requested frame reached the SDK"
    );

    let mut replay = session
        .subscribe_interactions_from(
            initial.cursor.clone(),
            InteractionSubscriptionOptions::default(),
        )
        .await
        .unwrap();
    let recovered = tokio::time::timeout(TIMEOUT, replay.recv())
        .await
        .expect("lost Requested frame was not replayed")
        .expect("Interaction replay ended")
        .unwrap();
    assert!(matches!(
        &recovered.payload,
        InteractionEventPayload::Requested { interaction }
            if interaction.request_id == pending.request_id
    ));

    let response = json!({"credential_ref": RESPONSE_VALUE});
    let (first, second) = tokio::join!(
        run.respond_interaction(&pending.request_id, response.clone()),
        run.respond_interaction(&pending.request_id, response.clone())
    );
    assert!(first.unwrap().resolved);
    assert!(second.unwrap().resolved);
    let removed = tokio::time::timeout(TIMEOUT, replay.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        &removed.payload,
        InteractionEventPayload::Removed { request_id, .. }
            if request_id == &pending.request_id
    ));
    assert_eq!(
        tokio::time::timeout(TIMEOUT, run.result())
            .await
            .unwrap()
            .unwrap()
            .status,
        whale_protocol::TurnStatus::Completed
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    let final_snapshot = session.interaction_snapshot().await.unwrap();
    assert!(final_snapshot.pending.is_empty());

    let projections = serde_json::to_string(&json!({
        "initial": initial,
        "recovered": recovered,
        "removed": removed,
        "final": final_snapshot
    }))
    .unwrap();
    assert!(!projections.contains(RESPONSE_VALUE));
    assert!(!projections.contains(RAW_CREDENTIAL));
    assert!(!projections.contains("fingerprint"));

    // Start one more suspension, then prove owner shutdown/EOF releases it.
    let eof_watch = session
        .watch_interactions(InteractionWatchOptions::default())
        .await
        .unwrap();
    let mut eof_events = eof_watch.events;
    let eof_run = session.start_turn("authorize again").await.unwrap();
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if !eof_run
                .pending_interactions()
                .await
                .unwrap()
                .pending
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtime.shutdown().await.unwrap();
    let stream_error = tokio::time::timeout(TIMEOUT, eof_events.recv())
        .await
        .expect("Interaction stream did not terminate at EOF")
        .expect("connection EOF reports one terminal stream error")
        .unwrap_err();
    assert!(matches!(
        stream_error,
        whale_sdk_rust::InteractionViewError::Sdk(SdkError::ChannelClosed(_))
    ));
    assert!(eof_events.recv().await.is_none());
    assert!(eof_run.result().await.is_err());

    let inbound = fixture.inbound.lock().unwrap().clone();
    let attempted_outbound = fixture.attempted_outbound.lock().unwrap().clone();
    let delivered_outbound = fixture.delivered_outbound.lock().unwrap().clone();
    let retained_server = fixture.server.clone();
    fixture.finish().await;
    assert!(retained_server.sessions().is_empty());
    assert_eq!(retained_server.approval_gate().pending_count(), 0);

    let records = backend.list().await.unwrap();
    assert_eq!(records.len(), 1);
    let stored = serde_json::to_string(&records).unwrap();
    let inbound_with_response: Vec<_> = inbound
        .iter()
        .filter(|line| line.contains(RESPONSE_VALUE))
        .collect();
    assert_eq!(inbound_with_response.len(), 2);
    assert!(inbound_with_response
        .iter()
        .all(|line| line.contains("turn.respond_interaction")));
    for captured in [&attempted_outbound, &delivered_outbound] {
        assert!(captured.iter().all(|line| !line.contains(RESPONSE_VALUE)));
        assert!(captured.iter().all(|line| !line.contains(RAW_CREDENTIAL)));
        assert!(captured.iter().all(|line| !line.contains("fingerprint")));
    }
    assert!(inbound.iter().all(|line| !line.contains(RAW_CREDENTIAL)));
    assert!(!stored.contains(RESPONSE_VALUE));
    assert!(!stored.contains(RAW_CREDENTIAL));
    assert!(!stored.contains(&recovery.secret));
    assert!(!stored.contains("fingerprint"));
    assert!(!stored.contains("pending_interaction"));

    let traces = traces.lock().unwrap();
    let trace_tail = &traces[trace_start..];
    assert!(trace_tail.contains("Received JSON-RPC frame"));
    assert!(!trace_tail.contains(RESPONSE_VALUE));
    assert!(!trace_tail.contains(RAW_CREDENTIAL));
    assert!(!trace_tail.contains("fingerprint"));
}

#[tokio::test]
#[ignore = "requires WHALE_INTERACTION_STDIO_FIXTURE=target/debug/examples/sdk_fixture"]
async fn managed_stdio_runs_the_public_interaction_chain() {
    let executable = std::env::var_os("WHALE_INTERACTION_STDIO_FIXTURE")
        .expect("set WHALE_INTERACTION_STDIO_FIXTURE to the built sdk_fixture example");
    let runtime = WhaleRuntime::open(RuntimeOptions::managed(executable))
        .await
        .unwrap();
    assert_eq!(runtime.info().mode, RuntimeMode::ManagedProcess);
    let completed = Arc::new(AtomicUsize::new(0));
    let mut definition = AgentDefinition::new("managed-interaction", "fixture");
    definition.tool_names = vec!["lookup".into()];
    let session = runtime
        .agent(
            definition,
            vec![Arc::new(HostInteractionTool {
                name: "lookup",
                completed: completed.clone(),
            })],
        )
        .unwrap()
        .with_interactions_enabled()
        .create_session()
        .await
        .unwrap();
    let mut watch = session
        .watch_interactions(InteractionWatchOptions::default())
        .await
        .unwrap();
    let run = session.start_turn("managed interaction").await.unwrap();
    let requested = tokio::time::timeout(TIMEOUT, watch.events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let request_id = match requested.payload {
        InteractionEventPayload::Requested { interaction } => interaction.request_id,
        other => panic!("expected Requested, got {other:?}"),
    };
    assert!(
        run.respond_interaction(
            &request_id,
            json!({"credential_ref": "credential://managed/fixture"})
        )
        .await
        .unwrap()
        .resolved
    );
    assert_eq!(
        run.result().await.unwrap().status,
        whale_protocol::TurnStatus::Completed
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
    runtime.shutdown().await.unwrap();
}
