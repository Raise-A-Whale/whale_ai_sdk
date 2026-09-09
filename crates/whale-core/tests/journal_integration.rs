use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Notify};
use whale_adapters::SamplingOptions;
use whale_core::{
    model::{ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest},
    AgentEngine, ApprovalGate, CancellationToken, CoreError, ThreadSession,
    ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::{
    contexts::RunContextInfo,
    events::UsageMetrics,
    models::ModelCapabilities,
    recovery::RecoveryKey,
    runs::{RunFailure, RunSnapshot, RunStatus, StartTurnParams},
    CanonicalItem, CanonicalToolOutput, MessagePhase,
};
use whale_store::{
    MemoryStore, SessionJournal, SessionRecord, SessionStore, StoreError, StoreRuntime,
};

struct ProbeStore {
    inner: MemoryStore,
    fail: usize,
    outcome_committed: Notify,
}
#[async_trait]
impl SessionStore for ProbeStore {
    fn durable(&self) -> bool {
        false
    }
    async fn create(&self, r: SessionRecord) -> whale_store::Result<()> {
        self.inner.create(r).await
    }
    async fn load(&self, id: &str) -> whale_store::Result<Option<SessionRecord>> {
        self.inner.load(id).await
    }
    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.inner.list().await
    }
    async fn compare_exchange(
        &self,
        id: &str,
        rev: u64,
        r: SessionRecord,
    ) -> whale_store::Result<()> {
        let request = r.runs.values().any(|run| !run.model_inputs.is_empty());
        let intent = r
            .runs
            .values()
            .any(|run| run.calls.iter().any(|c| c.intent.is_some()));
        let outcome = r
            .runs
            .values()
            .any(|run| run.calls.iter().any(|c| c.outcome.is_some()));
        if (self.fail == 1 && request) || (self.fail == 2 && intent) || (self.fail == 3 && outcome)
        {
            return Err(StoreError::Io("controlled commit failure".into()));
        }
        self.inner.compare_exchange(id, rev, r).await?;
        if outcome {
            self.outcome_committed.notify_one();
        }
        Ok(())
    }
}
struct Provider {
    store: Arc<ProbeStore>,
    key: RecoveryKey,
    verify_boundary: bool,
    parallel: bool,
    calls: AtomicUsize,
}
#[async_trait]
impl ModelProvider for Provider {
    fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
        let mut caps = ModelCapabilities::text_only();
        caps.tool_calls = true;
        Ok(caps)
    }
    async fn stream(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.verify_boundary {
            let saved = self
                .store
                .load(&self.key.recovery_id)
                .await
                .unwrap()
                .unwrap();
            let inputs = &saved.runs["turn"].model_inputs;
            assert_eq!(
                inputs.len(),
                request.step_index + 1,
                "provider ran before request was durable"
            );
            assert_eq!(
                inputs.last().unwrap().request,
                serde_json::to_value(&request).unwrap()
            );
        }
        let mut events = Vec::new();
        if request.step_index == 0 {
            if self.parallel {
                events.push(Ok(ModelEvent::ItemCompleted {
                    item: CanonicalItem::tool_call(
                        "waiting-call",
                        None,
                        "waiting",
                        Some(json!({})),
                        "{}",
                    ),
                }));
            }
            events.push(Ok(ModelEvent::ItemCompleted {
                item: CanonicalItem::tool_call("fast-call", None, "fast", Some(json!({})), "{}"),
            }));
        } else {
            events.push(Ok(ModelEvent::ItemCompleted {
                item: CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer),
            }));
        }
        events.push(Ok(ModelEvent::StepFinished {
            usage: UsageMetrics {
                input_tokens: 2,
                output_tokens: 1,
                ..Default::default()
            },
        }));
        Ok(Box::pin(futures::stream::iter(events)))
    }
}
struct Tool {
    name: &'static str,
    store: Arc<ProbeStore>,
    key: RecoveryKey,
    effects: Arc<AtomicUsize>,
    verify_boundary: bool,
}
#[async_trait]
impl ToolHandler for Tool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "Journal boundary probe"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        if self.verify_boundary {
            let saved = self
                .store
                .load(&self.key.recovery_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                saved.runs["turn"]
                    .calls
                    .iter()
                    .any(|call| call.tool_name == self.name && call.intent.is_some()),
                "host ran before dispatch intent was durable"
            );
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        if self.name == "waiting" {
            futures::future::pending::<()>().await;
        }
        Ok(CanonicalToolOutput::text("known result"))
    }
}
struct Fixture {
    engine: Arc<AgentEngine>,
    session: ThreadSession,
    journal: SessionJournal,
    store: Arc<ProbeStore>,
    provider: Arc<Provider>,
    effects: Arc<AtomicUsize>,
    input: Vec<CanonicalItem>,
}
fn snapshot(status: RunStatus) -> RunSnapshot {
    RunSnapshot {
        thread_id: "live".into(),
        turn_id: "turn".into(),
        status,
        items: vec![],
        usage: Default::default(),
        pending_approvals: vec![],
        tool_executions: vec![],
        last_seq: 0,
        result: None,
        error: None,
    }
}
async fn fixture(fail: usize, parallel: bool, verify: bool) -> Fixture {
    let store = Arc::new(ProbeStore {
        inner: MemoryStore::new(),
        fail,
        outcome_committed: Notify::new(),
    });
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            json!({"schema_version":1}),
            "owner".into(),
            "live".into(),
        )
        .await
        .unwrap();
    let provider = Arc::new(Provider {
        store: store.clone(),
        key: key.clone(),
        verify_boundary: verify,
        parallel,
        calls: AtomicUsize::new(0),
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(ToolRegistry::new());
    for name in ["waiting", "fast"] {
        tools
            .register(Arc::new(Tool {
                name,
                store: store.clone(),
                key: key.clone(),
                effects: effects.clone(),
                verify_boundary: verify,
            }))
            .unwrap();
    }
    let gate = Arc::new(ApprovalGate::new());
    let engine = Arc::new(AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate,
    ))));
    let mut session = ThreadSession::with_id_prompt_and_provider(
        "live",
        Some("projected prompt".into()),
        provider.clone(),
        tools,
        SamplingOptions::new("test"),
    );
    session.set_journal(Some(journal.clone()));
    let input = vec![CanonicalItem::user_text("input")];
    journal
        .begin_run(
            StartTurnParams {
                thread_id: "live".into(),
                turn_id: "turn".into(),
                input_items: input.clone(),
                options: None,
                max_steps: 3,
                timeout_ms: None,
            },
            snapshot(RunStatus::Running),
            serde_json::to_value(session.sampling_options()).unwrap(),
        )
        .await
        .unwrap();
    Fixture {
        engine,
        session,
        journal,
        store,
        provider,
        effects,
        input,
    }
}
async fn execute(
    f: &mut Fixture,
    cancellation: CancellationToken,
) -> Result<whale_core::RunTurnResult, CoreError> {
    let (tx, mut rx) = mpsc::channel(128);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = f
        .engine
        .run_turn_with_context(
            &mut f.session,
            RunContextInfo {
                agent_name: None,
                thread_id: "live".into(),
                turn_id: "turn".into(),
                deadline_unix_ms: None,
            },
            cancellation,
            f.input.clone(),
            3,
            tx,
            Arc::new(Default::default()),
        )
        .await;
    drain.await.unwrap();
    result
}

#[tokio::test]
async fn model_and_tool_dispatch_follow_durable_intents_and_results() {
    let mut f = fixture(0, false, true).await;
    execute(&mut f, CancellationToken::new()).await.unwrap();
    let record = f.journal.record().await.unwrap();
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.effects.load(Ordering::SeqCst), 1);
    assert_eq!(record.runs["turn"].model_inputs.len(), 2);
    assert!(record.runs["turn"]
        .model_inputs
        .iter()
        .all(|step| step.completed));
    assert_eq!(record.history, f.session.history());
}

#[tokio::test]
async fn request_commit_failure_prevents_provider_dispatch() {
    let mut f = fixture(1, false, false).await;
    assert!(matches!(
        execute(&mut f, CancellationToken::new()).await,
        Err(CoreError::Store(_))
    ));
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dispatch_intent_failure_prevents_host_effect() {
    let mut f = fixture(2, false, false).await;
    assert!(matches!(
        execute(&mut f, CancellationToken::new()).await,
        Err(CoreError::Store(_))
    ));
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn completed_parallel_outcome_survives_cancellation_before_batch_returns() {
    let mut f = fixture(0, true, true).await;
    let journal = f.journal.clone();
    let store = f.store.clone();
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let task = tokio::spawn(async move {
        let result = execute(&mut f, cancellation).await;
        (f, result)
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.outcome_committed.notified(),
    )
    .await
    .unwrap();
    let record = journal.record().await.unwrap();
    assert!(record.runs["turn"]
        .calls
        .iter()
        .find(|c| c.call_id == "fast-call")
        .unwrap()
        .outcome
        .is_some());
    cancel.cancel();
    let (_, result) = task.await.unwrap();
    assert!(result.is_err());
    let mut terminal = snapshot(RunStatus::Cancelled);
    terminal.error = Some(RunFailure {
        code: "CANCELLED".into(),
        message: "cancelled".into(),
    });
    let finalized = journal.finalize("turn", terminal).await.unwrap();
    let saved = journal.record().await.unwrap();
    assert_eq!(saved.unknown_executions.len(), 1);
    assert_eq!(saved.unknown_executions[0].call_id, "waiting-call");
    assert!(finalized.history.iter().any(|item| matches!(item, CanonicalItem::ToolResult { call_id, output, is_error: false, .. } if call_id == "fast-call" && *output == CanonicalToolOutput::text("known result"))));
}

#[tokio::test]
async fn later_parallel_commit_failure_is_not_hidden_by_earlier_pending_tool() {
    let mut f = fixture(3, true, false).await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        execute(&mut f, CancellationToken::new()),
    )
    .await
    .expect("later store failure was hidden by an earlier pending tool");
    assert!(matches!(result, Err(CoreError::Store(_))));
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
}
