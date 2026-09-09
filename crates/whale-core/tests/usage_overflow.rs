use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use whale_adapters::SamplingOptions;
use whale_core::engine::RunProgress;
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
    CanonicalItem, CanonicalToolOutput,
};
use whale_store::{MemoryStore, StoreRuntime};

struct Provider {
    first: UsageMetrics,
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
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(ModelEvent::ItemCompleted {
                item: CanonicalItem::tool_call(
                    format!("call-{}", request.step_index),
                    None,
                    "effect",
                    Some(json!({})),
                    "{}",
                ),
            }),
            Ok(ModelEvent::StepFinished {
                usage: if request.step_index == 0 {
                    self.first.clone()
                } else {
                    metrics(1)
                },
            }),
        ])))
    }
}
struct Effect(Arc<AtomicUsize>);
#[async_trait]
impl ToolHandler for Effect {
    fn name(&self) -> &str {
        "effect"
    }
    fn description(&self) -> &str {
        "count effects"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::text("committed effect"))
    }
}
fn metrics(n: u64) -> UsageMetrics {
    UsageMetrics {
        input_tokens: n,
        output_tokens: n,
        reasoning_tokens: n,
        cache_creation_input_tokens: n,
        cache_read_input_tokens: n,
    }
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
async fn check_overflow(persistent: bool) {
    for field in [
        "input_tokens",
        "output_tokens",
        "reasoning_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let mut value = serde_json::to_value(metrics(7)).unwrap();
        value[field] = json!(u64::MAX);
        let first: UsageMetrics = serde_json::from_value(value).unwrap();
        let provider = Arc::new(Provider {
            first: first.clone(),
            calls: AtomicUsize::new(0),
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(ToolRegistry::new());
        registry
            .register(Arc::new(Effect(effects.clone())))
            .unwrap();
        let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
            registry.clone(),
            Arc::new(ApprovalGate::new()),
        )));
        let mut session = ThreadSession::with_id_prompt_and_provider(
            "live",
            None,
            provider.clone(),
            registry,
            SamplingOptions::new("test"),
        );
        let input = vec![CanonicalItem::user_text("execute")];
        let runtime = StoreRuntime::open(Arc::new(MemoryStore::new()))
            .await
            .unwrap();
        let journal = if persistent {
            let journal = runtime
                .create(
                    RecoveryKey::new(),
                    json!({"version":1}),
                    "owner".into(),
                    "live".into(),
                )
                .await
                .unwrap();
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
            session.set_journal(Some(journal.clone()));
            Some(journal)
        } else {
            None
        };
        let progress = Arc::new(RunProgress::default());
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        let result = engine
            .run_turn_with_context(
                &mut session,
                RunContextInfo {
                    agent_name: None,
                    thread_id: "live".into(),
                    turn_id: "turn".into(),
                    deadline_unix_ms: None,
                },
                CancellationToken::new(),
                input,
                3,
                tx,
                progress.clone(),
            )
            .await;
        assert!(
            matches!(&result, Err(CoreError::Model(ModelError::Stream(message))) if message.contains("usage") && message.contains(field)),
            "{field}: {result:?}"
        );
        assert_eq!(
            progress.usage(),
            first,
            "partial update on {field} overflow"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            effects.load(Ordering::SeqCst),
            1,
            "overflow step must not dispatch"
        );
        let mut failed = false;
        while let Some(event) = rx.recv().await {
            if let whale_protocol::events::AgentStreamEvent::TurnFailed { error_code, .. } = event {
                assert_eq!(error_code, "RUN_FAILED");
                failed = true;
            }
        }
        assert!(failed);
        if let Some(journal) = journal {
            let record = journal.record().await.unwrap();
            assert!(record.runs["turn"].model_inputs[0].completed);
            assert!(!record.runs["turn"].model_inputs[1].completed);
            let mut terminal = snapshot(RunStatus::Failed);
            terminal.error = Some(RunFailure {
                code: "RUN_FAILED".into(),
                message: result.unwrap_err().to_string(),
            });
            let finalized = journal.finalize("turn", terminal).await.unwrap();
            assert_eq!(finalized.snapshot.status, RunStatus::Failed);
            assert_eq!(finalized.snapshot.usage, first);
            let record = journal.record().await.unwrap();
            assert!(record.unknown_executions.is_empty());
            assert!(finalized.history.iter().any(|item| matches!(item, CanonicalItem::ToolResult { call_id, is_error:false, .. } if call_id == "call-0")));
            assert!(finalized.history.iter().any(|item| matches!(item, CanonicalItem::ToolResult { call_id, is_error:true, .. } if call_id == "call-1")));
        }
    }
}
#[tokio::test]
async fn persistent_usage_overflow_preserves_commits_and_finalizes() {
    check_overflow(true).await;
}
#[tokio::test]
async fn ephemeral_usage_overflow_fails_without_partial_progress_or_extra_effects() {
    check_overflow(false).await;
}
