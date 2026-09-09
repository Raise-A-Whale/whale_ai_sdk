use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use whale_adapters::SamplingOptions;
use whale_core::{
    model::{ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest},
    AgentEngine, ApprovalGate, CancellationToken, ContextPolicy, CoreError, ThreadSession,
    ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::{
    contexts::{ContextBuildRequest, ModelContext},
    models::ModelCapabilities,
    recovery::RecoveryKey,
    retention::SessionLimits,
    runs::{RunFailure, RunSnapshot, RunStatus, StartTurnParams},
    CanonicalItem, CanonicalToolOutput, MessagePhase,
};
use whale_store::{MemoryStore, StoreRuntime};

struct Provider {
    calls: AtomicUsize,
    tool: bool,
    large_call: bool,
}
#[async_trait]
impl ModelProvider for Provider {
    fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
        let mut c = ModelCapabilities::text_only();
        c.tool_calls = true;
        Ok(c)
    }
    async fn stream(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let item = if self.tool && request.step_index == 0 {
            let args = if self.large_call {
                json!({"data":"x".repeat(8000)})
            } else {
                json!({})
            };
            CanonicalItem::tool_call("call", None, "lookup", Some(args.clone()), args.to_string())
        } else {
            CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(ModelEvent::ItemCompleted { item }),
            Ok(ModelEvent::StepFinished {
                usage: Default::default(),
            }),
        ])))
    }
}
struct Tool {
    calls: Arc<AtomicUsize>,
    large_result: bool,
}
#[async_trait]
impl ToolHandler for Tool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::text(if self.large_result {
            "known".repeat(2000)
        } else {
            "ok".into()
        }))
    }
}
struct Policy {
    calls: Arc<AtomicUsize>,
    large_prompt: bool,
}
#[async_trait]
impl ContextPolicy for Policy {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationToken,
    ) -> Result<ModelContext, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ModelContext {
            system_prompt: if self.large_prompt {
                Some("projected".repeat(2000))
            } else {
                request.system_prompt
            },
            items: request.history,
        })
    }
}
fn setup(
    tool: bool,
    large_call: bool,
    large_result: bool,
    large_prompt: bool,
) -> (
    AgentEngine,
    ThreadSession,
    Arc<Provider>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        tool,
        large_call,
    });
    let tools = Arc::new(ToolRegistry::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(AtomicUsize::new(0));
    tools
        .register(Arc::new(Tool {
            calls: effects.clone(),
            large_result,
        }))
        .unwrap();
    let mut session = ThreadSession::with_id_prompt_and_provider(
        "live",
        None,
        provider.clone(),
        tools.clone(),
        SamplingOptions::new("test"),
    );
    session.set_context_policy(Arc::new(Policy {
        calls: contexts.clone(),
        large_prompt,
    }));
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        tools,
        Arc::new(ApprovalGate::new()),
    )));
    (engine, session, provider, effects, contexts)
}
async fn run(
    engine: &AgentEngine,
    session: &mut ThreadSession,
    input: Vec<CanonicalItem>,
) -> Result<whale_core::RunTurnResult, CoreError> {
    let (tx, mut rx) = mpsc::channel(64);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = engine
        .run_turn_with_items(session, "turn", input, 3, tx)
        .await;
    drain.await.unwrap();
    result
}
fn snapshot(status: RunStatus) -> RunSnapshot {
    RunSnapshot {
        thread_id: "live".into(),
        turn_id: "turn".into(),
        status,
        last_seq: 0,
        items: vec![],
        usage: Default::default(),
        pending_approvals: vec![],
        tool_executions: vec![],
        result: None,
        error: None,
    }
}

#[tokio::test]
async fn rejected_history_admission_changes_no_history_count_or_callbacks() {
    let (engine, mut session, provider, effects, contexts) = setup(false, false, false, false);
    session
        .set_limits(Some(SessionLimits {
            max_history_bytes: Some(256),
            ..Default::default()
        }))
        .unwrap();
    let result = run(
        &engine,
        &mut session,
        vec![CanonicalItem::user_text("鲸".repeat(100))],
    )
    .await;
    assert!(matches!(result, Err(CoreError::LimitExceeded(_))));
    assert!(session.history().is_empty());
    assert_eq!(session.accepted_turns(), 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(contexts.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn direct_core_turn_quota_preserves_prior_history_and_supports_restored_count() {
    let (engine, mut session, provider, _, _) = setup(false, false, false, false);
    session
        .set_limits(Some(SessionLimits {
            max_accepted_turns: Some(2),
            ..Default::default()
        }))
        .unwrap();
    session.restore_accepted_turns(1);
    run(
        &engine,
        &mut session,
        vec![CanonicalItem::user_text("first")],
    )
    .await
    .unwrap();
    let history = session.history().to_vec();
    assert_eq!(session.accepted_turns(), 2);
    assert!(matches!(
        run(
            &engine,
            &mut session,
            vec![CanonicalItem::user_text("extra")]
        )
        .await,
        Err(CoreError::LimitExceeded(_))
    ));
    assert_eq!(session.history(), history);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn actual_projected_request_budget_prevents_model_dispatch() {
    let (engine, mut session, provider, _, contexts) = setup(false, false, false, true);
    session
        .set_limits(Some(SessionLimits {
            max_model_request_bytes: Some(4096),
            ..Default::default()
        }))
        .unwrap();
    assert!(matches!(
        run(
            &engine,
            &mut session,
            vec![CanonicalItem::user_text("small original")]
        )
        .await,
        Err(CoreError::LimitExceeded(_))
    ));
    assert_eq!(contexts.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(!serde_json::to_string(session.history())
        .unwrap()
        .contains("projected"));
}
#[tokio::test]
async fn oversized_model_call_prevents_tool_dispatch() {
    let (engine, mut session, provider, effects, _) = setup(true, true, false, false);
    session
        .set_limits(Some(SessionLimits {
            max_history_bytes: Some(2048),
            ..Default::default()
        }))
        .unwrap();
    assert!(matches!(
        run(
            &engine,
            &mut session,
            vec![CanonicalItem::user_text("first")]
        )
        .await,
        Err(CoreError::LimitExceeded(_))
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn oversized_completed_result_is_durable_and_blocks_the_next_model_step() {
    let (engine, mut session, provider, effects, _) = setup(true, false, true, false);
    let limits = SessionLimits {
        max_history_bytes: Some(2048),
        ..Default::default()
    };
    session.set_limits(Some(limits.clone())).unwrap();
    let runtime = StoreRuntime::open(Arc::new(MemoryStore::new()))
        .await
        .unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            json!({"session":{"limits":limits}}),
            "owner".into(),
            "live".into(),
        )
        .await
        .unwrap();
    let input = vec![CanonicalItem::user_text("first")];
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
            json!({}),
        )
        .await
        .unwrap();
    session.set_journal(Some(journal.clone()));
    assert!(matches!(
        run(&engine, &mut session, input).await,
        Err(CoreError::LimitExceeded(_))
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let mut terminal = snapshot(RunStatus::Failed);
    terminal.error = Some(RunFailure {
        code: "SESSION_LIMIT_EXCEEDED".into(),
        message: "history limit".into(),
    });
    let saved = journal.finalize("turn", terminal).await.unwrap();
    assert!(serde_json::to_string(&saved.history)
        .unwrap()
        .contains(&"known".repeat(2000)));
    assert_eq!(
        journal.record().await.unwrap().runs["turn"]
            .model_inputs
            .len(),
        1
    );
    assert!(runtime
        .inspect(&key)
        .await
        .unwrap()
        .unknown_executions
        .is_empty());
}
