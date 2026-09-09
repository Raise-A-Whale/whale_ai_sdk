use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use whale_core::{
    ApprovalDecision, ApprovalGate, ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput};
struct TypedTool {
    calls: Arc<AtomicUsize>,
    approval: bool,
}
#[async_trait]
impl ToolHandler for TypedTool {
    fn name(&self) -> &str {
        "typed"
    }
    fn description(&self) -> &str {
        "typed tool"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false})
    }
    fn require_approval(&self) -> bool {
        self.approval
    }
    async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::structured(args))
    }
}
fn fixture(
    approval: bool,
) -> (
    Arc<ToolExecutionCoordinator>,
    Arc<ApprovalGate>,
    Arc<AtomicUsize>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let registry = Arc::new(ToolRegistry::new());
    let calls = Arc::new(AtomicUsize::new(0));
    registry
        .register(Arc::new(TypedTool {
            calls: calls.clone(),
            approval,
        }))
        .expect("valid tool schema");
    (
        Arc::new(ToolExecutionCoordinator::new(registry, gate.clone())),
        gate,
        calls,
    )
}
#[tokio::test]
async fn invalid_argument_type_never_executes_tool() {
    let (coordinator, _, calls) = fixture(false);
    let results = coordinator
        .execute_calls(
            "r",
            vec![CanonicalItem::tool_call(
                "c",
                None,
                "typed",
                Some(json!({"query":1})),
                "{\"query\":1}",
            )],
            None,
        )
        .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "schema-invalid arguments reached tool"
    );
    assert!(matches!(
        &results[0],
        CanonicalItem::ToolResult { is_error: true, .. }
    ));
}
#[tokio::test]
async fn modified_arguments_are_revalidated_before_execution() {
    let (coordinator, gate, calls) = fixture(true);
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let execution = tokio::spawn(async move {
        coordinator
            .execute_calls(
                "r",
                vec![CanonicalItem::tool_call(
                    "c",
                    None,
                    "typed",
                    Some(json!({"query":"original"})),
                    "{}",
                )],
                Some(tx),
            )
            .await
    });
    let request = rx.recv().await.unwrap();
    let AgentStreamEvent::ApprovalRequested { request_id, .. } = request else {
        panic!("expected approval")
    };
    assert!(gate.resolve_approval(
        &request_id,
        ApprovalDecision::ModifyArguments {
            arguments: json!({"query":false})
        }
    ));
    let results = execution.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        &results[0],
        CanonicalItem::ToolResult { is_error: true, .. }
    ));
}
#[tokio::test]
async fn execution_event_records_original_and_effective_arguments() {
    let (coordinator, gate, calls) = fixture(true);
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let execution = tokio::spawn(async move {
        coordinator
            .execute_calls(
                "r",
                vec![CanonicalItem::tool_call(
                    "c",
                    None,
                    "typed",
                    Some(json!({"query":"original"})),
                    "{}",
                )],
                Some(tx),
            )
            .await
    });
    let AgentStreamEvent::ApprovalRequested { request_id, .. } = rx.recv().await.unwrap() else {
        panic!("expected approval")
    };
    gate.resolve_approval(
        &request_id,
        ApprovalDecision::ModifyArguments {
            arguments: json!({"query":"changed"}),
        },
    );
    execution.await.unwrap();
    let mut records = vec![];
    while let Some(event) = rx.recv().await {
        let value = serde_json::to_value(event).unwrap();
        if value["type"] == "tool_execution_started" {
            records.push(value);
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]["original_arguments"],
        json!({"query":"original"})
    );
    assert_eq!(records[0]["arguments"], json!({"query":"changed"}));
}

#[tokio::test]
async fn cancellation_interrupts_direct_core_pending_model() {
    use whale_core::{
        engine::RunProgress, execution::CancellationToken, AgentEngine, ThreadSession,
    };
    use whale_protocol::contexts::RunContextInfo;
    let (coordinator, _, _) = fixture(false);
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let engine = AgentEngine::new(coordinator).with_stream_provider(Arc::new(move |_, _| {
        signal.notify_one();
        Ok(Box::pin(futures::stream::pending()))
    }));
    let mut session = ThreadSession::new(
        Arc::new(whale_adapters::OpenAIAdapter::new("")),
        Arc::new(ToolRegistry::new()),
        whale_adapters::SamplingOptions::new("test"),
    );
    let token = CancellationToken::new();
    let run_token = token.clone();
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    let run = tokio::spawn(async move {
        engine
            .run_turn_with_context(
                &mut session,
                RunContextInfo {
                    agent_name: None,
                    thread_id: "s".into(),
                    turn_id: "r".into(),
                    deadline_unix_ms: None,
                },
                run_token,
                vec![],
                2,
                tx,
                Arc::new(RunProgress::default()),
            )
            .await
    });
    entered.notified().await;
    token.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_millis(200), run)
        .await
        .expect("core cancellation did not interrupt pending model")
        .unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn context_policy_failure_prevents_model_and_preserves_history() {
    use whale_core::{
        context::ContextPolicy, execution::CancellationToken, AgentEngine, ThreadSession,
    };
    use whale_protocol::contexts::{ContextBuildRequest, ModelContext};
    struct FailingPolicy;
    #[async_trait]
    impl ContextPolicy for FailingPolicy {
        async fn build(
            &self,
            _: ContextBuildRequest,
            _: CancellationToken,
        ) -> Result<ModelContext, String> {
            Err("projection unavailable".into())
        }
    }
    let (coordinator, _, _) = fixture(false);
    let invoked = Arc::new(AtomicUsize::new(0));
    let calls = invoked.clone();
    let engine = AgentEngine::new(coordinator).with_stream_provider(Arc::new(move |_, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::empty()))
    }));
    let mut session = ThreadSession::new(
        Arc::new(whale_adapters::OpenAIAdapter::new("")),
        Arc::new(ToolRegistry::new()),
        whale_adapters::SamplingOptions::new("test"),
    );
    session.append_item(CanonicalItem::user_text("prior"));
    let prior = session.clone_history();
    session.set_context_policy(Arc::new(FailingPolicy));
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    let error = engine
        .run_turn(&mut session, Some("new"), 2, tx)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("projection unavailable"));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert_eq!(&session.history()[..1], prior);
    assert_eq!(session.history().len(), 2);
}
#[tokio::test]
async fn tool_context_preserves_identity_and_rejects_progress_after_completion() {
    use whale_core::{
        engine::RunProgress,
        execution::{CancellationToken, ToolContext},
    };
    use whale_protocol::contexts::RunContextInfo;
    struct ContextTool(Arc<std::sync::Mutex<Option<ToolContext>>>);
    #[async_trait]
    impl ToolHandler for ContextTool {
        fn name(&self) -> &str {
            "context"
        }
        fn description(&self) -> &str {
            "context"
        }
        fn parameters(&self) -> Value {
            json!({})
        }
        async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
            panic!("context method must be called")
        }
        async fn execute_with_context(
            &self,
            context: ToolContext,
            _: Value,
        ) -> Result<(CanonicalToolOutput, bool), String> {
            assert!(context.report_progress("working", Some(0.25)).await?);
            *self.0.lock().unwrap() = Some(context);
            Ok((CanonicalToolOutput::text("done"), false))
        }
    }
    let saved = Arc::new(std::sync::Mutex::new(None));
    let registry = Arc::new(ToolRegistry::new());
    registry
        .register(Arc::new(ContextTool(saved.clone())))
        .unwrap();
    let coordinator = ToolExecutionCoordinator::new(registry, Arc::new(ApprovalGate::new()));
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let info = RunContextInfo {
        agent_name: Some("agent".into()),
        thread_id: "session".into(),
        turn_id: "outer".into(),
        deadline_unix_ms: Some(12345),
    };
    coordinator
        .execute_calls_with_context(
            info.clone(),
            CancellationToken::new(),
            Arc::new(RunProgress::default()),
            vec![CanonicalItem::tool_call(
                "model",
                None,
                "context",
                Some(json!({})),
                "{}",
            )],
            None,
            Some(tx),
        )
        .await;
    let context = saved.lock().unwrap().take().unwrap();
    assert_eq!(context.info.run, info);
    assert_eq!(context.info.call_id, "model");
    assert!(!context.report_progress("late", None).await.unwrap());
    assert!(context.report_progress("invalid", Some(2.0)).await.is_err());
    drop(context);
    let mut count = 0;
    while let Some(event) = rx.recv().await {
        if matches!(event, AgentStreamEvent::ToolProgress { .. }) {
            count += 1;
        }
    }
    assert_eq!(count, 1);
}
#[test]
fn schema_validation_supports_local_references_and_rejects_invalid_registration() {
    use whale_core::execution::compile_tool_schema;
    let validator=compile_tool_schema(&json!({"$defs":{"query":{"type":"string","minLength":2}},"type":"object","properties":{"query":{"$ref":"#/$defs/query"}},"required":["query"],"additionalProperties":false})).unwrap();
    assert!(validator.is_valid(&json!({"query":"ok"})));
    assert!(!validator.is_valid(&json!({"query":"x"})));
    assert!(!validator.is_valid(&json!({"query":"ok","extra":1})));
    struct SchemaTool(Value);
    #[async_trait]
    impl ToolHandler for SchemaTool {
        fn name(&self) -> &str {
            "schema"
        }
        fn description(&self) -> &str {
            "schema"
        }
        fn parameters(&self) -> Value {
            self.0.clone()
        }
        async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
            unreachable!()
        }
    }
    let registry = ToolRegistry::new();
    registry
        .register(Arc::new(SchemaTool(json!({"type":"object"}))))
        .unwrap();
    assert!(registry
        .register(Arc::new(SchemaTool(json!({"type": 1}))))
        .is_err());
    assert_eq!(
        registry.get("schema").unwrap().parameters(),
        json!({"type":"object"})
    );
    for schema in [
        json!({"type":1}),
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
    ] {
        assert!(compile_tool_schema(&schema).is_err());
    }
}

#[tokio::test]
async fn cancellation_finishes_when_event_consumer_is_backpressured() {
    use whale_core::{
        engine::RunProgress, execution::CancellationToken, AgentEngine, ThreadSession,
    };
    use whale_protocol::contexts::RunContextInfo;
    let (coordinator, _, _) = fixture(false);
    let engine = AgentEngine::new(coordinator)
        .with_stream_provider(Arc::new(|_, _| Ok(Box::pin(futures::stream::pending()))));
    let mut session = ThreadSession::new(
        Arc::new(whale_adapters::OpenAIAdapter::new("")),
        Arc::new(ToolRegistry::new()),
        whale_adapters::SamplingOptions::new("test"),
    );
    let token = CancellationToken::new();
    let run_token = token.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let run = tokio::spawn(async move {
        engine
            .run_turn_with_context(
                &mut session,
                RunContextInfo {
                    agent_name: None,
                    thread_id: "s".into(),
                    turn_id: "r".into(),
                    deadline_unix_ms: None,
                },
                run_token,
                vec![CanonicalItem::user_text("input")],
                2,
                tx,
                Arc::new(RunProgress::default()),
            )
            .await
    });
    // Free only the first slot; the next committed input event remains buffered.
    rx.recv().await.unwrap();
    while rx.is_empty() {
        tokio::task::yield_now().await;
    }
    token.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_millis(200), run)
        .await
        .expect("cancelled engine blocked sending terminal event")
        .unwrap();
    assert!(result.is_err());
}
