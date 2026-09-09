use std::sync::Arc;
use whale_adapters::{AdapterError, OpenAIAdapter, SamplingOptions};
use whale_core::{
    AgentEngine, ApprovalGate, ThreadSession, ToolExecutionCoordinator, ToolRegistry,
};
use whale_protocol::{
    canonical::CanonicalItem,
    events::{AgentStreamEvent, UsageMetrics},
};

fn setup(events: Vec<Result<AgentStreamEvent, AdapterError>>) -> (AgentEngine, ThreadSession) {
    let registry = Arc::new(ToolRegistry::new());
    let events = Arc::new(std::sync::Mutex::new(Some(events)));
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        registry.clone(),
        Arc::new(ApprovalGate::new()),
    )))
    .with_stream_provider(Arc::new(move |_, _| {
        Ok(Box::pin(futures::stream::iter(
            events.lock().unwrap().take().unwrap(),
        )))
    }));
    let session = ThreadSession::new(
        Arc::new(OpenAIAdapter::new("")),
        registry,
        SamplingOptions::new("test"),
    );
    (engine, session)
}

#[tokio::test]
async fn provider_terminal_is_not_an_outer_terminal_and_ids_are_normalized() {
    let (engine, mut session) = setup(vec![
        Ok(AgentStreamEvent::TextDelta {
            turn_id: "provider".into(),
            item_id: "i".into(),
            delta: "hello".into(),
        }),
        Ok(AgentStreamEvent::TurnCompleted {
            turn_id: "provider".into(),
            thread_id: "provider-thread".into(),
            usage: UsageMetrics::default(),
        }),
    ]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let result = engine.run_turn(&mut session, None, 2, tx).await.unwrap();
    let mut events = Vec::new();
    while let Some(e) = rx.recv().await {
        events.push(e);
    }
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e,
                AgentStreamEvent::TurnCompleted { .. } | AgentStreamEvent::TurnFailed { .. }
            ))
            .count(),
        1
    );
    for event in events {
        assert_eq!(
            serde_json::to_value(event).unwrap()["turn_id"],
            result.turn_id
        );
    }
}

#[tokio::test]
async fn provider_failure_cannot_be_success() {
    let (engine, mut session) = setup(vec![Ok(AgentStreamEvent::TurnFailed {
        turn_id: "provider".into(),
        thread_id: "".into(),
        error_code: "broken".into(),
        error_message: "broken stream".into(),
    })]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    assert!(engine.run_turn(&mut session, None, 1, tx).await.is_err());
    let mut terminal = Vec::new();
    while let Some(e) = rx.recv().await {
        if matches!(
            e,
            AgentStreamEvent::TurnFailed { .. } | AgentStreamEvent::TurnCompleted { .. }
        ) {
            terminal.push(e)
        }
    }
    assert_eq!(terminal.len(), 1);
    assert!(matches!(terminal[0], AgentStreamEvent::TurnFailed { .. }));
}

#[tokio::test]
async fn parse_error_cannot_be_success() {
    let (engine, mut session) = setup(vec![Err(AdapterError::StreamParseError(
        "invalid SSE".into(),
    ))]);
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    assert!(engine.run_turn(&mut session, None, 1, tx).await.is_err());
}

#[tokio::test]
async fn full_canonical_input_is_retained() {
    let (engine, mut session) = setup(vec![Ok(AgentStreamEvent::TurnCompleted {
        turn_id: "fixture".into(),
        thread_id: "fixture".into(),
        usage: UsageMetrics::default(),
    })]);
    let input = vec![
        CanonicalItem::user_text("one"),
        CanonicalItem::user_text("two"),
    ];
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    let result = engine
        .run_turn_with_items(&mut session, "outer", input.clone(), 1, tx)
        .await
        .unwrap();
    assert_eq!(session.history(), input.as_slice());
    assert_eq!(result.generated_items, input);
    assert_eq!(result.turn_id, "outer");
}

#[tokio::test]
async fn dropping_approval_wait_cleans_pending_request() {
    let gate = ApprovalGate::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let item = CanonicalItem::tool_call("c", None, "tool", None, "{}");
    {
        let wait = gate.request_approval("r", &item, None, &tx);
        tokio::pin!(wait);
        tokio::select! { _= &mut wait => panic!("must wait"), _=rx.recv()=>{} }
        assert_eq!(gate.pending_count(), 1);
        drop(wait);
    }
    assert_eq!(gate.pending_count(), 0);
}

#[tokio::test]
async fn exclusive_tools_in_different_sessions_do_not_share_a_barrier() {
    struct Exclusive {
        started: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait::async_trait]
    impl whale_core::ToolHandler for Exclusive {
        fn name(&self) -> &str {
            "exclusive"
        }
        fn description(&self) -> &str {
            "exclusive"
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({})
        }
        fn supports_parallel(&self) -> bool {
            false
        }
        async fn execute(
            &self,
            _: serde_json::Value,
        ) -> Result<whale_protocol::canonical::CanonicalToolOutput, String> {
            self.started.send(()).unwrap();
            let _permit = self.release.acquire().await.unwrap();
            Ok(whale_protocol::canonical::CanonicalToolOutput::text("done"))
        }
    }
    let (started, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        Arc::new(ApprovalGate::new()),
    ));
    let mut tasks = vec![];
    for _ in 0..2 {
        let registry = Arc::new(ToolRegistry::new());
        registry
            .register(Arc::new(Exclusive {
                started: started.clone(),
                release: release.clone(),
            }))
            .expect("valid tool schema");
        let coordinator = coordinator.clone();
        tasks.push(tokio::spawn(async move {
            coordinator
                .execute_calls_with_override_registry(
                    "turn",
                    vec![CanonicalItem::tool_call(
                        "call",
                        None,
                        "exclusive",
                        None,
                        "{}",
                    )],
                    Some(registry),
                    None,
                )
                .await
        }));
    }
    rx.recv().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("other session was blocked by global exclusive barrier");
    release.add_permits(2);
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn provider_failure_closes_unanswered_calls_without_replacing_recorded_results() {
    use whale_protocol::canonical::CanonicalToolOutput;
    struct RecordedTool;
    #[async_trait::async_trait]
    impl whale_core::ToolHandler for RecordedTool {
        fn name(&self) -> &str {
            "tool"
        }
        fn description(&self) -> &str {
            "fixture"
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({})
        }
        fn supports_parallel(&self) -> bool {
            false
        }
        fn require_approval(&self) -> bool {
            false
        }
        async fn execute(&self, _: serde_json::Value) -> Result<CanonicalToolOutput, String> {
            Ok(CanonicalToolOutput::text("recorded result"))
        }
    }
    let tools = Arc::new(ToolRegistry::new());
    tools.register(Arc::new(RecordedTool)).unwrap();
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        tools.clone(),
        Arc::new(ApprovalGate::new()),
    )))
    .with_stream_provider(Arc::new(|_, step| {
        let call = CanonicalItem::tool_call(
            if step == 0 { "answered" } else { "unanswered" },
            None,
            "tool",
            Some(serde_json::json!({})),
            "{}",
        );
        let mut events = vec![Ok(AgentStreamEvent::ItemCompleted {
            turn_id: "provider".into(),
            item: call,
        })];
        if step == 0 {
            events.push(Ok(AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: UsageMetrics::default(),
            }));
        } else {
            events.push(Err(AdapterError::StreamParseError(
                "truncated provider".into(),
            )));
        }
        Ok(Box::pin(futures::stream::iter(events)))
    }));
    let mut session = ThreadSession::new(
        Arc::new(OpenAIAdapter::new("")),
        tools,
        SamplingOptions::new("test"),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    assert!(engine.run_turn(&mut session, None, 2, tx).await.is_err());
    let results: Vec<_> = session
        .history()
        .iter()
        .filter(|item| matches!(item, CanonicalItem::ToolResult { .. }))
        .collect();
    assert_eq!(
        results.len(),
        2,
        "provider failure left an unanswered tool call"
    );
    assert!(
        matches!(results[0],CanonicalItem::ToolResult{call_id,output,is_error:false,..} if call_id=="answered" && output==&CanonicalToolOutput::text("recorded result")),
        "recorded output must not be overwritten"
    );
    assert!(
        matches!(results[1],CanonicalItem::ToolResult {call_id,is_error:true,..} if call_id=="unanswered")
    );
    let missing = serde_json::to_value(results[1]).unwrap()["output"]["text"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(missing.contains("unknown"));
    assert!(missing.contains("not retried"));
}
