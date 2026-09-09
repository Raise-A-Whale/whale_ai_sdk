//! whale-core: Agent core state machine, concurrency coordinator, session management, and turn engine.

pub mod approval;
pub mod context;
pub mod coordinator;
pub mod engine;
pub mod error;
pub mod execution;
pub mod http_provider;
pub mod interaction;
pub mod model;
pub mod provider;
pub mod session;

pub use approval::{ApprovalDecision, ApprovalGate};
pub use context::{ContextPolicy, FullHistoryContext, RecentTurnsContext};
pub use coordinator::{ToolExecutionCoordinator, ToolHandler, ToolRegistry};
pub use engine::{AgentEngine, RunTurnResult};
pub use error::CoreError;
pub use execution::{CancellationToken, ToolContext};
pub use interaction::{
    validate_interaction_request, validate_interaction_response, InteractionBeginGuard,
    InteractionBridge, InteractionTicket,
};
pub use session::ThreadSession;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use whale_adapters::{
        AdapterError, BoxedEventStream, ProtocolAdapter, SamplingOptions, ToolDefinition,
    };
    use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput, MessagePhase};
    use whale_protocol::events::{AgentStreamEvent, UsageMetrics};

    // --- Mock Tools for testing ---

    struct MockCalculator;

    #[async_trait]
    impl ToolHandler for MockCalculator {
        fn name(&self) -> &str {
            "calc"
        }
        fn description(&self) -> &str {
            "Basic calculator"
        }
        fn parameters(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "a": { "type": "number" },
                    "b": { "type": "number" }
                }
            })
        }
        fn supports_parallel(&self) -> bool {
            true
        }
        fn require_approval(&self) -> bool {
            false
        }
        async fn execute(
            &self,
            arguments: serde_json::Value,
        ) -> Result<CanonicalToolOutput, String> {
            let a = arguments.get("a").and_then(|v| v.as_i64()).unwrap_or(0);
            let b = arguments.get("b").and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(CanonicalToolOutput::text(format!("{}", a + b)))
        }
    }

    struct ExclusiveTool {
        active_count: Arc<AtomicUsize>,
        max_concurrent: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ToolHandler for ExclusiveTool {
        fn name(&self) -> &str {
            "exclusive_writer"
        }
        fn description(&self) -> &str {
            "Exclusive disk writer tool"
        }
        fn parameters(&self) -> serde_json::Value {
            json!({})
        }
        fn supports_parallel(&self) -> bool {
            false
        }
        async fn execute(&self, _args: serde_json::Value) -> Result<CanonicalToolOutput, String> {
            let count = self.active_count.fetch_add(1, Ordering::SeqCst) + 1;
            let mut current_max = self.max_concurrent.load(Ordering::SeqCst);
            while count > current_max {
                match self.max_concurrent.compare_exchange(
                    current_max,
                    count,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => current_max = actual,
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.active_count.fetch_sub(1, Ordering::SeqCst);
            Ok(CanonicalToolOutput::text("wrote safely"))
        }
    }

    struct HitlSensitiveTool;

    #[async_trait]
    impl ToolHandler for HitlSensitiveTool {
        fn name(&self) -> &str {
            "transfer_funds"
        }
        fn description(&self) -> &str {
            "Transfer funds requires human approval"
        }
        fn parameters(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "amount": { "type": "number" }
                }
            })
        }
        fn require_approval(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            arguments: serde_json::Value,
        ) -> Result<CanonicalToolOutput, String> {
            let amount = arguments
                .get("amount")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            Ok(CanonicalToolOutput::text(format!(
                "Transferred ${}",
                amount
            )))
        }
    }

    // --- Mock Adapter for Turn Engine testing ---

    struct MockStepAdapter;

    impl ProtocolAdapter for MockStepAdapter {
        fn capabilities(&self) -> whale_protocol::models::ModelCapabilities {
            let mut caps = whale_protocol::models::ModelCapabilities::text_only();
            caps.tool_calls = true;
            caps
        }
        fn provider_name(&self) -> &'static str {
            "mock"
        }

        fn serialize_request(
            &self,
            _system_prompt: Option<&str>,
            _history: &[CanonicalItem],
            _tools: &[ToolDefinition],
            _options: &SamplingOptions,
        ) -> Result<(serde_json::Value, reqwest::header::HeaderMap), AdapterError> {
            Ok((json!({}), reqwest::header::HeaderMap::new()))
        }

        fn parse_stream(
            &self,
            _byte_stream: std::pin::Pin<
                Box<dyn futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>,
            >,
        ) -> BoxedEventStream {
            Box::pin(async_stream::try_stream! {
                yield AgentStreamEvent::TurnCompleted {
                    turn_id: "mock_turn".to_string(),
                    thread_id: "mock_thread".to_string(),
                    usage: UsageMetrics::default(),
                };
            })
        }
    }

    #[tokio::test]
    async fn test_tool_registry_and_parallel_barrier() {
        let registry = Arc::new(ToolRegistry::new());
        let calc = Arc::new(MockCalculator);
        registry.register(calc).expect("valid tool schema");

        let active_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let exclusive = Arc::new(ExclusiveTool {
            active_count: Arc::clone(&active_count),
            max_concurrent: Arc::clone(&max_concurrent),
        });
        registry.register(exclusive).expect("valid tool schema");

        let approval_gate = Arc::new(ApprovalGate::new());
        let coordinator = ToolExecutionCoordinator::new(registry, approval_gate);

        // Parallel calls to calc
        let calls = vec![
            CanonicalItem::tool_call("c1", None, "calc", Some(json!({"a": 1, "b": 2})), ""),
            CanonicalItem::tool_call("c2", None, "calc", Some(json!({"a": 10, "b": 20})), ""),
        ];
        let results = coordinator.execute_calls("turn_1", calls, None).await;
        assert_eq!(results.len(), 2);
        if let CanonicalItem::ToolResult { output, .. } = &results[0] {
            assert_eq!(output, &CanonicalToolOutput::text("3"));
        } else {
            panic!("Expected ToolResult");
        }
        if let CanonicalItem::ToolResult { output, .. } = &results[1] {
            assert_eq!(output, &CanonicalToolOutput::text("30"));
        } else {
            panic!("Expected ToolResult");
        }

        // Exclusive calls
        let exclusive_calls = vec![
            CanonicalItem::tool_call("e1", None, "exclusive_writer", Some(json!({})), ""),
            CanonicalItem::tool_call("e2", None, "exclusive_writer", Some(json!({})), ""),
        ];
        let results_ex = coordinator
            .execute_calls("turn_2", exclusive_calls, None)
            .await;
        assert_eq!(results_ex.len(), 2);
        // Ensure max_concurrent never exceeded 1 because of exclusive write barrier!
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_approval_gate_accept_and_deny() {
        let registry = Arc::new(ToolRegistry::new());
        registry
            .register(Arc::new(HitlSensitiveTool))
            .expect("valid tool schema");

        let approval_gate = Arc::new(ApprovalGate::new());
        let coordinator = Arc::new(ToolExecutionCoordinator::new(
            registry,
            Arc::clone(&approval_gate),
        ));

        let (event_tx, mut event_rx) = mpsc::channel(32);

        let call_item = CanonicalItem::tool_call(
            "tx_1",
            None,
            "transfer_funds",
            Some(json!({ "amount": 500 })),
            "",
        );

        let coord_clone = Arc::clone(&coordinator);
        let gate_clone = Arc::clone(&approval_gate);

        // Spawn background execution
        let exec_handle = tokio::spawn(async move {
            coord_clone
                .execute_calls("turn_hitl", vec![call_item], Some(event_tx))
                .await
        });

        // Watch for ApprovalRequested event
        let mut req_id = String::new();
        while let Some(event) = event_rx.recv().await {
            if let AgentStreamEvent::ApprovalRequested { request_id, .. } = event {
                req_id = request_id;
                break;
            }
        }
        assert!(!req_id.is_empty());
        assert_eq!(gate_clone.pending_count(), 1);

        // Accept approval
        let resolved = gate_clone.resolve_approval(&req_id, ApprovalDecision::Accept);
        assert!(resolved);

        let results = exec_handle.await.unwrap();
        assert_eq!(results.len(), 1);
        if let CanonicalItem::ToolResult {
            output, is_error, ..
        } = &results[0]
        {
            assert!(!is_error);
            assert_eq!(output, &CanonicalToolOutput::text("Transferred $500"));
        } else {
            panic!("Expected ToolResult");
        }

        // Test Deny
        let (event_tx2, mut event_rx2) = mpsc::channel(32);
        let call_item2 = CanonicalItem::tool_call(
            "tx_2",
            None,
            "transfer_funds",
            Some(json!({ "amount": 10000 })),
            "",
        );
        let coord_clone2 = Arc::clone(&coordinator);
        let gate_clone2 = Arc::clone(&approval_gate);

        let exec_handle2 = tokio::spawn(async move {
            coord_clone2
                .execute_calls("turn_hitl_2", vec![call_item2], Some(event_tx2))
                .await
        });

        let mut req_id2 = String::new();
        while let Some(event) = event_rx2.recv().await {
            if let AgentStreamEvent::ApprovalRequested { request_id, .. } = event {
                req_id2 = request_id;
                break;
            }
        }

        let resolved = gate_clone2.resolve_approval(
            &req_id2,
            ApprovalDecision::Deny {
                reason: Some("Amount exceeds limits".to_string()),
            },
        );
        assert!(resolved);

        let results2 = exec_handle2.await.unwrap();
        if let CanonicalItem::ToolResult {
            output, is_error, ..
        } = &results2[0]
        {
            assert!(is_error);
            if let CanonicalToolOutput::Text { text } = output {
                assert!(text.contains("Amount exceeds limits"));
            }
        } else {
            panic!("Expected ToolResult");
        }
    }

    #[tokio::test]
    async fn test_approval_modify_arguments() {
        let registry = Arc::new(ToolRegistry::new());
        registry
            .register(Arc::new(HitlSensitiveTool))
            .expect("valid tool schema");

        let approval_gate = Arc::new(ApprovalGate::new());
        let coordinator = Arc::new(ToolExecutionCoordinator::new(
            registry,
            Arc::clone(&approval_gate),
        ));

        let (event_tx, mut event_rx) = mpsc::channel(32);
        let call_item = CanonicalItem::tool_call(
            "tx_mod",
            None,
            "transfer_funds",
            Some(json!({ "amount": 1000 })),
            "",
        );

        let coord_clone = Arc::clone(&coordinator);
        let gate_clone = Arc::clone(&approval_gate);

        let exec_handle = tokio::spawn(async move {
            coord_clone
                .execute_calls("turn_mod", vec![call_item], Some(event_tx))
                .await
        });

        let mut req_id = String::new();
        while let Some(event) = event_rx.recv().await {
            if let AgentStreamEvent::ApprovalRequested { request_id, .. } = event {
                req_id = request_id;
                break;
            }
        }

        // Modify amount to 50
        let resolved = gate_clone.resolve_approval(
            &req_id,
            ApprovalDecision::ModifyArguments {
                arguments: json!({ "amount": 50 }),
            },
        );
        assert!(resolved);

        let results = exec_handle.await.unwrap();
        if let CanonicalItem::ToolResult {
            output, is_error, ..
        } = &results[0]
        {
            assert!(!is_error);
            assert_eq!(output, &CanonicalToolOutput::text("Transferred $50"));
        } else {
            panic!("Expected ToolResult");
        }
    }

    #[tokio::test]
    async fn test_agent_engine_multi_step_turn_loop() {
        let registry = Arc::new(ToolRegistry::new());
        registry
            .register(Arc::new(MockCalculator))
            .expect("valid tool schema");

        let approval_gate = Arc::new(ApprovalGate::new());
        let coordinator = Arc::new(ToolExecutionCoordinator::new(
            registry.clone(),
            approval_gate,
        ));

        let step_counter = Arc::new(AtomicUsize::new(0));
        let step_counter_clone = Arc::clone(&step_counter);

        // Stream provider callback returning tool call on step 0, answer on step 1
        let engine = AgentEngine::new(coordinator).with_stream_provider(Arc::new(
            move |_session: &ThreadSession, step: usize| {
                step_counter_clone.fetch_add(1, Ordering::SeqCst);
                let s: BoxedEventStream = Box::pin(async_stream::try_stream! {
                    if step == 0 {
                        // Step 0: Model calls calculator
                        let tool_call = CanonicalItem::tool_call(
                            "call_calc_1",
                            None,
                            "calc",
                            Some(json!({ "a": 15, "b": 25 })),
                            "{\"a\":15,\"b\":25}",
                        );
                        yield AgentStreamEvent::ItemCompleted {
                            turn_id: "turn_test".to_string(),
                            item: tool_call,
                        };
                        yield AgentStreamEvent::TurnCompleted {turn_id:"fixture".into(),thread_id:"fixture".into(),usage:UsageMetrics::default()};
                    } else {
                        // Step 1: Model sees tool result "40" and replies
                        let assistant_item = CanonicalItem::assistant_text(
                            "The calculated result is 40.",
                            MessagePhase::FinalAnswer,
                        );
                        yield AgentStreamEvent::ItemCompleted {
                            turn_id: "turn_test".to_string(),
                            item: assistant_item,
                        };
                        yield AgentStreamEvent::TurnCompleted {
                            turn_id: "turn_test".to_string(),
                            thread_id: "thread_test".to_string(),
                            usage: UsageMetrics {
                                input_tokens: 120,
                                output_tokens: 45,
                                reasoning_tokens: 0,
                                cache_creation_input_tokens: 0,
                                cache_read_input_tokens: 0,
                            },
                        };
                    }
                });
                Ok(s)
            },
        ));

        let mut session = ThreadSession::new(
            Arc::new(MockStepAdapter),
            registry,
            SamplingOptions::new("mock-model"),
        );

        let (event_tx, mut event_rx) = mpsc::channel(64);

        // Collect emitted stream events in background
        let events_collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = event_rx.recv().await {
                events.push(event);
            }
            events
        });

        let run_result = engine
            .run_turn(&mut session, Some("Please calculate 15 + 25"), 5, event_tx)
            .await
            .expect("turn should succeed");

        let events = events_collector.await.unwrap();

        // 1. Verify steps taken
        assert_eq!(run_result.steps_taken, 2);
        assert_eq!(step_counter.load(Ordering::SeqCst), 2);

        // 2. Verify session history contains UserMessage -> ToolCall -> ToolResult -> AssistantMessage
        let history = session.history();
        assert_eq!(history.len(), 4);
        assert!(matches!(history[0], CanonicalItem::UserMessage { .. }));
        assert!(matches!(history[1], CanonicalItem::ToolCall { .. }));
        assert!(matches!(history[2], CanonicalItem::ToolResult { .. }));
        assert!(matches!(history[3], CanonicalItem::AssistantMessage { .. }));

        if let CanonicalItem::ToolResult { output, .. } = &history[2] {
            assert_eq!(output, &CanonicalToolOutput::text("40"));
        } else {
            panic!("Expected ToolResult at history[2]");
        }

        // 3. Verify emitted events sequence
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentStreamEvent::TurnStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentStreamEvent::TurnCompleted { .. })));

        // 4. Verify metrics
        assert_eq!(run_result.total_usage.input_tokens, 120);
        assert_eq!(run_result.total_usage.output_tokens, 45);
    }
}
