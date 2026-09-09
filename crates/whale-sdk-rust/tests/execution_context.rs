use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_protocol::runs::RunEventPayload;
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_sdk_rust::{
    AgentDefinition, CancellationSignal, ContextBuildRequest, DaemonServer, HostContextPolicy,
    HostTool, ModelContext, ToolContext, WhaleClient,
};

struct ContextTool {
    seen: Arc<Mutex<Vec<Value>>>,
    wait_for_cancel: bool,
    entered: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl HostTool for ContextTool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "Context-aware lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Err("Context-aware override was bypassed".into())
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<CanonicalToolOutput, String> {
        let info = context.info().expect("production bridge supplies identity");
        self.seen
            .lock()
            .unwrap()
            .push(json!({"identity":info,"arguments":arguments}));
        assert!(context
            .report_progress("loaded", Some(0.5))
            .await
            .map_err(|e| e.to_string())?);
        self.entered.notify_one();
        if self.wait_for_cancel {
            context.cancelled().await;
            self.seen
                .lock()
                .unwrap()
                .push(json!({"cancelled":context.is_cancelled()}));
        }
        Ok(CanonicalToolOutput::text("business-value"))
    }
}

fn client() -> WhaleClient {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, step| {
        let item = if step == 0 {
            CanonicalItem::tool_call(
                "model-call",
                None,
                "lookup",
                Some(json!({"query":"x"})),
                "{\"query\":\"x\"}",
            )
        } else {
            CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "step".into(),
                item,
            }),
            Ok(AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            }),
        ])))
    }));
    WhaleClient::in_process(Arc::new(DaemonServer::new(Arc::new(engine), gate)))
}

#[tokio::test]
async fn public_tool_context_preserves_identity_progress_and_audit() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut definition = AgentDefinition::new("analyst", "fixture");
    definition.tool_names = vec!["lookup".into()];
    definition.timeout_ms = Some(5000);
    let agent = client
        .agent(
            definition,
            vec![Arc::new(ContextTool {
                seen: seen.clone(),
                wait_for_cancel: false,
                entered: Arc::new(tokio::sync::Notify::new()),
            })],
        )
        .unwrap();
    let session = agent.create_session().await.unwrap();
    let run = session.start_turn("lookup").await.unwrap();
    let mut events = run.events().unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), run.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, whale_protocol::rpc::TurnStatus::Completed);
    let mut progress = false;
    while let Some(event) = events.recv().await.unwrap() {
        if let RunEventPayload::Stream {
            event:
                AgentStreamEvent::ToolProgress {
                    call_id,
                    progress: value,
                    ..
                },
        } = event.payload
        {
            assert_eq!(call_id, "model-call");
            assert_eq!(value, Some(0.5));
            progress = true;
        }
    }
    assert!(progress);
    let snapshot = run.snapshot().await.unwrap();
    assert_eq!(snapshot.tool_executions.len(), 1);
    assert_eq!(snapshot.tool_executions[0].arguments, json!({"query":"x"}));
    let captured = seen.lock().unwrap();
    assert_eq!(captured[0]["identity"]["agent_name"], "analyst");
    assert_eq!(captured[0]["identity"]["thread_id"], session.id());
    assert_eq!(captured[0]["identity"]["turn_id"], run.id());
    assert_eq!(captured[0]["identity"]["call_id"], "model-call");
    assert!(captured[0]["identity"]["deadline_unix_ms"]
        .as_u64()
        .is_some());
    drop(captured);
    client.close().await;
}

#[tokio::test]
async fn cancellation_reaches_the_host_callback() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut definition = AgentDefinition::new("cancellable", "fixture");
    definition.tool_names = vec!["lookup".into()];
    let session = client
        .agent(
            definition,
            vec![Arc::new(ContextTool {
                seen: seen.clone(),
                wait_for_cancel: true,
                entered: entered.clone(),
            })],
        )
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let run = session.start_turn("start").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    run.cancel().await.unwrap();
    run.result().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if seen.lock().unwrap().iter().any(|v| v["cancelled"] == true) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("callback observes cancellation, not only logical run termination");
    client.close().await;
}

struct ContextPolicy(Arc<Mutex<Vec<ContextBuildRequest>>>);
#[async_trait]
impl HostContextPolicy for ContextPolicy {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        self.0.lock().unwrap().push(request.clone());
        Ok(ModelContext {
            system_prompt: Some("projected".into()),
            items: request.history,
        })
    }
}

#[tokio::test]
async fn agent_binds_context_policy_per_session() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let definition = AgentDefinition::new("projection", "fixture");
    let agent = client
        .agent(definition, vec![])
        .unwrap()
        .with_context_policy(Arc::new(ContextPolicy(seen.clone())));
    let first = agent.create_session().await.unwrap();
    let second = agent.create_session().await.unwrap();
    first
        .start_turn("one")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    second
        .start_turn("two")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let requests = seen.lock().unwrap();
    assert!(requests.iter().any(|r| r.context.thread_id == first.id()));
    assert!(requests.iter().any(|r| r.context.thread_id == second.id()));
    assert!(requests
        .iter()
        .all(|r| r.context.agent_name.as_deref() == Some("projection")));
    drop(requests);
    client.close().await;
}
