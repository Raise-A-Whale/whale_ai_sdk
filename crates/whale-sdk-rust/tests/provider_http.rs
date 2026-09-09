//! Gated HTTP provider consumers. Ignored by default; requires controlled local HTTP fixture.
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use whale_protocol::{CanonicalContent, CanonicalItem, CanonicalToolOutput, TurnStatus};
use whale_sdk_rust::{
    AgentDefinition, CancellationSignal, ContextBuildRequest, HostContextPolicy, HostTool,
    ModelContext, ProviderApi, ProviderAuth, ProviderConfig, ToolContext, WhaleClient,
};

struct ContextLookup {
    calls: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    approval: bool,
    reject_initial: bool,
}
#[async_trait]
impl HostTool for ContextLookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "Context-aware business lookup"
    }
    fn require_approval(&self) -> bool {
        self.approval
    }
    fn parameters(&self) -> Value {
        if self.reject_initial {
            json!({"type":"object","properties":{"query":{"type":"integer"}},"required":["query"]})
        } else {
            json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
        }
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Err("context override bypassed".into())
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        args: Value,
    ) -> Result<CanonicalToolOutput, String> {
        assert!(context
            .report_progress("business query", Some(0.5))
            .await
            .map_err(|e| e.to_string())?);
        self.calls
            .lock()
            .unwrap()
            .push(json!({"context":context.info(),"arguments":args}));
        Ok(CanonicalToolOutput::structured(
            json!({"source":"rust-context","query":args["query"]}),
        ))
    }
}

fn context_definition(model: &str) -> AgentDefinition {
    let mut definition = AgentDefinition::new(model, model);
    definition.provider_config = Some(ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: Some(std::env::var("WHALE_PROVIDER_FIXTURE_URL").unwrap()),
        auth: Some(ProviderAuth::None),
    });
    definition.tool_names = vec!["lookup".into()];
    definition.timeout_ms = Some(5000);
    definition
}

#[tokio::test]
#[ignore = "requires controlled HTTP provider and built daemon"]
async fn real_http_context_progress_and_argument_validation() {
    use whale_protocol::runs::{RunApprovalDecision, RunEventPayload};
    let client = WhaleClient::spawn_daemon(std::env::var("WHALE_PROVIDER_DAEMON").unwrap())
        .await
        .unwrap();
    for (name, approval, reject_initial, modified) in [
        ("rust-context-tool", false, false, None),
        (
            "rust-context-valid-modified",
            true,
            false,
            Some(json!({"query":"approved"})),
        ),
        ("rust-context-invalid-initial", false, true, None),
        (
            "rust-context-invalid-modified",
            true,
            false,
            Some(json!({"query":42})),
        ),
    ] {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let session = client
            .agent(
                context_definition(name),
                vec![Arc::new(ContextLookup {
                    calls: calls.clone(),
                    approval,
                    reject_initial,
                })],
            )
            .unwrap()
            .create_session()
            .await
            .unwrap();
        let run = session.start_turn("audit-user").await.unwrap();
        let mut events = run.events().unwrap();
        let mut progress = false;
        while let Some(event) = events.recv().await.unwrap() {
            if let RunEventPayload::Stream { event } = event.payload {
                match event {
                    whale_protocol::AgentStreamEvent::ApprovalRequested { request_id, .. } => {
                        run.resolve_approval(
                            &request_id,
                            RunApprovalDecision::ModifyArguments,
                            modified.clone(),
                            None,
                        )
                        .await
                        .unwrap();
                    }
                    whale_protocol::AgentStreamEvent::ToolProgress {
                        progress: value, ..
                    } => {
                        assert_eq!(value, Some(0.5));
                        progress = true;
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(run.result().await.unwrap().status, TurnStatus::Completed);
        let snapshot = run.snapshot().await.unwrap();
        let executed = !reject_initial && name != "rust-context-invalid-modified";
        assert_eq!(progress, executed);
        assert_eq!(calls.lock().unwrap().len(), usize::from(executed));
        assert_eq!(snapshot.tool_executions.len(), usize::from(executed));
        if executed {
            let calls = calls.lock().unwrap();
            let info = &calls[0]["context"];
            assert_eq!(info["agent_name"], name);
            assert_eq!(info["thread_id"], session.id());
            assert_eq!(info["turn_id"], run.id());
            assert_eq!(info["call_id"], snapshot.tool_executions[0].call_id);
            assert!(info["deadline_unix_ms"].as_u64().is_some());
            assert_eq!(
                snapshot.tool_executions[0].original_arguments,
                json!({"query":"original"})
            );
            assert_eq!(
                snapshot.tool_executions[0].arguments,
                modified.clone().unwrap_or(json!({"query":"original"}))
            );
        } else {
            assert!(snapshot
                .items
                .iter()
                .any(|item| matches!(item, CanonicalItem::ToolResult { is_error: true, .. })));
        }
    }
    client.close().await;
}

struct Projection(Arc<std::sync::Mutex<Vec<ContextBuildRequest>>>);
#[async_trait]
impl HostContextPolicy for Projection {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        self.0.lock().unwrap().push(request.clone());
        let mut items = request.history;
        if request.step_index > 0 {
            if let Some(item) = items
                .iter_mut()
                .rev()
                .find(|item| matches!(item, CanonicalItem::UserMessage { .. }))
            {
                *item = CanonicalItem::user_text("projection-only-user");
            }
        }
        Ok(ModelContext {
            items,
            system_prompt: Some("Projected instructions from rust".into()),
        })
    }
}

#[tokio::test]
#[ignore = "requires controlled HTTP provider and built daemon"]
async fn real_http_host_projection_preserves_original_history() {
    let client = WhaleClient::spawn_daemon(std::env::var("WHALE_PROVIDER_DAEMON").unwrap())
        .await
        .unwrap();
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let session = client
        .agent(
            context_definition("rust-context-projection"),
            vec![Arc::new(Lookup("rust-projection"))],
        )
        .unwrap()
        .with_context_policy(Arc::new(Projection(captured.clone())))
        .create_session()
        .await
        .unwrap();
    for prompt in ["audit-user", "follow-up"] {
        let run = session.start_turn(prompt).await.unwrap();
        let result = run.result().await.unwrap();
        assert_eq!(result.status, TurnStatus::Completed);
        assert!(!serde_json::to_string(&result.items)
            .unwrap()
            .contains("projection-only-user"));
    }
    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(serde_json::to_string(&requests[2].history)
        .unwrap()
        .contains("audit-user"));
    assert!(requests.iter().all(|r| !serde_json::to_string(&r.history)
        .unwrap()
        .contains("projection-only-user")));
    drop(requests);
    client.close().await;
}

struct Lookup(&'static str);
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lookup business records"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
    }
    async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
        assert_eq!(args["query"], "original");
        Ok(CanonicalToolOutput::structured(json!({"source":self.0})))
    }
}

#[tokio::test]
#[ignore = "requires controlled HTTP provider and built daemon"]
async fn real_http_model_tool_loop_for_each_protocol() {
    let daemon = std::env::var("WHALE_PROVIDER_DAEMON").unwrap();
    let endpoint = std::env::var("WHALE_PROVIDER_FIXTURE_URL").unwrap();
    let client = WhaleClient::spawn_daemon(daemon).await.unwrap();
    for (api, name) in [
        (ProviderApi::OpenaiChatCompletions, "chat"),
        (ProviderApi::OpenaiResponses, "responses"),
        (ProviderApi::AnthropicMessages, "anthropic"),
    ] {
        let mut definition = AgentDefinition::new(format!("rust-{name}"), format!("rust-{name}"));
        definition.system_prompt = Some("Rust business instructions".into());
        definition.provider_config = Some(ProviderConfig {
            api,
            base_url: Some(endpoint.clone()),
            auth: Some(ProviderAuth::None),
        });
        definition.tool_names = vec!["lookup".into()];
        definition.default_options.max_tokens = Some(128);
        definition.timeout_ms = Some(5000);
        let agent = client
            .agent(definition, vec![Arc::new(Lookup(name))])
            .unwrap();
        let session = agent.create_session().await.unwrap();
        let run = session.start_turn("normal").await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(8), run.result())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.status,
            TurnStatus::Completed,
            "{:?}",
            run.snapshot().await.unwrap()
        );
        assert_eq!(
            (result.usage.input_tokens, result.usage.output_tokens),
            (6, 4)
        );
        assert!(result.items.iter().any(|item| matches!(item,CanonicalItem::AssistantMessage{content,..} if content.iter().any(|content|matches!(content,CanonicalContent::Text{text} if text=="Fixture answer")))));
        assert!(result.items.iter().any(|item| matches!(item,CanonicalItem::ToolResult{output:CanonicalToolOutput::Structured{data},..} if data["source"]==name)));
    }
    client.close().await;
}
