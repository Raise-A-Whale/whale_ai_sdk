//! Public SDK acceptance through the separately composed native-provider daemon.
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use whale_protocol::runs::RunEventPayload;
use whale_protocol::{
    AgentStreamEvent, CanonicalContent, CanonicalItem, CanonicalToolOutput, TurnStatus,
};
use whale_sdk_rust::{
    AgentDefinition, CancellationSignal, ContextBuildRequest, HostContextPolicy, HostTool,
    InspectProviderParams, ModelContext, WhaleClient,
};

struct Lookup(Arc<AtomicUsize>);
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "Native provider host lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
    }
    async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
        assert_eq!(args["query"], "original");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::structured(
            json!({"source":"rust-native","query":args["query"]}),
        ))
    }
}
fn definition(model: &str) -> AgentDefinition {
    let mut d = AgentDefinition::new("native-agent", model);
    d.provider_ref = Some("local".into());
    d.tool_names = vec!["lookup".into()];
    d.timeout_ms = Some(5000);
    d
}
async fn client() -> WhaleClient {
    WhaleClient::spawn_daemon(
        std::env::var("WHALE_MODEL_PROVIDER_DAEMON").expect("native daemon path"),
    )
    .await
    .unwrap()
}
fn final_json(items: &[CanonicalItem]) -> Value {
    let text = items
        .iter()
        .rev()
        .find_map(|item| match item {
            CanonicalItem::AssistantMessage { content, .. } => Some(
                content
                    .iter()
                    .filter_map(|part| match part {
                        CanonicalContent::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .expect("assistant completion");
    serde_json::from_str(&text).expect("fixture evidence")
}
#[tokio::test]
#[ignore = "requires native provider daemon fixture"]
async fn native_provider_inspection_and_two_step_tool_loop() {
    let client = client().await;
    let initialized = client.initialize().await.unwrap();
    assert_eq!(initialized.protocol_version, 1);
    assert_eq!(initialized, client.initialize().await.unwrap());
    let inspected = client
        .inspect_provider(InspectProviderParams {
            model: "rust-local-test".into(),
            provider_ref: Some("local".into()),
            provider: None,
            provider_config: None,
        })
        .await
        .unwrap();
    assert!(inspected.capabilities.tool_calls);
    assert_eq!(
        inspected.capabilities.scope,
        whale_sdk_rust::ModelCapabilityScope::Model
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let session = client
        .agent(
            definition("rust-local-test"),
            vec![Arc::new(Lookup(calls.clone()))],
        )
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let result = session
        .start_turn("native-user")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(result.status, TurnStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.usage.input_tokens, 6);
    assert_eq!(result.usage.output_tokens, 2);
    let evidence = final_json(&result.items);
    assert_eq!(evidence["source"], "native-provider");
    assert_eq!(evidence["user_text"], json!(["native-user"]));
    assert!(evidence["tool_results"].to_string().contains("original"));
    assert!(session.close().await.unwrap());
    client.close().await;
}
struct Projection(Arc<Mutex<Vec<Vec<CanonicalItem>>>>);
#[async_trait]
impl HostContextPolicy for Projection {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        self.0.lock().unwrap().push(request.history.clone());
        let start = request
            .history
            .iter()
            .rposition(|item| matches!(item, CanonicalItem::UserMessage { .. }))
            .unwrap();
        let mut items = request.history[start..].to_vec();
        if let CanonicalItem::UserMessage { content, .. } = &mut items[0] {
            *content = vec![CanonicalContent::text("projected-marker")];
        }
        Ok(ModelContext {
            system_prompt: Some("host-projected".into()),
            items,
        })
    }
}
#[tokio::test]
#[ignore = "requires native provider daemon fixture"]
async fn native_provider_observes_projection_while_history_remains_original() {
    let client = client().await;
    let histories = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let session = client
        .agent(
            definition("rust-local-projection"),
            vec![Arc::new(Lookup(calls.clone()))],
        )
        .unwrap()
        .with_context_policy(Arc::new(Projection(histories.clone())))
        .create_session()
        .await
        .unwrap();
    for original in ["private-first", "private-second"] {
        let result = session
            .start_turn(original)
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        assert_eq!(result.status, TurnStatus::Completed);
        let evidence = final_json(&result.items);
        assert_eq!(evidence["system_prompt"], "host-projected");
        assert_eq!(evidence["user_text"], json!(["projected-marker"]));
        assert!(serde_json::to_string(&result.items)
            .unwrap()
            .contains(original));
    }
    let recorded = histories.lock().unwrap();
    let users: Vec<_> = recorded
        .last()
        .unwrap()
        .iter()
        .filter_map(|item| match item {
            CanonicalItem::UserMessage { content, .. } => Some(
                content
                    .iter()
                    .filter_map(|part| match part {
                        CanonicalContent::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(users, vec!["private-first", "private-second"]);
    drop(recorded);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    session.close().await.unwrap();
    client.close().await;
}
#[tokio::test]
#[ignore = "requires native provider daemon fixture"]
async fn native_model_cancellation_and_session_close_preserve_peer() {
    let client = client().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let pending = client
        .agent(
            definition("rust-local-pending"),
            vec![Arc::new(Lookup(calls.clone()))],
        )
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let peer = client
        .agent(
            definition("rust-local-test"),
            vec![Arc::new(Lookup(calls.clone()))],
        )
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let run = pending.start_turn("wait").await.unwrap();
    let mut events = run.events().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let event = events.recv().await.unwrap().unwrap();
            if matches!(
                event.payload,
                RunEventPayload::Stream {
                    event: AgentStreamEvent::TextDelta { .. }
                }
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(pending.close().await.unwrap());
    assert_eq!(run.result().await.unwrap().status, TurnStatus::Interrupted);
    assert!(pending.start_turn("closed").await.is_err());
    assert_eq!(
        peer.start_turn("peer")
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    peer.close().await.unwrap();
    client.close().await;
}
#[tokio::test]
#[ignore = "requires native provider daemon fixture"]
async fn native_unknown_reference_and_unsupported_defaults_fail_before_execution() {
    let client = client().await;
    let mut unknown = definition("rust-local-test");
    unknown.provider_ref = Some("missing".into());
    assert!(client
        .agent(
            unknown,
            vec![Arc::new(Lookup(Arc::new(AtomicUsize::new(0))))]
        )
        .unwrap()
        .create_session()
        .await
        .is_err());
    let mut unsupported = definition("rust-local-test");
    unsupported.default_options.temperature = Some(0.4);
    assert!(client
        .agent(
            unsupported,
            vec![Arc::new(Lookup(Arc::new(AtomicUsize::new(0))))]
        )
        .unwrap()
        .create_session()
        .await
        .is_err());
    client.close().await;
}

#[tokio::test]
async fn public_options_forward_explicit_false_and_preserve_defaults() {
    use whale_core::model::{
        ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    };
    use whale_core::provider::ProviderRegistry;
    use whale_protocol::models::{ModelCapabilities, ModelOption};
    struct OptionsModel(Arc<Mutex<Vec<(Option<u32>, bool)>>>);
    #[async_trait]
    impl ModelProvider for OptionsModel {
        fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
            let mut caps = ModelCapabilities::text_only();
            caps.options = vec![ModelOption::ThinkingBudget, ModelOption::PromptCaching];
            Ok(caps)
        }
        async fn stream(
            &self,
            request: ModelRequest,
            _: whale_core::CancellationToken,
        ) -> Result<ModelEventStream, ModelError> {
            self.0.lock().unwrap().push((
                request.options.thinking_budget,
                request.options.prompt_caching,
            ));
            Ok(Box::pin(futures::stream::iter([
                Ok(ModelEvent::ItemCompleted {
                    item: CanonicalItem::assistant_text(
                        "done",
                        whale_protocol::MessagePhase::FinalAnswer,
                    ),
                }),
                Ok(ModelEvent::StepFinished {
                    usage: Default::default(),
                }),
            ])))
        }
    }
    let captures = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ProviderRegistry::new();
    registry
        .register_provider("options", Arc::new(OptionsModel(captures.clone())))
        .unwrap();
    let client = WhaleClient::in_process(Arc::new(
        whale_sdk_rust::DaemonServer::default_server().with_provider_registry(Arc::new(registry)),
    ));
    let mut d = AgentDefinition::new("options", "model");
    d.provider_ref = Some("options".into());
    d.default_options.thinking_budget = Some(1024);
    d.default_options.prompt_caching = Some(true);
    let session = client
        .agent(d, vec![])
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let run = session
        .start_turn_with_options(
            "override",
            whale_protocol::RunTurnOptions {
                thinking_budget: Some(2048),
                prompt_caching: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap().status, TurnStatus::Completed);
    assert_eq!(
        session
            .start_turn("default")
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    assert_eq!(
        *captures.lock().unwrap(),
        vec![(Some(2048), false), (Some(1024), true)]
    );
    session.close().await.unwrap();
    client.close().await;
}
