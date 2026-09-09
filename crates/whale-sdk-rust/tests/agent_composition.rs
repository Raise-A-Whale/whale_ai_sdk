use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_protocol::agents::{AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig};
use whale_protocol::rpc::RunTurnOptions;
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_sdk_rust::{DaemonServer, HostTool, WhaleClient};

struct Lookup;
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lookup business data"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("ok"))
    }
}

fn offline_config() -> ProviderConfig {
    ProviderConfig {
        api: ProviderApi::OpenaiChatCompletions,
        base_url: None,
        auth: Some(ProviderAuth::None),
    }
}

#[tokio::test]
async fn definitions_create_independent_sessions_and_preserve_defaults() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        move |session, _| {
            capture.lock().unwrap().push((
                session.system_prompt().unwrap_or_default().to_owned(),
                session.sampling_options().clone(),
                session.history().len(),
                session.get_tool_definitions().len(),
            ));
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(AgentStreamEvent::ItemCompleted {
                    turn_id: "model-step".into(),
                    item: CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer),
                }),
                Ok(AgentStreamEvent::TurnCompleted {
                    turn_id: "provider".into(),
                    thread_id: "provider".into(),
                    usage: Default::default(),
                }),
            ])))
        },
    )));
    let client = WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)));
    let mut definition = AgentDefinition::new("analyst", "default-model");
    definition.provider_config = Some(offline_config());
    definition.system_prompt = Some("Analyze business data".into());
    definition.tool_names = vec!["lookup".into()];
    definition.default_options.temperature = Some(0.25);
    definition.default_options.max_tokens = Some(128);
    let agent = client
        .agent(definition.clone(), vec![Arc::new(Lookup)])
        .unwrap();
    definition.system_prompt = Some("external mutation".into());
    let first = agent.create_session().await.unwrap();
    let second = agent.create_session().await.unwrap();
    assert_ne!(first.id(), second.id());
    first
        .start_turn("one")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    first
        .start_turn_with_options(
            "two",
            RunTurnOptions {
                model: Some("override".into()),
                temperature: Some(0.75),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    first
        .start_turn("three")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    second
        .start_turn("one")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let calls = requests.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls
        .iter()
        .all(|v| v.0 == "Analyze business data" && v.3 == 1));
    assert_eq!(calls[0].1.temperature, Some(0.25));
    assert_eq!(calls[1].1.model, "override");
    assert_eq!(calls[1].1.max_tokens, Some(128));
    assert_eq!(calls[2].1.model, "default-model");
    assert_eq!(calls[2].1.temperature, Some(0.25));
    assert!(calls[2].2 > calls[3].2);
    assert_eq!(calls[0].2, calls[3].2);
    drop(calls);
    client.close().await;
}

#[tokio::test]
async fn tool_bindings_must_match_the_portable_definition() {
    let gate = Arc::new(ApprovalGate::new());
    let engine = Arc::new(AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ))));
    let client = WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)));
    let mut definition = AgentDefinition::new("agent", "model");
    definition.tool_names = vec!["lookup".into()];
    assert!(client.agent(definition.clone(), vec![]).is_err());
    assert!(client
        .agent(definition.clone(), vec![Arc::new(Lookup), Arc::new(Lookup)])
        .is_err());
    definition.tool_names.clear();
    assert!(client.agent(definition, vec![Arc::new(Lookup)]).is_err());
    client.close().await;
}

#[tokio::test]
async fn definition_step_limit_and_deadline_are_enforced() {
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let model_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let capture = model_calls.clone();
    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        move |session, _| {
            capture.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if session.sampling_options().model == "blocked" {
                return Ok(Box::pin(futures::stream::pending()));
            }
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(AgentStreamEvent::ItemCompleted {
                    turn_id: "model".into(),
                    item: CanonicalItem::tool_call("call", None, "lookup", Some(json!({})), "{}"),
                }),
                Ok(AgentStreamEvent::TurnCompleted {
                    turn_id: "provider".into(),
                    thread_id: "provider".into(),
                    usage: Default::default(),
                }),
            ])))
        },
    )));
    let client = WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)));
    let mut definition = AgentDefinition::new("limited", "tool-loop");
    definition.tool_names = vec!["lookup".into()];
    definition.max_steps = 1;
    let agent = client.agent(definition, vec![Arc::new(Lookup)]).unwrap();
    let run = agent
        .create_session()
        .await
        .unwrap()
        .start_turn("run")
        .await
        .unwrap();
    assert_eq!(
        run.result().await.unwrap().status,
        whale_protocol::TurnStatus::Failed
    );
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let mut definition = AgentDefinition::new("deadline", "blocked");
    definition.timeout_ms = Some(10);
    let agent = client.agent(definition, vec![]).unwrap();
    let run = agent
        .create_session()
        .await
        .unwrap()
        .start_turn("run")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), run.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        run.snapshot().await.unwrap().error.unwrap().code,
        "DEADLINE_EXCEEDED"
    );
    client.close().await;
}
