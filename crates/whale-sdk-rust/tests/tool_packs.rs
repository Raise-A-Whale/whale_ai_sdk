use async_trait::async_trait;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_sdk_rust::{
    AgentDefinition, BoundToolPack, DaemonServer, HostTool, ProviderApi, ProviderAuth,
    ProviderConfig, RecoveryKey, SessionBindContext, SessionBindKind, ToolPack, ToolPackError,
    ToolPackManifest, ToolPackTool, WhaleClient, WhaleRuntime,
};

struct EmptyBoundPack;

#[async_trait]
impl BoundToolPack for EmptyBoundPack {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        Vec::new()
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        Ok(())
    }
}

#[derive(Default)]
struct EmptyPack;

#[async_trait]
impl ToolPack for EmptyPack {
    fn manifest(&self) -> ToolPackManifest {
        ToolPackManifest {
            id: "workspace".into(),
            tools: vec![ToolPackTool {
                name: "read_workspace".into(),
                description: "Read one workspace file".into(),
                parameters: serde_json::json!({"type":"object"}),
                supports_parallel: true,
                require_approval: false,
            }],
        }
    }

    async fn bind(&self, _: SessionBindContext) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        panic!("Agent composition must not bind a ToolPack")
    }
}

#[tokio::test]
async fn public_tool_pack_contract_composes_without_changing_agent() {
    let client = WhaleClient::in_process(Arc::new(DaemonServer::default_server()));
    let mut definition = AgentDefinition::new("reader", "fixture");
    definition.tool_names = vec!["read_workspace".into()];

    let agent = client
        .agent_with_tool_packs(definition, vec![], vec![Arc::new(EmptyPack)])
        .unwrap();

    assert_eq!(agent.definition().tool_names, vec!["read_workspace"]);
    assert_eq!(SessionBindKind::Ephemeral, SessionBindKind::Ephemeral);
    let error = ToolPackError::new("fixture");
    assert_eq!(error.message(), "fixture");
    let mut bound = EmptyBoundPack;
    assert!(bound.tools().is_empty());
    bound.close().await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn runtime_exposes_the_same_additive_composition() {
    let runtime = WhaleRuntime::open(Default::default()).await.unwrap();
    let mut definition = AgentDefinition::new("reader", "fixture");
    definition.tool_names = vec!["read_workspace".into()];

    let agent = runtime
        .agent_with_tool_packs(definition, vec![], vec![Arc::new(EmptyPack)])
        .unwrap();

    assert_eq!(agent.definition().tool_names, vec!["read_workspace"]);
    runtime.shutdown().await.unwrap();
}

#[test]
fn persistent_bind_kinds_expose_only_the_safe_recovery_id() {
    let key = RecoveryKey::new();
    let kinds = [
        SessionBindKind::PersistentCreate {
            recovery_id: key.recovery_id.clone(),
        },
        SessionBindKind::PersistentAttach {
            recovery_id: key.recovery_id.clone(),
        },
    ];

    let rendered = format!("{kinds:?}");
    assert!(rendered.contains(&key.recovery_id));
    assert!(!rendered.contains(&key.secret));
}

struct SessionTool {
    session_id: String,
}

#[async_trait]
impl HostTool for SessionTool {
    fn name(&self) -> &str {
        "session_probe"
    }

    fn description(&self) -> &str {
        "Reports its bound Session identity"
    }

    fn parameters(&self) -> Value {
        serde_json::json!({"type": "object"})
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text(self.session_id.clone()))
    }
}

struct SessionBoundPack {
    tool: Arc<SessionTool>,
    normal_close: Arc<AtomicUsize>,
    emergency_close: Arc<AtomicUsize>,
}

#[async_trait]
impl BoundToolPack for SessionBoundPack {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        vec![self.tool.clone()]
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        self.normal_close.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn emergency_close(&mut self) {
        self.emergency_close.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct SessionProbePack {
    bind_count: AtomicUsize,
    contexts: Mutex<Vec<SessionBindContext>>,
    tools: Mutex<Vec<Arc<SessionTool>>>,
    normal_close: Arc<AtomicUsize>,
    emergency_close: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolPack for SessionProbePack {
    fn manifest(&self) -> ToolPackManifest {
        ToolPackManifest {
            id: "session-probe".into(),
            tools: vec![ToolPackTool {
                name: "session_probe".into(),
                description: "Reports its bound Session identity".into(),
                parameters: serde_json::json!({"type": "object"}),
                supports_parallel: true,
                require_approval: false,
            }],
        }
    }

    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        self.bind_count.fetch_add(1, Ordering::SeqCst);
        let tool = Arc::new(SessionTool {
            session_id: context.session_id().into(),
        });
        self.contexts.lock().unwrap().push(context);
        self.tools.lock().unwrap().push(tool.clone());
        Ok(Box::new(SessionBoundPack {
            tool,
            normal_close: self.normal_close.clone(),
            emergency_close: self.emergency_close.clone(),
        }))
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
async fn fresh_sessions_bind_once_per_session_with_distinct_resources() {
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        |session, _| {
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(AgentStreamEvent::ItemCompleted {
                    turn_id: "model-step".into(),
                    item: CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer),
                }),
                Ok(AgentStreamEvent::TurnCompleted {
                    turn_id: "provider".into(),
                    thread_id: session.id().into(),
                    usage: Default::default(),
                }),
            ])))
        },
    )));
    let client = WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)));
    let probe = Arc::new(SessionProbePack::default());
    let mut definition = AgentDefinition::new("session-agent", "fixture-model");
    definition.provider_config = Some(offline_config());
    definition.tool_names = vec!["session_probe".into()];
    let agent = client
        .agent_with_tool_packs(definition, vec![], vec![probe.clone()])
        .unwrap();

    let first = agent.create_session().await.unwrap();
    let second = agent.create_session().await.unwrap();
    for _ in 0..2 {
        first
            .start_turn("verify one bind")
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
    }

    assert_eq!(probe.bind_count.load(Ordering::SeqCst), 2);
    let contexts = probe.contexts.lock().unwrap();
    assert_ne!(contexts[0].session_id(), contexts[1].session_id());
    assert!(contexts
        .iter()
        .all(|context| context.kind() == &SessionBindKind::Ephemeral));
    assert!(contexts
        .iter()
        .all(|context| context.agent_name() == "session-agent"));
    assert!(contexts
        .iter()
        .all(|context| !context.session_cancelled().is_cancelled()));
    drop(contexts);
    let tools = probe.tools.lock().unwrap();
    assert!(!Arc::ptr_eq(&tools[0], &tools[1]));
    assert_eq!(tools[0].session_id, first.id());
    assert_eq!(tools[1].session_id, second.id());
    drop(tools);

    assert!(first.close().await.unwrap());
    assert_eq!(probe.normal_close.load(Ordering::SeqCst), 1);
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 0);
    second
        .start_turn("the other Session remains open")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(second.close().await.unwrap());
    assert_eq!(probe.normal_close.load(Ordering::SeqCst), 2);
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn close_session_awaits_normal_pack_close_once() {
    let client = WhaleClient::in_process(Arc::new(DaemonServer::default_server()));
    let probe = Arc::new(SessionProbePack::default());
    let mut definition = AgentDefinition::new("session-agent", "fixture-model");
    definition.provider_config = Some(offline_config());
    definition.tool_names = vec!["session_probe".into()];
    let agent = client
        .agent_with_tool_packs(definition, vec![], vec![probe.clone()])
        .unwrap();
    let thread = agent.create_session().await.unwrap();

    assert!(thread.close().await.unwrap());
    assert!(!thread.close().await.unwrap());
    assert_eq!(probe.normal_close.load(Ordering::SeqCst), 1);
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 0);
    client.close().await;
    assert_eq!(probe.normal_close.load(Ordering::SeqCst), 1);
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn close_runtime_drop_emergency_drains_packs_while_client_clone_remains() {
    let runtime = WhaleRuntime::open(Default::default()).await.unwrap();
    let capability = runtime.client().clone();
    let probe = Arc::new(SessionProbePack::default());
    let mut definition = AgentDefinition::new("session-agent", "fixture-model");
    definition.provider_config = Some(offline_config());
    definition.tool_names = vec!["session_probe".into()];
    let agent = runtime
        .agent_with_tool_packs(definition, vec![], vec![probe.clone()])
        .unwrap();
    let thread = agent.create_session().await.unwrap();

    drop(runtime);

    assert_eq!(probe.normal_close.load(Ordering::SeqCst), 0);
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 1);
    assert!(thread.start_turn("closed").await.is_err());
    capability.close().await;
    assert_eq!(probe.emergency_close.load(Ordering::SeqCst), 1);
}
