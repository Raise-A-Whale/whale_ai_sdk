use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_protocol::runs::RunEventPayload;
use whale_protocol::{
    AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase, TurnStatus,
};
use whale_sdk_rust::{
    AgentDefinition, CancellationSignal, ContextBuildRequest, DaemonServer, HostContextPolicy,
    HostTool, ModelContext, ProviderApi, ProviderAuth, ProviderConfig, ToolContext, WhaleClient,
};

struct DropProbe(Arc<AtomicBool>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
struct Lookup {
    pending: bool,
    approval: bool,
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "business lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
    }
    fn require_approval(&self) -> bool {
        self.approval
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        unreachable!()
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        args: Value,
    ) -> Result<CanonicalToolOutput, String> {
        let _drop = DropProbe(self.dropped.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(context.info().is_some());
        self.entered.notify_one();
        if self.pending {
            futures::future::pending::<()>().await;
        }
        Ok(CanonicalToolOutput::structured(
            json!({"source":"rust-session","query":args["query"]}),
        ))
    }
}
struct PendingPolicy {
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}
#[async_trait]
impl HostContextPolicy for PendingPolicy {
    async fn build(
        &self,
        _: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        let _drop = DropProbe(self.dropped.clone());
        self.entered.notify_one();
        futures::future::pending().await
    }
}
async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("session lifecycle stalled")
}
fn client() -> WhaleClient {
    let gate = Arc::new(ApprovalGate::new());
    let next = Arc::new(AtomicUsize::new(0));
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |session, step| {
        if session.sampling_options().model == "pending-model" {
            return Ok(Box::pin(futures::stream::pending()));
        }
        let item = if step == 0 && session.get_tool_definitions().len() > 0 {
            CanonicalItem::tool_call(
                format!("call-{}", next.fetch_add(1, Ordering::SeqCst)),
                None,
                "lookup",
                Some(json!({"query":"original"})),
                "{\"query\":\"original\"}",
            )
        } else {
            CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer)
        };
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "model".into(),
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
fn lookup(pending: bool, approval: bool) -> Arc<Lookup> {
    Arc::new(Lookup {
        pending,
        approval,
        entered: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicBool::new(false)),
        calls: Arc::new(AtomicUsize::new(0)),
    })
}
fn definition(model: &str) -> AgentDefinition {
    let mut d = AgentDefinition::new(model, model);
    d.tool_names = vec!["lookup".into()];
    d
}
async fn close_pending_tool_and_continue(
    client: WhaleClient,
    first: AgentDefinition,
    second: AgentDefinition,
) {
    let tool = lookup(true, false);
    let a = client
        .agent(first, vec![tool.clone()])
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let other = lookup(false, false);
    let b = client
        .agent(second, vec![other.clone()])
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let run = a.start_turn("lookup").await.unwrap();
    bounded(tool.entered.notified()).await;
    assert!(bounded(a.close()).await.unwrap());
    assert!(tool.dropped.load(Ordering::SeqCst));
    assert_eq!(run.result().await.unwrap().status, TurnStatus::Interrupted);
    assert!(run.snapshot().await.is_err());
    assert!(a.start_turn("late").await.is_err());
    assert!(!a.close().await.unwrap());
    assert_eq!(
        bounded(b.start_turn("other"))
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    assert_eq!(other.calls.load(Ordering::SeqCst), 1);
    assert!(b.close().await.unwrap());
    client.close().await;
}
#[tokio::test]
async fn closing_pending_tool_releases_its_future_and_leaves_another_session_usable() {
    close_pending_tool_and_continue(client(), definition("tool-a"), definition("tool-b")).await;
}
#[tokio::test]
async fn closing_pending_context_policy_releases_its_future() {
    let c = client();
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let a = c
        .agent(AgentDefinition::new("policy", "fixture"), vec![])
        .unwrap()
        .with_context_policy(Arc::new(PendingPolicy {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }))
        .create_session()
        .await
        .unwrap();
    let run = a.start_turn("policy").await.unwrap();
    bounded(entered.notified()).await;
    assert!(bounded(a.close()).await.unwrap());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(run.result().await.unwrap().status, TurnStatus::Interrupted);
    let b = c.create_thread("fixture", None).await.unwrap();
    assert_eq!(
        b.start_turn("other")
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    b.close().await.unwrap();
    c.close().await;
}
#[tokio::test]
async fn close_while_approval_and_registration_are_pending_is_not_blocked() {
    let c = client();
    let tool = lookup(false, true);
    let a = c
        .agent(definition("approval"), vec![tool.clone()])
        .unwrap()
        .create_session()
        .await
        .unwrap();
    let run = a.start_turn("lookup").await.unwrap();
    let mut events = run.events().unwrap();
    bounded(async {
        loop {
            if matches!(
                events.recv().await.unwrap().unwrap().payload,
                RunEventPayload::Stream {
                    event: AgentStreamEvent::ApprovalRequested { .. }
                }
            ) {
                break;
            }
        }
    })
    .await;
    let clone = a.clone();
    let registering = tokio::spawn(async move { clone.register_tool(lookup(false, false)).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!registering.is_finished());
    assert!(bounded(a.close()).await.unwrap());
    assert!(bounded(registering).await.unwrap().is_err());
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(run.result().await.unwrap().status, TurnStatus::Interrupted);
    assert!(c.get_run(a.id(), run.id()).await.is_err());
    let b = c.create_thread("fixture", None).await.unwrap();
    assert_eq!(
        b.start_turn("continue")
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    b.close().await.unwrap();
    c.close().await;
}
#[tokio::test]
async fn closing_a_legacy_run_preserves_its_interrupted_result() {
    let c = client();
    let a = c.create_thread("pending-model", None).await.unwrap();
    let clone = a.clone();
    let running = tokio::spawn(async move { clone.run_turn("legacy").await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(bounded(a.close()).await.unwrap());
    let (result, _) = bounded(running).await.unwrap().unwrap();
    assert_eq!(result.status, TurnStatus::Interrupted);
    c.close().await;
}
#[tokio::test]
#[ignore = "requires production daemon and HTTP fixture"]
async fn real_http_close_pending_host_tool_keeps_another_session_running() {
    let path = std::env::var("WHALE_PROVIDER_DAEMON").unwrap();
    let c = WhaleClient::spawn_daemon(path).await.unwrap();
    let base = std::env::var("WHALE_PROVIDER_FIXTURE_URL").unwrap();
    let mut a = definition("rust-session-tool-close");
    let mut b = definition("rust-session-other-after-close");
    for d in [&mut a, &mut b] {
        d.provider_config = Some(ProviderConfig {
            api: ProviderApi::OpenaiResponses,
            base_url: Some(base.clone()),
            auth: Some(ProviderAuth::None),
        });
    }
    close_pending_tool_and_continue(c, a, b).await;
}
