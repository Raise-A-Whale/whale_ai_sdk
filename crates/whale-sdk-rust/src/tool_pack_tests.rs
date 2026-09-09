use crate::{
    AgentDefinition, BoundToolPack, HostTool, SdkError, SessionBindContext, ToolPack,
    ToolPackError, ToolPackManifest, ToolPackTool, WhaleClient,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{mpsc, Notify, Semaphore};
use whale_protocol::CanonicalToolOutput;

struct NeverBound;

#[async_trait]
impl BoundToolPack for NeverBound {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        unreachable!("Agent composition must not inspect bound tools")
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        unreachable!("Agent composition must not close a bound pack")
    }
}

struct ManifestPack {
    manifest: Mutex<ToolPackManifest>,
    manifest_calls: AtomicUsize,
    bind_calls: AtomicUsize,
    panic_manifest: AtomicBool,
}

impl ManifestPack {
    fn new(id: &str, tools: Vec<ToolPackTool>) -> Arc<Self> {
        Arc::new(Self {
            manifest: Mutex::new(ToolPackManifest {
                id: id.into(),
                tools,
            }),
            manifest_calls: AtomicUsize::new(0),
            bind_calls: AtomicUsize::new(0),
            panic_manifest: AtomicBool::new(false),
        })
    }

    fn panic_manifest() -> Arc<Self> {
        let pack = Self::new("panic", vec![tool("never")]);
        pack.panic_manifest.store(true, Ordering::SeqCst);
        pack
    }
}

#[async_trait]
impl ToolPack for ManifestPack {
    fn manifest(&self) -> ToolPackManifest {
        self.manifest_calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_manifest.load(Ordering::SeqCst),
            "fixture manifest panic"
        );
        self.manifest.lock().unwrap().clone()
    }

    async fn bind(&self, _: SessionBindContext) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        self.bind_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(NeverBound))
    }
}

struct StaticTool(&'static str);

#[async_trait]
impl HostTool for StaticTool {
    fn name(&self) -> &str {
        self.0
    }

    fn description(&self) -> &str {
        "static fixture"
    }

    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("ok"))
    }
}

fn tool(name: &str) -> ToolPackTool {
    ToolPackTool {
        name: name.into(),
        description: format!("{name} fixture"),
        parameters: json!({"type": "object"}),
        supports_parallel: true,
        require_approval: false,
    }
}

fn definition(names: &[&str]) -> AgentDefinition {
    let mut definition = AgentDefinition::new("fixture-agent", "fixture-model");
    definition.tool_names = names.iter().map(|name| (*name).into()).collect();
    definition
}

fn assert_invalid(result: Result<crate::Agent, SdkError>) {
    assert!(matches!(result, Err(SdkError::InvalidConfiguration(_))));
}

#[tokio::test]
async fn agent_rejects_invalid_pack_identity_and_shape_without_binding() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));
    let cases = [
        ManifestPack::new("", vec![tool("one")]),
        ManifestPack::new(" padded ", vec![tool("one")]),
        ManifestPack::new("empty", vec![]),
        ManifestPack::new("blank-tool", vec![tool(" ")]),
        ManifestPack::new("padded-tool", vec![tool(" padded ")]),
        ManifestPack::new(
            "blank-description",
            vec![ToolPackTool {
                description: " ".into(),
                ..tool("one")
            }],
        ),
        ManifestPack::new(
            "invalid-schema",
            vec![ToolPackTool {
                parameters: json!({"$ref": "https://example.invalid/schema"}),
                ..tool("one")
            }],
        ),
    ];

    for pack in cases {
        assert_invalid(client.agent_with_tool_packs(
            definition(&["one"]),
            vec![],
            vec![pack.clone()],
        ));
        assert_eq!(pack.manifest_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pack.bind_calls.load(Ordering::SeqCst), 0);
    }
    client.close().await;
}

#[tokio::test]
async fn agent_rejects_duplicate_pack_and_tool_identities_without_binding() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));

    let first = ManifestPack::new("same", vec![tool("one")]);
    let second = ManifestPack::new("same", vec![tool("two")]);
    assert_invalid(client.agent_with_tool_packs(
        definition(&["one", "two"]),
        vec![],
        vec![first.clone(), second.clone()],
    ));

    let within = ManifestPack::new("within", vec![tool("same"), tool("same")]);
    assert_invalid(client.agent_with_tool_packs(
        definition(&["same"]),
        vec![],
        vec![within.clone()],
    ));

    let across_a = ManifestPack::new("a", vec![tool("same")]);
    let across_b = ManifestPack::new("b", vec![tool("same")]);
    assert_invalid(client.agent_with_tool_packs(
        definition(&["same"]),
        vec![],
        vec![across_a.clone(), across_b.clone()],
    ));

    let static_duplicate = ManifestPack::new("pack", vec![tool("same")]);
    assert_invalid(client.agent_with_tool_packs(
        definition(&["same"]),
        vec![Arc::new(StaticTool("same"))],
        vec![static_duplicate.clone()],
    ));

    for pack in [first, second, within, across_a, across_b, static_duplicate] {
        assert_eq!(pack.bind_calls.load(Ordering::SeqCst), 0);
    }
    client.close().await;
}

#[tokio::test]
async fn agent_rejects_manifest_panic_and_name_union_mismatch_without_binding() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));
    let panicking = ManifestPack::panic_manifest();
    assert_invalid(client.agent_with_tool_packs(
        definition(&["never"]),
        vec![],
        vec![panicking.clone()],
    ));
    assert_eq!(panicking.manifest_calls.load(Ordering::SeqCst), 1);
    assert_eq!(panicking.bind_calls.load(Ordering::SeqCst), 0);

    let mismatch = ManifestPack::new("pack", vec![tool("actual")]);
    assert_invalid(client.agent_with_tool_packs(
        definition(&["expected"]),
        vec![],
        vec![mismatch.clone()],
    ));
    assert_eq!(mismatch.bind_calls.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn agent_freezes_manifest_once_without_binding() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));
    let pack = ManifestPack::new("workspace", vec![tool("read_workspace")]);
    let factory: Arc<dyn ToolPack> = pack.clone();
    let frozen = pack.manifest.lock().unwrap().clone();

    let agent = client
        .agent_with_tool_packs(
            definition(&["read_workspace"]),
            vec![],
            vec![factory.clone()],
        )
        .unwrap();
    *pack.manifest.lock().unwrap() = ToolPackManifest {
        id: "mutated".into(),
        tools: vec![tool("mutated")],
    };

    assert_eq!(pack.manifest_calls.load(Ordering::SeqCst), 1);
    assert_eq!(pack.bind_calls.load(Ordering::SeqCst), 0);
    assert_eq!(agent.packs.len(), 1);
    assert_eq!(agent.packs[0].manifest, frozen);
    assert!(Arc::ptr_eq(&agent.packs[0].factory, &factory));
    client.close().await;
}

#[tokio::test]
async fn static_and_pack_tools_form_one_order_independent_name_union() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));
    let pack = ManifestPack::new("workspace", vec![tool("from_pack")]);

    let agent = client
        .agent_with_tool_packs(
            definition(&["from_pack", "static"]),
            vec![Arc::new(StaticTool("static"))],
            vec![pack.clone()],
        )
        .unwrap();

    assert_eq!(agent.packs.len(), 1);
    assert_eq!(pack.manifest_calls.load(Ordering::SeqCst), 1);
    assert_eq!(pack.bind_calls.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn existing_static_agent_keeps_its_validation_contract() {
    let client = WhaleClient::in_process(Arc::new(crate::DaemonServer::default_server()));
    let agent = client
        .agent(
            definition(&["static"]),
            vec![Arc::new(StaticTool("static"))],
        )
        .unwrap();
    assert!(agent.packs.is_empty());
    assert_invalid(client.agent(definition(&["static"]), vec![]));
    client.close().await;
}

#[derive(Default)]
pub(super) struct EventLog {
    entries: Mutex<Vec<String>>,
    changed: Notify,
}

impl EventLog {
    fn push(&self, event: impl Into<String>) {
        self.entries.lock().unwrap().push(event.into());
        self.changed.notify_one();
    }

    pub(super) fn snapshot(&self) -> Vec<String> {
        self.entries.lock().unwrap().clone()
    }

    pub(super) async fn wait_for(&self, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if self
                    .entries
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| entry == expected)
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected:?}: {:?}", self.snapshot()));
    }
}

#[derive(Clone, Copy)]
enum BindBehavior {
    Ok,
    Error,
    Panic,
}

#[derive(Clone, Copy)]
enum CloseBehavior {
    Ok,
    Error,
    Panic,
}

struct BindGate {
    entered: Semaphore,
    release: Semaphore,
    log_partial_drop: bool,
}

impl BindGate {
    fn new(log_partial_drop: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            log_partial_drop,
        })
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.entered.acquire())
            .await
            .expect("bind did not enter")
            .expect("bind gate closed")
            .forget();
    }

    fn release(&self) {
        self.release.add_permits(1);
    }
}

struct PartialAllocation {
    id: String,
    log: Arc<EventLog>,
    armed: bool,
}

impl Drop for PartialAllocation {
    fn drop(&mut self) {
        if self.armed {
            self.log.push(format!("partial-drop:{}", self.id));
        }
    }
}

#[derive(Clone)]
struct ProbeToolSpec {
    name: String,
    description: String,
    parameters: Value,
    supports_parallel: bool,
    require_approval: bool,
    panic_metadata: bool,
}

impl ProbeToolSpec {
    fn manifest(&self) -> ToolPackTool {
        ToolPackTool {
            name: self.name.clone(),
            description: self.description.clone(),
            parameters: self.parameters.clone(),
            supports_parallel: self.supports_parallel,
            require_approval: self.require_approval,
        }
    }
}

struct BoundProbeTool(ProbeToolSpec);

#[async_trait]
impl HostTool for BoundProbeTool {
    fn name(&self) -> &str {
        assert!(!self.0.panic_metadata, "fixture handler metadata panic");
        &self.0.name
    }

    fn description(&self) -> &str {
        &self.0.description
    }

    fn parameters(&self) -> Value {
        self.0.parameters.clone()
    }

    fn supports_parallel(&self) -> bool {
        self.0.supports_parallel
    }

    fn require_approval(&self) -> bool {
        self.0.require_approval
    }

    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("probe"))
    }
}

struct ProbeBoundPack {
    id: String,
    handlers: Vec<Arc<dyn HostTool>>,
    tools_panic: bool,
    close_behavior: CloseBehavior,
    close_count: Arc<AtomicUsize>,
    emergency_count: Arc<AtomicUsize>,
    log: Arc<EventLog>,
    close_gate: Option<Arc<BindGate>>,
}

#[async_trait]
impl BoundToolPack for ProbeBoundPack {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        assert!(!self.tools_panic, "fixture tools panic");
        self.handlers.clone()
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        self.close_count.fetch_add(1, Ordering::SeqCst);
        self.log.push(format!("close:{}", self.id));
        if let Some(gate) = &self.close_gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        match self.close_behavior {
            CloseBehavior::Ok => Ok(()),
            CloseBehavior::Error => Err(ToolPackError::new(format!(
                "{} close fixture error",
                self.id
            ))),
            CloseBehavior::Panic => panic!("{} close fixture panic", self.id),
        }
    }

    fn emergency_close(&mut self) {
        self.emergency_count.fetch_add(1, Ordering::SeqCst);
        self.log.push(format!("emergency:{}", self.id));
    }
}

pub(super) struct TransactionProbePack {
    id: String,
    manifest_tools: Mutex<Vec<ToolPackTool>>,
    bound_tools: Vec<ProbeToolSpec>,
    bind_behavior: BindBehavior,
    close_behavior: CloseBehavior,
    tools_panic: bool,
    gate: Option<Arc<BindGate>>,
    close_gate: Option<Arc<BindGate>>,
    bind_count: AtomicUsize,
    close_count: Arc<AtomicUsize>,
    emergency_count: Arc<AtomicUsize>,
    contexts: Mutex<Vec<SessionBindContext>>,
    log: Arc<EventLog>,
}

impl TransactionProbePack {
    pub(super) fn new(id: &str, tool_name: &str, log: Arc<EventLog>) -> Self {
        let spec = ProbeToolSpec {
            name: tool_name.into(),
            description: format!("{tool_name} description"),
            parameters: json!({"type": "object"}),
            supports_parallel: true,
            require_approval: false,
            panic_metadata: false,
        };
        Self {
            id: id.into(),
            manifest_tools: Mutex::new(vec![spec.manifest()]),
            bound_tools: vec![spec],
            bind_behavior: BindBehavior::Ok,
            close_behavior: CloseBehavior::Ok,
            tools_panic: false,
            gate: None,
            close_gate: None,
            bind_count: AtomicUsize::new(0),
            close_count: Arc::new(AtomicUsize::new(0)),
            emergency_count: Arc::new(AtomicUsize::new(0)),
            contexts: Mutex::new(Vec::new()),
            log,
        }
    }

    fn bind_error(mut self) -> Self {
        self.bind_behavior = BindBehavior::Error;
        self
    }

    fn bind_panic(mut self) -> Self {
        self.bind_behavior = BindBehavior::Panic;
        self
    }

    fn close_error(mut self) -> Self {
        self.close_behavior = CloseBehavior::Error;
        self
    }

    fn close_panic(mut self) -> Self {
        self.close_behavior = CloseBehavior::Panic;
        self
    }

    fn mismatched_description(mut self) -> Self {
        self.bound_tools[0].description = "different live description".into();
        self
    }

    fn tools_panic(mut self) -> Self {
        self.tools_panic = true;
        self
    }

    fn metadata_panic(mut self) -> Self {
        self.bound_tools[0].panic_metadata = true;
        self
    }

    fn gated(mut self, gate: Arc<BindGate>) -> Self {
        self.gate = Some(gate);
        self
    }

    fn close_gated(mut self, gate: Arc<BindGate>) -> Self {
        self.close_gate = Some(gate);
        self
    }

    pub(super) fn replace_manifest_tool(&self, tool: ToolPackTool) {
        *self.manifest_tools.lock().unwrap() = vec![tool];
    }

    pub(super) fn bind_count(&self) -> usize {
        self.bind_count.load(Ordering::SeqCst)
    }

    pub(super) fn normal_close_count(&self) -> usize {
        self.close_count.load(Ordering::SeqCst)
    }

    pub(super) fn emergency_close_count(&self) -> usize {
        self.emergency_count.load(Ordering::SeqCst)
    }

    pub(super) fn contexts(&self) -> Vec<SessionBindContext> {
        self.contexts.lock().unwrap().clone()
    }
}

#[async_trait]
impl ToolPack for TransactionProbePack {
    fn manifest(&self) -> ToolPackManifest {
        ToolPackManifest {
            id: self.id.clone(),
            tools: self.manifest_tools.lock().unwrap().clone(),
        }
    }

    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        self.bind_count.fetch_add(1, Ordering::SeqCst);
        self.contexts.lock().unwrap().push(context);
        self.log.push(format!("bind:{}", self.id));
        let mut partial = self.gate.as_ref().map(|gate| {
            gate.entered.add_permits(1);
            PartialAllocation {
                id: self.id.clone(),
                log: self.log.clone(),
                armed: gate.log_partial_drop,
            }
        });
        if let Some(gate) = &self.gate {
            gate.release
                .acquire()
                .await
                .expect("bind release gate closed")
                .forget();
            if let Some(partial) = &mut partial {
                partial.armed = false;
            }
        }
        match self.bind_behavior {
            BindBehavior::Error => {
                return Err(ToolPackError::new(format!(
                    "{} bind fixture error",
                    self.id
                )))
            }
            BindBehavior::Panic => panic!("{} bind fixture panic", self.id),
            BindBehavior::Ok => {}
        }
        let handlers = self
            .bound_tools
            .iter()
            .cloned()
            .map(|spec| Arc::new(BoundProbeTool(spec)) as Arc<dyn HostTool>)
            .collect();
        Ok(Box::new(ProbeBoundPack {
            id: self.id.clone(),
            handlers,
            tools_panic: self.tools_panic,
            close_behavior: self.close_behavior,
            close_count: self.close_count.clone(),
            emergency_count: self.emergency_count.clone(),
            log: self.log.clone(),
            close_gate: self.close_gate.clone(),
        }))
    }
}

pub(super) fn transaction_client() -> (WhaleClient, mpsc::Receiver<String>) {
    let state = crate::ClientState::new();
    let (sender, receiver) = mpsc::channel(32);
    (
        WhaleClient {
            inner: Arc::new(crate::ClientInner {
                state,
                writer: crate::ManagedWriter::channel(sender),
                compatibility_owner: None,
            }),
        },
        receiver,
    )
}

fn reply_result(client: &WhaleClient, request: &Value, result: Value) {
    client.inner.state.incoming(
        &json!({"jsonrpc": "2.0", "id": request["id"], "result": result}).to_string(),
        &client.inner.writer,
    );
}

fn reply_error(client: &WhaleClient, request: &Value, code: i64, message: &str) {
    client.inner.state.incoming(
        &json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "error": {"code": code, "message": message}
        })
        .to_string(),
        &client.inner.writer,
    );
}

async fn spawn_initialized_creation(
    client: &WhaleClient,
    receiver: &mut mpsc::Receiver<String>,
    agent: crate::Agent,
) -> tokio::task::JoinHandle<Result<crate::WhaleThread, SdkError>> {
    let creation = tokio::spawn(async move { agent.create_session().await });
    let request = crate::initialization_tests::next(receiver).await;
    assert_eq!(
        request["method"],
        whale_protocol::initialization::METHOD_INITIALIZE
    );
    reply_result(
        client,
        &request,
        crate::initialization_tests::valid_reply(&request),
    );
    creation
}

async fn finish_without_session_rpc(
    mut creation: tokio::task::JoinHandle<Result<crate::WhaleThread, SdkError>>,
    receiver: &mut mpsc::Receiver<String>,
) -> Result<crate::WhaleThread, SdkError> {
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut creation => result.expect("creation task panicked"),
            frame = receiver.recv() => panic!("unexpected Session RPC: {frame:?}"),
        }
    })
    .await
    .expect("creation did not finish")
}

pub(super) fn transaction_agent(
    client: &WhaleClient,
    names: &[&str],
    packs: Vec<Arc<dyn ToolPack>>,
) -> crate::Agent {
    client
        .agent_with_tool_packs(definition(names), Vec::new(), packs)
        .unwrap()
}

pub(super) fn assert_build_state_removed(client: &WhaleClient) {
    assert!(client.inner.state.tools.is_empty());
    assert!(client.inner.state.tool_bindings.is_empty());
    assert!(client.inner.state.context_policies.is_empty());
    assert!(client.inner.state.session_event_hubs.is_empty());
    assert!(client.inner.state.sessions.is_empty());
}

mod rollback {
    use super::*;

    #[tokio::test]
    async fn later_bind_failure_closes_prior_packs_in_reverse_without_rpc() {
        let log = Arc::new(EventLog::default());
        let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()));
        let c = Arc::new(TransactionProbePack::new("C", "c", log.clone()).bind_error());
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(
            &client,
            &["a", "b", "c"],
            vec![a.clone(), b.clone(), c.clone()],
        );

        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
        assert!(matches!(
            finish_without_session_rpc(creation, &mut receiver).await,
            Err(SdkError::Internal(message)) if message.contains("C")
        ));

        assert_eq!(
            log.snapshot(),
            ["bind:A", "bind:B", "bind:C", "close:B", "close:A"]
        );
        assert_eq!(a.close_count.load(Ordering::SeqCst), 1);
        assert_eq!(b.close_count.load(Ordering::SeqCst), 1);
        assert_eq!(a.emergency_count.load(Ordering::SeqCst), 0);
        assert_eq!(b.emergency_count.load(Ordering::SeqCst), 0);
        assert_build_state_removed(&client);
        client.close().await;
    }

    #[tokio::test]
    async fn metadata_mismatch_closes_the_returned_pack_before_earlier_packs() {
        let log = Arc::new(EventLog::default());
        let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()).mismatched_description());
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a", "b"], vec![a.clone(), b.clone()]);

        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
        assert!(matches!(
            finish_without_session_rpc(creation, &mut receiver).await,
            Err(SdkError::InvalidConfiguration(message)) if message.contains("B")
        ));

        assert_eq!(log.snapshot(), ["bind:A", "bind:B", "close:B", "close:A"]);
        assert_build_state_removed(&client);
        client.close().await;
    }

    #[tokio::test]
    async fn bind_tools_and_handler_metadata_panics_are_contained() {
        for (label, failing) in [
            (
                "bind",
                TransactionProbePack::new("B", "b", Arc::new(EventLog::default())).bind_panic(),
            ),
            (
                "tools",
                TransactionProbePack::new("B", "b", Arc::new(EventLog::default())).tools_panic(),
            ),
            (
                "metadata",
                TransactionProbePack::new("B", "b", Arc::new(EventLog::default())).metadata_panic(),
            ),
        ] {
            let log = failing.log.clone();
            let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
            let b = Arc::new(failing);
            let (client, mut receiver) = transaction_client();
            let agent = transaction_agent(&client, &["a", "b"], vec![a, b]);
            let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
            assert!(matches!(
                finish_without_session_rpc(creation, &mut receiver).await,
                Err(SdkError::Internal(message)) if message.contains("B") && message.contains(label)
            ));
            assert!(log.snapshot().ends_with(&["close:A".into()]));
            if label != "bind" {
                assert!(log.snapshot().contains(&"close:B".into()));
            }
            assert_build_state_removed(&client);
            client.close().await;
        }
    }

    #[tokio::test]
    async fn rejected_request_and_invalid_ack_close_packs_and_remove_routes() {
        for invalid_ack in [false, true] {
            let log = Arc::new(EventLog::default());
            let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
            let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()));
            let (client, mut receiver) = transaction_client();
            let agent = transaction_agent(&client, &["a", "b"], vec![a, b]);
            let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
            let request = crate::initialization_tests::next(&mut receiver).await;
            assert_eq!(
                request["method"],
                whale_protocol::rpc::METHOD_SESSION_START_THREAD
            );
            if invalid_ack {
                reply_result(
                    &client,
                    &request,
                    json!({"thread_id": "wrong-session", "created_at": "fixture"}),
                );
            } else {
                reply_error(
                    &client,
                    &request,
                    whale_protocol::rpc::JSONRPCError::INVALID_PARAMS,
                    "fixture rejection",
                );
            }
            assert!(creation.await.unwrap().is_err());
            assert_eq!(log.snapshot(), ["bind:A", "bind:B", "close:B", "close:A"]);
            assert_build_state_removed(&client);
            client.close().await;
        }
    }

    #[tokio::test]
    async fn cleanup_errors_are_aggregated_without_skipping_earlier_packs() {
        let log = Arc::new(EventLog::default());
        let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()).close_error());
        let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()).close_panic());
        let c = Arc::new(TransactionProbePack::new("C", "c", log.clone()).bind_error());
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a", "b", "c"], vec![a, b, c]);

        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
        let error = match finish_without_session_rpc(creation, &mut receiver).await {
            Err(error) => error,
            Ok(_) => panic!("bind and cleanup unexpectedly succeeded"),
        };
        let SdkError::Internal(message) = error else {
            panic!("unexpected error: {error}");
        };
        assert!(message.contains("A") && message.contains("B") && message.contains("C"));
        assert_eq!(
            log.snapshot(),
            ["bind:A", "bind:B", "bind:C", "close:B", "close:A"]
        );
        assert_build_state_removed(&client);
        client.close().await;
    }
}

mod fresh {
    use super::*;

    #[tokio::test]
    async fn successful_ack_transfers_the_owner_and_exact_routes() {
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack]);
        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
        let request = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::rpc::METHOD_SESSION_START_THREAD
        );
        let session_id = request["params"]["session_id"].as_str().unwrap().to_owned();
        let binding_id = request["params"]["tools"][0]["binding_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(request["params"]["tools"][0]["name"], "a");
        reply_result(
            &client,
            &request,
            json!({"thread_id": session_id, "created_at": "fixture"}),
        );
        let thread = creation.await.unwrap().unwrap();

        assert_eq!(client.inner.state.attached_pack_count(thread.id()), 1);
        assert!(client
            .inner
            .state
            .tools
            .contains_key(&(thread.id().into(), "a".into())));
        assert!(client
            .inner
            .state
            .tool_bindings
            .contains_key(&(thread.id().into(), binding_id)));
        client.close().await;
    }

    #[tokio::test]
    async fn canceled_waiter_after_start_ack_closes_remote_and_pack_normally() {
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack.clone()]);
        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;
        let start = crate::initialization_tests::next(&mut receiver).await;
        let session_id = start["params"]["session_id"].as_str().unwrap().to_owned();
        creation.abort();
        assert!(matches!(creation.await, Err(error) if error.is_cancelled()));
        reply_result(
            &client,
            &start,
            json!({"thread_id": session_id, "created_at": "fixture"}),
        );

        let close = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(
            close["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        assert_eq!(close["params"]["thread_id"], session_id);
        reply_result(
            &client,
            &close,
            json!({"thread_id": session_id, "closed": true}),
        );
        log.wait_for("close:A").await;

        assert_eq!(pack.close_count.load(Ordering::SeqCst), 1);
        assert_eq!(pack.emergency_count.load(Ordering::SeqCst), 0);
        assert!(client.inner.state.tools.is_empty());
        assert!(client.inner.state.tool_bindings.is_empty());
        assert_eq!(client.inner.state.attached_pack_count(&session_id), 0);
        client.close().await;
    }

    #[tokio::test]
    async fn canceled_waiter_after_bind_rolls_back_before_session_rpc() {
        let log = Arc::new(EventLog::default());
        let gate = BindGate::new(false);
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()).gated(gate.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack.clone()]);
        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;

        tokio::select! {
            result = gate.wait_entered() => result,
            frame = receiver.recv() => panic!("Session RPC preceded ToolPack bind: {frame:?}"),
        }
        let context = pack.contexts.lock().unwrap()[0].clone();
        assert!(matches!(
            client
                .inner
                .state
                .with_existing_session_open(context.session_id(), || ()),
            Err(SdkError::SessionClosed(_))
        ));
        creation.abort();
        assert!(matches!(creation.await, Err(error) if error.is_cancelled()));
        gate.release();
        log.wait_for("close:A").await;

        assert!(receiver.try_recv().is_err());
        assert_eq!(pack.close_count.load(Ordering::SeqCst), 1);
        assert_eq!(pack.emergency_count.load(Ordering::SeqCst), 0);
        assert!(context.session_cancelled().is_cancelled());
        assert_build_state_removed(&client);
        client.close().await;
    }

    #[tokio::test]
    async fn connection_shutdown_drops_current_bind_and_emergency_rolls_back_prior_pack() {
        let log = Arc::new(EventLog::default());
        let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let gate = BindGate::new(true);
        let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()).gated(gate.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a", "b"], vec![a.clone(), b]);
        let creation = spawn_initialized_creation(&client, &mut receiver, agent).await;

        tokio::select! {
            result = gate.wait_entered() => result,
            frame = receiver.recv() => panic!("Session RPC preceded all ToolPack binds: {frame:?}"),
        }
        client.close().await;
        assert!(creation.await.unwrap().is_err());
        log.wait_for("partial-drop:B").await;
        log.wait_for("emergency:A").await;

        assert_eq!(
            log.snapshot(),
            ["bind:A", "bind:B", "partial-drop:B", "emergency:A"]
        );
        assert_eq!(a.close_count.load(Ordering::SeqCst), 0);
        assert_eq!(a.emergency_count.load(Ordering::SeqCst), 1);
        assert_build_state_removed(&client);
    }
}

mod persistent {
    use super::*;
    use whale_protocol::recovery::{
        RecoveryKey, CAPABILITY_SESSION_RECOVERY, METHOD_RECOVERY_ATTACH, METHOD_RECOVERY_INSPECT,
        METHOD_SESSION_CREATE_PERSISTENT,
    };

    async fn spawn_initialized_persistent(
        client: &WhaleClient,
        receiver: &mut mpsc::Receiver<String>,
        agent: crate::Agent,
        key: RecoveryKey,
    ) -> tokio::task::JoinHandle<Result<crate::WhaleThread, SdkError>> {
        let creation = tokio::spawn(async move { agent.create_persistent_session(&key).await });
        let request = crate::initialization_tests::next(receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::initialization::METHOD_INITIALIZE
        );
        let mut reply = crate::initialization_tests::valid_reply(&request);
        reply["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!(CAPABILITY_SESSION_RECOVERY));
        reply_result(client, &request, reply);
        creation
    }

    fn accepted(request: &Value) -> Value {
        json!({
            "thread": {
                "thread_id": request["params"]["session"]["session_id"],
                "created_at": "fixture"
            },
            "key": request["params"]["key"],
            "epoch": 1
        })
    }

    fn snapshot(key: &RecoveryKey, revision: u64) -> Value {
        json!({
            "recovery_id": key.recovery_id,
            "revision": revision,
            "epoch": 1,
            "attached": false,
            "configuration": {},
            "history": [],
            "runs": [],
            "unknown_executions": []
        })
    }

    async fn close_session(
        client: &WhaleClient,
        receiver: &mut mpsc::Receiver<String>,
        thread: crate::WhaleThread,
    ) {
        let sid = thread.id().to_owned();
        let closing = tokio::spawn(async move { thread.close().await });
        let request = crate::initialization_tests::next(receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        reply_result(client, &request, json!({"thread_id": sid, "closed": true}));
        assert!(closing.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn persistent_manifest_mismatch_rolls_back_before_rpc() {
        for attach in [false, true] {
            let log = Arc::new(EventLog::default());
            let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
            let b =
                Arc::new(TransactionProbePack::new("B", "b", log.clone()).mismatched_description());
            let (client, mut receiver) = transaction_client();
            let agent = transaction_agent(&client, &["a", "b"], vec![a, b]);
            let key = RecoveryKey::new();
            let owned_key = key.clone();
            let creation = tokio::spawn(async move {
                if attach {
                    agent.recover_session(&owned_key).await
                } else {
                    agent.create_persistent_session(&owned_key).await
                }
            });
            let initialize = crate::initialization_tests::next(&mut receiver).await;
            let mut reply = crate::initialization_tests::valid_reply(&initialize);
            reply["capabilities"]
                .as_array_mut()
                .unwrap()
                .push(json!(CAPABILITY_SESSION_RECOVERY));
            reply_result(&client, &initialize, reply);
            if attach {
                let inspect = crate::initialization_tests::next(&mut receiver).await;
                assert_eq!(inspect["method"], METHOD_RECOVERY_INSPECT);
                reply_result(&client, &inspect, snapshot(&key, 2));
            }

            assert!(matches!(
                finish_without_session_rpc(creation, &mut receiver).await,
                Err(SdkError::InvalidConfiguration(message)) if message.contains("B")
            ));
            assert_eq!(log.snapshot(), ["bind:A", "bind:B", "close:B", "close:A"]);
            assert_build_state_removed(&client);
            client.close().await;
        }
    }

    #[tokio::test]
    async fn persistent_requests_use_frozen_metadata_and_fresh_binding_ids() {
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack.clone()]);
        pack.replace_manifest_tool(ToolPackTool {
            name: "a".into(),
            description: "mutated after Agent construction".into(),
            parameters: json!({"type": "object", "properties": {"changed": {"type": "boolean"}}}),
            supports_parallel: false,
            require_approval: true,
        });
        let key = RecoveryKey::new();

        let creation =
            spawn_initialized_persistent(&client, &mut receiver, agent.clone(), key.clone()).await;
        let request = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(request["method"], METHOD_SESSION_CREATE_PERSISTENT);
        let first_tool = request["params"]["session"]["tools"][0].clone();
        assert_eq!(first_tool["name"], "a");
        assert_eq!(first_tool["description"], "a description");
        assert_eq!(first_tool["supports_parallel"], true);
        assert_eq!(first_tool["require_approval"], false);
        let first_binding = first_tool["binding_id"].as_str().unwrap().to_owned();
        reply_result(&client, &request, accepted(&request));
        let first = creation.await.unwrap().unwrap();

        let registration = tokio::spawn({
            let first = first.clone();
            async move { first.register_tool(Arc::new(StaticTool("dynamic"))).await }
        });
        let request = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::rpc::METHOD_SESSION_REGISTER_TOOLS
        );
        reply_result(&client, &request, json!({"registered_count": 1}));
        registration.await.unwrap().unwrap();
        close_session(&client, &mut receiver, first).await;

        let recovering = tokio::spawn({
            let agent = agent.clone();
            let key = key.clone();
            async move { agent.recover_session(&key).await }
        });
        let inspect = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(inspect["method"], METHOD_RECOVERY_INSPECT);
        reply_result(&client, &inspect, snapshot(&key, 2));
        let attach = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(attach["method"], METHOD_RECOVERY_ATTACH);
        assert_eq!(
            attach["params"]["session"]["tools"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let attached_tool = attach["params"]["session"]["tools"][0].clone();
        assert_eq!(attached_tool["name"], "a");
        assert_eq!(attached_tool["description"], "a description");
        assert_eq!(attached_tool["parameters"], json!({"type": "object"}));
        assert_eq!(attached_tool["supports_parallel"], true);
        assert_eq!(attached_tool["require_approval"], false);
        assert_ne!(attached_tool["binding_id"].as_str().unwrap(), first_binding);
        reply_result(&client, &attach, accepted(&attach));
        let attached = recovering.await.unwrap().unwrap();
        close_session(&client, &mut receiver, attached).await;

        assert_eq!(pack.bind_count(), 2);
        assert_eq!(pack.normal_close_count(), 2);
        assert_eq!(pack.emergency_close_count(), 0);
        client.close().await;
    }
}

mod lifecycle {
    use super::*;

    async fn accepted_session(
        client: &WhaleClient,
        receiver: &mut mpsc::Receiver<String>,
        agent: crate::Agent,
    ) -> crate::WhaleThread {
        let creation = spawn_initialized_creation(client, receiver, agent).await;
        let request = crate::initialization_tests::next(receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::rpc::METHOD_SESSION_START_THREAD
        );
        let session_id = request["params"]["session_id"].as_str().unwrap().to_owned();
        reply_result(
            client,
            &request,
            json!({"thread_id": session_id, "created_at": "fixture"}),
        );
        creation.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn pack_owned_name_is_rejected_before_registration_state_or_rpc() {
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack]);
        let thread = accepted_session(&client, &mut receiver, agent).await;
        let initial_tools = client.inner.state.tools.len();
        let initial_bindings = client.inner.state.tool_bindings.len();

        let mut collision = Box::pin(thread.register_tool(Arc::new(StaticTool("a"))));
        let error = tokio::select! {
            result = &mut collision => result.unwrap_err(),
            frame = receiver.recv() => panic!("pack-name collision sent an RPC: {frame:?}"),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("pack-name collision stalled"),
        };
        assert!(matches!(error, SdkError::InvalidConfiguration(message) if message.contains("a")));
        assert!(client.inner.state.registration_locks.is_empty());
        assert_eq!(client.inner.state.tools.len(), initial_tools);
        assert_eq!(client.inner.state.tool_bindings.len(), initial_bindings);

        let mut unrelated = Box::pin(thread.register_tool(Arc::new(StaticTool("dynamic"))));
        let request = tokio::select! {
            frame = receiver.recv() => serde_json::from_str::<Value>(&frame.unwrap()).unwrap(),
            result = &mut unrelated => panic!("unrelated registration returned before its ACK: {result:?}"),
        };
        assert_eq!(
            request["method"],
            whale_protocol::rpc::METHOD_SESSION_REGISTER_TOOLS
        );
        assert_eq!(request["params"]["tools"][0]["name"], "dynamic");
        reply_result(&client, &request, json!({"registered_count": 1}));
        unrelated.await.unwrap();
        assert!(client
            .inner
            .state
            .tools
            .contains_key(&(thread.id().into(), "dynamic".into())));
        client.close().await;
    }

    #[tokio::test]
    async fn connection_close_emergency_drains_attached_packs_in_reverse() {
        let log = Arc::new(EventLog::default());
        let a = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let b = Arc::new(TransactionProbePack::new("B", "b", log.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a", "b"], vec![a.clone(), b.clone()]);
        let _thread = accepted_session(&client, &mut receiver, agent).await;

        client.close().await;

        assert_eq!(
            log.snapshot(),
            ["bind:A", "bind:B", "emergency:B", "emergency:A"]
        );
        for pack in [a, b] {
            assert_eq!(pack.close_count.load(Ordering::SeqCst), 0);
            assert_eq!(pack.emergency_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn disconnect_wins_close_race_with_one_emergency_path() {
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack.clone()]);
        let thread = accepted_session(&client, &mut receiver, agent).await;
        let closer = thread.clone();
        let closing = tokio::spawn(async move { closer.close().await });
        let request = crate::initialization_tests::next(&mut receiver).await;
        assert_eq!(
            request["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );

        client.close().await;
        assert!(closing.await.unwrap().is_err());

        assert_eq!(log.snapshot(), ["bind:A", "emergency:A"]);
        assert_eq!(pack.close_count.load(Ordering::SeqCst), 0);
        assert_eq!(pack.emergency_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn normal_close_wins_disconnect_race_without_emergency_retry() {
        let log = Arc::new(EventLog::default());
        let gate = BindGate::new(false);
        let pack =
            Arc::new(TransactionProbePack::new("A", "a", log.clone()).close_gated(gate.clone()));
        let (client, mut receiver) = transaction_client();
        let agent = transaction_agent(&client, &["a"], vec![pack.clone()]);
        let thread = accepted_session(&client, &mut receiver, agent).await;
        let closer = thread.clone();
        let closing = tokio::spawn(async move { closer.close().await });
        let request = crate::initialization_tests::next(&mut receiver).await;
        reply_result(
            &client,
            &request,
            json!({"thread_id": thread.id(), "closed": true}),
        );
        gate.wait_entered().await;

        client.close().await;
        gate.release();
        assert!(closing.await.unwrap().unwrap());

        assert_eq!(log.snapshot(), ["bind:A", "close:A"]);
        assert_eq!(pack.close_count.load(Ordering::SeqCst), 1);
        assert_eq!(pack.emergency_count.load(Ordering::SeqCst), 0);
    }

    struct EmergencyProbe {
        id: &'static str,
        panic: bool,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BoundToolPack for EmergencyProbe {
        fn tools(&self) -> Vec<Arc<dyn HostTool>> {
            Vec::new()
        }

        async fn close(&mut self) -> Result<(), ToolPackError> {
            panic!("normal close must not run")
        }

        fn emergency_close(&mut self) {
            self.events
                .lock()
                .unwrap()
                .push(format!("emergency:{}", self.id));
            assert!(!self.panic, "{} emergency panic", self.id);
        }
    }

    #[test]
    fn owner_drop_is_runtime_independent_reverse_and_panic_resilient() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut owner = crate::tool_packs::SessionPackOwner::default();
        for (id, panic) in [("A", false), ("B", false), ("C", true)] {
            owner.push(crate::tool_packs::BoundPackLease::new(
                id.into(),
                Box::new(EmergencyProbe {
                    id,
                    panic,
                    events: events.clone(),
                }),
            ));
        }

        std::thread::spawn(move || drop(owner)).join().unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["emergency:C", "emergency:B", "emergency:A"]
        );
    }
}
