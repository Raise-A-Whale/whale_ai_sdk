//! Server implementation for whale-daemon JSON-RPC 2.0 protocol and Reverse RPC bridge.

use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};
use uuid::Uuid;

use whale_adapters::SamplingOptions;
use whale_core::{
    AgentEngine, ApprovalGate, ThreadSession, ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::canonical::CanonicalToolOutput;
use whale_protocol::events::AgentStreamEvent;
use whale_protocol::rpc::{
    ApprovalDecision, ApprovalResolveParams, ApprovalResolveResult, JSONRPCError, JSONRPCMessage,
    JSONRPCNotification, JSONRPCRequest, JSONRPCResponse, RegisterToolDefinition,
    RegisterToolsParams, RegisterToolsResult, RequestId, RunTurnParams, RunTurnResult,
    StartThreadParams, StartThreadResult, StreamEventsParams, ToolExecuteHostParams,
    ToolExecuteHostResult, TurnStatus, METHOD_APPROVAL_RESOLVE, METHOD_SESSION_REGISTER_TOOLS,
    METHOD_SESSION_START_THREAD, METHOD_THREAD_RUN_TURN, METHOD_TOOL_EXECUTE_HOST,
    METHOD_TOOL_EXECUTE_HOST_RESULT, METHOD_TURN_STREAM_EVENTS,
};

use crate::context_bridge::{send_cancel, HostContextPolicy, PendingContexts};
use std::collections::HashMap;
use tokio::sync::{watch, Notify};
use whale_core::context::{ContextPolicy, FullHistoryContext, RecentTurnsContext};
use whale_core::execution::{compile_tool_schema, CancellationToken, ToolContext};
use whale_protocol::contexts::*;
use whale_protocol::runs::*;

use crate::transport::{AnyTransportWriter, OutgoingTransport};
#[path = "session_lifecycle.rs"]
mod session_lifecycle;
use session_lifecycle::SessionLifecycle;
#[path = "initialization.rs"]
mod initialization;
use initialization::Initializations;
#[path = "recovery.rs"]
mod recovery;
#[path = "session_views.rs"]
mod session_views;
use session_views::SessionViewRegistry;
#[path = "session_management.rs"]
mod session_management;
use session_management::{RunPublicationMutation, SessionManagementRegistry};
#[path = "interactions.rs"]
mod interactions;
use interactions::{DaemonInteractionBridge, InteractionRegistry};
#[path = "session_catalog.rs"]
mod session_catalog;
#[path = "session_history.rs"]
mod session_history;
use whale_protocol::sessions::METHOD_SESSION_CLOSE;

/// Bridge tool handler that invokes Host-side tools via Reverse RPC.
pub struct HostToolBridge {
    tool_name: String,
    binding_id: Option<String>,
    tool_description: String,
    parameters_schema: Value,
    supports_parallel: bool,
    require_approval: bool,
    transport: AnyTransportWriter,
    thread_id: Option<String>,
    active_invocations: Arc<DashMap<String, ToolContext>>,
    pending_host_tool_calls:
        Arc<DashMap<String, oneshot::Sender<Result<(CanonicalToolOutput, bool), String>>>>,
}

impl HostToolBridge {
    pub fn new(
        def: RegisterToolDefinition,
        transport: AnyTransportWriter,
        pending_host_tool_calls: Arc<
            DashMap<String, oneshot::Sender<Result<(CanonicalToolOutput, bool), String>>>,
        >,
    ) -> Self {
        Self {
            tool_name: def.name,
            binding_id: def.binding_id,
            tool_description: def.description,
            parameters_schema: def.parameters,
            supports_parallel: def.supports_parallel,
            require_approval: def.require_approval,
            transport,
            thread_id: None,
            active_invocations: Arc::new(DashMap::new()),
            pending_host_tool_calls,
        }
    }
}

#[async_trait]
impl ToolHandler for HostToolBridge {
    fn name(&self) -> &str {
        &self.tool_name
    }

    fn description(&self) -> &str {
        &self.tool_description
    }

    fn parameters(&self) -> Value {
        self.parameters_schema.clone()
    }

    fn supports_parallel(&self) -> bool {
        self.supports_parallel
    }

    fn require_approval(&self) -> bool {
        self.require_approval
    }

    async fn execute(&self, arguments: Value) -> Result<CanonicalToolOutput, String> {
        self.execute_with_status(arguments)
            .await
            .map(|(output, _)| output)
    }

    async fn execute_with_status(
        &self,
        arguments: Value,
    ) -> Result<(CanonicalToolOutput, bool), String> {
        self.execute_host(None, arguments).await
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<(CanonicalToolOutput, bool), String> {
        self.execute_host(Some(context), arguments).await
    }
}
impl HostToolBridge {
    async fn execute_host(
        &self,
        context: Option<ToolContext>,
        arguments: Value,
    ) -> Result<(CanonicalToolOutput, bool), String> {
        if context.as_ref().is_some_and(|context| !context.is_active()) {
            return Err("Host invocation cancelled before dispatch".into());
        }
        let call_id = format!("{}:call_{}", self.transport.connection_id(), Uuid::new_v4());
        let (tx, rx) = oneshot::channel();
        self.pending_host_tool_calls.insert(call_id.clone(), tx);
        if let Some(context) = &context {
            self.active_invocations
                .insert(call_id.clone(), context.clone());
        }
        let _cleanup = HostCallCleanup {
            pending: self.pending_host_tool_calls.clone(),
            id: call_id.clone(),
            active: self.active_invocations.clone(),
            transport: self.transport.clone(),
        };

        let params = ToolExecuteHostParams {
            binding_id: self.binding_id.clone(),
            thread_id: self.thread_id.clone(),
            call_id: call_id.clone(),
            namespace: None,
            name: self.tool_name.clone(),
            arguments,
            context: context.as_ref().map(|context| context.info.clone()),
        };

        let req = match JSONRPCRequest::new(
            format!("reverse_{}", call_id),
            METHOD_TOOL_EXECUTE_HOST,
            Some(params),
        ) {
            Ok(r) => r,
            Err(e) => {
                self.pending_host_tool_calls.remove(&call_id);
                return Err(format!("Failed to construct host tool request: {}", e));
            }
        };

        let req_json = match serde_json::to_string(&req) {
            Ok(s) => s,
            Err(e) => {
                self.pending_host_tool_calls.remove(&call_id);
                return Err(format!("Failed to serialize host tool request: {}", e));
            }
        };

        if let Err(e) = self.transport.send_line(&req_json).await {
            self.pending_host_tool_calls.remove(&call_id);
            return Err(format!("Failed to send reverse RPC tool request: {}", e));
        }

        match rx.await {
            Ok(output) => output,
            Err(_) => {
                self.pending_host_tool_calls.remove(&call_id);
                Err(
                    "Host client disconnected or dropped reverse tool execution channel"
                        .to_string(),
                )
            }
        }
    }
}

/// JSON-RPC 2.0 daemon server managing sessions and Reverse RPC.
#[derive(Clone)]
pub struct DaemonServer {
    store: Option<Arc<whale_store::StoreRuntime>>,
    persistent_sessions: Arc<DashMap<String, whale_store::SessionJournal>>,
    initializations: Arc<Initializations>,
    provider_registry: Arc<whale_core::provider::ProviderRegistry>,
    sessions: Arc<DashMap<String, Arc<Mutex<ThreadSession>>>>,
    session_views: SessionViewRegistry,
    session_management: SessionManagementRegistry,
    interactions: InteractionRegistry,
    lifecycle: Arc<SessionLifecycle>,
    runs: Arc<Mutex<retention::RunRegistry>>,
    retention: Arc<retention::Retention>,
    engine: Arc<AgentEngine>,
    approval_gate: Arc<ApprovalGate>,
    pending_host_tool_calls:
        Arc<DashMap<String, oneshot::Sender<Result<(CanonicalToolOutput, bool), String>>>>,
    active_invocations: Arc<DashMap<String, ToolContext>>,
    pending_contexts: PendingContexts,
}

impl DaemonServer {
    /// Creates a new DaemonServer instance.
    pub fn new(engine: Arc<AgentEngine>, approval_gate: Arc<ApprovalGate>) -> Self {
        let interactions = InteractionRegistry::default();
        let runs = Arc::new(Mutex::new(retention::RunRegistry::default()));
        let session_views = SessionViewRegistry::default();
        let session_management = SessionManagementRegistry::default();
        approval_gate.install_interaction_bridge(Arc::new(DaemonInteractionBridge::new(
            interactions.clone(),
            Arc::downgrade(&runs),
            session_views.clone(),
            session_management.clone(),
        )));
        Self {
            store: None,
            persistent_sessions: Arc::new(DashMap::new()),
            initializations: Arc::new(Initializations::default()),
            provider_registry: Arc::new(whale_core::provider::ProviderRegistry::new()),
            lifecycle: Arc::new(SessionLifecycle::default()),
            runs,
            retention: Arc::new(retention::Retention::default()),
            sessions: Arc::new(DashMap::new()),
            session_views,
            session_management,
            interactions,
            engine,
            approval_gate,
            pending_host_tool_calls: Arc::new(DashMap::new()),
            active_invocations: Arc::new(DashMap::new()),
            pending_contexts: Arc::new(DashMap::new()),
        }
    }

    /// Selects providers registered by the application at daemon startup.
    /// Registry contents are immutable after sharing; sessions retain their selected instance.
    pub fn with_provider_registry(
        mut self,
        registry: Arc<whale_core::provider::ProviderRegistry>,
    ) -> Self {
        self.provider_registry = registry;
        self
    }

    fn select_provider(
        &self,
        params: &whale_protocol::models::InspectProviderParams,
    ) -> Result<
        (
            Arc<dyn whale_core::model::ModelProvider>,
            Option<Arc<dyn whale_adapters::ProtocolAdapter>>,
        ),
        String,
    > {
        params.validate()?;
        match params.provider_ref.as_deref() {
            Some(reference) => self
                .provider_registry
                .resolve(reference, &params.model)
                .map(|provider| (provider, None))
                .map_err(|error| error.to_string()),
            None => {
                let adapter = whale_core::provider::build_adapter(
                    &params.model,
                    params.provider.as_deref(),
                    params.provider_config.as_ref(),
                )
                .map_err(|error| error.to_string())?;
                Ok((
                    Arc::new(whale_core::http_provider::HttpModelProvider::new(
                        adapter.clone(),
                    )),
                    Some(adapter),
                ))
            }
        }
    }

    async fn handle_inspect_provider(
        &self,
        id: RequestId,
        params: Option<Value>,
    ) -> JSONRPCResponse {
        let params: whale_protocol::models::InspectProviderParams =
            match serde_json::from_value(params.unwrap_or(Value::Null)) {
                Ok(params) => params,
                Err(error) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(error.to_string()),
                    )
                }
            };
        let (provider, _) = match self.select_provider(&params) {
            Ok(provider) => provider,
            Err(error) => return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
        };
        let capabilities = match provider.capabilities(&params.model) {
            Ok(capabilities) => capabilities,
            Err(error) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error.to_string()))
            }
        };
        JSONRPCResponse::success(
            id,
            whale_protocol::models::InspectProviderResult {
                model: params.model,
                provider_ref: params.provider_ref,
                capabilities,
            },
        )
        .unwrap()
    }

    /// Creates a default DaemonServer with initialized ToolRegistry and Coordinator.
    pub fn default_server() -> Self {
        let registry = Arc::new(ToolRegistry::new());
        let approval_gate = Arc::new(ApprovalGate::new());
        let coordinator = Arc::new(ToolExecutionCoordinator::new(
            registry,
            Arc::clone(&approval_gate),
        ));
        let engine = Arc::new(AgentEngine::new(coordinator));
        Self::new(engine, approval_gate)
    }

    /// Returns the active sessions map.
    pub fn sessions(&self) -> &Arc<DashMap<String, Arc<Mutex<ThreadSession>>>> {
        &self.sessions
    }

    /// Returns the engine reference.
    pub fn engine(&self) -> &Arc<AgentEngine> {
        &self.engine
    }

    /// Returns the approval gate reference.
    pub fn approval_gate(&self) -> &Arc<ApprovalGate> {
        &self.approval_gate
    }

    /// Returns the pending reverse RPC tool calls map.
    pub fn pending_host_tool_calls(
        &self,
    ) -> &Arc<DashMap<String, oneshot::Sender<Result<(CanonicalToolOutput, bool), String>>>> {
        &self.pending_host_tool_calls
    }

    /// Runs the server on an incoming lines stream and outgoing transport.
    pub async fn run<R, T>(&self, mut lines_reader: R, transport: T) -> Result<(), std::io::Error>
    where
        R: StreamExt<Item = Result<String, std::io::Error>> + Unpin,
        T: OutgoingTransport + 'static,
    {
        self.retention.start(
            &self.runs,
            &self.session_views,
            &self.session_management,
            &self.interactions,
            self.store.as_ref(),
        );
        let transport_writer = AnyTransportWriter::new(Arc::new(transport));

        while let Some(line_res) = lines_reader.next().await {
            let line = match line_res {
                Ok(l) => l,
                Err(e) => {
                    warn!("Error reading line from transport: {}", e);
                    break;
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            debug!("Received JSON-RPC frame ({} bytes)", trimmed.len());
            let s_self = self.clone();
            let tw = transport_writer.clone();
            let line_owned = trimmed.to_string();
            tokio::spawn(async move {
                s_self.handle_message(&line_owned, &tw).await;
            });
        }

        self.disconnect_connection(&transport_writer).await;
        info!("Connection loop finished; reader reached EOF");
        Ok(())
    }

    /// Dispatches a single incoming raw JSON string message.
    pub async fn handle_message(&self, raw_json: &str, transport: &AnyTransportWriter) {
        self.retention.start(
            &self.runs,
            &self.session_views,
            &self.session_management,
            &self.interactions,
            self.store.as_ref(),
        );
        let msg: JSONRPCMessage = match serde_json::from_str(raw_json) {
            Ok(m) => m,
            Err(e) => {
                let err_resp = JSONRPCResponse::error(
                    0,
                    JSONRPCError::new(
                        JSONRPCError::PARSE_ERROR,
                        format!("Parse error: {}", e),
                        None,
                    ),
                );
                if let Ok(s) = serde_json::to_string(&err_resp) {
                    let _ = transport.send_line(&s).await;
                }
                return;
            }
        };

        match msg {
            JSONRPCMessage::Request(req) => {
                let starts_run = req.method == METHOD_THREAD_START_TURN;
                let persistent_start = (starts_run
                    || req.method == METHOD_THREAD_RUN_TURN
                    || req.method == METHOD_SESSION_REGISTER_TOOLS)
                    && req
                        .params
                        .as_ref()
                        .and_then(|p| p.get("thread_id"))
                        .and_then(Value::as_str)
                        .is_some_and(|thread| self.persistent_sessions.contains_key(thread));
                let mut recovery_reply = (recovery::is_recovery_method(&req.method)
                    || persistent_start)
                    .then(|| recovery::RecoveryReplyGuard::new(self.clone(), transport.clone()));
                let (resp, initialization) =
                    if req.method == whale_protocol::initialization::METHOD_INITIALIZE {
                        self.initializations.begin(
                            req,
                            transport.connection_id(),
                            self.store.is_some(),
                            self.retention.policy.is_enabled(),
                        )
                    } else {
                        (self.dispatch_request(req, transport).await, None)
                    };
                let acceptance = if starts_run {
                    let key = resp.result.as_ref().and_then(|result| {
                        Some((
                            result.get("thread_id")?.as_str()?.to_owned(),
                            result.get("turn_id")?.as_str()?.to_owned(),
                        ))
                    });
                    match key {
                        Some(key) => self.runs.lock().await.get(&key).cloned().map(|run| {
                            StartAcceptanceGuard(
                                run,
                                self.session_views.clone(),
                                self.session_management.clone(),
                            )
                        }),
                        None => None,
                    }
                } else {
                    None
                };
                let serialized = match serde_json::to_string(&resp) {
                    Ok(serialized) => serialized,
                    Err(_) => {
                        self.disconnect_connection(transport).await;
                        return;
                    }
                };
                if transport.send_line(&serialized).await.is_err() {
                    self.disconnect_connection(transport).await;
                    return;
                }
                if let Some(recovery_reply) = &mut recovery_reply {
                    recovery_reply.delivered();
                }
                if let Some(initialization) = &initialization {
                    initialization.delivered();
                }
                if let Some(acceptance) = &acceptance {
                    acceptance.delivered();
                }
            }
            JSONRPCMessage::Response(resp) => {
                self.handle_incoming_response(resp, transport).await;
            }
            JSONRPCMessage::Notification(notif) => {
                self.handle_notification(notif).await;
            }
        }
    }

    /// Handles client responses, such as answering reverse RPC `tool.execute_host`.
    async fn handle_incoming_response(
        &self,
        resp: JSONRPCResponse,
        transport: &AnyTransportWriter,
    ) {
        debug!("Received JSON-RPC Response id={:?}", resp.id);
        if let RequestId::String(id) = &resp.id {
            if id.starts_with(&format!("context_{}:", transport.connection_id())) {
                if let Some((_, sender)) = self.pending_contexts.remove(id) {
                    let result = match (resp.result, resp.error) {
                        (_, Some(error)) => Err(format!("Host context error: {}", error.message)),
                        (Some(value), None) => serde_json::from_value::<ModelContext>(value)
                            .map_err(|e| format!("Invalid host model context: {e}")),
                        _ => Err("Host returned no model context".into()),
                    };
                    let _ = sender.send(result);
                }
                return;
            }
        }
        let call_id = match &resp.id {
            RequestId::String(s) => {
                if let Some(stripped) = s.strip_prefix("reverse_") {
                    stripped.to_string()
                } else {
                    s.clone()
                }
            }
            RequestId::Number(n) => n.to_string(),
        };

        if !call_id.starts_with(&format!("{}:", transport.connection_id())) {
            return;
        }
        if let Some((_, sender)) = self.pending_host_tool_calls.remove(&call_id) {
            if let Some((_, context)) = self.active_invocations.remove(&call_id) {
                context.finish();
            }
            if let Some(result_val) = resp.result {
                if let Ok(host_res) =
                    serde_json::from_value::<ToolExecuteHostResult>(result_val.clone())
                {
                    let _ = sender.send(Ok((host_res.output, host_res.is_error)));
                    return;
                } else if let Ok(out) = serde_json::from_value::<CanonicalToolOutput>(result_val) {
                    let _ = sender.send(Ok((out, false)));
                    return;
                }
            }
            if let Some(err) = resp.error {
                let _ = sender.send(Err(format!("Host error: {}", err.message)));
            } else {
                let _ = sender.send(Err("Host returned invalid output".into()));
            }
        }
    }

    async fn handle_notification(&self, notif: JSONRPCNotification) {
        debug!("Received JSON-RPC notification method={}", notif.method);
    }

    /// Dispatches a business request after connection initialization.
    /// Send `protocol.initialize` through `handle_message`: readiness requires
    /// acknowledgement delivery and cannot be established by a response value.
    pub async fn dispatch_request(
        &self,
        req: JSONRPCRequest,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let id = req.id.clone();
        if let Err(error) = self.initializations.ready(transport.connection_id()).await {
            return JSONRPCResponse::error(id, error);
        }
        if self.lifecycle.connection_closed(transport.connection_id()) {
            return JSONRPCResponse::error(id, JSONRPCError::internal_error("ConnectionClosed"));
        }
        if recovery::is_recovery_method(&req.method) {
            return self.handle_recovery(req, transport).await;
        }
        let owner_scoped_method = matches!(
            req.method.as_str(),
            whale_protocol::session_management::METHOD_SESSION_GET_V2
                | whale_protocol::session_management::METHOD_SESSION_SUBSCRIBE_V2
                | whale_protocol::session_management::METHOD_SESSION_METADATA_REPLACE
                | whale_protocol::session_management::METHOD_SESSION_LIST
                | whale_protocol::session_management::METHOD_SESSION_HISTORY
                | whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_GET
                | whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_SUBSCRIBE
                | whale_protocol::interactions::METHOD_TURN_INTERACTIONS_GET
                | whale_protocol::interactions::METHOD_TURN_REQUEST_INTERACTION
                | whale_protocol::interactions::METHOD_TURN_RESPOND_INTERACTION
        );
        if !owner_scoped_method {
            if let Some(thread_id) = req
                .params
                .as_ref()
                .and_then(|p| p.get("thread_id"))
                .and_then(Value::as_str)
            {
                if self
                    .lifecycle
                    .is_foreign(thread_id, transport.connection_id())
                {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params("SessionNotFound"),
                    );
                }
            }
        }
        match req.method.as_str() {
            whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_GET
            | whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_SUBSCRIBE
            | whale_protocol::interactions::METHOD_TURN_INTERACTIONS_GET => {
                self.handle_interaction_read(id, req.method.as_str(), req.params, transport)
                    .await
            }
            whale_protocol::interactions::METHOD_TURN_REQUEST_INTERACTION => {
                self.handle_request_interaction(id, req.params, transport)
                    .await
            }
            whale_protocol::interactions::METHOD_TURN_RESPOND_INTERACTION => {
                self.handle_respond_interaction(id, req.params, transport)
                    .await
            }
            whale_protocol::session_views::METHOD_SESSION_GET
            | whale_protocol::session_views::METHOD_SESSION_SUBSCRIBE => {
                self.handle_session_view(id, req.method.as_str(), req.params, transport)
            }
            whale_protocol::session_management::METHOD_SESSION_GET_V2
            | whale_protocol::session_management::METHOD_SESSION_SUBSCRIBE_V2 => {
                self.handle_session_management_read(id, req.method.as_str(), req.params, transport)
            }
            whale_protocol::session_management::METHOD_SESSION_METADATA_REPLACE => {
                self.handle_replace_session_metadata(id, req.params, transport)
                    .await
            }
            whale_protocol::session_management::METHOD_SESSION_LIST => {
                self.handle_session_catalog(id, req.params, transport)
            }
            whale_protocol::session_management::METHOD_SESSION_HISTORY => {
                self.handle_session_history(id, req.params, transport)
            }
            whale_protocol::models::METHOD_PROVIDER_INSPECT => {
                self.handle_inspect_provider(id, req.params).await
            }
            METHOD_SESSION_CLOSE => self.handle_close_session(id, req.params, transport).await,
            METHOD_TOOL_REPORT_PROGRESS => {
                self.handle_tool_progress(id, req.params, transport).await
            }
            METHOD_THREAD_START_TURN => self.handle_start_turn(id, req.params, transport).await,
            METHOD_TURN_GET | METHOD_TURN_CANCEL => {
                self.handle_run_ref(id, req.params, transport, req.method == METHOD_TURN_CANCEL)
                    .await
            }
            METHOD_TURN_RESOLVE_APPROVAL => {
                self.handle_run_approval(id, req.params, transport).await
            }
            METHOD_SESSION_START_THREAD => {
                self.handle_start_thread(id, req.params, transport).await
            }
            METHOD_THREAD_RUN_TURN => self.handle_run_turn(id, req.params, transport).await,
            METHOD_TOOL_EXECUTE_HOST_RESULT => {
                self.handle_tool_execute_host_result(id, req.params, transport)
                    .await
            }
            METHOD_APPROVAL_RESOLVE => {
                self.handle_approval_resolve(id, req.params, transport)
                    .await
            }
            METHOD_SESSION_REGISTER_TOOLS => {
                let server = self.clone();
                let transport = transport.clone();
                let response_id = id.clone();
                match tokio::spawn(async move {
                    server
                        .handle_register_tools(id, req.params, &transport)
                        .await
                })
                .await
                {
                    Ok(response) => response,
                    Err(_) => JSONRPCResponse::error(
                        response_id,
                        JSONRPCError::new(
                            whale_protocol::recovery::STORE_FAILED,
                            "Tool registration did not complete",
                            None,
                        ),
                    ),
                }
            }
            other => JSONRPCResponse::error(id, JSONRPCError::method_not_found(other)),
        }
    }

    async fn handle_tool_progress(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: ToolReportProgressParams =
            match serde_json::from_value(params.unwrap_or(Value::Null)) {
                Ok(params) => params,
                Err(error) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(error.to_string()),
                    )
                }
            };
        if let Err(error) = params.validate() {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }
        let context = if params
            .call_id
            .starts_with(&format!("{}:", transport.connection_id()))
        {
            self.active_invocations
                .get(&params.call_id)
                .map(|context| context.value().clone())
        } else {
            None
        };
        let accepted = if let Some(context) = context {
            context
                .report_progress(params.message, params.progress)
                .await
                .unwrap_or(false)
        } else {
            false
        };
        JSONRPCResponse::success(id, ToolReportProgressResult { accepted }).unwrap()
    }

    /// Method: "session.start_thread"
    async fn handle_start_thread(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let (params, interactions_enabled): (StartThreadParams, bool) = match params_opt {
            Some(v) if v.get("interactions_enabled").is_some() => {
                match serde_json::from_value::<
                    whale_protocol::interactions::StartThreadWithInteractionsParams,
                >(v)
                {
                    Ok(wrapper) => (wrapper.session, wrapper.interactions_enabled),
                    Err(e) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(format!("Invalid StartThreadParams: {e}")),
                        );
                    }
                }
            }
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => (p, false),
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!("Invalid StartThreadParams: {}", e)),
                    );
                }
            },
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Missing parameters for start_thread"),
                );
            }
        };

        if params
            .agent_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty() || name.trim() != name)
        {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("agent_name must be nonempty and unpadded"),
            );
        }
        if let Err(error) =
            whale_protocol::session_management::validate_session_metadata_replacement(
                &params.metadata,
            )
        {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    whale_protocol::retention::SESSION_LIMIT_EXCEEDED,
                    error,
                    None,
                ),
            );
        }
        let agent_name = params.agent_name.clone();
        let metadata = params.metadata.clone();
        let created_at_ms = session_views::unix_ms();

        let (thread_id, session) = match self.prepare_thread(id.clone(), params, transport) {
            Ok(prepared) => prepared,
            Err(error) => return error,
        };
        let max_history_bytes = session.limits().and_then(|limits| limits.max_history_bytes);
        // Session publication and close/disconnect use the same short lifecycle gate.
        if let Err(error) = self
            .lifecycle
            .publish(&thread_id, transport.connection_id(), || {
                self.session_views
                    .insert(
                        transport.connection_id(),
                        thread_id.clone(),
                        agent_name.clone(),
                        metadata.clone(),
                        Vec::new(),
                        created_at_ms,
                        transport.clone(),
                    )
                    .expect("validated fresh Session view");
                self.session_management
                    .insert(
                        transport.connection_id(),
                        thread_id.clone(),
                        agent_name.clone(),
                        metadata.clone(),
                        Vec::new(),
                        created_at_ms,
                        whale_protocol::session_management::SessionPersistenceV2::Ephemeral,
                        max_history_bytes,
                        transport.clone(),
                    )
                    .expect("validated fresh V2 Session view");
                self.interactions
                    .insert(
                        transport.connection_id(),
                        thread_id.clone(),
                        interactions_enabled,
                        transport.clone(),
                    )
                    .expect("validated fresh Interaction view");
                self.sessions
                    .insert(thread_id.clone(), Arc::new(Mutex::new(session)));
            })
        {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }

        let created_at = chrono::DateTime::from_timestamp_millis(created_at_ms as i64)
            .expect("nonnegative current timestamp")
            .to_rfc3339();
        let id_clone = id.clone();
        JSONRPCResponse::success(
            id,
            StartThreadResult {
                thread_id,
                created_at,
            },
        )
        .unwrap_or_else(|e| {
            JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string()))
        })
    }

    // Pure validation and construction: durable creation validates before claiming a lease.
    fn prepare_thread(
        &self,
        id: RequestId,
        params: StartThreadParams,
        transport: &AnyTransportWriter,
    ) -> Result<(String, ThreadSession), JSONRPCResponse> {
        let thread_id = params
            .session_id
            .unwrap_or_else(|| format!("th_{}", Uuid::new_v4()));
        let mut sampling_options = SamplingOptions::new(params.model);
        if let Some(options) = params.options.as_ref() {
            apply_options(&mut sampling_options, options);
        }
        if sampling_options.model.trim().is_empty()
            || sampling_options.max_tokens == Some(0)
            || sampling_options.temperature.is_some_and(|temperature| {
                !temperature.is_finite() || !(0.0..=2.0).contains(&temperature)
            })
        {
            return Err(JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("Model and positive sampling limits are required"),
            ));
        }
        let (provider, adapter) =
            match self.select_provider(&whale_protocol::models::InspectProviderParams {
                model: sampling_options.model.clone(),
                provider_ref: params.provider_ref.clone(),
                provider: params.provider.clone(),
                provider_config: params.provider_config.clone(),
            }) {
                Ok(provider) => provider,
                Err(error) => {
                    return Err(JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(error),
                    ))
                }
            };
        let capabilities = match provider.capabilities(&sampling_options.model) {
            Ok(capabilities) => capabilities,
            Err(error) => {
                return Err(JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params(error.to_string()),
                ))
            }
        };
        let definitions: Vec<_> = params
            .tools
            .iter()
            .map(|tool| whale_adapters::ToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.parameters.clone(),
            })
            .collect();
        if let Err(error) = whale_core::model::validate_model_configuration(
            &sampling_options.model,
            &definitions,
            &sampling_options,
            &capabilities,
        ) {
            return Err(JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params(error.to_string()),
            ));
        }

        // Built-in protocol combinations may be stricter than a capability set.
        // Pure serialization validates those options before publication, without HTTP.
        if let Some(adapter) = &adapter {
            if let Err(error) = adapter.serialize_request(
                params.system_prompt.as_deref(),
                &[],
                &definitions,
                &sampling_options,
            ) {
                return Err(JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params(error.to_string()),
                ));
            }
        }

        let policy: Arc<dyn ContextPolicy> = match params.context_policy.unwrap_or_default() {
            ContextPolicyConfig::FullHistory => Arc::new(FullHistoryContext),
            ContextPolicyConfig::RecentTurns { max_turns } => {
                match RecentTurnsContext::new(max_turns) {
                    Ok(policy) => Arc::new(policy),
                    Err(error) => {
                        return Err(JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error),
                        ))
                    }
                }
            }
            ContextPolicyConfig::Host => Arc::new(HostContextPolicy {
                transport: transport.clone(),
                pending: self.pending_contexts.clone(),
            }),
        };
        let mut names = std::collections::HashSet::new();
        for definition in &params.tools {
            if definition.name.trim().is_empty() || !names.insert(definition.name.as_str()) {
                return Err(JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Tool names must be nonempty and unique"),
                ));
            }
            if definition
                .binding_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty())
            {
                return Err(JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Tool binding_id must be nonempty when provided"),
                ));
            }
            if let Err(error) = compile_tool_schema(&definition.parameters) {
                return Err(JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params(error),
                ));
            }
        }
        let tools = Arc::new(ToolRegistry::new());

        // Register initial tools passed in params
        for tool_def in params.tools {
            if tool_def.is_host_tool {
                let mut bridge = HostToolBridge::new(
                    tool_def,
                    transport.clone(),
                    Arc::clone(&self.pending_host_tool_calls),
                );
                bridge.thread_id = Some(thread_id.clone());
                bridge.active_invocations = self.active_invocations.clone();
                tools.register(Arc::new(bridge)).expect("valid tool schema");
            }
        }

        let mut session = if let Some(adapter) = adapter {
            ThreadSession::with_id_and_prompt(
                thread_id.clone(),
                params.system_prompt,
                adapter,
                tools,
                sampling_options,
            )
        } else {
            ThreadSession::with_id_prompt_and_provider(
                thread_id.clone(),
                params.system_prompt,
                provider,
                tools,
                sampling_options,
            )
        };

        if let Err(error) = session.set_limits(params.limits) {
            return Err(JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params(error.to_string()),
            ));
        }
        session.set_agent_name(params.agent_name);
        session.set_context_policy(policy);
        Ok((thread_id, session))
    }

    /// Legacy RPC backed by the same reservation, cancellation and terminal state.
    async fn handle_run_turn(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: RunTurnParams = match serde_json::from_value(params.unwrap_or(Value::Null)) {
            Ok(params) => params,
            Err(e) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(e.to_string()))
            }
        };
        let start = StartTurnParams {
            thread_id: params.thread_id,
            turn_id: Uuid::new_v4().to_string(),
            input_items: params.input_items,
            options: params.options,
            max_steps: 10,
            timeout_ms: None,
        };
        let response = self
            .handle_start_turn(
                id.clone(),
                Some(serde_json::to_value(&start).unwrap()),
                transport,
            )
            .await;
        if response.error.is_some() {
            return response;
        }
        let reference = RunRefParams {
            thread_id: start.thread_id,
            turn_id: start.turn_id,
        };
        let run = match self.find_run(&reference, transport).await {
            Ok(run) => run,
            Err(error) => return JSONRPCResponse::error(id, error),
        };
        run.legacy.store(true, std::sync::atomic::Ordering::Relaxed);
        run.deliver_acceptance(&self.session_views, &self.session_management);
        loop {
            let notified = run.finished.notified();
            let snapshot = run.snapshot.lock().await.clone();
            if run
                .legacy_events_finished
                .load(std::sync::atomic::Ordering::Acquire)
            {
                if let Some(result) = snapshot.result {
                    return JSONRPCResponse::success(id, result).unwrap();
                }
            }
            notified.await;
        }
    }

    /// Method: "tool.execute_host_result"
    async fn handle_tool_execute_host_result(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: ToolExecuteHostResult = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!(
                            "Invalid ToolExecuteHostResult: {}",
                            e
                        )),
                    );
                }
            },
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Missing parameters for tool.execute_host_result"),
                );
            }
        };

        if !params
            .call_id
            .starts_with(&format!("{}:", transport.connection_id()))
        {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params("HostCallNotFound"));
        }
        if let Some((_, sender)) = self.pending_host_tool_calls.remove(&params.call_id) {
            if let Some((_, context)) = self.active_invocations.remove(&params.call_id) {
                context.finish();
            }
            let _ = sender.send(Ok((params.output, params.is_error)));
            let id_clone = id.clone();
            JSONRPCResponse::success(id, json!({ "acknowledged": true })).unwrap_or_else(|e| {
                JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string()))
            })
        } else {
            JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    JSONRPCError::INVALID_PARAMS,
                    format!("No pending tool call found for call_id: {}", params.call_id),
                    None,
                ),
            )
        }
    }

    /// Method: "approval.resolve"
    async fn handle_approval_resolve(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: ApprovalResolveParams = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!(
                            "Invalid ApprovalResolveParams: {}",
                            e
                        )),
                    );
                }
            },
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Missing parameters for approval.resolve"),
                );
            }
        };

        if let Some((thread_id, turn_id)) = self
            .interactions
            .find_request(transport.connection_id(), &params.request_id)
        {
            let converted = RunApprovalParams {
                thread_id,
                turn_id,
                request_id: params.request_id.clone(),
                decision: match params.decision {
                    ApprovalDecision::Approve => RunApprovalDecision::Approve,
                    ApprovalDecision::Reject => RunApprovalDecision::Reject,
                },
                arguments: None,
                feedback: params.feedback.clone(),
            };
            return self
                .handle_run_approval(
                    id,
                    Some(serde_json::to_value(converted).unwrap()),
                    transport,
                )
                .await;
        }
        let runs: Vec<_> = self.runs.lock().await.values().cloned().collect();
        for run in runs {
            let snapshot = run.snapshot.lock().await.clone();
            if snapshot
                .pending_approvals
                .iter()
                .any(|a| a.request_id == params.request_id)
            {
                let converted = RunApprovalParams {
                    thread_id: snapshot.thread_id,
                    turn_id: snapshot.turn_id,
                    request_id: params.request_id,
                    decision: match params.decision {
                        ApprovalDecision::Approve => RunApprovalDecision::Approve,
                        ApprovalDecision::Reject => RunApprovalDecision::Reject,
                    },
                    arguments: None,
                    feedback: params.feedback,
                };
                return self
                    .handle_run_approval(
                        id,
                        Some(serde_json::to_value(converted).unwrap()),
                        transport,
                    )
                    .await;
            }
        }
        JSONRPCResponse::success(
            id,
            ApprovalResolveResult {
                resolved: false,
                request_id: params.request_id,
            },
        )
        .unwrap()
    }

    /// Method: "session.register_tools"
    async fn handle_register_tools(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: RegisterToolsParams = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!("Invalid RegisterToolsParams: {}", e)),
                    );
                }
            },
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Missing parameters for register_tools"),
                );
            }
        };

        if let Err(error) = self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }
        let session_arc = match self.sessions.get(&params.thread_id) {
            Some(s) => Arc::clone(s.value()),
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::new(
                        JSONRPCError::INVALID_PARAMS,
                        format!("Session thread '{}' not found", params.thread_id),
                        None,
                    ),
                );
            }
        };

        let mut names = std::collections::HashSet::new();
        for definition in &params.tools {
            if definition.name.trim().is_empty() || !names.insert(definition.name.as_str()) {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Tool names must be nonempty and unique"),
                );
            }
            if definition
                .binding_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty())
            {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Tool binding_id must be nonempty when provided"),
                );
            }
            if let Err(error) = compile_tool_schema(&definition.parameters) {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
            }
        }
        let session_guard = session_arc.lock().await;
        let _publication = match self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            Ok(guard) => guard,
            Err(error) => return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
        };
        drop(_publication);
        if let Some(journal) = session_guard.journal() {
            let mut record = match journal.record().await {
                Ok(record) => record,
                Err(error) => return JSONRPCResponse::error(id, recovery::store_error(error)),
            };
            let definitions = record
                .configuration
                .get_mut("session")
                .and_then(|s| s.get_mut("tools"))
                .and_then(Value::as_array_mut);
            let Some(definitions) = definitions else {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::internal_error("Invalid durable configuration"),
                );
            };
            for definition in &params.tools {
                let mut definition =
                    serde_json::to_value(definition).expect("tool definition serializes");
                definition.as_object_mut().unwrap().remove("binding_id");
                definitions.retain(|old| old["name"] != definition["name"]);
                definitions.push(definition);
            }
            definitions.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            if let Err(error) = journal.replace_configuration(record.configuration).await {
                return JSONRPCResponse::error(id, recovery::store_error(error));
            }
        }
        let _publication = match self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            Ok(guard) => guard,
            Err(error) => {
                return JSONRPCResponse::error(
                    id,
                    if session_guard.journal().is_some() {
                        JSONRPCError::new(whale_protocol::recovery::STORE_FAILED, "Session closed after durable tool registration; recover its configuration", None)
                    } else {
                        JSONRPCError::invalid_params(error)
                    },
                )
            }
        };
        let mut count = 0;

        for tool_def in params.tools {
            if tool_def.is_host_tool {
                let mut bridge = HostToolBridge::new(
                    tool_def,
                    transport.clone(),
                    Arc::clone(&self.pending_host_tool_calls),
                );
                bridge.thread_id = Some(params.thread_id.clone());
                bridge.active_invocations = self.active_invocations.clone();
                session_guard
                    .tools()
                    .register(Arc::new(bridge))
                    .expect("valid tool schema");
                count += 1;
            }
        }

        let result = RegisterToolsResult {
            registered_count: count,
        };
        let id_clone = id.clone();
        JSONRPCResponse::success(id, result).unwrap_or_else(|e| {
            JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string()))
        })
    }
}

struct HostCallCleanup {
    pending: Arc<DashMap<String, oneshot::Sender<Result<(CanonicalToolOutput, bool), String>>>>,
    id: String,
    active: Arc<DashMap<String, ToolContext>>,
    transport: AnyTransportWriter,
}
impl Drop for HostCallCleanup {
    fn drop(&mut self) {
        if let Some((_, context)) = self.active.remove(&self.id) {
            context.finish();
        }
        if self.pending.remove(&self.id).is_some() {
            send_cancel(
                self.transport.clone(),
                METHOD_TOOL_CANCEL_HOST,
                json!({"call_id": self.id}),
            );
        }
    }
}

struct StartAcceptanceGuard(
    Arc<RunRecord>,
    SessionViewRegistry,
    SessionManagementRegistry,
);
impl StartAcceptanceGuard {
    fn delivered(&self) {
        self.0.deliver_acceptance(&self.1, &self.2);
    }
}
impl Drop for StartAcceptanceGuard {
    fn drop(&mut self) {
        if self.0.acceptance.send_if_modified(|state| {
            if state.is_none() {
                *state = Some(false);
                true
            } else {
                false
            }
        }) {
            self.0.cancel.send_replace(true);
            self.0.cancellation.cancel();
        }
    }
}

#[derive(Default)]
struct RunPublication {
    live: bool,
    pending: Vec<session_management::CommittedDualPublication>,
}

struct RunRecord {
    owner: String,
    params: StartTurnParams,
    snapshot: Mutex<RunSnapshot>,
    cancel: watch::Sender<bool>,
    cancellation: CancellationToken,
    context: RunContextInfo,
    acceptance: watch::Sender<Option<bool>>,
    preparation: watch::Sender<Option<Result<(), JSONRPCError>>>,
    finished: Notify,
    terminal_delivery: watch::Sender<Option<Result<(), String>>>,
    publication: Mutex<RunPublication>,
    decisions: Mutex<HashMap<String, RunApprovalParams>>,
    interactions: InteractionRegistry,
    legacy: std::sync::atomic::AtomicBool,
    legacy_events_finished: std::sync::atomic::AtomicBool,
    progress: Arc<whale_core::engine::RunProgress>,
    deadline: Option<tokio::time::Instant>,
    accepted_ordinal: u64,
}

impl RunRecord {
    async fn commit_session_event(
        &self,
        views: &SessionViewRegistry,
        management: &SessionManagementRegistry,
        mutation: RunPublicationMutation,
    ) -> Result<(), String> {
        // This gate spans cursor assignment and publication so concurrent RPC and
        // execution tasks cannot enqueue a later cursor first.
        let mut publication = self.publication.lock().await;
        let committed = management
            .publish_run(views, &self.owner, mutation, session_views::unix_ms())
            .await
            .map_err(|error| format!("{error:?}"))?;
        if publication.live {
            views.publish_committed(&self.owner, committed.v1);
            management.publish_committed(&self.owner, committed.v2);
        } else {
            publication.pending.push(committed);
        }
        Ok(())
    }

    fn deliver_acceptance(
        &self,
        views: &SessionViewRegistry,
        management: &SessionManagementRegistry,
    ) {
        let mut publication = self
            .publication
            .try_lock()
            .expect("Run projection commit finishes before start acknowledgement");
        self.acceptance.send_if_modified(|state| {
            if state.is_none() {
                publication.pending.sort_by_key(|event| event.v1.cursor.seq);
                for event in publication.pending.drain(..) {
                    views.publish_committed(&self.owner, event.v1);
                    management.publish_committed(&self.owner, event.v2);
                }
                publication.live = true;
                *state = Some(true);
                true
            } else {
                false
            }
        });
    }

    async fn prepared(&self) -> Result<(), JSONRPCError> {
        let mut ready = self.preparation.subscribe();
        loop {
            if let Some(result) = ready.borrow_and_update().clone() {
                return result;
            }
            if ready.changed().await.is_err() {
                return Err(JSONRPCError::internal_error("Run preparation ended"));
            }
        }
    }
    // Both cancellation acceptance and terminal commit hold the snapshot lock.
    // Publish daemon intent before waking Core, whose cleanup may return an error.
    fn request_cancel_locked(&self, snapshot: &mut RunSnapshot) -> bool {
        if !snapshot.status.is_terminal() {
            if snapshot.status == RunStatus::Cancelling {
                return false;
            }
            snapshot.status = RunStatus::Cancelling;
            self.interactions.clear_run(
                &self.owner,
                &self.params.thread_id,
                &self.params.turn_id,
                whale_protocol::interactions::INTERACTION_REMOVAL_CANCELLED,
            );
            self.cancel.send_replace(true);
            self.cancellation.cancel();
            true
        } else {
            false
        }
    }

    async fn request_cancel(&self) {
        let mut snapshot = self.snapshot.lock().await;
        let _ = self.request_cancel_locked(&mut snapshot);
    }
}

impl DaemonServer {
    async fn handle_start_turn(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: StartTurnParams = match serde_json::from_value(params.unwrap_or(Value::Null)) {
            Ok(params) => params,
            Err(e) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(e.to_string()))
            }
        };
        if params.turn_id.is_empty() || params.max_steps == 0 || params.timeout_ms == Some(0) {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("turn_id and positive run limits are required"),
            );
        }
        let deadline = match params.timeout_ms {
            Some(ms) => match tokio::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(ms))
            {
                Some(deadline) => Some(deadline),
                None => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params("timeout_ms is too large"),
                    )
                }
            },
            None => None,
        };
        let key = (params.thread_id.clone(), params.turn_id.clone());
        let mut runs = self.runs.lock().await;
        let _publication = match self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            Ok(guard) => guard,
            Err(error) => return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
        };
        if runs.expired(&key, transport.connection_id()) {
            return JSONRPCResponse::error(id, retention::RunRegistry::expired_error());
        }
        if let Some(run) = runs.get(&key).cloned() {
            if run.owner == transport.connection_id() && run.params == params {
                drop(_publication);
                drop(runs);
                if let Err(error) = run.prepared().await {
                    return JSONRPCResponse::error(id, error);
                }
                return JSONRPCResponse::success(
                    id,
                    StartTurnResult {
                        thread_id: params.thread_id,
                        turn_id: params.turn_id,
                    },
                )
                .unwrap();
            }
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params("RunConflict"));
        }
        let session = match self.sessions.get(&params.thread_id) {
            Some(session) => session.value().clone(),
            None => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params("SessionNotFound"))
            }
        };
        // The reservation is acquired before returning acceptance; legacy and new runs share it.
        let session = match session.try_lock_owned() {
            Ok(session) => session,
            Err(_) => {
                return JSONRPCResponse::error(id, JSONRPCError::new(-32001, "SessionBusy", None))
            }
        };
        if let Err(error) = session.check_run_admission(&params.input_items) {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    whale_protocol::retention::SESSION_LIMIT_EXCEEDED,
                    error.to_string(),
                    None,
                ),
            );
        }
        let Some(accepted_ordinal) = session.accepted_turns().checked_add(1) else {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    whale_protocol::retention::SESSION_LIMIT_EXCEEDED,
                    "Accepted turn count exhausted",
                    None,
                ),
            );
        };
        let journal = session.journal().cloned();
        let mut effective_options = session.sampling_options().clone();
        if let Some(options) = &params.options {
            apply_options(&mut effective_options, options);
        }
        let (cancel, cancel_rx) = watch::channel(false);
        let initial_snapshot = RunSnapshot {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            status: RunStatus::Running,
            items: vec![],
            usage: Default::default(),
            pending_approvals: vec![],
            tool_executions: vec![],
            last_seq: 0,
            result: None,
            error: None,
        };
        let run = Arc::new(RunRecord {
            owner: transport.connection_id().into(),
            params: params.clone(),
            snapshot: Mutex::new(initial_snapshot.clone()),
            cancel,
            cancellation: CancellationToken::new(),
            context: RunContextInfo {
                agent_name: session.agent_name().map(str::to_owned),
                thread_id: params.thread_id.clone(),
                turn_id: params.turn_id.clone(),
                deadline_unix_ms: params
                    .timeout_ms
                    .map(|ms| (chrono::Utc::now().timestamp_millis() as u64).saturating_add(ms)),
            },
            acceptance: watch::channel(None).0,
            preparation: watch::channel(if journal.is_some() {
                None
            } else {
                Some(Ok(()))
            })
            .0,
            finished: Notify::new(),
            terminal_delivery: watch::channel(None).0,
            publication: Mutex::new(RunPublication::default()),
            decisions: Mutex::new(HashMap::new()),
            interactions: self.interactions.clone(),
            legacy: std::sync::atomic::AtomicBool::new(false),
            legacy_events_finished: std::sync::atomic::AtomicBool::new(false),
            progress: Arc::new(whale_core::engine::RunProgress::default()),
            deadline,
            accepted_ordinal,
        });
        if let Err(error) = self.interactions.register_run(
            transport.connection_id(),
            &params.thread_id,
            &params.turn_id,
            Arc::as_ptr(&run) as usize,
        ) {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }
        runs.insert(key.clone(), run.clone());
        drop(_publication);
        drop(runs);
        if journal.is_none() {
            if let Err(error) = run
                .commit_session_event(
                    &self.session_views,
                    &self.session_management,
                    RunPublicationMutation::Begin {
                        snapshot: initial_snapshot,
                        identity: Arc::as_ptr(&run) as usize,
                    },
                )
                .await
            {
                self.runs.lock().await.remove(&key);
                self.interactions.unregister_run(
                    transport.connection_id(),
                    &params.thread_id,
                    &params.turn_id,
                    Arc::as_ptr(&run) as usize,
                );
                return JSONRPCResponse::error(id, session_views::rpc_error(error));
            }
        }
        let server = self.clone();
        let writer = transport.clone();
        let preparing = run.clone();
        tokio::spawn(async move {
            if let Some(journal) = journal {
                let candidate = preparing.snapshot.lock().await.clone();
                let begun = journal
                    .begin_run(
                        preparing.params.clone(),
                        candidate,
                        serde_json::to_value(effective_options)
                            .expect("sampling options serialize"),
                    )
                    .await;
                if let Err(error) = begun {
                    preparing
                        .preparation
                        .send_replace(Some(Err(recovery::store_error(error))));
                    // No acceptance was delivered and no execution was started.
                    preparing.acceptance.send_replace(Some(false));
                    drop(session);
                    server.runs.lock().await.remove(&key);
                    server.interactions.unregister_run(
                        &preparing.owner,
                        &preparing.params.thread_id,
                        &preparing.params.turn_id,
                        Arc::as_ptr(&preparing) as usize,
                    );
                    preparing.terminal_delivery.send_replace(Some(Ok(())));
                    preparing.finished.notify_waiters();
                    return;
                }
                let snapshot = preparing.snapshot.lock().await.clone();
                if let Err(error) = preparing
                    .commit_session_event(
                        &server.session_views,
                        &server.session_management,
                        RunPublicationMutation::Begin {
                            snapshot,
                            identity: Arc::as_ptr(&preparing) as usize,
                        },
                    )
                    .await
                {
                    preparing
                        .preparation
                        .send_replace(Some(Err(session_views::rpc_error(error))));
                    preparing.acceptance.send_replace(Some(false));
                    drop(session);
                    server.runs.lock().await.remove(&key);
                    server.interactions.unregister_run(
                        &preparing.owner,
                        &preparing.params.thread_id,
                        &preparing.params.turn_id,
                        Arc::as_ptr(&preparing) as usize,
                    );
                    preparing.terminal_delivery.send_replace(Some(Ok(())));
                    preparing.finished.notify_waiters();
                    return;
                }
                preparing.preparation.send_replace(Some(Ok(())));
            }
            server
                .execute_run(preparing, session, writer, cancel_rx)
                .await;
        });
        if let Err(error) = run.prepared().await {
            return JSONRPCResponse::error(id, error);
        }
        JSONRPCResponse::success(
            id,
            StartTurnResult {
                thread_id: params.thread_id,
                turn_id: params.turn_id,
            },
        )
        .unwrap()
    }

    async fn find_run(
        &self,
        params: &RunRefParams,
        transport: &AnyTransportWriter,
    ) -> Result<Arc<RunRecord>, JSONRPCError> {
        let runs = self.runs.lock().await;
        let key = (params.thread_id.clone(), params.turn_id.clone());
        if runs.expired(&key, transport.connection_id()) {
            return Err(retention::RunRegistry::expired_error());
        }
        runs.get(&key)
            .filter(|run| run.owner == transport.connection_id())
            .cloned()
            .ok_or_else(|| JSONRPCError::invalid_params("RunNotFound"))
    }

    async fn handle_run_ref(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
        cancel: bool,
    ) -> JSONRPCResponse {
        let params: RunRefParams = match serde_json::from_value(params.unwrap_or(Value::Null)) {
            Ok(params) => params,
            Err(e) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(e.to_string()))
            }
        };
        let run = match self.find_run(&params, transport).await {
            Ok(run) => run,
            Err(error) => return JSONRPCResponse::error(id, error),
        };
        let mut snapshot = run.snapshot.lock().await;
        snapshot.usage = run.progress.usage();
        snapshot.tool_executions = run.progress.tool_executions();
        let changed = cancel && run.request_cancel_locked(&mut snapshot);
        let result = snapshot.clone();
        if changed {
            if let Err(error) = run
                .commit_session_event(
                    &self.session_views,
                    &self.session_management,
                    RunPublicationMutation::Changed(result.clone()),
                )
                .await
            {
                warn!(error=%error, "Failed to commit Session cancellation projection");
            }
        }
        JSONRPCResponse::success(id, result).unwrap()
    }

    async fn handle_run_approval(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: RunApprovalParams = match serde_json::from_value(params.unwrap_or(Value::Null))
        {
            Ok(params) => params,
            Err(e) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(e.to_string()))
            }
        };
        let reference = RunRefParams {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
        };
        let run = match self.find_run(&reference, transport).await {
            Ok(run) => run,
            Err(error) => return JSONRPCResponse::error(id, error),
        };
        let response = match params.decision {
            RunApprovalDecision::Approve => json!({"decision":"approve"}),
            RunApprovalDecision::Reject => {
                let mut response = serde_json::Map::new();
                response.insert("decision".into(), Value::String("reject".into()));
                if let Some(feedback) = &params.feedback {
                    response.insert("feedback".into(), Value::String(feedback.clone()));
                }
                Value::Object(response)
            }
            RunApprovalDecision::ModifyArguments => match params.arguments.clone() {
                Some(arguments) => {
                    json!({"decision":"modify_arguments","arguments":arguments})
                }
                None => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params("Modified arguments required"),
                    )
                }
            },
        };
        let interaction = whale_protocol::interactions::RespondInteractionParams::new(
            params.thread_id.clone(),
            params.turn_id.clone(),
            params.request_id.clone(),
            response,
        )
        .expect("typed approval identity was decoded");
        if let Err(error) = self
            .resolve_interaction_response(&run, &interaction, transport.connection_id(), false)
            .await
        {
            let error = match error.code {
                whale_protocol::interactions::INTERACTION_CONFLICT => {
                    JSONRPCError::invalid_params("ApprovalConflict")
                }
                whale_protocol::interactions::INTERACTION_NOT_FOUND => {
                    JSONRPCError::invalid_params("ApprovalNotFound")
                }
                _ => error.rpc(),
            };
            return JSONRPCResponse::error(id, error);
        }
        run.decisions
            .lock()
            .await
            .insert(params.request_id.clone(), params.clone());
        JSONRPCResponse::success(
            id,
            ApprovalResolveResult {
                resolved: true,
                request_id: params.request_id,
            },
        )
        .unwrap()
    }

    async fn execute_run(
        &self,
        run: Arc<RunRecord>,
        mut session: tokio::sync::OwnedMutexGuard<ThreadSession>,
        transport: AnyTransportWriter,
        mut cancel: watch::Receiver<bool>,
    ) {
        // Closing a pending run cannot overtake its acceptance response. Only
        // a delivered ACK (or a disconnected/abandoned request) resolves this gate.
        let mut acceptance = run.acceptance.subscribe();
        let accepted = loop {
            if let Some(accepted) = *acceptance.borrow_and_update() {
                break accepted;
            }
            if acceptance.changed().await.is_err() {
                break false;
            }
        };
        let history_start = session.history().len();
        let defaults = session.sampling_options().clone();
        if let Some(options) = &run.params.options {
            apply_options(session.sampling_options_mut(), options);
        }
        let (tx, mut rx) = mpsc::channel(128);
        let deadline = async {
            match run.deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => futures::future::pending::<()>().await,
            }
        };
        let outcome = if !accepted {
            Err((
                RunStatus::Cancelled,
                "CONNECTION_CLOSED",
                "Run acceptance was not delivered".to_owned(),
            ))
        } else {
            let execution = self.engine.run_turn_with_context(
                &mut session,
                run.context.clone(),
                run.cancellation.clone(),
                run.params.input_items.clone(),
                run.params.max_steps,
                tx,
                run.progress.clone(),
            );
            tokio::pin!(execution, deadline);
            loop {
                tokio::select! {
                    biased;
                    _ = async { if !*cancel.borrow() { let _ = cancel.changed().await; } } => break Err((RunStatus::Cancelled, "CANCELLED", "Run cancelled".to_owned())),
                    _ = &mut deadline => {
                        self.interactions.clear_run(
                            &run.owner,
                            &run.params.thread_id,
                            &run.params.turn_id,
                            whale_protocol::interactions::INTERACTION_REMOVAL_CANCELLED,
                        );
                        break Err((RunStatus::Failed, "DEADLINE_EXCEEDED", "Run deadline exceeded".to_owned()));
                    },
                    result = &mut execution => break result.map_err(|error| {
                        let code = match &error {
                            whale_core::CoreError::LimitExceeded(_) | whale_core::CoreError::Store(whale_store::StoreError::LimitExceeded(_)) => "SESSION_LIMIT_EXCEEDED",
                            whale_core::CoreError::Store(_) => "STORE_FAILED",
                            _ => "RUN_FAILED",
                        };
                        (RunStatus::Failed, code, error.to_string())
                    }),
                    Some(event) = rx.recv() => { self.publish_stream(&run, &transport, event).await; }
                }
            }
        }; // Dropping execution cancels model/tool futures and their pending approval/host guards.
        self.interactions.clear_run(
            &run.owner,
            &run.params.thread_id,
            &run.params.turn_id,
            whale_protocol::interactions::INTERACTION_REMOVAL_RUN_FINISHED,
        );
        run.cancellation.cancel();
        while let Ok(event) = rx.try_recv() {
            self.publish_stream(&run, &transport, event).await;
        }
        *session.sampling_options_mut() = defaults;
        // Acceptance consumes a slot even when Core never began (cancel/deadline/EOF).
        session.restore_accepted_turns(run.accepted_ordinal);
        let snapshot = {
            let mut snapshot = run.snapshot.lock().await;
            snapshot.usage = run.progress.usage();
            snapshot.tool_executions = run.progress.tool_executions();
            // This lock is also held when turn.cancel acknowledges Cancelling.
            // Execution may finish during a select poll after its cancel branch
            // was checked, so arbitrate again at terminal commit. Only daemon
            // intent is authoritative: Core also cancels its token on success.
            let outcome =
                if accepted && (snapshot.status == RunStatus::Cancelling || *run.cancel.borrow()) {
                    Err((
                        RunStatus::Cancelled,
                        "CANCELLED",
                        "Run cancelled".to_owned(),
                    ))
                } else {
                    outcome
                };
            match outcome {
                Ok(result) => {
                    snapshot.status = RunStatus::Completed;
                    snapshot.items = result.generated_items;
                    snapshot.usage = result.total_usage;
                }
                Err((status, code, message)) => {
                    snapshot.status = status;
                    snapshot.error = Some(RunFailure {
                        code: code.into(),
                        message,
                    });
                }
            }
            // History is committed before event delivery, which can be cancelled.
            snapshot.items = session.history()[history_start..].to_vec();
            snapshot.pending_approvals.clear();
            snapshot.last_seq += 1;
            snapshot.result = Some(RunTurnResult {
                thread_id: snapshot.thread_id.clone(),
                turn_id: snapshot.turn_id.clone(),
                status: match snapshot.status {
                    RunStatus::Completed => TurnStatus::Completed,
                    RunStatus::Cancelled => TurnStatus::Interrupted,
                    _ => TurnStatus::Failed,
                },
                items: snapshot.items.clone(),
                usage: snapshot.usage.clone(),
            });
            if let Some(journal) = session.journal().cloned() {
                match journal.finalize(&snapshot.turn_id, snapshot.clone()).await {
                    Ok(finalized) => {
                        session.replace_history(finalized.history);
                        *snapshot = finalized.snapshot;
                    }
                    Err(_) => {
                        // A failed commit is never observable as durable success.
                        // The poisoned journal rejects subsequent dispatch until recovery.
                        snapshot.status = RunStatus::Failed;
                        snapshot.error = Some(RunFailure { code: "STORE_FAILED".into(), message: "Session storage commit failed; close and reopen the store before recovery".into() });
                        if let Some(result) = &mut snapshot.result {
                            result.status = TurnStatus::Failed;
                        }
                    }
                }
            }
            snapshot.clone()
        };
        drop(session);
        let event = RunEvent {
            thread_id: snapshot.thread_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            seq: snapshot.last_seq,
            payload: RunEventPayload::Finished {
                snapshot: snapshot.clone(),
            },
        };
        if accepted {
            if let Err(error) = run
                .commit_session_event(
                    &self.session_views,
                    &self.session_management,
                    RunPublicationMutation::Event(event.clone()),
                )
                .await
            {
                warn!(error=%error, "Failed to commit terminal Session projection");
            }
        }
        run.finished.notify_waiters();
        if !accepted {
            run.legacy_events_finished
                .store(true, std::sync::atomic::Ordering::Release);
            run.finished.notify_waiters();
            run.terminal_delivery
                .send_replace(Some(Err("Run acceptance was not delivered".into())));
            return;
        }
        if run.legacy.load(std::sync::atomic::Ordering::Relaxed) {
            let terminal = if snapshot.status == RunStatus::Completed {
                AgentStreamEvent::TurnCompleted {
                    turn_id: snapshot.turn_id.clone(),
                    thread_id: snapshot.thread_id.clone(),
                    usage: snapshot.usage.clone(),
                }
            } else {
                let error = snapshot.error.as_ref().unwrap();
                AgentStreamEvent::TurnFailed {
                    turn_id: snapshot.turn_id.clone(),
                    thread_id: snapshot.thread_id.clone(),
                    error_code: error.code.clone(),
                    error_message: error.message.clone(),
                }
            };
            Self::send_legacy_event(&transport, &snapshot.thread_id, terminal).await;
            run.legacy_events_finished
                .store(true, std::sync::atomic::Ordering::Release);
            run.finished.notify_waiters();
        }
        let delivered = Self::send_run_event(&transport, event).await;
        if delivered {
            self.runs
                .lock()
                .await
                .delivered(&run, tokio::time::Instant::now());
        }
        run.terminal_delivery.send_replace(Some(if delivered {
            Ok(())
        } else {
            Err("Run terminal event could not be delivered".into())
        }));
    }

    async fn publish_stream(
        &self,
        run: &RunRecord,
        transport: &AnyTransportWriter,
        event: AgentStreamEvent,
    ) {
        let mut snapshot = run.snapshot.lock().await;
        snapshot.tool_executions = run.progress.tool_executions();
        match &event {
            AgentStreamEvent::TurnCompleted { usage, .. } => {
                snapshot.usage = usage.clone();
                return;
            }
            AgentStreamEvent::TurnFailed { .. } => return,
            AgentStreamEvent::ItemCompleted { item, .. } => snapshot.items.push(item.clone()),
            AgentStreamEvent::ApprovalRequested {
                request_id,
                tool_call,
                reason,
                ..
            } => {
                if snapshot.status == RunStatus::Cancelling {
                    return;
                }
                snapshot.status = RunStatus::WaitingApproval;
                snapshot.pending_approvals.push(PendingApproval {
                    request_id: request_id.clone(),
                    tool_call: tool_call.clone(),
                    reason: reason.clone(),
                });
            }
            _ => {}
        }
        snapshot.last_seq += 1;
        let legacy = event.clone();
        let thread_id = snapshot.thread_id.clone();
        let envelope = RunEvent {
            thread_id: snapshot.thread_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            seq: snapshot.last_seq,
            payload: RunEventPayload::Stream { event },
        };
        drop(snapshot);
        if let Err(error) = run
            .commit_session_event(
                &self.session_views,
                &self.session_management,
                RunPublicationMutation::Event(envelope.clone()),
            )
            .await
        {
            warn!(error=%error, "Failed to commit streamed Session projection");
        }
        if !*run.cancel.borrow()
            && !run
                .deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            let mut cancelled = run.cancel.subscribe();
            tokio::select! {
                biased;
                _ = cancelled.changed() => {},
                _ = async { match run.deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => futures::future::pending::<()>().await } } => {},
                _ = async {
                    if run.legacy.load(std::sync::atomic::Ordering::Relaxed) { Self::send_legacy_event(transport, &thread_id, legacy).await; }
                    let _ = Self::send_run_event(transport, envelope).await;
                } => {},
            }
        }
    }

    async fn send_legacy_event(
        transport: &AnyTransportWriter,
        thread_id: &str,
        event: AgentStreamEvent,
    ) {
        let turn_id = serde_json::to_value(&event).unwrap()["turn_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if let Ok(notification) = JSONRPCNotification::new(
            METHOD_TURN_STREAM_EVENTS,
            Some(StreamEventsParams {
                thread_id: thread_id.into(),
                turn_id,
                event,
            }),
        ) {
            if let Ok(line) = serde_json::to_string(&notification) {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    transport.send_line(&line),
                )
                .await;
            }
        }
    }

    async fn send_run_event(transport: &AnyTransportWriter, event: RunEvent) -> bool {
        let Ok(notification) = JSONRPCNotification::new(METHOD_TURN_EVENT, Some(event)) else {
            return false;
        };
        let Ok(line) = serde_json::to_string(&notification) else {
            return false;
        };
        matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                transport.send_line(&line)
            )
            .await,
            Ok(Ok(()))
        )
    }
}

fn apply_options(sampling: &mut SamplingOptions, options: &whale_protocol::rpc::RunTurnOptions) {
    if let Some(model) = &options.model {
        sampling.model = model.clone();
    }
    if let Some(temperature) = options.temperature {
        sampling.temperature = Some(temperature);
    }
    if let Some(max_tokens) = options.max_tokens {
        sampling.max_tokens = Some(max_tokens);
    }
    if let Some(reasoning_effort) = &options.reasoning_effort {
        sampling.reasoning_effort = Some(reasoning_effort.clone());
    }
    if let Some(thinking_budget) = options.thinking_budget {
        sampling.thinking_budget = Some(thinking_budget);
    }
    if let Some(prompt_caching) = options.prompt_caching {
        sampling.prompt_caching = prompt_caching;
    }
}

#[path = "retention.rs"]
mod retention;
