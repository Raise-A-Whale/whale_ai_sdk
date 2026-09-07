//! Server implementation for whale-daemon JSON-RPC 2.0 protocol and Reverse RPC bridge.

use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};
use uuid::Uuid;

use whale_adapters::{
    AnthropicAdapter, OpenAIAdapter, ProtocolAdapter, SamplingOptions,
};
use whale_core::{
    AgentEngine, ApprovalDecision as CoreApprovalDecision, ApprovalGate,
    ThreadSession, ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::canonical::{
    CanonicalContent, CanonicalItem, CanonicalToolOutput,
};
use whale_protocol::events::AgentStreamEvent;
use whale_protocol::rpc::{
    ApprovalDecision, ApprovalResolveParams, ApprovalResolveResult, JSONRPCError,
    JSONRPCMessage, JSONRPCNotification, JSONRPCRequest, JSONRPCResponse, RegisterToolDefinition,
    RegisterToolsParams, RegisterToolsResult, RequestId, RunTurnParams, RunTurnResult,
    StartThreadParams, StartThreadResult, StreamEventsParams, ToolExecuteHostParams,
    ToolExecuteHostResult, TurnStatus, METHOD_APPROVAL_RESOLVE, METHOD_SESSION_REGISTER_TOOLS,
    METHOD_SESSION_START_THREAD, METHOD_THREAD_RUN_TURN, METHOD_TOOL_EXECUTE_HOST,
    METHOD_TOOL_EXECUTE_HOST_RESULT, METHOD_TURN_STREAM_EVENTS,
};

use crate::transport::{AnyTransportWriter, OutgoingTransport};

/// Bridge tool handler that invokes Host-side tools via Reverse RPC.
pub struct HostToolBridge {
    tool_name: String,
    tool_description: String,
    parameters_schema: Value,
    supports_parallel: bool,
    require_approval: bool,
    transport: AnyTransportWriter,
    pending_host_tool_calls: Arc<DashMap<String, oneshot::Sender<CanonicalToolOutput>>>,
}

impl HostToolBridge {
    pub fn new(
        def: RegisterToolDefinition,
        transport: AnyTransportWriter,
        pending_host_tool_calls: Arc<DashMap<String, oneshot::Sender<CanonicalToolOutput>>>,
    ) -> Self {
        Self {
            tool_name: def.name,
            tool_description: def.description,
            parameters_schema: def.parameters,
            supports_parallel: def.supports_parallel,
            require_approval: def.require_approval,
            transport,
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
        let call_id = format!("call_{}", Uuid::new_v4());
        let (tx, rx) = oneshot::channel();
        self.pending_host_tool_calls.insert(call_id.clone(), tx);

        let params = ToolExecuteHostParams {
            call_id: call_id.clone(),
            namespace: None,
            name: self.tool_name.clone(),
            arguments,
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
            Ok(output) => Ok(output),
            Err(_) => {
                self.pending_host_tool_calls.remove(&call_id);
                Err("Host client disconnected or dropped reverse tool execution channel".to_string())
            }
        }
    }
}

/// JSON-RPC 2.0 daemon server managing sessions and Reverse RPC.
#[derive(Clone)]
pub struct DaemonServer {
    sessions: Arc<DashMap<String, Arc<Mutex<ThreadSession>>>>,
    engine: Arc<AgentEngine>,
    approval_gate: Arc<ApprovalGate>,
    pending_host_tool_calls: Arc<DashMap<String, oneshot::Sender<CanonicalToolOutput>>>,
}

impl DaemonServer {
    /// Creates a new DaemonServer instance.
    pub fn new(engine: Arc<AgentEngine>, approval_gate: Arc<ApprovalGate>) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            engine,
            approval_gate,
            pending_host_tool_calls: Arc::new(DashMap::new()),
        }
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
    ) -> &Arc<DashMap<String, oneshot::Sender<CanonicalToolOutput>>> {
        &self.pending_host_tool_calls
    }

    /// Runs the server on an incoming lines stream and outgoing transport.
    pub async fn run<R, T>(&self, mut lines_reader: R, transport: T) -> Result<(), std::io::Error>
    where
        R: StreamExt<Item = Result<String, std::io::Error>> + Unpin,
        T: OutgoingTransport + 'static,
    {
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

            debug!("Received JSON-RPC message: {}", trimmed);
            let s_self = self.clone();
            let tw = transport_writer.clone();
            let line_owned = trimmed.to_string();
            tokio::spawn(async move {
                s_self.handle_message(&line_owned, &tw).await;
            });
        }

        info!("Connection loop finished; reader reached EOF");
        Ok(())
    }

    /// Dispatches a single incoming raw JSON string message.
    pub async fn handle_message(&self, raw_json: &str, transport: &AnyTransportWriter) {
        let msg: JSONRPCMessage = match serde_json::from_str(raw_json) {
            Ok(m) => m,
            Err(e) => {
                let err_resp = JSONRPCResponse::error(
                    0,
                    JSONRPCError::new(JSONRPCError::PARSE_ERROR, format!("Parse error: {}", e), None),
                );
                if let Ok(s) = serde_json::to_string(&err_resp) {
                    let _ = transport.send_line(&s).await;
                }
                return;
            }
        };

        match msg {
            JSONRPCMessage::Request(req) => {
                let resp = self.dispatch_request(req, transport).await;
                if let Ok(s) = serde_json::to_string(&resp) {
                    let _ = transport.send_line(&s).await;
                }
            }
            JSONRPCMessage::Response(resp) => {
                self.handle_incoming_response(resp).await;
            }
            JSONRPCMessage::Notification(notif) => {
                self.handle_notification(notif).await;
            }
        }
    }

    /// Handles client responses, such as answering reverse RPC `tool.execute_host`.
    async fn handle_incoming_response(&self, resp: JSONRPCResponse) {
        debug!("Received JSON-RPC Response id={:?}", resp.id);
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

        if let Some((_, sender)) = self.pending_host_tool_calls.remove(&call_id) {
            if let Some(result_val) = resp.result {
                if let Ok(host_res) = serde_json::from_value::<ToolExecuteHostResult>(result_val.clone()) {
                    let _ = sender.send(host_res.output);
                    return;
                } else if let Ok(out) = serde_json::from_value::<CanonicalToolOutput>(result_val) {
                    let _ = sender.send(out);
                    return;
                }
            }
            if let Some(err) = resp.error {
                let _ = sender.send(CanonicalToolOutput::text(format!(
                    "Host error: {}",
                    err.message
                )));
            } else {
                let _ = sender.send(CanonicalToolOutput::text("Host returned null output"));
            }
        }
    }

    async fn handle_notification(&self, notif: JSONRPCNotification) {
        debug!("Received JSON-RPC notification method={}", notif.method);
    }

    /// Dispatches a JSON-RPC request and produces a response.
    pub async fn dispatch_request(
        &self,
        req: JSONRPCRequest,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let id = req.id.clone();
        match req.method.as_str() {
            METHOD_SESSION_START_THREAD => {
                self.handle_start_thread(id, req.params, transport).await
            }
            METHOD_THREAD_RUN_TURN => {
                self.handle_run_turn(id, req.params, transport).await
            }
            METHOD_TOOL_EXECUTE_HOST_RESULT => {
                self.handle_tool_execute_host_result(id, req.params).await
            }
            METHOD_APPROVAL_RESOLVE => {
                self.handle_approval_resolve(id, req.params).await
            }
            METHOD_SESSION_REGISTER_TOOLS => {
                self.handle_register_tools(id, req.params, transport).await
            }
            other => JSONRPCResponse::error(id, JSONRPCError::method_not_found(other)),
        }
    }

    /// Method: "session.start_thread"
    async fn handle_start_thread(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: StartThreadParams = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
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

        let thread_id = params
            .session_id
            .unwrap_or_else(|| format!("th_{}", Uuid::new_v4()));
        let model = params.model.clone();

        // Create adapter according to provider or model name
        let provider = params
            .provider
            .as_deref()
            .unwrap_or_else(|| {
                if model.contains("claude") {
                    "anthropic"
                } else {
                    "openai"
                }
            });

        let adapter: Arc<dyn ProtocolAdapter> = match provider {
            "anthropic" => {
                let api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_default();
                Arc::new(AnthropicAdapter::new(api_key))
            }
            "openai" | _ => {
                let api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
                Arc::new(OpenAIAdapter::new(api_key))
            }
        };

        let tools = Arc::new(ToolRegistry::new());

        // Register initial tools passed in params
        for tool_def in params.tools {
            if tool_def.is_host_tool {
                let bridge = HostToolBridge::new(
                    tool_def,
                    transport.clone(),
                    Arc::clone(&self.pending_host_tool_calls),
                );
                tools.register(Arc::new(bridge));
            }
        }

        let sampling_options = SamplingOptions::new(model);
        let session = ThreadSession::with_id_and_prompt(
            thread_id.clone(),
            params.system_prompt,
            adapter,
            tools,
            sampling_options,
        );

        self.sessions
            .insert(thread_id.clone(), Arc::new(Mutex::new(session)));

        let created_at = chrono::Utc::now().to_rfc3339();
        let id_clone = id.clone();
        JSONRPCResponse::success(id, StartThreadResult { thread_id, created_at })
            .unwrap_or_else(|e| JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string())))
    }

    /// Method: "thread.run_turn"
    async fn handle_run_turn(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
        _transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: RunTurnParams = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!("Invalid RunTurnParams: {}", e)),
                    );
                }
            },
            None => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Missing parameters for run_turn"),
                );
            }
        };

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

        let engine = Arc::clone(&self.engine);
        let thread_id = params.thread_id.clone();
        let (event_tx, mut event_rx) = mpsc::channel::<AgentStreamEvent>(128);

        // Forward events as JSON-RPC notifications ("turn.stream_events")
        let thread_id_for_task = thread_id.clone();
        let transport_clone = _transport.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let turn_id = match &event {
                    AgentStreamEvent::TurnStarted { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ItemStarted { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::TextDelta { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ReasoningDelta { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ReasoningSignature { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ToolCallDelta { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ItemCompleted { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::ApprovalRequested { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::TurnCompleted { turn_id, .. } => turn_id.clone(),
                    AgentStreamEvent::TurnFailed { turn_id, .. } => turn_id.clone(),
                };

                let notif_params = StreamEventsParams {
                    turn_id,
                    thread_id: thread_id_for_task.clone(),
                    event,
                };

                if let Ok(notif) =
                    JSONRPCNotification::new(METHOD_TURN_STREAM_EVENTS, Some(notif_params))
                {
                    if let Ok(s) = serde_json::to_string(&notif) {
                        let _ = transport_clone.send_line(&s).await;
                    }
                }
            }
        });

        // Run turn on locked session
        let mut session_guard = session_arc.lock().await;

        // Apply any options overrides
        if let Some(opts) = params.options {
            if let Some(m) = opts.model {
                session_guard.sampling_options_mut().model = m;
            }
            if let Some(t) = opts.temperature {
                session_guard.sampling_options_mut().temperature = Some(t);
            }
            if let Some(mt) = opts.max_tokens {
                session_guard.sampling_options_mut().max_tokens = Some(mt);
            }
            if let Some(re) = opts.reasoning_effort {
                session_guard.sampling_options_mut().reasoning_effort = Some(re);
            }
        }

        // Add any input items
        let mut user_input_text = None;
        for item in params.input_items {
            if let CanonicalItem::UserMessage { ref content, .. } = item {
                if user_input_text.is_none() {
                    for c in content {
                        if let CanonicalContent::Text { ref text } = c {
                            user_input_text = Some(text.clone());
                            break;
                        }
                    }
                }
            }
        }

        match engine
            .run_turn(&mut session_guard, user_input_text.as_deref(), 10, event_tx)
            .await
        {
            Ok(turn_res) => {
                let result = RunTurnResult {
                    turn_id: turn_res.turn_id,
                    thread_id,
                    status: TurnStatus::Completed,
                    items: turn_res.generated_items,
                    usage: turn_res.total_usage,
                };
                let id_clone = id.clone();
                JSONRPCResponse::success(id, result)
                    .unwrap_or_else(|e| JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string())))
            }
            Err(e) => {
                JSONRPCResponse::error(
                    id,
                    JSONRPCError::internal_error(format!("Turn execution error: {}", e)),
                )
            }
        }
    }

    /// Method: "tool.execute_host_result"
    async fn handle_tool_execute_host_result(
        &self,
        id: RequestId,
        params_opt: Option<Value>,
    ) -> JSONRPCResponse {
        let params: ToolExecuteHostResult = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!("Invalid ToolExecuteHostResult: {}", e)),
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

        if let Some((_, sender)) = self.pending_host_tool_calls.remove(&params.call_id) {
            let _ = sender.send(params.output);
            let id_clone = id.clone();
            JSONRPCResponse::success(id, json!({ "acknowledged": true }))
                .unwrap_or_else(|e| JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string())))
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
    ) -> JSONRPCResponse {
        let params: ApprovalResolveParams = match params_opt {
            Some(v) => match serde_json::from_value(v) {
                Ok(p) => p,
                Err(e) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(format!("Invalid ApprovalResolveParams: {}", e)),
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

        let core_decision = match params.decision {
            ApprovalDecision::Approve => CoreApprovalDecision::Accept,
            ApprovalDecision::Reject => CoreApprovalDecision::Deny {
                reason: params.feedback,
            },
        };

        let resolved = self
            .approval_gate
            .resolve_approval(&params.request_id, core_decision);

        let result = ApprovalResolveResult {
            resolved,
            request_id: params.request_id,
        };

        let id_clone = id.clone();
        JSONRPCResponse::success(id, result)
            .unwrap_or_else(|e| JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string())))
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

        let session_guard = session_arc.lock().await;
        let mut count = 0;

        for tool_def in params.tools {
            if tool_def.is_host_tool {
                let bridge = HostToolBridge::new(
                    tool_def,
                    transport.clone(),
                    Arc::clone(&self.pending_host_tool_calls),
                );
                session_guard.tools().register(Arc::new(bridge));
                count += 1;
            }
        }

        let result = RegisterToolsResult {
            registered_count: count,
        };
        let id_clone = id.clone();
        JSONRPCResponse::success(id, result)
            .unwrap_or_else(|e| JSONRPCResponse::error(id_clone, JSONRPCError::internal_error(e.to_string())))
    }
}
