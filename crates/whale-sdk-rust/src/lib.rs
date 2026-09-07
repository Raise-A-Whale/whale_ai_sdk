//! whale-sdk-rust: Ergonomic native Rust client SDK for Whale AI agents.
//!
//! Supports:
//! - In-process embedded DaemonServer
//! - Spawning subprocess whale-daemon over Stdio
//! - Connecting to existing whale-daemon Unix Domain Socket (UDS)
//! - Bi-directional JSON-RPC 2.0 streaming events
//! - Host Reverse RPC tool execution
//! - HITL Approval gate resolution

use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::warn;

use whale_daemon::{
    AnyTransportWriter, DaemonServer, OutgoingTransport, UnixStreamWriter,
};
use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput};
use whale_protocol::events::AgentStreamEvent;
use whale_protocol::rpc::{
    ApprovalDecision, ApprovalResolveParams, ApprovalResolveResult, JSONRPCError,
    JSONRPCMessage, JSONRPCRequest, JSONRPCResponse, RegisterToolDefinition,
    RegisterToolsParams, RegisterToolsResult, RequestId, RunTurnParams,
    RunTurnResult, StartThreadParams, StartThreadResult, StreamEventsParams, ToolExecuteHostParams,
    ToolExecuteHostResult, METHOD_APPROVAL_RESOLVE, METHOD_SESSION_REGISTER_TOOLS,
    METHOD_SESSION_START_THREAD, METHOD_THREAD_RUN_TURN, METHOD_TOOL_EXECUTE_HOST,
    METHOD_TURN_STREAM_EVENTS,
};

#[derive(Debug, Error)]
pub enum SdkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("RPC error (code {code}): {message}")]
    Rpc { code: i64, message: String },

    #[error("Communication channel closed: {0}")]
    ChannelClosed(String),

    #[error("Internal SDK error: {0}")]
    Internal(String),
}

/// Trait for local host tools handled directly in Rust.
#[async_trait]
pub trait HostTool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    fn supports_parallel(&self) -> bool {
        true
    }
    fn require_approval(&self) -> bool {
        false
    }
    async fn execute(&self, arguments: Value) -> Result<CanonicalToolOutput, String>;
}

/// Underlying client connection transport.
enum ClientTransport {
    InProcess {
        _server: Arc<DaemonServer>,
        outgoing_writer: AnyTransportWriter,
    },
    External {
        writer: Arc<dyn OutgoingTransport>,
        _child: Option<Arc<Mutex<Child>>>,
    },
}

/// Whale AI SDK Client.
#[derive(Clone)]
pub struct WhaleClient {
    transport: Arc<ClientTransport>,
    next_req_id: Arc<AtomicI64>,
    pending_requests: Arc<DashMap<String, oneshot::Sender<Result<Value, JSONRPCError>>>>,
    event_subscribers: Arc<DashMap<String, mpsc::Sender<AgentStreamEvent>>>,
    host_tools: Arc<DashMap<String, Arc<dyn HostTool>>>,
}

impl WhaleClient {
    /// Connects to an in-process embedded DaemonServer.
    pub fn in_process(server: Arc<DaemonServer>) -> Self {
        let (client_tx, mut client_rx) = mpsc::channel::<String>(128);
        let (server_tx, mut server_rx) = mpsc::channel::<String>(128);

        let pending_requests: Arc<DashMap<String, oneshot::Sender<Result<Value, JSONRPCError>>>> =
            Arc::new(DashMap::new());
        let event_subscribers: Arc<DashMap<String, mpsc::Sender<AgentStreamEvent>>> =
            Arc::new(DashMap::new());
        let host_tools: Arc<DashMap<String, Arc<dyn HostTool>>> = Arc::new(DashMap::new());

        struct ChannelWriter(mpsc::Sender<String>);
        #[async_trait]
        impl OutgoingTransport for ChannelWriter {
            async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
                self.0
                    .send(line.to_string())
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
            }
        }

        let to_server_writer = AnyTransportWriter::new(Arc::new(ChannelWriter(server_tx.clone())));
        let to_client_writer = AnyTransportWriter::new(Arc::new(ChannelWriter(client_tx.clone())));

        // Background task running server message pump
        let server_clone = Arc::clone(&server);
        let to_client_clone = to_client_writer.clone();
        tokio::spawn(async move {
            while let Some(line) = server_rx.recv().await {
                let s_clone = Arc::clone(&server_clone);
                let tc_clone = to_client_clone.clone();
                tokio::spawn(async move {
                    s_clone.handle_message(&line, &tc_clone).await;
                });
            }
        });

        // Background task receiving messages from server to client
        let pending_clone = Arc::clone(&pending_requests);
        let events_clone = Arc::clone(&event_subscribers);
        let host_tools_clone = Arc::clone(&host_tools);
        let to_server_writer_clone = to_server_writer.clone();
        tokio::spawn(async move {
            while let Some(line) = client_rx.recv().await {
                Self::process_incoming_client_line(
                    &line,
                    &pending_clone,
                    &events_clone,
                    &host_tools_clone,
                    &to_server_writer_clone,
                )
                .await;
            }
        });

        Self {
            transport: Arc::new(ClientTransport::InProcess {
                _server: server,
                outgoing_writer: to_server_writer,
            }),
            next_req_id: Arc::new(AtomicI64::new(1)),
            pending_requests,
            event_subscribers,
            host_tools,
        }
    }

    /// Spawns a `whale-daemon` binary subprocess using Stdio IPC.
    pub async fn spawn_daemon(
        daemon_binary_path: impl AsRef<Path>,
    ) -> Result<Self, SdkError> {
        let mut child = Command::new(daemon_binary_path.as_ref())
            .arg("--listen")
            .arg("stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| {
            SdkError::Internal("Failed to capture daemon stdin".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SdkError::Internal("Failed to capture daemon stdout".to_string())
        })?;

        struct AsyncWriteHalf(Arc<Mutex<tokio::process::ChildStdin>>);
        #[async_trait]
        impl OutgoingTransport for AsyncWriteHalf {
            async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
                let mut guard = self.0.lock().await;
                guard.write_all(line.as_bytes()).await?;
                guard.write_all(b"\n").await?;
                guard.flush().await?;
                Ok(())
            }
        }

        let writer = Arc::new(AsyncWriteHalf(Arc::new(Mutex::new(stdin))));
        let any_writer = AnyTransportWriter::new(writer.clone());

        let pending_requests: Arc<DashMap<String, oneshot::Sender<Result<Value, JSONRPCError>>>> =
            Arc::new(DashMap::new());
        let event_subscribers: Arc<DashMap<String, mpsc::Sender<AgentStreamEvent>>> =
            Arc::new(DashMap::new());
        let host_tools: Arc<DashMap<String, Arc<dyn HostTool>>> = Arc::new(DashMap::new());

        let pending_clone = Arc::clone(&pending_requests);
        let events_clone = Arc::clone(&event_subscribers);
        let host_tools_clone = Arc::clone(&host_tools);
        let any_writer_clone = any_writer.clone();

        // Spawn background reader loop on stdout
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                Self::process_incoming_client_line(
                    &line,
                    &pending_clone,
                    &events_clone,
                    &host_tools_clone,
                    &any_writer_clone,
                )
                .await;
            }
        });

        Ok(Self {
            transport: Arc::new(ClientTransport::External {
                writer,
                _child: Some(Arc::new(Mutex::new(child))),
            }),
            next_req_id: Arc::new(AtomicI64::new(1)),
            pending_requests,
            event_subscribers,
            host_tools,
        })
    }

    /// Connects to an existing whale-daemon over Unix Domain Socket.
    pub async fn connect_uds(uds_path: impl AsRef<Path>) -> Result<Self, SdkError> {
        let stream = UnixStream::connect(uds_path).await?;
        let (read_half, write_half) = stream.into_split();
        let writer = Arc::new(UnixStreamWriter::new(write_half));
        let any_writer = AnyTransportWriter::new(writer.clone());

        let pending_requests: Arc<DashMap<String, oneshot::Sender<Result<Value, JSONRPCError>>>> =
            Arc::new(DashMap::new());
        let event_subscribers: Arc<DashMap<String, mpsc::Sender<AgentStreamEvent>>> =
            Arc::new(DashMap::new());
        let host_tools: Arc<DashMap<String, Arc<dyn HostTool>>> = Arc::new(DashMap::new());

        let pending_clone = Arc::clone(&pending_requests);
        let events_clone = Arc::clone(&event_subscribers);
        let host_tools_clone = Arc::clone(&host_tools);
        let any_writer_clone = any_writer.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(read_half).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                Self::process_incoming_client_line(
                    &line,
                    &pending_clone,
                    &events_clone,
                    &host_tools_clone,
                    &any_writer_clone,
                )
                .await;
            }
        });

        Ok(Self {
            transport: Arc::new(ClientTransport::External {
                writer,
                _child: None,
            }),
            next_req_id: Arc::new(AtomicI64::new(1)),
            pending_requests,
            event_subscribers,
            host_tools,
        })
    }

    /// Internal line processor for incoming messages from daemon.
    async fn process_incoming_client_line(
        line: &str,
        pending_requests: &Arc<DashMap<String, oneshot::Sender<Result<Value, JSONRPCError>>>>,
        event_subscribers: &Arc<DashMap<String, mpsc::Sender<AgentStreamEvent>>>,
        host_tools: &Arc<DashMap<String, Arc<dyn HostTool>>>,
        outgoing_writer: &AnyTransportWriter,
    ) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }

        let msg: JSONRPCMessage = match serde_json::from_str(trimmed) {
            Ok(m) => m,
            Err(e) => {
                warn!("Client failed to parse message: {}. Raw: {}", e, trimmed);
                return;
            }
        };

        match msg {
            JSONRPCMessage::Response(resp) => {
                let req_key = match &resp.id {
                    RequestId::Number(n) => n.to_string(),
                    RequestId::String(s) => s.clone(),
                };
                if let Some((_, sender)) = pending_requests.remove(&req_key) {
                    if let Some(err) = resp.error {
                        let _ = sender.send(Err(err));
                    } else if let Some(res) = resp.result {
                        let _ = sender.send(Ok(res));
                    } else {
                        let _ = sender.send(Ok(Value::Null));
                    }
                }
            }
            JSONRPCMessage::Notification(notif) => {
                if notif.method == METHOD_TURN_STREAM_EVENTS {
                    if let Some(params_val) = notif.params {
                        if let Ok(params) =
                            serde_json::from_value::<StreamEventsParams>(params_val)
                        {
                            if let Some(sub) = event_subscribers.get(&params.thread_id) {
                                let _ = sub.send(params.event).await;
                            }
                        }
                    }
                }
            }
            JSONRPCMessage::Request(req) => {
                // Reverse RPC Tool Execution request from daemon!
                if req.method == METHOD_TOOL_EXECUTE_HOST {
                    let req_id = req.id.clone();
                    let params_opt = req.params;
                    let host_tools_map = Arc::clone(host_tools);
                    let writer = outgoing_writer.clone();

                    tokio::spawn(async move {
                        let (call_id, output, is_error) = match params_opt {
                            Some(v) => match serde_json::from_value::<ToolExecuteHostParams>(v) {
                                Ok(p) => {
                                    if let Some(tool) = host_tools_map.get(&p.name) {
                                        match tool.execute(p.arguments).await {
                                            Ok(out) => (p.call_id, out, false),
                                            Err(err) => (p.call_id, CanonicalToolOutput::text(err), true),
                                        }
                                    } else {
                                        (
                                            p.call_id,
                                            CanonicalToolOutput::text(format!(
                                                "Host tool '{}' not registered in client",
                                                p.name
                                            )),
                                            true,
                                        )
                                    }
                                }
                                Err(e) => (
                                    "unknown".to_string(),
                                    CanonicalToolOutput::text(format!("Invalid params: {}", e)),
                                    true,
                                ),
                            },
                            None => (
                                "unknown".to_string(),
                                CanonicalToolOutput::text("Missing host tool params"),
                                true,
                            ),
                        };

                        let host_result = ToolExecuteHostResult {
                            call_id,
                            output,
                            is_error,
                        };

                        let resp = JSONRPCResponse::success(req_id, host_result)
                            .unwrap_or_else(|e| JSONRPCResponse::error(0, JSONRPCError::internal_error(e.to_string())));

                        if let Ok(s) = serde_json::to_string(&resp) {
                            let _ = writer.send_line(&s).await;
                        }
                    });
                }
            }
        }
    }

    /// Sends a JSON-RPC request to daemon and awaits response.
    async fn request<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Option<P>,
    ) -> Result<R, SdkError> {
        let req_id = self.next_req_id.fetch_add(1, Ordering::SeqCst);
        let id_str = req_id.to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_requests.insert(id_str.clone(), tx);

        let req = JSONRPCRequest::new(req_id, method, params)?;
        let line = serde_json::to_string(&req)?;

        match &*self.transport {
            ClientTransport::InProcess { outgoing_writer, .. } => {
                outgoing_writer.send_line(&line).await?;
            }
            ClientTransport::External { writer, .. } => {
                writer.send_line(&line).await?;
            }
        }

        match rx.await {
            Ok(Ok(val)) => Ok(serde_json::from_value(val)?),
            Ok(Err(rpc_err)) => Err(SdkError::Rpc {
                code: rpc_err.code,
                message: rpc_err.message,
            }),
            Err(_) => {
                self.pending_requests.remove(&id_str);
                Err(SdkError::ChannelClosed("Response channel closed".to_string()))
            }
        }
    }

    /// Creates a conversation thread session.
    pub async fn create_thread(
        &self,
        model: impl Into<String>,
        system_prompt: Option<String>,
    ) -> Result<WhaleThread, SdkError> {
        let params = StartThreadParams {
            session_id: None,
            provider: None,
            model: model.into(),
            system_prompt,
            tools: Vec::new(),
            metadata: serde_json::Map::new(),
        };

        let result: StartThreadResult = self
            .request(METHOD_SESSION_START_THREAD, Some(params))
            .await?;

        Ok(WhaleThread {
            client: self.clone(),
            thread_id: result.thread_id,
        })
    }

    /// Resolves an HITL approval request.
    pub async fn resolve_approval(
        &self,
        request_id: impl Into<String>,
        decision: ApprovalDecision,
        feedback: Option<String>,
    ) -> Result<bool, SdkError> {
        let params = ApprovalResolveParams {
            request_id: request_id.into(),
            decision,
            feedback,
        };

        let res: ApprovalResolveResult = self
            .request(METHOD_APPROVAL_RESOLVE, Some(params))
            .await?;
        Ok(res.resolved)
    }
}

/// Active conversation thread handle.
pub struct WhaleThread {
    client: WhaleClient,
    thread_id: String,
}

impl WhaleThread {
    /// Returns thread ID.
    pub fn id(&self) -> &str {
        &self.thread_id
    }

    /// Registers a local host tool with Reverse RPC support.
    pub async fn register_tool(&self, tool: Arc<dyn HostTool>) -> Result<(), SdkError> {
        self.client
            .host_tools
            .insert(tool.name().to_string(), Arc::clone(&tool));

        let tool_def = RegisterToolDefinition {
            name: tool.name().to_string(),
            description: tool.description().to_string(),
            parameters: tool.parameters(),
            supports_parallel: tool.supports_parallel(),
            require_approval: tool.require_approval(),
            is_host_tool: true,
        };

        let params = RegisterToolsParams {
            thread_id: self.thread_id.clone(),
            tools: vec![tool_def],
        };

        let _res: RegisterToolsResult = self
            .client
            .request(METHOD_SESSION_REGISTER_TOOLS, Some(params))
            .await?;
        Ok(())
    }

    /// Runs a turn with input text and returns a stream of `AgentStreamEvent`s.
    pub async fn run_turn(
        &self,
        input_text: &str,
    ) -> Result<(RunTurnResult, mpsc::Receiver<AgentStreamEvent>), SdkError> {
        let (event_tx, event_rx) = mpsc::channel(128);
        self.client
            .event_subscribers
            .insert(self.thread_id.clone(), event_tx);

        let params = RunTurnParams {
            thread_id: self.thread_id.clone(),
            input_items: vec![CanonicalItem::user_text(input_text)],
            options: None,
        };

        let res: RunTurnResult = self
            .client
            .request(METHOD_THREAD_RUN_TURN, Some(params))
            .await?;

        self.client.event_subscribers.remove(&self.thread_id);

        Ok((res, event_rx))
    }
}
