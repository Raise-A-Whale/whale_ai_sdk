#![doc = include_str!("../README.md")]

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, OnceLock, Weak,
};
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWrite;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use whale_daemon::transport::FrameWriter;
pub use whale_daemon::DaemonServer;
use whale_daemon::OutgoingTransport;
use whale_protocol::rpc::*;
use whale_protocol::runs::*;
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput};

mod initialization;
#[cfg(test)]
mod interaction_tests;
mod interactions;
pub use whale_protocol::initialization::{InitializeParams, InitializeResult, PeerInfo};
mod agent;
mod connection;
#[cfg(test)]
mod connection_tests;
mod runtime;
#[cfg(test)]
mod tool_pack_tests;
mod tool_packs;
#[cfg(test)]
mod weak_route_drop_regressions;
pub use agent::{Agent, AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig};
pub use interactions::{
    InteractionEventStream, InteractionSubscriptionOptions, InteractionViewError, InteractionWatch,
    InteractionWatchOptions,
};
pub use runtime::{
    RuntimeError, RuntimeInfo, RuntimeMode, RuntimeOptions, RuntimeShutdown, RuntimeShutdownPhase,
    RuntimeSource, ShutdownDisposition, WhaleRuntime,
};
pub use tool_packs::{
    BoundToolPack, SessionBindContext, SessionBindKind, ToolPack, ToolPackError, ToolPackManifest,
    ToolPackTool,
};
pub use whale_protocol::interactions::{
    InteractionCursor, InteractionEventEnvelope, InteractionEventPayload, InteractionReplayGap,
    InteractionReplayGapReason, InteractionRequest, InteractionResponse, InteractionSnapshot,
    PendingInteraction, RespondInteractionResult, TurnInteractionSnapshot,
};
pub use whale_protocol::models::{
    InspectProviderParams, InspectProviderResult, ModelCapabilities, ModelCapabilityScope,
    ModelContentKind, ModelOption,
};
mod run;
pub use run::{RunEventStream, RunHandle};
mod session_management;
pub use session_management::{
    SessionEventStreamV2, SessionHistoryOptions, SessionHistoryPage, SessionListOptions,
    SessionListPage, SessionManagementError, SessionManagementWatchOptions, SessionViewHandle,
    SessionWatchV2,
};
mod session_views;
pub use session_views::{
    RunEventEnvelope, RunEventSubscription, SessionEventStream, SessionViewError, SessionWatch,
    SessionWatchOptions, SubscriptionOptions,
};
pub use whale_protocol::retention::SessionLimits;
pub use whale_protocol::session_management::{
    ReplaceSessionMetadataResult, SessionCursorV2, SessionEventEnvelopeV2, SessionEventPayloadV2,
    SessionHistoryAnchor, SessionHistoryPageCursor, SessionLifecycleState, SessionListCursor,
    SessionListEntry, SessionPersistenceV2, SessionRunHeadline, SessionSnapshotV2,
    SessionSummaryV2,
};
pub use whale_protocol::session_views::{
    InProgressItemProjection, ReplayGap, ReplayGapReason, SessionCursor, SessionEventEnvelope,
    SessionEventPayload, SessionHistoryWindow, SessionProjectionError, SessionRunSummary,
    SessionRunView, SessionSnapshot, SessionSummary,
};
#[cfg(test)]
mod context_lifecycle_tests;
mod contexts;
mod recovery;
#[cfg(test)]
mod retention_tests;
#[cfg(test)]
mod session_close_tests;
mod sessions;
pub use whale_protocol::recovery::{
    ModelInputRecord, RecoveredRun, RecoveryKey, RecoverySnapshot, UnknownExecution,
};
#[cfg(test)]
mod recovery_tests;
pub use contexts::{
    CancellationSignal, ContextBuildRequest, ContextPolicyConfig, HostContextPolicy, ModelContext,
    RunContextInfo, ToolContext, ToolContextInfo,
};

#[derive(Debug, Error)]
pub enum SdkError {
    #[error("Protocol compatibility error: {0}")]
    ProtocolCompatibility(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("RPC error (code {code}): {message}")]
    Rpc { code: i64, message: String },
    #[error("Run expired: {0}")]
    RunExpired(String),
    #[error("Session limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("Communication channel closed: {0}")]
    ChannelClosed(String),
    #[error("SessionClosed: {0}")]
    SessionClosed(String),
    #[error("Internal SDK error: {0}")]
    Internal(String),
    #[error("Event consumer fell behind; use snapshot() for the authoritative state")]
    EventLagged,
    #[error("This run already has an event consumer")]
    AlreadySubscribed,
    #[error("RPC request timed out: {0}")]
    RequestTimeout(String),
}

#[cfg(test)]
mod writer_tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};

    #[tokio::test]
    async fn close_interrupts_a_blocked_write() {
        let (io, _unread_peer) = tokio::io::duplex(1);
        let writer = ManagedWriter::io(io);
        let send_writer = writer.clone();
        let task = tokio::spawn(async move { send_writer.send_line("frame too large").await });
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_millis(100), writer.close())
            .await
            .expect("close must not wait for peer to consume a blocked write");
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn cancelling_a_request_write_does_not_corrupt_next_frame() {
        let (io, peer) = tokio::io::duplex(1);
        let writer = ManagedWriter::io(io);
        let first_writer = writer.clone();
        let first = tokio::spawn(async move { first_writer.send_line("first frame").await });
        let mut reader = BufReader::new(peer);
        let mut first_byte = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut first_byte)
            .await
            .unwrap();
        first.abort();
        let _ = first.await;
        let second_writer = writer.clone();
        let second = tokio::spawn(async move { second_writer.send_line("second frame").await });
        let mut rest = String::new();
        reader.read_line(&mut rest).await.unwrap();
        assert_eq!(
            format!("{}{}", first_byte[0] as char, rest),
            "first frame\n"
        );
        rest.clear();
        reader.read_line(&mut rest).await.unwrap();
        assert_eq!(rest, "second frame\n");
        second.await.unwrap().unwrap();
        writer.close().await;
    }
}

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
    async fn execute_with_context(
        &self,
        _context: ToolContext,
        arguments: Value,
    ) -> Result<CanonicalToolOutput, String> {
        self.execute(arguments).await
    }
}

pub(crate) enum InternalRequestError {
    Sdk(SdkError),
    Remote(JSONRPCError),
}
type Reply = Result<Value, InternalRequestError>;

impl From<SdkError> for InternalRequestError {
    fn from(error: SdkError) -> Self {
        Self::Sdk(error)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SessionRequestAccess<'a> {
    Unscoped,
    Writable(&'a str),
    Viewable(&'a str),
}

struct ClientState {
    initialization: initialization::Initialization,
    next_id: AtomicI64,
    pending: DashMap<String, oneshot::Sender<Reply>>,
    pending_sessions: DashMap<String, String>,
    approval_sessions: DashMap<String, String>,
    approval_turns: DashMap<String, String>,
    sessions: DashMap<String, Arc<sessions::SessionLifecycle>>,
    session_event_hubs: DashMap<String, Arc<session_views::SessionEventHub>>,
    session_event_hubs_v2: DashMap<String, Weak<session_management::SessionEventHubV2>>,
    interaction_event_hubs: DashMap<String, Weak<interactions::InteractionEventHub>>,
    interaction_sessions: DashMap<String, ()>,
    runs: DashMap<String, Arc<run::RunState>>,
    // Live routes own pending runs. This index only preserves held handle identity.
    run_handles: std::sync::Mutex<HashMap<String, (String, Weak<run::RunState>)>>,
    tools: DashMap<(String, String), Arc<dyn HostTool>>,
    tool_bindings: DashMap<(String, String), Arc<dyn HostTool>>,
    registration_locks: DashMap<(String, String), Arc<Mutex<()>>>,
    context_policies: DashMap<String, Arc<dyn HostContextPolicy>>,
    tool_callbacks: DashMap<String, contexts::ScopedCallback>,
    context_callbacks: DashMap<String, contexts::ScopedCallback>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
    connection: OnceLock<connection::ConnectionStop>,
}

impl ClientState {
    fn new() -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        Arc::new(Self {
            initialization: std::sync::OnceLock::new(),
            next_id: AtomicI64::new(1),
            pending: DashMap::new(),
            pending_sessions: DashMap::new(),
            approval_sessions: DashMap::new(),
            approval_turns: DashMap::new(),
            sessions: DashMap::new(),
            session_event_hubs: DashMap::new(),
            session_event_hubs_v2: DashMap::new(),
            interaction_event_hubs: DashMap::new(),
            interaction_sessions: DashMap::new(),
            runs: DashMap::new(),
            run_handles: std::sync::Mutex::new(HashMap::new()),
            tools: DashMap::new(),
            tool_bindings: DashMap::new(),
            registration_locks: DashMap::new(),
            context_policies: DashMap::new(),
            tool_callbacks: DashMap::new(),
            context_callbacks: DashMap::new(),
            closed: AtomicBool::new(false),
            shutdown,
            connection: OnceLock::new(),
        })
    }
    fn cache_run(self: &Arc<Self>, state: &Arc<run::RunState>) {
        let _ = state.owner.set(Arc::downgrade(self));
        self.run_handles.lock().unwrap().insert(
            state.turn_id.clone(),
            (state.thread_id.clone(), Arc::downgrade(state)),
        );
    }

    fn cached_run(&self, turn_id: &str) -> Option<Arc<run::RunState>> {
        self.run_handles
            .lock()
            .unwrap()
            .get(turn_id)
            .and_then(|(_, run)| run.upgrade())
    }

    fn disconnect(&self, message: &str) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.close_all_session_event_hubs(Some(message.to_owned()));
        self.close_all_session_event_hubs_v2(Some(message.to_owned()));
        self.close_all_interaction_event_hubs(Some(message.to_owned()));
        self.shutdown.send_replace(true);
        for callback in self.tool_callbacks.iter() {
            callback.stop();
        }
        for callback in self.context_callbacks.iter() {
            callback.stop();
        }
        self.context_policies.clear();
        self.approval_sessions.clear();
        self.approval_turns.clear();
        let ids: Vec<_> = self.pending.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            if let Some((_, tx)) = self.pending.remove(&id) {
                let _ = tx.send(Err(InternalRequestError::Sdk(SdkError::ChannelClosed(
                    message.to_owned(),
                ))));
            }
        }
        for state in self.runs.iter() {
            state.value().fail(message);
        }
        self.runs.clear();
        self.run_handles.lock().unwrap().clear();
        self.tools.clear();
        self.tool_bindings.clear();
        self.registration_locks.clear();
        self.pending_sessions.clear();
        self.tool_callbacks.clear();
        self.context_callbacks.clear();
        self.emergency_drain_session_packs();
    }
    fn incoming(self: &Arc<Self>, line: &str, writer: &Arc<ManagedWriter>) {
        let message: JSONRPCMessage = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(error) => {
                self.disconnect(&format!("Invalid daemon message: {error}"));
                return;
            }
        };
        match message {
            JSONRPCMessage::Response(response) => {
                let key = match &response.id {
                    RequestId::String(s) => s.clone(),
                    RequestId::Number(n) => n.to_string(),
                };
                if let Some((_, tx)) = self.pending.remove(&key) {
                    let reply = match (response.result, response.error) {
                        (_, Some(error)) => Err(InternalRequestError::Remote(error)),
                        (Some(value), None) => Ok(value),
                        _ => Err(InternalRequestError::Sdk(SdkError::Internal(
                            "Response has no result or error".into(),
                        ))),
                    };
                    let _ = tx.send(reply);
                }
            }
            JSONRPCMessage::Notification(notification) => {
                if !self.is_initialized() {
                    return;
                }
                if contexts::handle_notification(self, &notification) {
                    return;
                }
                if notification.method == METHOD_TURN_EVENT {
                    match notification
                        .params
                        .and_then(|v| serde_json::from_value::<RunEvent>(v).ok())
                    {
                        Some(event) => {
                            let state = self.runs.get(&event.turn_id).map(|route| {
                                let state = route.value().clone();
                                if state.thread_id == event.thread_id
                                    && state.turn_id == event.turn_id
                                    && event.seq > 0
                                {
                                    state.claim_event_route();
                                }
                                state
                            });
                            if let Some(state) = state {
                                if let RunEventPayload::Stream {
                                    event: AgentStreamEvent::ApprovalRequested { request_id, .. },
                                } = &event.payload
                                {
                                    self.while_session_exists(&event.thread_id, || {
                                        self.approval_sessions
                                            .insert(request_id.clone(), event.thread_id.clone());
                                        self.approval_turns
                                            .insert(request_id.clone(), event.turn_id.clone());
                                    });
                                }
                                if state.accept(event) {
                                    self.runs.remove_if(&state.turn_id, |_, value| {
                                        Arc::ptr_eq(value, &state)
                                    });
                                }
                            }
                        }
                        None => self.disconnect("Invalid turn.event payload"),
                    }
                } else if notification.method == whale_protocol::session_views::METHOD_SESSION_EVENT
                {
                    match notification.params.and_then(|value| {
                        serde_json::from_value::<
                            whale_protocol::session_views::SessionEventEnvelope,
                        >(value)
                        .ok()
                    }) {
                        Some(event) => {
                            if let Err(error) = self.route_session_event(event) {
                                self.disconnect(&format!("Invalid session.event payload: {error}"));
                            }
                        }
                        None => self.disconnect("Invalid session.event payload"),
                    }
                } else if notification.method
                    == whale_protocol::session_management::METHOD_SESSION_EVENT_V2
                {
                    match notification.params.and_then(|value| {
                        serde_json::from_value::<
                            whale_protocol::session_management::SessionEventEnvelopeV2,
                        >(value)
                        .ok()
                    }) {
                        Some(event) => {
                            if let Err(error) = self.route_session_event_v2(event) {
                                self.disconnect(&format!(
                                    "Invalid session.event.v2 payload: {error}"
                                ));
                            }
                        }
                        None => self.disconnect("Invalid session.event.v2 payload"),
                    }
                } else if notification.method
                    == whale_protocol::interactions::METHOD_SESSION_INTERACTION_EVENT
                {
                    match notification.params.and_then(|value| {
                        serde_json::from_value::<
                            whale_protocol::interactions::InteractionEventEnvelope,
                        >(value)
                        .ok()
                    }) {
                        Some(event) => {
                            if let Err(error) = self.route_interaction_event(event) {
                                self.disconnect(&format!(
                                    "Invalid session.interaction_event payload: {error}"
                                ));
                            }
                        }
                        None => self.disconnect("Invalid session.interaction_event payload"),
                    }
                }
            }
            JSONRPCMessage::Request(request) => {
                if !self.is_initialized() {
                    contexts::send_reply(
                        writer.clone(),
                        JSONRPCResponse::error(
                            request.id,
                            JSONRPCError {
                                code: whale_protocol::initialization::PROTOCOL_NOT_INITIALIZED,
                                message: "Connection is not initialized".into(),
                                data: None,
                            },
                        ),
                    );
                    return;
                }
                contexts::dispatch_request(self, writer, request);
            }
        }
    }
}

impl ClientState {
    async fn request<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        writer: &Arc<ManagedWriter>,
        method: &str,
        params: Option<P>,
    ) -> Result<R, SdkError> {
        self.require_initialized()?;
        self.request_inner(writer, method, params, None).await
    }
    async fn request_for_session<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        writer: &Arc<ManagedWriter>,
        method: &str,
        params: Option<P>,
        sid: &str,
    ) -> Result<R, SdkError> {
        self.require_initialized()?;
        self.request_inner(writer, method, params, Some(sid)).await
    }
    async fn request_inner<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        writer: &Arc<ManagedWriter>,
        method: &str,
        params: Option<P>,
        sid: Option<&str>,
    ) -> Result<R, SdkError> {
        let access = sid
            .map(SessionRequestAccess::Writable)
            .unwrap_or(SessionRequestAccess::Unscoped);
        self.request_with_remote_error(writer, method, params, access)
            .await
            .map_err(|error| match error {
                InternalRequestError::Sdk(error) => error,
                InternalRequestError::Remote(error) => match error.code {
                    whale_protocol::retention::RUN_EXPIRED => SdkError::RunExpired(error.message),
                    whale_protocol::retention::SESSION_LIMIT_EXCEEDED => {
                        SdkError::LimitExceeded(error.message)
                    }
                    code => SdkError::Rpc {
                        code,
                        message: error.message,
                    },
                },
            })
    }

    pub(crate) async fn request_with_remote_error<
        P: serde::Serialize,
        R: serde::de::DeserializeOwned,
    >(
        &self,
        writer: &Arc<ManagedWriter>,
        method: &str,
        params: Option<P>,
        access: SessionRequestAccess<'_>,
    ) -> Result<R, InternalRequestError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()).into());
        }
        let writable_sid = match access {
            SessionRequestAccess::Writable(sid) => Some(sid),
            SessionRequestAccess::Unscoped => None,
            SessionRequestAccess::Viewable(sid) => {
                self.ensure_session_viewable(sid)?;
                None
            }
        };
        if let Some(sid) = writable_sid {
            self.with_existing_session_open(sid, || ())?;
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = JSONRPCRequest::new(id, method, params)
            .map_err(SdkError::from)
            .map_err(InternalRequestError::Sdk)?;
        let line = serde_json::to_string(&request)
            .map_err(SdkError::from)
            .map_err(InternalRequestError::Sdk)?;
        let (tx, rx) = oneshot::channel();
        let key = id.to_string();
        if let Some(sid) = writable_sid {
            self.with_existing_session_open(sid, || {
                self.pending.insert(key.clone(), tx);
                self.pending_sessions.insert(key.clone(), sid.into());
            })?;
        } else {
            self.pending.insert(key.clone(), tx);
        }
        struct PendingGuard<'a> {
            state: &'a ClientState,
            key: String,
        }
        impl Drop for PendingGuard<'_> {
            fn drop(&mut self) {
                self.state.pending.remove(&self.key);
                self.state.pending_sessions.remove(&self.key);
            }
        }
        let _guard = PendingGuard { state: self, key };
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()).into());
        }
        let request = async {
            writer.send_line(&line).await.map_err(SdkError::from)?;
            match rx.await {
                Ok(Ok(value)) => serde_json::from_value(value)
                    .map_err(SdkError::from)
                    .map_err(InternalRequestError::Sdk),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(InternalRequestError::Sdk(SdkError::ChannelClosed(
                    "Response channel closed".into(),
                ))),
            }
        };
        let released = writable_sid.map(|sid| self.session_released(sid));
        let result = tokio::select! {
            biased;
            _ = async {
                match &released {
                    Some(released) => released.cancelled().await,
                    None => futures::future::pending::<()>().await,
                }
            } => Err(InternalRequestError::Sdk(SdkError::SessionClosed(writable_sid.unwrap().into()))),
            result = tokio::time::timeout(Duration::from_secs(60), request) =>
                result.map_err(|_| InternalRequestError::Sdk(SdkError::RequestTimeout(method.into())))?,
        };
        if let Some(sid) = writable_sid {
            if self.session_closed(sid) {
                return Err(SdkError::SessionClosed(sid.into()).into());
            }
        }
        result
    }
}

enum WriterTarget {
    Channel(mpsc::Sender<String>),
    Io(FrameWriter),
}
struct ManagedWriter {
    target: Mutex<Option<WriterTarget>>,
    shutdown: watch::Sender<bool>,
}
impl ManagedWriter {
    fn new(target: WriterTarget) -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        Arc::new(Self {
            target: Mutex::new(Some(target)),
            shutdown,
        })
    }
    fn channel(tx: mpsc::Sender<String>) -> Arc<Self> {
        Self::new(WriterTarget::Channel(tx))
    }
    fn io(io: impl AsyncWrite + Send + Unpin + 'static) -> Arc<Self> {
        Self::new(WriterTarget::Io(FrameWriter::new(io)))
    }
    async fn close(&self) {
        self.shutdown.send_replace(true);
        match self.target.lock().await.take() {
            Some(WriterTarget::Io(io)) => io.close().await,
            Some(WriterTarget::Channel(sender)) => drop(sender),
            None => {}
        }
    }
}
#[async_trait]
impl OutgoingTransport for ManagedWriter {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        let closed = || std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Client closed");
        if *shutdown.borrow() {
            return Err(closed());
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => Err(closed()),
            result = async {
                match self.target.lock().await.as_mut() {
                    Some(WriterTarget::Channel(tx)) => tx.send(line.to_owned()).await.map_err(|_| closed()),
                    Some(WriterTarget::Io(io)) => io.send_line(line).await,
                    None => Err(closed()),
                }
            } => result,
        }
    }
}

struct ClientInner {
    state: Arc<ClientState>,
    writer: Arc<ManagedWriter>,
    // Legacy constructors keep the strong owner here. Runtime construction will
    // return it separately while client clones retain only ClientState's weak stop.
    compatibility_owner: Option<connection::ConnectionOwner>,
}
impl Drop for ClientInner {
    fn drop(&mut self) {
        self.state.disconnect("Client dropped");
    }
}

#[derive(Clone)]
pub struct WhaleClient {
    inner: Arc<ClientInner>,
}

impl WhaleClient {
    pub fn in_process(server: Arc<DaemonServer>) -> Self {
        connection::open_embedded(server, connection::LEGACY_SHUTDOWN_TIMEOUT).into_legacy()
    }

    pub async fn spawn_daemon(path: impl AsRef<Path>) -> Result<Self, SdkError> {
        Self::spawn_daemon_with_args(path, &[]).await
    }

    /// Starts an owned stdio daemon with explicit CLI configuration, for example
    /// `&["--session-store", "/path/to/sessions.sqlite"]`.
    pub async fn spawn_daemon_with_args(
        path: impl AsRef<Path>,
        args: &[&str],
    ) -> Result<Self, SdkError> {
        let mut child = Command::new(path.as_ref())
            .args(["--listen", "stdio"])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SdkError::Internal("Missing daemon stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SdkError::Internal("Missing daemon stdout".into()))?;
        Ok(Self::with_io(stdout, ManagedWriter::io(stdin), Some(child)))
    }

    pub async fn connect_uds(path: impl AsRef<Path>) -> Result<Self, SdkError> {
        let (read, write) = UnixStream::connect(path).await?.into_split();
        Ok(Self::with_io(read, ManagedWriter::io(write), None))
    }

    fn with_io(
        read: impl tokio::io::AsyncRead + Send + Unpin + 'static,
        writer: Arc<ManagedWriter>,
        child: Option<Child>,
    ) -> Self {
        connection::open_io(read, writer, child, connection::LEGACY_SHUTDOWN_TIMEOUT).into_legacy()
    }

    async fn request<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Option<P>,
    ) -> Result<R, SdkError> {
        self.initialize().await?;
        self.inner
            .state
            .request(&self.inner.writer, method, params)
            .await
    }

    async fn request_for_session<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Option<P>,
        sid: &str,
    ) -> Result<R, SdkError> {
        self.initialize().await?;
        self.inner
            .state
            .request_for_session(&self.inner.writer, method, params, sid)
            .await
    }

    pub async fn create_thread(
        &self,
        model: impl Into<String>,
        system_prompt: Option<String>,
    ) -> Result<WhaleThread, SdkError> {
        let result: StartThreadResult = self
            .request(
                METHOD_SESSION_START_THREAD,
                Some(StartThreadParams {
                    limits: None,
                    agent_name: None,
                    context_policy: None,
                    provider_config: None,
                    provider_ref: None,
                    options: None,
                    session_id: None,
                    provider: None,
                    model: model.into(),
                    system_prompt,
                    tools: Vec::new(),
                    metadata: serde_json::Map::new(),
                }),
            )
            .await?;
        self.inner.state.ensure_session_open(&result.thread_id)?;
        Ok(WhaleThread {
            recovery_key: None,
            client: self.clone(),
            thread_id: result.thread_id,
            max_steps: 10,
            timeout_ms: None,
        })
    }

    pub async fn resolve_approval(
        &self,
        request_id: impl Into<String>,
        decision: ApprovalDecision,
        feedback: Option<String>,
    ) -> Result<bool, SdkError> {
        let request_id = request_id.into();
        let sid = self
            .inner
            .state
            .approval_sessions
            .get(&request_id)
            .map(|entry| entry.value().clone());
        if let Some((sid, turn_id)) = sid.as_ref().and_then(|sid| {
            self.inner
                .state
                .approval_turns
                .get(&request_id)
                .map(|turn| (sid.clone(), turn.value().clone()))
        }) {
            let supports_interactions =
                self.initialize()
                    .await?
                    .capabilities
                    .iter()
                    .any(|capability| {
                        capability == whale_protocol::interactions::CAPABILITY_INTERACTIONS
                    });
            if supports_interactions && self.inner.state.interactions_enabled(&sid) {
                return Ok(self
                    .respond_interaction(
                        &sid,
                        &turn_id,
                        &request_id,
                        crate::interactions::legacy_approval_response(decision, feedback),
                    )
                    .await?
                    .resolved);
            }
        }
        let params = Some(ApprovalResolveParams {
            request_id,
            decision,
            feedback,
        });
        let result: ApprovalResolveResult = match sid {
            Some(sid) => {
                self.request_for_session(METHOD_APPROVAL_RESOLVE, params, &sid)
                    .await?
            }
            None => self.request(METHOD_APPROVAL_RESOLVE, params).await?,
        };
        Ok(result.resolved)
    }

    pub async fn get_run(
        &self,
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Result<RunHandle, SdkError> {
        self.initialize().await?;
        let thread_id = thread_id.into();
        let turn_id = turn_id.into();
        let mut session_probe = self.inner.state.probe_session(&thread_id);
        let (state, lease) =
            self.inner
                .state
                .with_existing_session_open(&thread_id, || {
                    match self.inner.state.runs.entry(turn_id.clone()) {
                        dashmap::mapref::entry::Entry::Occupied(entry) => {
                            let state = entry.get().clone();
                            let lease = run::QueryRouteLease::acquire(
                                self.inner.state.clone(),
                                state.clone(),
                            );
                            (state, lease)
                        }
                        dashmap::mapref::entry::Entry::Vacant(entry) => {
                            if let Some(state) = self.inner.state.cached_run(&turn_id) {
                                let lease = run::QueryRouteLease::acquire(
                                    self.inner.state.clone(),
                                    state.clone(),
                                );
                                if state.pending_query() {
                                    entry.insert(state.clone());
                                }
                                (state, lease)
                            } else {
                                let state =
                                    run::RunState::new_query(thread_id.clone(), turn_id.clone());
                                self.inner.state.cache_run(&state);
                                let lease = run::QueryRouteLease::acquire(
                                    self.inner.state.clone(),
                                    state.clone(),
                                );
                                entry.insert(state.clone());
                                (state, lease)
                            }
                        }
                    }
                })?;
        if state.thread_id != thread_id {
            return Err(SdkError::Internal("Run belongs to another session".into()));
        }
        let handle = RunHandle {
            client: self.clone(),
            state,
        };
        let snapshot = handle.snapshot().await?;
        lease.observe(&snapshot);
        session_probe.commit();
        self.inner.state.ensure_session_open(&thread_id)?;
        Ok(handle)
    }

    /// Closes this connection and every handle sharing it; does not stop another client's daemon.
    pub async fn close(&self) {
        self.inner.state.disconnect("Client closed");
        if let Some(owner) = &self.inner.compatibility_owner {
            let _ = owner.shutdown("Client closed").await;
        } else if let Some(stop) = self.inner.state.connection.get() {
            let _ = stop.shutdown("Client closed").await;
        } else {
            self.inner.writer.close().await;
        }
    }
}

#[derive(Clone)]
pub struct WhaleThread {
    recovery_key: Option<RecoveryKey>,
    client: WhaleClient,
    thread_id: String,
    max_steps: usize,
    timeout_ms: Option<u64>,
}
impl WhaleThread {
    pub async fn close(&self) -> Result<bool, SdkError> {
        self.client.close_session(self.thread_id.clone()).await
    }
    pub fn id(&self) -> &str {
        &self.thread_id
    }

    /// Registers one session tool, serializing updates of the same name until
    /// the daemon acknowledges each update. Once dispatched, dropping this
    /// future stops waiting but does not cancel the registration transaction:
    /// it commits on success or restores the old binding on an explicit rejection.
    /// Current daemons route each invocation by its immutable binding ID; earlier
    /// successful handlers stay alive until session or connection cleanup for in-flight calls.
    /// Older peers without binding IDs use the latest name binding.
    /// A timeout or transport failure leaves the remote outcome unknown and closes
    /// the shared connection, so later calls cannot use inconsistent bindings.
    pub async fn register_tool(&self, tool: Arc<dyn HostTool>) -> Result<(), SdkError> {
        self.client.initialize().await?;
        self.client
            .inner
            .state
            .ensure_session_open(&self.thread_id)?;
        let tool_name = tool.name().to_string();
        self.client
            .inner
            .state
            .ensure_dynamic_tool_name_available(&self.thread_id, &tool_name)?;
        let key = (self.thread_id.clone(), tool_name.clone());
        let binding_id = uuid::Uuid::new_v4().to_string();
        let def = RegisterToolDefinition {
            binding_id: Some(binding_id.clone()),
            name: tool_name,
            description: tool.description().into(),
            parameters: tool.parameters(),
            supports_parallel: tool.supports_parallel(),
            require_approval: tool.require_approval(),
            is_host_tool: true,
        };
        let lock = self
            .client
            .inner
            .state
            .with_session_open(&self.thread_id, || {
                self.client
                    .inner
                    .state
                    .registration_locks
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            })?;
        // Waiting for a preceding transaction is cancellable without changing
        // bindings or sending another RPC. The background task owns the lock
        // after this point, even if its caller stops waiting.
        let registration = lock.lock_owned().await;
        self.client
            .inner
            .state
            .ensure_session_open(&self.thread_id)?;
        let client = self.client.clone();
        let thread_id = self.thread_id.clone();
        let (reply, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _registration = registration;
            if client.inner.state.closed.load(Ordering::SeqCst) {
                let _ = reply.send(Err(SdkError::ChannelClosed("Client is closed".into())));
                return;
            }
            let binding_key = (thread_id.clone(), binding_id);
            let old = match client.inner.state.with_session_open(&thread_id, || {
                client
                    .inner
                    .state
                    .tool_bindings
                    .insert(binding_key.clone(), tool.clone());
                client.inner.state.tools.insert(key.clone(), tool)
            }) {
                Ok(old) => old,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
            };
            let result: Result<RegisterToolsResult, SdkError> = client
                .request_for_session(
                    METHOD_SESSION_REGISTER_TOOLS,
                    Some(RegisterToolsParams {
                        thread_id: thread_id.clone(),
                        tools: vec![def],
                    }),
                    &thread_id,
                )
                .await;
            let result = match result {
                Ok(_) if client.inner.state.session_closed(&thread_id) => {
                    Err(SdkError::SessionClosed(thread_id.clone()))
                }
                Ok(_) => Ok(()),
                Err(error @ SdkError::SessionClosed(_)) => Err(error),
                Err(error @ SdkError::Rpc { .. })
                    if !client.inner.state.closed.load(Ordering::SeqCst) =>
                {
                    client.inner.state.tool_bindings.remove(&binding_key);
                    // Hold this shard while checking shutdown: disconnect's
                    // clear must not race with reinserting a retired callback.
                    client.inner.state.while_session_exists(&thread_id, || {
                        match client.inner.state.tools.entry(key) {
                            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                                if client.inner.state.closed.load(Ordering::SeqCst) || old.is_none()
                                {
                                    entry.remove();
                                } else if let Some(old) = old {
                                    entry.insert(old);
                                }
                            }
                            dashmap::mapref::entry::Entry::Vacant(entry) => {
                                if !client.inner.state.closed.load(Ordering::SeqCst) {
                                    if let Some(old) = old {
                                        entry.insert(old);
                                    }
                                }
                            }
                        }
                    });
                    Err(error)
                }
                Err(error) => {
                    client.inner.state.tool_bindings.remove(&binding_key);
                    client.inner.state.tools.remove(&key);
                    client
                        .inner
                        .state
                        .disconnect("Tool registration outcome is unknown");
                    client.close().await;
                    Err(error)
                }
            };
            let _ = reply.send(result);
        });
        receiver
            .await
            .map_err(|_| SdkError::ChannelClosed("Registration task stopped".into()))?
    }

    pub async fn start_turn(&self, text: &str) -> Result<RunHandle, SdkError> {
        self.start_turn_with_items(
            vec![CanonicalItem::user_text(text)],
            None,
            self.max_steps,
            self.timeout_ms,
        )
        .await
    }

    pub async fn start_turn_with_options(
        &self,
        text: &str,
        options: RunTurnOptions,
    ) -> Result<RunHandle, SdkError> {
        self.start_turn_with_items(
            vec![CanonicalItem::user_text(text)],
            Some(options),
            self.max_steps,
            self.timeout_ms,
        )
        .await
    }

    pub async fn start_turn_with_items(
        &self,
        input_items: Vec<CanonicalItem>,
        options: Option<RunTurnOptions>,
        max_steps: usize,
        timeout_ms: Option<u64>,
    ) -> Result<RunHandle, SdkError> {
        self.start_turn_internal(
            input_items,
            options,
            max_steps,
            timeout_ms.or(self.timeout_ms),
            false,
        )
        .await
    }

    async fn start_turn_internal(
        &self,
        input_items: Vec<CanonicalItem>,
        options: Option<RunTurnOptions>,
        max_steps: usize,
        timeout_ms: Option<u64>,
        legacy_buffer: bool,
    ) -> Result<RunHandle, SdkError> {
        self.client.initialize().await?;
        self.client
            .inner
            .state
            .ensure_session_open(&self.thread_id)?;
        let turn_id = uuid::Uuid::new_v4().to_string();
        let state = run::RunState::new(self.thread_id.clone(), turn_id.clone());
        if legacy_buffer {
            state.buffer_legacy_events();
        }
        self.client
            .inner
            .state
            .with_session_open(&self.thread_id, || {
                self.client.inner.state.cache_run(&state);
                self.client
                    .inner
                    .state
                    .runs
                    .insert(turn_id.clone(), state.clone());
            })?;
        let mut pending_route =
            run::PendingStartRoute::new(self.client.inner.state.clone(), state.clone());
        let result: Result<StartTurnResult, SdkError> = self
            .client
            .request_for_session(
                METHOD_THREAD_START_TURN,
                Some(StartTurnParams {
                    thread_id: self.thread_id.clone(),
                    turn_id: turn_id.clone(),
                    input_items,
                    options,
                    max_steps,
                    timeout_ms,
                }),
                &self.thread_id,
            )
            .await;
        match result {
            Ok(accepted) if accepted.turn_id == turn_id && accepted.thread_id == self.thread_id => {
                pending_route.commit();
                Ok(RunHandle {
                    client: self.client.clone(),
                    state,
                })
            }
            Ok(_) => Err(SdkError::Internal(
                "Daemon returned a different run identity".into(),
            )),
            Err(error) => Err(error),
        }
    }

    /// Compatibility helper that buffers events while running. Prefer `start_turn` for streaming and approvals.
    pub async fn run_turn(
        &self,
        text: &str,
    ) -> Result<(RunTurnResult, mpsc::Receiver<AgentStreamEvent>), SdkError> {
        // This legacy return type exposes events only after completion, so it
        // necessarily retains the full turn. Install collection before acceptance.
        let run = self
            .start_turn_internal(
                vec![CanonicalItem::user_text(text)],
                None,
                self.max_steps,
                self.timeout_ms,
                true,
            )
            .await?;
        let result = run.result().await;
        let mut buffered = Vec::new();
        for event in run.state.take_legacy_events() {
            match event.payload {
                RunEventPayload::Stream { event } => buffered.push(event),
                RunEventPayload::Finished { snapshot } => {
                    if snapshot.status == RunStatus::Completed {
                        buffered.push(AgentStreamEvent::TurnCompleted {
                            turn_id: snapshot.turn_id,
                            thread_id: snapshot.thread_id,
                            usage: snapshot.usage,
                        });
                    } else {
                        let error = snapshot.error.unwrap_or(RunFailure {
                            code: "cancelled".into(),
                            message: "Run cancelled".into(),
                        });
                        buffered.push(AgentStreamEvent::TurnFailed {
                            turn_id: snapshot.turn_id,
                            thread_id: snapshot.thread_id,
                            error_code: error.code,
                            error_message: error.message,
                        });
                    }
                }
            }
        }
        let (tx, rx) = mpsc::channel(buffered.len().max(1));
        for event in buffered {
            let _ = tx.try_send(event);
        }
        Ok((result?, rx))
    }
}

#[cfg(test)]
mod tool_registration_tests {
    use super::*;
    use serde_json::json;

    struct NamedTool(&'static str);
    #[async_trait]
    impl HostTool for NamedTool {
        fn name(&self) -> &str {
            "lookup"
        }
        fn description(&self) -> &str {
            self.0
        }
        fn parameters(&self) -> Value {
            json!({"type":"object"})
        }
        async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
            Ok(CanonicalToolOutput::text(self.0))
        }
    }
    async fn fixture() -> (WhaleThread, mpsc::Receiver<String>) {
        let state = ClientState::new();
        let (tx, mut rx) = mpsc::channel(16);
        let client = WhaleClient {
            inner: Arc::new(ClientInner {
                state,
                writer: ManagedWriter::channel(tx),
                compatibility_owner: None,
            }),
        };
        crate::initialization_tests::initialize_peer(&client, &mut rx).await;
        (
            WhaleThread {
                recovery_key: None,
                client,
                thread_id: "session".into(),
                max_steps: 10,
                timeout_ms: None,
            },
            rx,
        )
    }
    fn install(thread: &WhaleThread, label: &'static str) {
        thread.client.inner.state.tools.insert(
            ("session".into(), "lookup".into()),
            Arc::new(NamedTool(label)),
        );
        thread.client.inner.state.tool_bindings.insert(
            ("session".into(), "old-version".into()),
            Arc::new(NamedTool(label)),
        );
    }
    fn bound(thread: &WhaleThread) -> Option<String> {
        thread
            .client
            .inner
            .state
            .tools
            .get(&("session".into(), "lookup".into()))
            .map(|tool| tool.description().to_owned())
    }
    async fn request(rx: &mut mpsc::Receiver<String>) -> JSONRPCRequest {
        serde_json::from_str(
            &tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }
    fn answer(thread: &WhaleThread, request: JSONRPCRequest, error: bool) {
        let response = if error {
            JSONRPCResponse::error(
                request.id,
                JSONRPCError::invalid_params("Rejected registration"),
            )
        } else {
            JSONRPCResponse::success(
                request.id,
                RegisterToolsResult {
                    registered_count: 1,
                },
            )
            .unwrap()
        };
        thread.client.inner.state.incoming(
            &serde_json::to_string(&response).unwrap(),
            &thread.client.inner.writer,
        );
    }

    #[tokio::test]
    async fn dropped_registration_waiter_still_commits_an_accepted_binding() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let registering = thread.clone();
        let task =
            tokio::spawn(
                async move { registering.register_tool(Arc::new(NamedTool("new"))).await },
            );
        let req = request(&mut rx).await;
        task.abort();
        let _ = task.await;
        assert!(
            !thread.client.inner.state.pending.is_empty(),
            "registration transaction must survive its caller"
        );
        answer(&thread, req, false);
        tokio::task::yield_now().await;
        assert_eq!(bound(&thread).as_deref(), Some("new"));
        assert_eq!(thread.client.inner.state.tool_bindings.len(), 2);
        assert!(thread.client.inner.state.pending.is_empty());
        thread.client.close().await;
    }

    #[tokio::test]
    async fn dropped_registration_waiter_rolls_back_a_rejected_binding() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let registering = thread.clone();
        let task =
            tokio::spawn(
                async move { registering.register_tool(Arc::new(NamedTool("new"))).await },
            );
        let req = request(&mut rx).await;
        task.abort();
        let _ = task.await;
        answer(&thread, req, true);
        tokio::task::yield_now().await;
        assert_eq!(bound(&thread).as_deref(), Some("old"));
        assert_eq!(thread.client.inner.state.tool_bindings.len(), 1);
        thread.client.close().await;
        assert!(thread.client.inner.state.tool_bindings.is_empty());
    }

    #[tokio::test]
    async fn same_tool_registrations_wait_for_prior_acknowledgement() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let first_thread = thread.clone();
        let first = tokio::spawn(async move {
            first_thread
                .register_tool(Arc::new(NamedTool("rejected")))
                .await
        });
        let first_request = request(&mut rx).await;
        let second_thread = thread.clone();
        let second = tokio::spawn(async move {
            second_thread
                .register_tool(Arc::new(NamedTool("accepted")))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), rx.recv())
                .await
                .is_err(),
            "concurrent register RPC can be applied by daemon in reverse order"
        );
        answer(&thread, first_request, true);
        assert!(first.await.unwrap().is_err());
        let second_request = request(&mut rx).await;
        answer(&thread, second_request, false);
        second.await.unwrap().unwrap();
        assert_eq!(bound(&thread).as_deref(), Some("accepted"));
        thread.client.close().await;
    }

    #[tokio::test]
    async fn later_rejection_keeps_the_last_acknowledged_tool() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let first_thread = thread.clone();
        let first = tokio::spawn(async move {
            first_thread
                .register_tool(Arc::new(NamedTool("accepted")))
                .await
        });
        let first_request = request(&mut rx).await;
        answer(&thread, first_request, false);
        first.await.unwrap().unwrap();
        let second_thread = thread.clone();
        let second = tokio::spawn(async move {
            second_thread
                .register_tool(Arc::new(NamedTool("rejected")))
                .await
        });
        let second_request = request(&mut rx).await;
        answer(&thread, second_request, true);
        assert!(second.await.unwrap().is_err());
        assert_eq!(bound(&thread).as_deref(), Some("accepted"));
        assert!(!thread.client.inner.state.closed.load(Ordering::SeqCst));
        thread.client.close().await;
    }

    #[tokio::test]
    async fn ambiguous_registration_reply_closes_the_connection() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let registering = thread.clone();
        let task =
            tokio::spawn(
                async move { registering.register_tool(Arc::new(NamedTool("new"))).await },
            );
        let req = request(&mut rx).await;
        let reply = JSONRPCResponse::success(req.id, json!({"unexpected":"response"})).unwrap();
        thread.client.inner.state.incoming(
            &serde_json::to_string(&reply).unwrap(),
            &thread.client.inner.writer,
        );
        assert!(task.await.unwrap().is_err());
        assert!(thread.client.inner.state.closed.load(Ordering::SeqCst));
        assert!(bound(&thread).is_none());
        assert!(thread.client.inner.state.tool_bindings.is_empty());
        assert!(thread
            .register_tool(Arc::new(NamedTool("later")))
            .await
            .is_err());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn aborting_a_waiter_behind_registration_sends_no_rpc() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "old");
        let first_thread = thread.clone();
        let first = tokio::spawn(async move {
            first_thread
                .register_tool(Arc::new(NamedTool("accepted")))
                .await
        });
        let first_request = request(&mut rx).await;
        let waiting = thread.clone();
        let second =
            tokio::spawn(
                async move { waiting.register_tool(Arc::new(NamedTool("aborted"))).await },
            );
        tokio::task::yield_now().await;
        second.abort();
        let _ = second.await;
        answer(&thread, first_request, false);
        first.await.unwrap().unwrap();
        tokio::task::yield_now().await;
        assert_eq!(bound(&thread).as_deref(), Some("accepted"));
        assert!(rx.try_recv().is_err());
        thread.client.close().await;
    }

    #[tokio::test]
    async fn registering_during_a_turn_keeps_that_turn_on_its_original_handler() {
        use std::sync::atomic::AtomicUsize;
        use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
        struct VersionTool(&'static str, Arc<std::sync::Mutex<Vec<&'static str>>>);
        #[async_trait]
        impl HostTool for VersionTool {
            fn name(&self) -> &str {
                "lookup"
            }
            fn description(&self) -> &str {
                self.0
            }
            fn parameters(&self) -> Value {
                json!({"type":"object"})
            }
            async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
                self.1.lock().unwrap().push(self.0);
                Ok(CanonicalToolOutput::text(self.0))
            }
        }
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let started = entered.clone();
        let resume = release.clone();
        let turns = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(ApprovalGate::new());
        let engine=AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(Arc::new(ToolRegistry::new()),gate.clone())))
            .with_stream_provider(Arc::new(move |_,step| {
                if step!=0 {
                    return Ok(Box::pin(futures::stream::iter(vec![Ok(AgentStreamEvent::ItemCompleted {
                        turn_id:"model".into(),item:CanonicalItem::assistant_text("done",whale_protocol::MessagePhase::FinalAnswer),
                    }), Ok(AgentStreamEvent::TurnCompleted { turn_id:"model".into(),thread_id:"model".into(),usage:Default::default() })])));
                }
                let index=turns.fetch_add(1,Ordering::SeqCst);let started=started.clone();let resume=resume.clone();
                Ok(Box::pin(async_stream::stream! {
                    if index==0 {started.notify_one();resume.notified().await;}
                    yield Ok(AgentStreamEvent::ItemCompleted {turn_id:"model".into(),item:CanonicalItem::tool_call(format!("call-{index}"),None,"lookup",Some(json!({})),"{}")});
                    yield Ok(AgentStreamEvent::TurnCompleted { turn_id:"model".into(),thread_id:"model".into(),usage:Default::default() });
                }))
            }));
        let client = WhaleClient::in_process(Arc::new(DaemonServer::new(Arc::new(engine), gate)));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut definition = AgentDefinition::new("versions", "fixture");
        definition.tool_names = vec!["lookup".into()];
        let session = client
            .agent(definition, vec![Arc::new(VersionTool("A", seen.clone()))])
            .unwrap()
            .create_session()
            .await
            .unwrap();
        let run = session.start_turn("first").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let registering = session.clone();
        let capture = seen.clone();
        let update = tokio::spawn(async move {
            registering
                .register_tool(Arc::new(VersionTool("B", capture)))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client
                    .inner
                    .state
                    .tools
                    .get(&(session.id().into(), "lookup".into()))
                    .is_some_and(|tool| tool.description() == "B")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !update.is_finished(),
            "daemon registration must wait for the running session"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), run.result())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["A"],
            "in-flight daemon binding was retargeted by speculative name replacement"
        );
        update.await.unwrap().unwrap();
        session
            .start_turn("second")
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec!["A", "B"]);
        assert_eq!(client.inner.state.tool_bindings.len(), 2);
        client.close().await;
        assert!(client.inner.state.tool_bindings.is_empty());
    }

    #[tokio::test]
    async fn unknown_binding_id_must_not_execute_the_name_binding() {
        let (thread, mut rx) = fixture().await;
        install(&thread, "name-fallback");
        thread.client.inner.state.incoming(&json!({"jsonrpc":"2.0","id":"reverse-unknown","method":"tool.execute_host","params":{
            "thread_id":"session","binding_id":"unknown","call_id":"correlation","name":"lookup","arguments":{}
        }}).to_string(),&thread.client.inner.writer);
        let reply: Value = serde_json::from_str(
            &tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            reply["result"]["is_error"], true,
            "unknown immutable binding must not fall back to mutable name routing"
        );
        thread.client.close().await;
    }
}

#[cfg(test)]
mod provider_tests;

#[cfg(test)]
mod initialization_tests;
