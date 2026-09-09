//! Host callback contexts. Business resources remain in the host callback object.

use std::sync::{atomic::Ordering, Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::watch;
use whale_protocol::contexts::*;
use whale_protocol::rpc::*;
use whale_protocol::CanonicalToolOutput;

use crate::{ClientState, ManagedWriter, OutgoingTransport, SdkError};

pub use whale_protocol::contexts::{
    ContextBuildRequest, ContextPolicyConfig, ModelContext, RunContextInfo, ToolContextInfo,
};

/// Cooperative cancellation or expiry of a callback's run deadline.
#[derive(Clone)]
pub struct CancellationSignal {
    cancelled: watch::Sender<bool>,
    deadline_unix_ms: Option<u64>,
}

impl CancellationSignal {
    pub(crate) fn new(deadline_unix_ms: Option<u64>) -> Self {
        let (cancelled, _) = watch::channel(false);
        Self {
            cancelled,
            deadline_unix_ms,
        }
    }
    pub(crate) fn cancel(&self) {
        self.cancelled.send_replace(true);
    }
    pub fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_unix_ms
    }
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
            || self
                .deadline_unix_ms
                .is_some_and(|deadline| deadline <= now_ms())
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        if self.is_cancelled() {
            return;
        }
        tokio::select! {
            _ = async { while !*receiver.borrow_and_update() { if receiver.changed().await.is_err() {break;} } } => {},
            _ = async {
                match self.deadline_unix_ms {
                    Some(deadline) => tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now_ms()))).await,
                    None => futures::future::pending::<()>().await,
                }
            } => {},
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct ToolContext {
    info: Option<ToolContextInfo>,
    cancellation: CancellationSignal,
    finished: CancellationSignal,
    correlation_id: String,
    state: Weak<ClientState>,
    writer: Weak<ManagedWriter>,
}

impl ToolContext {
    /// Older daemons may not supply identity; production context-aware requests do.
    pub fn info(&self) -> Option<&ToolContextInfo> {
        self.info.as_ref()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
    pub fn deadline_unix_ms(&self) -> Option<u64> {
        self.cancellation.deadline_unix_ms()
    }
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
    /// True means the daemon accepted progress for this active invocation.
    pub async fn report_progress(
        &self,
        message: impl Into<String>,
        progress: Option<f64>,
    ) -> Result<bool, SdkError> {
        let params = ToolReportProgressParams {
            call_id: self.correlation_id.clone(),
            message: message.into(),
            progress,
        };
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        if self.is_cancelled() || self.finished.is_cancelled() || self.info.is_none() {
            return Ok(false);
        }
        let state = self
            .state
            .upgrade()
            .ok_or_else(|| SdkError::ChannelClosed("Client dropped".into()))?;
        let writer = self
            .writer
            .upgrade()
            .ok_or_else(|| SdkError::ChannelClosed("Client dropped".into()))?;
        let result: ToolReportProgressResult = tokio::select! {
            result=state.request(&writer,METHOD_TOOL_REPORT_PROGRESS,Some(params)) => result?,
            _=self.cancelled() => return Ok(false),
            _=self.finished.cancelled() => return Ok(false),
        };
        Ok(result.accepted)
    }

    /// Suspends this active host-tool invocation until the owning application
    /// answers the generic Interaction. The daemon binds the request to this
    /// exact callback and clears it when the callback finishes or is cancelled.
    pub async fn request_interaction(
        &self,
        request: whale_protocol::interactions::InteractionRequest,
    ) -> Result<serde_json::Value, SdkError> {
        request.validate().map_err(SdkError::InvalidConfiguration)?;
        let info = self.info.as_ref().ok_or_else(|| {
            SdkError::ProtocolCompatibility(
                "The daemon did not supply an Interaction-capable ToolContext identity".into(),
            )
        })?;
        if self.is_cancelled() || self.finished.is_cancelled() {
            return Err(SdkError::ChannelClosed(
                "Host tool callback is no longer active".into(),
            ));
        }
        let state = self
            .state
            .upgrade()
            .ok_or_else(|| SdkError::ChannelClosed("Client dropped".into()))?;
        let writer = self
            .writer
            .upgrade()
            .ok_or_else(|| SdkError::ChannelClosed("Client dropped".into()))?;
        state.require_initialized()?;
        if !state.supports_interactions() {
            return Err(SdkError::ProtocolCompatibility(
                "Daemon does not advertise interactions.v1".into(),
            ));
        }
        if !state.interactions_enabled(&info.run.thread_id) {
            return Err(SdkError::InvalidConfiguration(format!(
                "Interactions are not enabled for Session {}",
                info.run.thread_id
            )));
        }
        let request_id = uuid::Uuid::new_v4().to_string();
        let params = whale_protocol::interactions::RequestInteractionParams::new(
            info.run.thread_id.clone(),
            info.run.turn_id.clone(),
            self.correlation_id.clone(),
            request_id.clone(),
            request,
        )
        .map_err(SdkError::InvalidConfiguration)?;
        let result: whale_protocol::interactions::InteractionResponse = tokio::select! {
            biased;
            _ = self.cancelled() => {
                return Err(SdkError::ChannelClosed("Host tool callback was cancelled".into()));
            }
            _ = self.finished.cancelled() => {
                return Err(SdkError::ChannelClosed("Host tool callback finished".into()));
            }
            result = state.request_for_session(
                &writer,
                whale_protocol::interactions::METHOD_TURN_REQUEST_INTERACTION,
                Some(params),
                &info.run.thread_id,
            ) => result?,
        };
        result.validate().map_err(SdkError::Internal)?;
        if result.request_id != request_id {
            return Err(SdkError::Internal(
                "Daemon returned a different nested Interaction identity".into(),
            ));
        }
        Ok(result.response)
    }
}

#[async_trait]
pub trait HostContextPolicy: Send + Sync {
    /// Builds a model-only projection from a copy of committed canonical history.
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationSignal,
    ) -> Result<ModelContext, String>;
}

pub(crate) fn handle_notification(state: &ClientState, notification: &JSONRPCNotification) -> bool {
    let parsed = match notification.method.as_str() {
        METHOD_TOOL_CANCEL_HOST => notification
            .params
            .clone()
            .and_then(|p| serde_json::from_value::<ToolCancelHostParams>(p).ok())
            .map(|p| (false, p.call_id)),
        METHOD_CONTEXT_CANCEL_HOST => notification
            .params
            .clone()
            .and_then(|p| serde_json::from_value::<ContextCancelHostParams>(p).ok())
            .map(|p| (true, p.request_id)),
        _ => return false,
    };
    if let Some((context, key)) = parsed {
        let registry = if context {
            &state.context_callbacks
        } else {
            &state.tool_callbacks
        };
        if let Some(cancellation) = registry.get(&key) {
            cancellation.cancel();
        }
    }
    true
}

#[derive(Clone)]
pub(crate) struct ScopedCallback {
    pub(crate) session_id: Option<String>,
    cancellation: CancellationSignal,
    stop: CancellationSignal,
    pub(crate) finished: CancellationSignal,
}
impl ScopedCallback {
    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }
    #[cfg(test)]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
    pub(crate) fn stop(&self) {
        self.cancel();
        self.stop.cancel();
    }
}

struct InvocationGuard {
    state: Arc<ClientState>,
    key: String,
    context: bool,
    finished: Option<CancellationSignal>,
}
impl Drop for InvocationGuard {
    fn drop(&mut self) {
        let registry = if self.context {
            &self.state.context_callbacks
        } else {
            &self.state.tool_callbacks
        };
        registry.remove(&self.key);
        if let Some(finished) = &self.finished {
            finished.cancel();
        }
    }
}

pub(crate) fn send_reply(writer: Arc<ManagedWriter>, reply: JSONRPCResponse) {
    tokio::spawn(async move {
        if let Ok(line) = serde_json::to_string(&reply) {
            let _ = writer.send_line(&line).await;
        }
    });
}

/// Register invocation state on the reader before scheduling user code. A cancel
/// notification immediately following the request must never overtake registration.
pub(crate) fn dispatch_request(
    state: &Arc<ClientState>,
    writer: &Arc<ManagedWriter>,
    request: JSONRPCRequest,
) {
    if request.method == METHOD_CONTEXT_BUILD_HOST {
        dispatch_context(state, writer, request);
        return;
    }
    let id = request.id;
    if request.method != METHOD_TOOL_EXECUTE_HOST {
        send_reply(
            writer.clone(),
            JSONRPCResponse::error(id, JSONRPCError::method_not_found(&request.method)),
        );
        return;
    }
    let params = match request
        .params
        .and_then(|p| serde_json::from_value::<ToolExecuteHostParams>(p).ok())
    {
        Some(params) => params,
        None => {
            send_reply(
                writer.clone(),
                JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Invalid tool.execute_host parameters"),
                ),
            );
            return;
        }
    };
    if params.context.as_ref().is_some_and(|context| {
        context.run.thread_id.is_empty()
            || context.run.turn_id.is_empty()
            || context.call_id.is_empty()
            || params
                .thread_id
                .as_ref()
                .is_some_and(|thread| thread != &context.run.thread_id)
    }) {
        send_reply(
            writer.clone(),
            JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("Invalid tool execution identity"),
            ),
        );
        return;
    }
    let cancellation =
        CancellationSignal::new(params.context.as_ref().and_then(|c| c.run.deadline_unix_ms));
    let thread = params
        .thread_id
        .as_ref()
        .or_else(|| params.context.as_ref().map(|c| &c.run.thread_id));
    let tool = if let Some(binding_id) = params.binding_id.as_ref() {
        // An explicit version is authoritative. Unknown versions, including
        // requests lacking session identity, must never execute a name fallback.
        thread.and_then(|thread| {
            state
                .tool_bindings
                .get(&(thread.clone(), binding_id.clone()))
                .map(|tool| tool.value().clone())
        })
    } else if let Some(thread) = thread {
        state
            .tools
            .get(&(thread.clone(), params.name.clone()))
            .map(|v| v.value().clone())
    } else {
        let mut bindings = state
            .tools
            .iter()
            .filter(|t| t.key().1 == params.name)
            .map(|t| t.value().clone());
        let first = bindings.next();
        if bindings.next().is_none() {
            first
        } else {
            None
        }
    };
    let scope_id = thread.cloned().or_else(|| {
        if params.binding_id.is_some() {
            return None;
        }
        let mut bindings = state
            .tools
            .iter()
            .filter(|tool| tool.key().1 == params.name)
            .map(|tool| tool.key().0.clone());
        let first = bindings.next();
        if bindings.next().is_none() {
            first
        } else {
            None
        }
    });
    let invocation = match insert_invocation(
        state,
        &state.tool_callbacks,
        &params.call_id,
        &cancellation,
        scope_id,
    ) {
        Ok(invocation) => invocation,
        Err(error) => {
            send_reply(
                writer.clone(),
                JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
            );
            return;
        }
    };
    let guard = InvocationGuard {
        state: state.clone(),
        key: params.call_id.clone(),
        context: false,
        finished: Some(invocation.finished.clone()),
    };
    let context = ToolContext {
        info: params.context.clone(),
        cancellation: cancellation.clone(),
        finished: invocation.finished.clone(),
        correlation_id: params.call_id.clone(),
        state: Arc::downgrade(state),
        writer: Arc::downgrade(writer),
    };
    let state = state.clone();
    let writer = writer.clone();
    tokio::spawn(async move {
        let _guard = guard;
        let mut shutdown = state.shutdown.subscribe();
        if cancellation.is_cancelled() || invocation.stop.is_cancelled() || *shutdown.borrow() {
            return;
        }
        let output = match tool {
            Some(tool) => tokio::select! {
                result=std::panic::AssertUnwindSafe(tool.execute_with_context(context,params.arguments)).catch_unwind()=>
                    result.unwrap_or_else(|_|Err("Host tool panicked".into())),
                _=shutdown.changed()=>return,
                _=invocation.stop.cancelled()=>return,
            },
            None => Err(format!(
                "Host tool '{}' is not bound to this session",
                params.name
            )),
        };
        if cancellation.is_cancelled() || state.closed.load(Ordering::SeqCst) {
            return;
        }
        let (output, is_error) = match output {
            Ok(output) => (output, false),
            Err(error) => (CanonicalToolOutput::text(error), true),
        };
        let reply = JSONRPCResponse::success(
            id,
            ToolExecuteHostResult {
                call_id: params.call_id,
                output,
                is_error,
            },
        )
        .unwrap();
        if let Ok(line) = serde_json::to_string(&reply) {
            // FrameWriter owns a complete frame once queued; stopping this wait
            // cannot truncate it, and teardown must not wait on transport capacity.
            tokio::select! {
                biased;
                _ = invocation.stop.cancelled() => {},
                _ = shutdown.changed() => {},
                _ = writer.send_line(&line) => {},
            }
        }
    });
}

fn insert_invocation(
    state: &ClientState,
    registry: &dashmap::DashMap<String, ScopedCallback>,
    key: &str,
    cancellation: &CancellationSignal,
    session_id: Option<String>,
) -> Result<ScopedCallback, String> {
    let invocation = ScopedCallback {
        session_id: session_id.clone(),
        cancellation: cancellation.clone(),
        stop: CancellationSignal::new(None),
        finished: CancellationSignal::new(None),
    };
    let install = || match registry.entry(key.into()) {
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(invocation.clone());
            Ok(invocation)
        }
        dashmap::mapref::entry::Entry::Occupied(_) => Err("Duplicate host invocation".into()),
    };
    match session_id {
        Some(sid) => state
            .with_session_open(&sid, install)
            .map_err(|error| error.to_string())?,
        None => install(),
    }
}

fn dispatch_context(
    state: &Arc<ClientState>,
    writer: &Arc<ManagedWriter>,
    request: JSONRPCRequest,
) {
    let id = request.id;
    let key = match &id {
        RequestId::String(key) => key.clone(),
        RequestId::Number(key) => key.to_string(),
    };
    let params = match request
        .params
        .and_then(|p| serde_json::from_value::<ContextBuildRequest>(p).ok())
    {
        Some(params) => params,
        None => {
            send_reply(
                writer.clone(),
                JSONRPCResponse::error(
                    id,
                    JSONRPCError::invalid_params("Invalid context.build_host parameters"),
                ),
            );
            return;
        }
    };
    let policy = state
        .context_policies
        .get(&params.context.thread_id)
        .map(|p| p.value().clone());
    let Some(policy) = policy else {
        send_reply(
            writer.clone(),
            JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("No host context policy bound to this session"),
            ),
        );
        return;
    };
    let cancellation = CancellationSignal::new(params.context.deadline_unix_ms);
    let invocation = match insert_invocation(
        state,
        &state.context_callbacks,
        &key,
        &cancellation,
        Some(params.context.thread_id.clone()),
    ) {
        Ok(invocation) => invocation,
        Err(error) => {
            send_reply(
                writer.clone(),
                JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
            );
            return;
        }
    };
    let guard = InvocationGuard {
        state: state.clone(),
        key,
        context: true,
        finished: Some(invocation.finished.clone()),
    };
    let state = state.clone();
    let writer = writer.clone();
    tokio::spawn(async move {
        let _guard = guard;
        let mut shutdown = state.shutdown.subscribe();
        if cancellation.is_cancelled() || invocation.stop.is_cancelled() || *shutdown.borrow() {
            return;
        }
        let output = tokio::select! {
            result=std::panic::AssertUnwindSafe(policy.build(params,cancellation.clone())).catch_unwind()=>
                result.unwrap_or_else(|_|Err("Host context policy panicked".into())),
            _=shutdown.changed()=>return,
            _=invocation.stop.cancelled()=>return,
        };
        if cancellation.is_cancelled() || state.closed.load(Ordering::SeqCst) {
            return;
        }
        let reply = match output {
            Ok(context) => JSONRPCResponse::success(id, context).unwrap(),
            Err(error) => JSONRPCResponse::error(id, JSONRPCError::internal_error(error)),
        };
        if let Ok(line) = serde_json::to_string(&reply) {
            // FrameWriter owns a complete frame once queued; stopping this wait
            // cannot truncate it, and teardown must not wait on transport capacity.
            tokio::select! {
                biased;
                _ = invocation.stop.cancelled() => {},
                _ = shutdown.changed() => {},
                _ = writer.send_line(&line) => {},
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HostTool;
    use serde_json::json;
    use std::sync::Mutex;

    struct Capture(Arc<Mutex<Option<ToolContext>>>);
    #[async_trait]
    impl HostTool for Capture {
        fn name(&self) -> &str {
            "lookup"
        }
        fn description(&self) -> &str {
            "lookup"
        }
        fn parameters(&self) -> Value {
            json!({})
        }
        async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
            unreachable!()
        }
        async fn execute_with_context(
            &self,
            context: ToolContext,
            _: Value,
        ) -> Result<CanonicalToolOutput, String> {
            *self.0.lock().unwrap() = Some(context);
            Ok(CanonicalToolOutput::text("ok"))
        }
    }
    use serde_json::Value;
    fn request() -> String {
        json!({"jsonrpc":"2.0","id":"reverse","method":"tool.execute_host","params":{
            "thread_id":"s","call_id":"correlation","name":"lookup","arguments":{},
            "context":{"thread_id":"s","turn_id":"r","call_id":"model-call"}
        }})
        .to_string()
    }

    #[tokio::test]
    async fn successful_callback_expires_progress_without_reporting_cancellation() {
        let state = ClientState::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let writer = ManagedWriter::channel(tx);
        let client = crate::WhaleClient {
            inner: Arc::new(crate::ClientInner {
                state: state.clone(),
                writer: writer.clone(),
                compatibility_owner: None,
            }),
        };
        crate::initialization_tests::initialize_peer(&client, &mut rx).await;
        let captured = Arc::new(Mutex::new(None));
        state.tools.insert(
            ("s".into(), "lookup".into()),
            Arc::new(Capture(captured.clone())),
        );
        state.incoming(&request(), &writer);
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::task::yield_now().await;
        let context = captured.lock().unwrap().clone().unwrap();
        assert!(
            !context.is_cancelled(),
            "normal completion is not a cancellation"
        );
        assert!(!context.report_progress("late", None).await.unwrap());
        assert!(state.tool_callbacks.is_empty());
    }

    #[tokio::test]
    async fn cancellation_immediately_after_request_prevents_callback_dispatch() {
        let state = ClientState::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let writer = ManagedWriter::channel(tx);
        let client = crate::WhaleClient {
            inner: Arc::new(crate::ClientInner {
                state: state.clone(),
                writer: writer.clone(),
                compatibility_owner: None,
            }),
        };
        crate::initialization_tests::initialize_peer(&client, &mut rx).await;
        let captured = Arc::new(Mutex::new(None));
        state.tools.insert(
            ("s".into(), "lookup".into()),
            Arc::new(Capture(captured.clone())),
        );
        state.incoming(&request(), &writer);
        assert!(state.tool_callbacks.contains_key("correlation"));
        state.incoming(&json!({"jsonrpc":"2.0","method":"tool.cancel_host","params":{"call_id":"correlation"}}).to_string(),&writer);
        tokio::task::yield_now().await;
        assert!(captured.lock().unwrap().is_none());
        assert!(state.tool_callbacks.is_empty());
    }

    #[tokio::test]
    async fn local_deadline_signals_without_a_daemon_notification() {
        let signal = CancellationSignal::new(Some(now_ms() + 10));
        tokio::time::timeout(Duration::from_secs(1), signal.cancelled())
            .await
            .unwrap();
        assert!(signal.is_cancelled());
    }
}
