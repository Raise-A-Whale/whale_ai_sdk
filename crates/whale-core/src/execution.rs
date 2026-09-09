//! Cooperative execution cancellation and per-invocation identity.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;
use whale_protocol::{
    contexts::ToolContextInfo, events::AgentStreamEvent, interactions::InteractionRequest,
};

use crate::{
    error::CoreError,
    interaction::{validate_interaction_request, validate_interaction_response, InteractionBridge},
};

#[derive(Clone, Debug)]
pub struct CancellationToken(watch::Sender<bool>);
impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}
impl CancellationToken {
    pub fn new() -> Self {
        Self(watch::channel(false).0)
    }
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

/// Local resources stay in the handler; only `info` is portable.
#[derive(Clone)]
pub struct ToolContext {
    pub info: ToolContextInfo,
    pub cancellation: CancellationToken,
    active: Arc<AtomicBool>,
    finished: watch::Sender<bool>,
    events: Option<mpsc::Sender<AgentStreamEvent>>,
    interaction_bridge: Option<Arc<dyn InteractionBridge>>,
}
impl ToolContext {
    pub fn new(
        info: ToolContextInfo,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<AgentStreamEvent>>,
    ) -> Self {
        let (finished, _) = watch::channel(false);
        Self {
            info,
            cancellation,
            active: Arc::new(AtomicBool::new(true)),
            finished,
            events,
            interaction_bridge: None,
        }
    }
    pub fn with_interaction_bridge(mut self, bridge: Arc<dyn InteractionBridge>) -> Self {
        self.interaction_bridge = Some(bridge);
        self
    }
    pub async fn request_interaction(
        &self,
        request: InteractionRequest,
    ) -> Result<serde_json::Value, CoreError> {
        validate_interaction_request(&request)?;
        if !self.is_active() {
            return Err(CoreError::InteractionCancelled(
                "Tool invocation is no longer active".into(),
            ));
        }
        let bridge = self.interaction_bridge.as_ref().ok_or_else(|| {
            CoreError::InteractionUnavailable("No Interaction bridge is configured".into())
        })?;
        let request_id = Uuid::new_v4().to_string();
        let response_contract = request.clone();
        let ticket = tokio::select! {
            biased;
            _ = self.cancelled() => {
                return Err(CoreError::InteractionCancelled("Run was cancelled".into()));
            }
            _ = self.origin_finished() => {
                return Err(CoreError::InteractionCancelled("Tool invocation finished".into()));
            }
            result = bridge.begin(&self.info.run, request_id.clone(), request) => result?,
        };
        if ticket.request_id() != request_id {
            return Err(CoreError::InteractionUnavailable(format!(
                "Interaction bridge returned ticket '{}' for request '{request_id}'",
                ticket.request_id()
            )));
        }
        if !self.is_active() {
            drop(ticket);
            return Err(CoreError::InteractionCancelled(
                "Tool invocation is no longer active".into(),
            ));
        }
        let response = ticket.response();
        tokio::pin!(response);
        tokio::select! {
            biased;
            _ = self.cancelled() => {
                Err(CoreError::InteractionCancelled("Run was cancelled".into()))
            }
            _ = self.origin_finished() => {
                Err(CoreError::InteractionCancelled("Tool invocation finished".into()))
            }
            response = &mut response => {
                let response = response?;
                validate_interaction_response(&response_contract, &response)?;
                Ok(response)
            },
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
    pub fn finish(&self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.finished.send_replace(true);
        }
    }
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire) && !self.is_cancelled()
    }
    async fn origin_finished(&self) {
        let mut receiver = self.finished.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    }

    /// Waits until this exact tool invocation has completed or been cancelled.
    ///
    /// Runtime bridges use this to bind a nested host request to the lifetime
    /// of the reverse-RPC callback that created it.
    pub async fn finished(&self) {
        self.origin_finished().await;
    }
    pub async fn report_progress(
        &self,
        message: impl Into<String>,
        progress: Option<f64>,
    ) -> Result<bool, String> {
        if progress.is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v)) {
            return Err("Progress must be finite and between 0 and 1".into());
        }
        if !self.is_active() {
            return Ok(false);
        }
        let Some(tx) = &self.events else {
            return Ok(false);
        };
        let permit = tokio::select! { _ = self.cancelled() => return Ok(false), permit = tx.reserve() => permit.map_err(|_| "Run event stream closed".to_owned())? };
        if !self.is_active() {
            return Ok(false);
        }
        permit.send(AgentStreamEvent::ToolProgress {
            turn_id: self.info.run.turn_id.clone(),
            call_id: self.info.call_id.clone(),
            message: message.into(),
            progress,
        });
        Ok(true)
    }
}

pub(crate) struct InvocationGuard(pub ToolContext);
impl Drop for InvocationGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}
pub(crate) struct CancelOnDrop(pub CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct NoExternalSchemas;
impl jsonschema::Retrieve for NoExternalSchemas {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("External schema references are disabled".into())
    }
}
/// Compiles schemas without HTTP or filesystem retrieval, including custom retrievers.
pub fn compile_tool_schema(schema: &serde_json::Value) -> Result<jsonschema::Validator, String> {
    jsonschema::options()
        .with_retriever(NoExternalSchemas)
        .build(schema)
        .map_err(|error| format!("Invalid tool parameter schema: {error}"))
}
