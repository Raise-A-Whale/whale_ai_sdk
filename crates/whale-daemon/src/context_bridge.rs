//! Connection-scoped host model-context callbacks.
use crate::transport::{AnyTransportWriter, OutgoingTransport};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use whale_core::{context::ContextPolicy, execution::CancellationToken};
use whale_protocol::{
    contexts::*,
    rpc::{JSONRPCNotification, JSONRPCRequest},
};

pub(crate) type PendingContexts =
    Arc<DashMap<String, oneshot::Sender<Result<ModelContext, String>>>>;
pub(crate) struct HostContextPolicy {
    pub transport: AnyTransportWriter,
    pub pending: PendingContexts,
}
#[async_trait]
impl ContextPolicy for HostContextPolicy {
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelContext, String> {
        if cancellation.is_cancelled() {
            return Err("Context construction cancelled".into());
        }
        let id = format!(
            "context_{}:{}",
            self.transport.connection_id(),
            uuid::Uuid::new_v4()
        );
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id.clone(), tx);
        let _guard = ContextCleanup {
            pending: self.pending.clone(),
            id: id.clone(),
            transport: self.transport.clone(),
        };
        let request = JSONRPCRequest::new(id, METHOD_CONTEXT_BUILD_HOST, Some(request))
            .map_err(|e| e.to_string())?;
        self.transport
            .send_line(&serde_json::to_string(&request).map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err("Context construction cancelled".into()),
            result = rx => result.map_err(|_| "Host context callback disconnected".to_owned())?,
        }
    }
}
struct ContextCleanup {
    pending: PendingContexts,
    id: String,
    transport: AnyTransportWriter,
}
impl Drop for ContextCleanup {
    fn drop(&mut self) {
        if self.pending.remove(&self.id).is_some() {
            send_cancel(
                self.transport.clone(),
                METHOD_CONTEXT_CANCEL_HOST,
                serde_json::json!({"request_id": self.id}),
            );
        }
    }
}
/// Cancellation cleanup cannot await or share the cancelled run future.
pub(crate) fn send_cancel(
    transport: AnyTransportWriter,
    method: &'static str,
    params: serde_json::Value,
) {
    let notification =
        JSONRPCNotification::new(method, Some(params)).expect("JSON cancellation notification");
    let line = serde_json::to_string(&notification).expect("JSON cancellation notification");
    tokio::spawn(async move {
        let _ = transport.send_line(&line).await;
    });
}
