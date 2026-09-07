//! Human-in-the-loop (HITL) approval gate.
//!
//! Provides asynchronous approval pausing and resumption for sensitive actions.

use std::sync::Arc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
use whale_protocol::canonical::CanonicalItem;
use whale_protocol::events::AgentStreamEvent;

use crate::error::CoreError;

/// Human decision on a pending tool execution approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Accept the tool execution as originally requested.
    Accept,
    /// Deny the tool execution, optionally with feedback reason.
    Deny {
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Modify the tool arguments before execution.
    ModifyArguments {
        arguments: serde_json::Value,
    },
}

/// Thread-safe coordinator for tracking and resolving pending approvals.
#[derive(Debug, Default)]
pub struct ApprovalGate {
    pending_approvals: Arc<DashMap<String, oneshot::Sender<ApprovalDecision>>>,
}

impl ApprovalGate {
    /// Creates a new approval gate.
    pub fn new() -> Self {
        Self {
            pending_approvals: Arc::new(DashMap::new()),
        }
    }

    /// Requests approval for a tool call, emitting an `ApprovalRequested` event and awaiting human resolution.
    pub async fn request_approval(
        &self,
        turn_id: &str,
        tool_call: &CanonicalItem,
        reason: Option<String>,
        event_tx: &mpsc::Sender<AgentStreamEvent>,
    ) -> Result<ApprovalDecision, CoreError> {
        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending_approvals.insert(request_id.clone(), tx);

        // Emit ApprovalRequested event
        let event = AgentStreamEvent::ApprovalRequested {
            turn_id: turn_id.to_string(),
            request_id: request_id.clone(),
            tool_call: tool_call.clone(),
            reason,
        };

        if event_tx.send(event).await.is_err() {
            self.pending_approvals.remove(&request_id);
            return Err(CoreError::EventChannelClosed);
        }

        // Wait for human decision
        match rx.await {
            Ok(decision) => Ok(decision),
            Err(_) => Err(CoreError::ApprovalChannelClosed(request_id)),
        }
    }

    /// Resolves a pending approval by request ID.
    ///
    /// Returns `true` if the request was found and successfully notified, `false` otherwise.
    pub fn resolve_approval(&self, request_id: &str, decision: ApprovalDecision) -> bool {
        if let Some((_, sender)) = self.pending_approvals.remove(request_id) {
            sender.send(decision).is_ok()
        } else {
            false
        }
    }

    /// Returns the number of currently pending approval requests.
    pub fn pending_count(&self) -> usize {
        self.pending_approvals.len()
    }

    /// Returns all currently pending approval request IDs.
    pub fn pending_request_ids(&self) -> Vec<String> {
        self.pending_approvals.iter().map(|entry| entry.key().clone()).collect()
    }
}
