//! Human-in-the-loop (HITL) approval gate.
//!
//! Provides asynchronous approval pausing and resumption for sensitive actions.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{
    fmt,
    sync::{Arc, RwLock},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
use whale_protocol::canonical::CanonicalItem;
use whale_protocol::contexts::RunContextInfo;
use whale_protocol::events::AgentStreamEvent;
use whale_protocol::interactions::{
    InteractionRequest, INTERACTION_REMOVAL_PUBLICATION_FAILED, KIND_TOOL_APPROVAL,
};

use crate::error::CoreError;
use crate::execution::CancellationToken;
use crate::interaction::{
    validate_interaction_request, validate_interaction_response, InteractionBridge,
};

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
    ModifyArguments { arguments: serde_json::Value },
}

/// Thread-safe coordinator for tracking and resolving pending approvals.
pub struct ApprovalGate {
    pending_approvals: Arc<DashMap<String, PendingApproval>>,
    interaction_bridge: RwLock<Option<Arc<dyn InteractionBridge>>>,
}

enum PendingApproval {
    Legacy(oneshot::Sender<ApprovalDecision>),
    Bridged {
        bridge: Arc<dyn InteractionBridge>,
        context: RunContextInfo,
    },
}

impl fmt::Debug for ApprovalGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApprovalGate")
            .field("pending_approvals", &self.pending_approvals.len())
            .field(
                "has_interaction_bridge",
                &self.interaction_bridge.read().unwrap().is_some(),
            )
            .finish()
    }
}

impl Default for ApprovalGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ApprovalGate {
    /// Creates a new approval gate.
    pub fn new() -> Self {
        Self {
            pending_approvals: Arc::new(DashMap::new()),
            interaction_bridge: RwLock::new(None),
        }
    }

    /// Adds the optional generic Interaction producer bridge.
    pub fn with_interaction_bridge(self, bridge: Arc<dyn InteractionBridge>) -> Self {
        *self.interaction_bridge.write().unwrap() = Some(bridge);
        self
    }

    /// Installs the process runtime's bridge on an already shared gate.
    ///
    /// The daemon constructs its engine and gate as one graph, so installation
    /// must preserve the `Arc<ApprovalGate>` already held by the coordinator.
    pub fn install_interaction_bridge(&self, bridge: Arc<dyn InteractionBridge>) {
        *self.interaction_bridge.write().unwrap() = Some(bridge);
    }

    pub(crate) fn interaction_bridge(&self) -> Option<Arc<dyn InteractionBridge>> {
        self.interaction_bridge.read().unwrap().clone()
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

        self.pending_approvals
            .insert(request_id.clone(), PendingApproval::Legacy(tx));
        let _cleanup = PendingApprovalCleanup {
            pending: self.pending_approvals.clone(),
            id: request_id.clone(),
        };

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

    pub(crate) async fn request_approval_with_context(
        &self,
        context: &RunContextInfo,
        cancellation: &CancellationToken,
        tool_call: &CanonicalItem,
        original_arguments: &Value,
        reason: Option<String>,
        event_tx: &mpsc::Sender<AgentStreamEvent>,
    ) -> Result<ApprovalDecision, CoreError> {
        let Some(bridge) = self.interaction_bridge() else {
            return self
                .request_approval(&context.turn_id, tool_call, reason, event_tx)
                .await;
        };

        let request_id = Uuid::new_v4().to_string();
        let request =
            tool_approval_request(tool_call, original_arguments.clone(), reason.as_deref())?;
        validate_interaction_request(&request)?;
        let response_contract = request.clone();
        let ticket = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(CoreError::InteractionCancelled("Run was cancelled".into()));
            }
            result = bridge.begin(context, request_id.clone(), request) => result?,
        };
        if ticket.request_id() != request_id {
            return Err(CoreError::InteractionUnavailable(format!(
                "Interaction bridge returned ticket '{}' for request '{request_id}'",
                ticket.request_id()
            )));
        }

        self.pending_approvals.insert(
            request_id.clone(),
            PendingApproval::Bridged {
                bridge: bridge.clone(),
                context: context.clone(),
            },
        );
        let _cleanup = PendingApprovalCleanup {
            pending: self.pending_approvals.clone(),
            id: request_id.clone(),
        };
        let event = AgentStreamEvent::ApprovalRequested {
            turn_id: context.turn_id.clone(),
            request_id: request_id.clone(),
            tool_call: tool_call.clone(),
            reason,
        };
        let send_result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                drop(ticket);
                return Err(CoreError::InteractionCancelled("Run was cancelled".into()));
            }
            result = event_tx.send(event) => result,
        };
        if send_result.is_err() {
            bridge.clear(context, &request_id, INTERACTION_REMOVAL_PUBLICATION_FAILED);
            drop(ticket);
            return Err(CoreError::EventChannelClosed);
        }

        let response = ticket.response();
        tokio::pin!(response);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                Err(CoreError::InteractionCancelled("Run was cancelled".into()))
            }
            value = &mut response => {
                let value = value?;
                validate_interaction_response(&response_contract, &value)?;
                parse_tool_approval_response(value)
            }
        }
    }

    /// Resolves a pending approval by request ID.
    ///
    /// Returns `true` if the request was found and its current resolver accepted
    /// the decision, `false` otherwise.
    ///
    /// A bridged resolver may finish projection and event publication
    /// asynchronously after this method returns. Application hosts that need the
    /// committed outcome should await the daemon RPC through the Rust SDK instead.
    pub fn resolve_approval(&self, request_id: &str, decision: ApprovalDecision) -> bool {
        enum Target {
            Legacy,
            Bridged {
                bridge: Arc<dyn InteractionBridge>,
                context: RunContextInfo,
            },
        }

        let target = {
            let Some(pending) = self.pending_approvals.get(request_id) else {
                return false;
            };
            match pending.value() {
                PendingApproval::Legacy(_) => Target::Legacy,
                PendingApproval::Bridged { bridge, context } => Target::Bridged {
                    bridge: bridge.clone(),
                    context: context.clone(),
                },
            }
        };

        match target {
            Target::Legacy => self
                .pending_approvals
                .remove_if(request_id, |_, pending| {
                    matches!(pending, PendingApproval::Legacy(_))
                })
                .is_some_and(|(_, pending)| match pending {
                    PendingApproval::Legacy(sender) => sender.send(decision).is_ok(),
                    PendingApproval::Bridged { .. } => unreachable!("remove_if checked variant"),
                }),
            Target::Bridged { bridge, context } => bridge
                .respond(&context, request_id, approval_decision_response(decision))
                .is_ok(),
        }
    }

    /// Returns the number of currently pending approval requests.
    pub fn pending_count(&self) -> usize {
        self.pending_approvals.len()
    }

    /// Returns all currently pending approval request IDs.
    pub fn pending_request_ids(&self) -> Vec<String> {
        self.pending_approvals
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

fn approval_decision_response(decision: ApprovalDecision) -> Value {
    match decision {
        ApprovalDecision::Accept => json!({"decision":"approve"}),
        ApprovalDecision::Deny { reason } => {
            let mut response = Map::new();
            response.insert("decision".into(), Value::String("reject".into()));
            if let Some(reason) = reason {
                response.insert("feedback".into(), Value::String(reason));
            }
            Value::Object(response)
        }
        ApprovalDecision::ModifyArguments { arguments } => {
            json!({"decision":"modify_arguments","arguments":arguments})
        }
    }
}

fn tool_approval_request(
    tool_call: &CanonicalItem,
    original_arguments: Value,
    reason: Option<&str>,
) -> Result<InteractionRequest, CoreError> {
    let mut payload = Map::new();
    payload.insert("tool_call".into(), serde_json::to_value(tool_call)?);
    payload.insert("original_arguments".into(), original_arguments);
    if let Some(reason) = reason {
        payload.insert("reason".into(), Value::String(reason.into()));
    }
    InteractionRequest::new(
        KIND_TOOL_APPROVAL,
        "Tool approval",
        Value::Object(payload),
        Some(tool_approval_response_schema()),
    )
    .map_err(CoreError::InteractionRequestInvalid)
}

fn tool_approval_response_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "oneOf": [
            {
                "type": "object",
                "properties": {"decision": {"const": "approve"}},
                "required": ["decision"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": {
                    "decision": {"const": "reject"},
                    "feedback": {"type": "string"}
                },
                "required": ["decision"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": {
                    "decision": {"const": "modify_arguments"},
                    "arguments": {},
                    "feedback": {"type": "string"}
                },
                "required": ["decision", "arguments"],
                "additionalProperties": false
            }
        ]
    })
}

#[derive(Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
enum ToolApprovalResponse {
    Approve,
    Reject {
        #[serde(default)]
        feedback: Option<String>,
    },
    ModifyArguments {
        arguments: Value,
        #[serde(default)]
        feedback: Option<String>,
    },
}

fn parse_tool_approval_response(value: Value) -> Result<ApprovalDecision, CoreError> {
    match serde_json::from_value::<ToolApprovalResponse>(value)
        .map_err(|error| CoreError::InteractionResponseInvalid(error.to_string()))?
    {
        ToolApprovalResponse::Approve => Ok(ApprovalDecision::Accept),
        ToolApprovalResponse::Reject { feedback } => {
            Ok(ApprovalDecision::Deny { reason: feedback })
        }
        ToolApprovalResponse::ModifyArguments {
            arguments,
            feedback,
        } => {
            drop(feedback);
            Ok(ApprovalDecision::ModifyArguments { arguments })
        }
    }
}

struct PendingApprovalCleanup {
    pending: Arc<DashMap<String, PendingApproval>>,
    id: String,
}
impl Drop for PendingApprovalCleanup {
    fn drop(&mut self) {
        self.pending.remove(&self.id);
    }
}
