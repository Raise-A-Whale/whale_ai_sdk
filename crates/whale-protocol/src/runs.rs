//! Application-facing asynchronous turn lifecycle.
//!
//! A run is the execution of one turn. These envelopes keep model-step events
//! separate from the authoritative terminal snapshot of the outer run.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::CanonicalItem;
use crate::events::{AgentStreamEvent, UsageMetrics};
use crate::rpc::{RunTurnOptions, RunTurnResult};

pub const METHOD_THREAD_START_TURN: &str = "thread.start_turn";
pub const METHOD_TURN_GET: &str = "turn.get";
pub const METHOD_TURN_CANCEL: &str = "turn.cancel";
pub const METHOD_TURN_RESOLVE_APPROVAL: &str = "turn.resolve_approval";
pub const METHOD_TURN_EVENT: &str = "turn.event";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartTurnParams {
    pub thread_id: String,
    /// Client-generated identity lets the SDK install routing before dispatch.
    pub turn_id: String,
    pub input_items: Vec<CanonicalItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<RunTurnOptions>,
    #[serde(default = "default_max_steps")]
    pub max_steps: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

fn default_max_steps() -> usize {
    10
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTurnResult {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRefParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    WaitingApproval,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingApproval {
    pub request_id: String,
    pub tool_call: CanonicalItem,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSnapshot {
    #[serde(default)]
    pub tool_executions: Vec<crate::contexts::ToolExecutionRecord>,
    pub thread_id: String,
    pub turn_id: String,
    pub status: RunStatus,
    pub items: Vec<CanonicalItem>,
    pub usage: UsageMetrics,
    pub pending_approvals: Vec<PendingApproval>,
    pub last_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<RunTurnResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RunFailure>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub thread_id: String,
    pub turn_id: String,
    pub seq: u64,
    #[serde(flatten)]
    pub payload: RunEventPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEventPayload {
    Stream { event: AgentStreamEvent },
    Finished { snapshot: RunSnapshot },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunApprovalDecision {
    Approve,
    Reject,
    ModifyArguments,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunApprovalParams {
    pub thread_id: String,
    pub turn_id: String,
    pub request_id: String,
    pub decision: RunApprovalDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}
