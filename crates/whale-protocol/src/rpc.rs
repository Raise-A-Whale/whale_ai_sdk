//! JSON-RPC 2.0 protocol specifications and specific RPC schemas for Whale AI SDK.
//!
//! Covers client-to-daemon and daemon-to-host reverse RPC schemas.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{CanonicalItem, CanonicalToolOutput};
use crate::events::{AgentStreamEvent, UsageMetrics};

/// JSON-RPC 2.0 request identifier (integer or string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    /// Numeric request identifier.
    Number(i64),
    /// String request identifier.
    String(String),
}

impl From<i64> for RequestId {
    fn from(n: i64) -> Self {
        Self::Number(n)
    }
}

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

/// Standard JSON-RPC 2.0 Request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCRequest {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JSONRPCRequest {
    /// Constructs a new JSON-RPC 2.0 request with typed parameters.
    pub fn new<P: Serialize>(
        id: impl Into<RequestId>,
        method: impl Into<String>,
        params: Option<P>,
    ) -> Result<Self, serde_json::Error> {
        let params_val = match params {
            Some(p) => Some(serde_json::to_value(p)?),
            None => None,
        };
        Ok(Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            method: method.into(),
            params: params_val,
        })
    }
}

/// Standard JSON-RPC 2.0 Error Object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JSONRPCError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JSONRPCError {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL_ERROR: i64 = -32603;

    pub fn new(code: i64, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            code,
            message: message.into(),
            data,
        }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            Self::METHOD_NOT_FOUND,
            format!("Method not found: {}", method),
            None,
        )
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self::new(Self::INVALID_PARAMS, msg, None)
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self::new(Self::INTERNAL_ERROR, msg, None)
    }
}

/// Standard JSON-RPC 2.0 Response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JSONRPCError>,
}

impl JSONRPCResponse {
    /// Creates a successful response.
    pub fn success<R: Serialize>(
        id: impl Into<RequestId>,
        result: R,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            result: Some(serde_json::to_value(result)?),
            error: None,
        })
    }

    /// Creates an error response.
    pub fn error(id: impl Into<RequestId>, error: JSONRPCError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            result: None,
            error: Some(error),
        }
    }
}

/// Standard JSON-RPC 2.0 Notification (no id field).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JSONRPCNotification {
    /// Constructs a new notification with typed parameters.
    pub fn new<P: Serialize>(
        method: impl Into<String>,
        params: Option<P>,
    ) -> Result<Self, serde_json::Error> {
        let params_val = match params {
            Some(p) => Some(serde_json::to_value(p)?),
            None => None,
        };
        Ok(Self {
            jsonrpc: "2.0".to_string(),
            method: method.into(),
            params: params_val,
        })
    }
}

/// Top-level JSON-RPC 2.0 message container.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JSONRPCMessage {
    Request(JSONRPCRequest),
    Response(JSONRPCResponse),
    Notification(JSONRPCNotification),
}

impl JSONRPCMessage {
    pub fn jsonrpc(&self) -> &str {
        match self {
            Self::Request(r) => &r.jsonrpc,
            Self::Response(r) => &r.jsonrpc,
            Self::Notification(n) => &n.jsonrpc,
        }
    }
}

// ============================================================================
// RPC Schema Definitions
// ============================================================================

/// Method: "session.start_thread"
pub const METHOD_SESSION_START_THREAD: &str = "session.start_thread";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartThreadParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<crate::retention::SessionLimits>,
    /// Selects a registered model implementation instead of HTTP configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<crate::contexts::ContextPolicyConfig>,
    /// Explicit wire protocol, endpoint and credential reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_config: Option<crate::agents::ProviderConfig>,
    /// Session generation defaults; individual turns may override them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<RunTurnOptions>,
    /// Optional parent session identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Optional provider configuration (e.g. "anthropic", "openai"). If omitted, inferred from model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Model provider configuration or identifier (e.g., "claude-3-7-sonnet", "gpt-4o").
    pub model: String,
    /// System prompt instructions for the thread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Initial tools (including host tools) available to this thread.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<RegisterToolDefinition>,
    /// Additional metadata for the thread.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartThreadResult {
    pub thread_id: String,
    pub created_at: String,
}

/// Method: "thread.run_turn"
pub const METHOD_THREAD_RUN_TURN: &str = "thread.run_turn";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTurnParams {
    pub thread_id: String,
    /// Input items for this turn (e.g. UserMessage).
    pub input_items: Vec<CanonicalItem>,
    /// Optional overrides for model parameters (temperature, max_tokens, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<RunTurnOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunTurnOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_budget: Option<u32>,
    /// None inherits the Session default; false explicitly disables caching.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_caching: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTurnResult {
    pub turn_id: String,
    pub thread_id: String,
    pub status: TurnStatus,
    pub items: Vec<CanonicalItem>,
    pub usage: UsageMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Completed,
    Failed,
    Interrupted,
    RequiresApproval,
}

/// Method: "turn.stream_events" (Notification method used by daemon to stream events to client)
pub const METHOD_TURN_STREAM_EVENTS: &str = "turn.stream_events";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEventsParams {
    pub turn_id: String,
    pub thread_id: String,
    pub event: AgentStreamEvent,
}

/// Reverse RPC Method: "tool.execute_host"
///
/// Sent by whale-daemon to the Rust host requesting application-side execution
/// of a tool, such as a product integration or host file operation.
pub const METHOD_TOOL_EXECUTE_HOST: &str = "tool.execute_host";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecuteHostParams {
    /// Exact host callback version. When present, clients must not fall back to name routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<crate::contexts::ToolContextInfo>,
    /// Session scope for resolving a host tool; omitted by legacy peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub call_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub name: String,
    pub arguments: Value,
}

/// Host response result for "tool.execute_host" or method "tool.execute_host_result"
pub const METHOD_TOOL_EXECUTE_HOST_RESULT: &str = "tool.execute_host_result";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecuteHostResult {
    pub call_id: String,
    pub output: CanonicalToolOutput,
    pub is_error: bool,
}

/// Method: "approval.resolve"
///
/// Sent by client to daemon to accept or reject a pending Human-In-The-Loop approval request.
pub const METHOD_APPROVAL_RESOLVE: &str = "approval.resolve";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalResolveParams {
    pub request_id: String,
    pub decision: ApprovalDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalResolveResult {
    pub resolved: bool,
    pub request_id: String,
}

/// Tool definition parameter for dynamic registration via RPC ("session.register_tools").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterToolDefinition {
    /// Opaque host binding identity, separate from the name shown to the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<String>,
    pub name: String,
    pub description: String,
    pub parameters: Value,
    #[serde(default = "default_true")]
    pub supports_parallel: bool,
    #[serde(default)]
    pub require_approval: bool,
    #[serde(default)]
    pub is_host_tool: bool,
}

fn default_true() -> bool {
    true
}

/// Method: "session.register_tools"
pub const METHOD_SESSION_REGISTER_TOOLS: &str = "session.register_tools";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterToolsParams {
    pub thread_id: String,
    pub tools: Vec<RegisterToolDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterToolsResult {
    pub registered_count: usize,
}
