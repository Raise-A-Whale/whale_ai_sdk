//! Portable execution identity and model-context projection contracts.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::CanonicalItem;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunContextInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolContextInfo {
    #[serde(flatten)]
    pub run: RunContextInfo,
    /// The model's canonical tool-call ID, distinct from the reverse RPC key.
    pub call_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextPolicyConfig {
    #[default]
    FullHistory,
    RecentTurns {
        max_turns: usize,
    },
    Host,
}

impl ContextPolicyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if matches!(self, Self::RecentTurns { max_turns: 0 }) {
            return Err("Context policy max_turns must be positive".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextBuildRequest {
    pub context: RunContextInfo,
    /// Zero-based model step within the outer run.
    pub step_index: usize,
    pub model: String,
    pub system_prompt: Option<String>,
    pub history: Vec<CanonicalItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelContext {
    /// Complete projected instructions; null explicitly selects no instructions.
    pub system_prompt: Option<String>,
    pub items: Vec<CanonicalItem>,
}

pub const METHOD_TOOL_CANCEL_HOST: &str = "tool.cancel_host";
pub const METHOD_TOOL_REPORT_PROGRESS: &str = "tool.report_progress";
pub const METHOD_CONTEXT_BUILD_HOST: &str = "context.build_host";
pub const METHOD_CONTEXT_CANCEL_HOST: &str = "context.cancel_host";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCancelHostParams {
    /// The unique correlation key from ToolExecuteHostParams, not a model ID.
    pub call_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolReportProgressParams {
    pub call_id: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
}

impl ToolReportProgressParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.call_id.is_empty() {
            return Err("Progress requires a host invocation call_id".into());
        }
        if self
            .progress
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        {
            return Err("Progress must be finite and between 0 and 1".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolReportProgressResult {
    pub accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCancelHostParams {
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecutionRecord {
    pub call_id: String,
    pub original_arguments: Value,
    pub arguments: Value,
}
