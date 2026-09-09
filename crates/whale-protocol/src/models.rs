//! Model selection and capability inspection shared by language clients.

use serde::{Deserialize, Serialize};

use crate::agents::ProviderConfig;

pub const METHOD_PROVIDER_INSPECT: &str = "provider.inspect";

/// Whether the descriptor is a wire implementation ceiling or model-specific.
/// Protocol scope does not claim every model on a remote endpoint supports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCapabilityScope {
    Protocol,
    Model,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelContentKind {
    Text,
    Image,
    Audio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOption {
    Temperature,
    MaxTokens,
    ReasoningEffort,
    ThinkingBudget,
    PromptCaching,
}

/// Supported input forms and options. Structured tool results use text in the
/// existing HTTP adapters. Content support is positional, not a global flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCapabilities {
    pub scope: ModelCapabilityScope,
    pub user_content: Vec<ModelContentKind>,
    pub assistant_content: Vec<ModelContentKind>,
    pub tool_result_content: Vec<ModelContentKind>,
    pub tool_calls: bool,
    pub reasoning_text: bool,
    pub reasoning_signatures: bool,
    pub encrypted_reasoning: bool,
    pub options: Vec<ModelOption>,
}

impl ModelCapabilities {
    /// Conservative descriptor for a native text model. Enable each additional
    /// capability explicitly when implementing a provider.
    pub fn text_only() -> Self {
        Self {
            scope: ModelCapabilityScope::Model,
            user_content: vec![ModelContentKind::Text],
            assistant_content: vec![ModelContentKind::Text],
            tool_result_content: vec![ModelContentKind::Text],
            tool_calls: false,
            reasoning_text: false,
            reasoning_signatures: false,
            encrypted_reasoning: false,
            options: Vec::new(),
        }
    }
}

/// A reference selects a daemon-registered factory. It cannot be combined with
/// either legacy protocol-family inference or explicit HTTP configuration.
pub fn validate_provider_selection(
    provider_ref: Option<&str>,
    provider: Option<&str>,
    provider_config: Option<&ProviderConfig>,
) -> Result<(), String> {
    if let Some(reference) = provider_ref {
        if reference.trim().is_empty() || reference.trim() != reference {
            return Err("provider_ref must be nonempty without surrounding whitespace".into());
        }
        if provider.is_some() || provider_config.is_some() {
            return Err("provider_ref conflicts with provider or provider_config".into());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectProviderParams {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_config: Option<ProviderConfig>,
}

impl InspectProviderParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.model.trim().is_empty() {
            return Err("Model must not be empty".into());
        }
        validate_provider_selection(
            self.provider_ref.as_deref(),
            self.provider.as_deref(),
            self.provider_config.as_ref(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectProviderResult {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    pub capabilities: ModelCapabilities,
}
