//! Portable Agent configuration. Host functions and credentials are bound separately.

use crate::rpc::RunTurnOptions;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderApi {
    OpenaiChatCompletions,
    OpenaiResponses,
    AnthropicMessages,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAuth {
    Env { variable: String },
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub api: ProviderApi,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Omitted means the provider's conventional environment variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ProviderAuth>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<crate::retention::SessionLimits>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<crate::contexts::ContextPolicyConfig>,
    pub name: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_config: Option<ProviderConfig>,
    /// Reference to a provider registered in the daemon at startup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    #[serde(default)]
    pub tool_names: Vec<String>,
    #[serde(default)]
    pub default_options: RunTurnOptions,
    #[serde(default = "default_max_steps")]
    pub max_steps: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

fn default_max_steps() -> usize {
    10
}

impl AgentDefinition {
    pub fn new(name: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            limits: None,
            context_policy: None,
            name: name.into(),
            model: model.into(),
            system_prompt: None,
            provider_config: None,
            provider_ref: None,
            tool_names: Vec::new(),
            default_options: RunTurnOptions::default(),
            max_steps: 10,
            timeout_ms: None,
        }
    }

    /// Validates portable structure only; the runtime validates endpoint/auth at session creation.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(limits) = &self.limits {
            limits.validate()?;
        }
        crate::models::validate_provider_selection(
            self.provider_ref.as_deref(),
            None,
            self.provider_config.as_ref(),
        )?;
        if let Some(policy) = &self.context_policy {
            policy.validate()?;
        }
        if self.name.trim().is_empty() || self.model.trim().is_empty() {
            return Err("Agent name and model must not be empty".into());
        }
        if self.max_steps == 0 || self.timeout_ms == Some(0) {
            return Err("max_steps and timeout_ms must be positive".into());
        }
        let mut names = std::collections::HashSet::new();
        for name in &self.tool_names {
            if name.trim().is_empty() || !names.insert(name) {
                return Err("Tool names must be nonempty and unique".into());
            }
        }
        if let Some(value) = self.default_options.temperature {
            if !value.is_finite() || !(0.0..=2.0).contains(&value) {
                return Err("temperature must be finite and between 0 and 2".into());
            }
        }
        if self.default_options.max_tokens == Some(0) {
            return Err("max_tokens must be positive".into());
        }
        if self.default_options.thinking_budget == Some(0) {
            return Err("thinking_budget must be positive".into());
        }
        if self
            .default_options
            .model
            .as_ref()
            .is_some_and(|v| v.trim().is_empty())
        {
            return Err("Default model override must not be empty".into());
        }
        Ok(())
    }
}
