//! Core traits and common types for LLM protocol adapters.

use futures::Stream;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;
use whale_protocol::canonical::CanonicalItem;
use whale_protocol::events::AgentStreamEvent;

/// Pinned, boxed asynchronous stream of `AgentStreamEvent` items.
pub type BoxedEventStream =
    Pin<Box<dyn Stream<Item = Result<AgentStreamEvent, AdapterError>> + Send>>;

/// Errors that can occur during protocol adaptation, serialization, or streaming.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// Serialization error when preparing request payloads.
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Network or HTTP transport error.
    #[error("HTTP transport error: {0}")]
    HttpError(#[from] reqwest::Error),

    /// Error encountered during Server-Sent Events (SSE) parsing.
    #[error("SSE stream parse error: {0}")]
    StreamParseError(String),

    /// API error response returned by the provider.
    #[error("API error from provider {provider}: status={status}, message={message}")]
    ApiError {
        provider: &'static str,
        status: u16,
        message: String,
    },

    /// Protocol incompatibility or unexpected payload structure.
    #[error("Protocol error: {0}")]
    ProtocolError(String),
}

/// Tool/function definition conforming to canonical agent capabilities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// Tool function name.
    pub name: String,
    /// Detailed description of the tool.
    pub description: String,
    /// JSON Schema describing tool parameters.
    pub parameters: serde_json::Value,
}

impl ToolDefinition {
    /// Creates a new tool definition.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// Model sampling and execution options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SamplingOptions {
    /// Target model identifier (e.g., "claude-3-7-sonnet-20250219", "gpt-4o").
    pub model: String,
    /// Sampling temperature (0.0 - 2.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Maximum number of tokens to generate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Reasoning effort level for reasoning models (e.g. "low", "medium", "high").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Thinking budget tokens (e.g., for Anthropic extended thinking).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_budget: Option<u32>,
    /// Whether prompt caching (e.g., Anthropic ephemeral cache) is enabled.
    #[serde(default)]
    pub prompt_caching: bool,
}

impl SamplingOptions {
    /// Creates default sampling options for a given model.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            temperature: None,
            max_tokens: None,
            reasoning_effort: None,
            thinking_budget: None,
            prompt_caching: false,
        }
    }
}

/// Abstract protocol adapter trait implemented by provider-specific adapters (Anthropic, OpenAI, etc.).
pub trait ProtocolAdapter: Send + Sync {
    /// Legacy custom adapters default to text-only; override to declare their wire subset.
    fn capabilities(&self) -> whale_protocol::models::ModelCapabilities {
        let mut capabilities = whale_protocol::models::ModelCapabilities::text_only();
        capabilities.scope = whale_protocol::models::ModelCapabilityScope::Protocol;
        capabilities
    }

    /// Identifier for the adapter provider.
    fn provider_name(&self) -> &'static str;

    /// Provider HTTP endpoint URL for request dispatch.
    fn endpoint_url(&self) -> String {
        String::new()
    }

    /// Serializes canonical conversation items, tools, and options into provider-specific JSON request body and headers.
    fn serialize_request(
        &self,
        system_prompt: Option<&str>,
        history: &[CanonicalItem],
        tools: &[ToolDefinition],
        options: &SamplingOptions,
    ) -> Result<(serde_json::Value, HeaderMap), AdapterError>;

    /// Transforms a raw incoming SSE byte stream into a stream of canonical `AgentStreamEvent`s.
    fn parse_stream(
        &self,
        byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    ) -> BoxedEventStream;
}
