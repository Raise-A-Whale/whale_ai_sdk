//! Transport-independent model steps. Runtime identity and approvals belong to Engine.
use crate::CancellationToken;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};
use thiserror::Error;
use whale_adapters::{AdapterError, SamplingOptions, ToolDefinition};
use whale_protocol::contexts::{ModelContext, RunContextInfo};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};
pub use whale_protocol::models::ModelCapabilities;
use whale_protocol::{CanonicalItem, MessagePhase};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ModelRequest {
    pub context: RunContextInfo,
    pub step_index: usize,
    pub step_id: String,
    pub model_context: ModelContext,
    pub tools: Vec<ToolDefinition>,
    pub options: SamplingOptions,
}
#[derive(Debug, Error)]
pub enum ModelError {
    #[error("Invalid model request: {0}")]
    InvalidRequest(String),
    #[error("Unsupported model capability: {0}")]
    UnsupportedCapability(String),
    #[error("Model transport error: {0}")]
    Transport(String),
    #[error("Model stream error: {0}")]
    Stream(String),
}
impl From<AdapterError> for ModelError {
    fn from(error: AdapterError) -> Self {
        match error {
            AdapterError::HttpError(_) | AdapterError::ApiError { .. } => {
                Self::Transport(error.to_string())
            }
            AdapterError::ProtocolError(_) | AdapterError::SerializationError(_) => {
                Self::InvalidRequest(error.to_string())
            }
            AdapterError::StreamParseError(_) => Self::Stream(error.to_string()),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum ModelEvent {
    ItemStarted {
        item_id: String,
        item_type: String,
        phase: Option<MessagePhase>,
    },
    TextDelta {
        item_id: String,
        delta: String,
    },
    ReasoningDelta {
        item_id: String,
        delta: String,
    },
    ReasoningSignature {
        item_id: String,
        signature: String,
    },
    ToolCallDelta {
        item_id: String,
        call_id: String,
        delta: String,
    },
    ItemCompleted {
        item: CanonicalItem,
    },
    StepFinished {
        usage: UsageMetrics,
    },
}
pub type ModelEventStream = Pin<Box<dyn Stream<Item = Result<ModelEvent, ModelError>> + Send>>;
#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError>;
    async fn stream(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelEventStream, ModelError>;
}
pub trait ProviderFactory: Send + Sync {
    fn create(&self, model: &str) -> Result<Arc<dyn ModelProvider>, ModelError>;
}
impl<F> ProviderFactory for F
where
    F: Fn(&str) -> Result<Arc<dyn ModelProvider>, ModelError> + Send + Sync,
{
    fn create(&self, model: &str) -> Result<Arc<dyn ModelProvider>, ModelError> {
        self(model)
    }
}
pub fn validate_model_configuration(
    model: &str,
    tools: &[ToolDefinition],
    options: &SamplingOptions,
    caps: &ModelCapabilities,
) -> Result<(), ModelError> {
    whale_adapters::capabilities::validate_configuration(model, tools, options, caps)
        .map_err(|error| ModelError::UnsupportedCapability(error.to_string()))
}
pub fn validate_model_request(
    request: &ModelRequest,
    caps: &ModelCapabilities,
) -> Result<(), ModelError> {
    validate_model_configuration(
        &request.options.model,
        &request.tools,
        &request.options,
        caps,
    )?;
    crate::context::validate_model_context(&request.model_context)
        .map_err(ModelError::InvalidRequest)?;
    whale_adapters::capabilities::validate_items(&request.model_context.items, caps)
        .map_err(|error| ModelError::UnsupportedCapability(error.to_string()))
}
impl ModelEvent {
    pub(crate) fn into_agent(self, turn_id: &str, thread_id: &str) -> AgentStreamEvent {
        let turn_id = turn_id.to_owned();
        match self {
            Self::ItemStarted {
                item_id,
                item_type,
                phase,
            } => AgentStreamEvent::ItemStarted {
                turn_id,
                item_id,
                item_type,
                phase,
            },
            Self::TextDelta { item_id, delta } => AgentStreamEvent::TextDelta {
                turn_id,
                item_id,
                delta,
            },
            Self::ReasoningDelta { item_id, delta } => AgentStreamEvent::ReasoningDelta {
                turn_id,
                item_id,
                delta,
            },
            Self::ReasoningSignature { item_id, signature } => {
                AgentStreamEvent::ReasoningSignature {
                    turn_id,
                    item_id,
                    signature,
                }
            }
            Self::ToolCallDelta {
                item_id,
                call_id,
                delta,
            } => AgentStreamEvent::ToolCallDelta {
                turn_id,
                item_id,
                call_id,
                delta,
            },
            Self::ItemCompleted { item } => AgentStreamEvent::ItemCompleted { turn_id, item },
            Self::StepFinished { usage } => AgentStreamEvent::TurnCompleted {
                turn_id,
                thread_id: thread_id.into(),
                usage,
            },
        }
    }
}
/// Explicit compatibility conversion. EOF is never synthesized into successful completion.
pub(crate) fn adapt_stream(mut source: whale_adapters::BoxedEventStream) -> ModelEventStream {
    Box::pin(async_stream::try_stream! {
        while let Some(event)=source.next().await {
            let event=match event? {
                AgentStreamEvent::TurnStarted{..}=>continue,
                AgentStreamEvent::ItemStarted{item_id,item_type,phase,..}=>ModelEvent::ItemStarted{item_id,item_type,phase},
                AgentStreamEvent::TextDelta{item_id,delta,..}=>ModelEvent::TextDelta{item_id,delta},
                AgentStreamEvent::ReasoningDelta{item_id,delta,..}=>ModelEvent::ReasoningDelta{item_id,delta},
                AgentStreamEvent::ReasoningSignature{item_id,signature,..}=>ModelEvent::ReasoningSignature{item_id,signature},
                AgentStreamEvent::ToolCallDelta{item_id,call_id,delta,..}=>ModelEvent::ToolCallDelta{item_id,call_id,delta},
                AgentStreamEvent::ItemCompleted{item,..}=>ModelEvent::ItemCompleted{item},
                AgentStreamEvent::TurnCompleted{usage,..}=>ModelEvent::StepFinished{usage},
                AgentStreamEvent::TurnFailed{error_code,error_message,..}=>Err(ModelError::Stream(format!("{error_code}: {error_message}")))?,
                _=>Err(ModelError::Stream("Adapter emitted a runtime-only event".into()))?,
            };
            yield event;
        }
    })
}
