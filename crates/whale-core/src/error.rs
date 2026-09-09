//! Errors returned by the Whale AI core engine and components.

use thiserror::Error;
use whale_adapters::AdapterError;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Session limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("Session store error: {0}")]
    Store(#[from] whale_store::StoreError),

    #[error("Model error: {0}")]
    Model(#[from] crate::model::ModelError),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Adapter error: {0}")]
    Adapter(#[from] AdapterError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    #[error("Tool execution error: {0}")]
    ToolExecution(String),

    #[error("Approval rejected: {0}")]
    ApprovalRejected(String),

    #[error("Approval channel closed: {0}")]
    ApprovalChannelClosed(String),

    #[error("Event channel closed")]
    EventChannelClosed,

    #[error("Interaction unavailable: {0}")]
    InteractionUnavailable(String),

    #[error("Invalid Interaction request: {0}")]
    InteractionRequestInvalid(String),

    #[error("Interaction cancelled: {0}")]
    InteractionCancelled(String),

    #[error("Invalid Interaction response: {0}")]
    InteractionResponseInvalid(String),

    #[error("Turn max steps exceeded: {0}")]
    MaxStepsExceeded(usize),

    #[error("Internal error: {0}")]
    Internal(String),
}
