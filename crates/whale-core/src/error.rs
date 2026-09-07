//! Errors returned by the Whale AI core engine and components.

use thiserror::Error;
use whale_adapters::AdapterError;

#[derive(Debug, Error)]
pub enum CoreError {
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

    #[error("Turn max steps exceeded: {0}")]
    MaxStepsExceeded(usize),

    #[error("Internal error: {0}")]
    Internal(String),
}
