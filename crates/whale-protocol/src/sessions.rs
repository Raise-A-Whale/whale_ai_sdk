//! Explicit Session resource lifecycle, independent of the client connection.

use serde::{Deserialize, Serialize};

pub const METHOD_SESSION_CLOSE: &str = "session.close";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSessionParams {
    pub thread_id: String,
}

impl CloseSessionParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.thread_id.trim().is_empty() {
            return Err("thread_id must not be empty".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSessionResult {
    pub thread_id: String,
    /// True for the invocation that closes an existing owned Session;
    /// false when the Session was already closed or was not found.
    pub closed: bool,
}
