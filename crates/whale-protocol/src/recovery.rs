//! Optional durable sessions. Recovery keys are bearer secrets, not live thread IDs.

use crate::{
    canonical::CanonicalItem,
    events::UsageMetrics,
    rpc::{StartThreadParams, StartThreadResult},
    runs::RunSnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, fmt};
use uuid::Uuid;

pub const CAPABILITY_SESSION_RECOVERY: &str = "session_recovery.v1";
pub const METHOD_SESSION_CREATE_PERSISTENT: &str = "session.create_persistent";
pub const METHOD_RECOVERY_INSPECT: &str = "session.recovery.inspect";
pub const METHOD_RECOVERY_ATTACH: &str = "session.recovery.attach";
pub const METHOD_RECOVERY_ACKNOWLEDGE: &str = "session.recovery.acknowledge";
pub const METHOD_RECOVERY_FORGET: &str = "session.recovery.forget";
pub const RECOVERY_UNAVAILABLE: i64 = -32020;
pub const RECOVERY_REJECTED: i64 = -32021;
/// An I/O/commit outcome is uncertain. SDKs close rather than retry a mutation.
pub const STORE_FAILED: i64 = -32022;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryKey {
    pub recovery_id: String,
    pub secret: String,
}
impl fmt::Debug for RecoveryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryKey")
            .field("recovery_id", &self.recovery_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
impl Default for RecoveryKey {
    fn default() -> Self {
        Self::new()
    }
}
impl RecoveryKey {
    pub fn new() -> Self {
        Self {
            recovery_id: Uuid::new_v4().to_string(),
            secret: format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if Uuid::parse_str(&self.recovery_id).is_err() || self.recovery_id.len() != 36 {
            return Err("Recovery ID must be a UUID".into());
        }
        if self.secret.len() != 64 || !self.secret.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("Recovery secret must contain 64 hexadecimal characters".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRunDefaults {
    #[serde(default = "default_max_steps")]
    pub max_steps: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}
fn default_max_steps() -> usize {
    10
}
impl Default for SessionRunDefaults {
    fn default() -> Self {
        Self {
            max_steps: 10,
            timeout_ms: None,
        }
    }
}
impl SessionRunDefaults {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_steps == 0 || self.timeout_ms == Some(0) {
            return Err("Run defaults must be positive".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePersistentSessionParams {
    pub key: RecoveryKey,
    pub session: StartThreadParams,
    #[serde(default)]
    pub run_defaults: SessionRunDefaults,
}
impl CreatePersistentSessionParams {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        self.run_defaults.validate()?;
        validate_fresh_thread(&self.session)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachRecoveryParams {
    pub key: RecoveryKey,
    pub expected_revision: u64,
    pub session: StartThreadParams,
    #[serde(default)]
    pub run_defaults: SessionRunDefaults,
}
impl AttachRecoveryParams {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        validate_revision(self.expected_revision)?;
        self.run_defaults.validate()?;
        validate_fresh_thread(&self.session)
    }
}
fn validate_fresh_thread(params: &StartThreadParams) -> Result<(), String> {
    if params
        .session_id
        .as_deref()
        .is_none_or(|id| Uuid::parse_str(id).is_err() || id.len() != 36)
    {
        return Err("Persistent sessions require a fresh client-generated UUID session_id".into());
    }
    Ok(())
}
fn validate_revision(revision: u64) -> Result<(), String> {
    if revision == 0 {
        return Err("expected_revision must be positive".into());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRecoveryParams {
    pub key: RecoveryKey,
}
impl InspectRecoveryParams {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentSessionResult {
    pub thread: StartThreadResult,
    pub key: RecoveryKey,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgeUnknownParams {
    pub key: RecoveryKey,
    pub expected_revision: u64,
    pub execution_ids: Vec<String>,
}
impl AcknowledgeUnknownParams {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        validate_revision(self.expected_revision)?;
        let mut seen = HashSet::new();
        if self.execution_ids.is_empty()
            || self
                .execution_ids
                .iter()
                .any(|id| id.is_empty() || id.trim() != id || !seen.insert(id))
        {
            return Err("execution_ids must be nonempty, unique and unpadded".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgetRecoveryParams {
    pub key: RecoveryKey,
    pub expected_revision: u64,
}
impl ForgetRecoveryParams {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        validate_revision(self.expected_revision)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgetRecoveryResult {
    pub recovery_id: String,
    pub forgotten: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelInputRecord {
    pub step_id: String,
    pub step_index: usize,
    pub history_revision: u64,
    /// Serialized validated ModelRequest; excludes resolved credentials/headers.
    pub request: Value,
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageMetrics>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveredRun {
    /// Archived execution identity; not a live handle in the new attachment.
    pub snapshot: RunSnapshot,
    pub model_inputs: Vec<ModelInputRecord>,
    pub effective_options: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnknownExecution {
    pub execution_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_arguments: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
    pub reason: String,
    pub acknowledged: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoverySnapshot {
    pub recovery_id: String,
    pub revision: u64,
    pub epoch: u64,
    pub attached: bool,
    /// Versioned, sanitized Session configuration without runtime callback IDs.
    pub configuration: Value,
    pub history: Vec<CanonicalItem>,
    pub runs: Vec<RecoveredRun>,
    pub unknown_executions: Vec<UnknownExecution>,
}
