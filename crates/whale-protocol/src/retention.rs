//! Explicit payload retirement policies and session admission budgets.
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use std::io::Write;

pub const CAPABILITY_SESSION_LIMITS: &str = "session_limits.v1";
pub const CAPABILITY_RUN_RETENTION: &str = "run_retention.v1";
pub const RUN_EXPIRED: i64 = -32030;
pub const SESSION_LIMIT_EXCEEDED: i64 = -32031;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRetentionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_terminal_runs_per_session: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreRetentionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detached_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retained_sessions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retained_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    #[serde(default = "default_interval")]
    pub sweep_interval_ms: u64,
    #[serde(default)]
    pub runs: RunRetentionPolicy,
    #[serde(default)]
    pub store: StoreRetentionPolicy,
}
fn default_interval() -> u64 {
    1000
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            sweep_interval_ms: default_interval(),
            runs: Default::default(),
            store: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_accepted_turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_history_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_request_bytes: Option<u64>,
}

macro_rules! validate_options {
    ($kind:ty, $($field:ident),+ $(,)?) => {
        impl $kind {
            pub fn validate(&self) -> Result<(), String> {
                $(if self.$field == Some(0) {return Err(concat!(stringify!($field), " must be positive").into());})+
                Ok(())
            }
            pub fn is_enabled(&self) -> bool { false $(|| self.$field.is_some())+ }
        }
    };
}
validate_options!(
    RunRetentionPolicy,
    terminal_ttl_ms,
    max_terminal_runs_per_session
);
validate_options!(
    StoreRetentionPolicy,
    detached_ttl_ms,
    max_retained_sessions,
    max_retained_payload_bytes
);
validate_options!(
    SessionLimits,
    max_accepted_turns,
    max_history_bytes,
    max_model_request_bytes
);
impl RetentionPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.sweep_interval_ms == 0 {
            return Err("sweep_interval_ms must be positive".into());
        }
        self.runs.validate()?;
        self.store.validate()
    }
    pub fn is_enabled(&self) -> bool {
        self.runs.is_enabled() || self.store.is_enabled()
    }
}

/// Counts UTF-8 JSON bytes without allocating a second copy of the payload.
pub fn serialized_bytes<T: Serialize + ?Sized>(value: &T) -> Result<u64, serde_json::Error> {
    struct Count(u64);
    impl Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("Serialized byte count overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

/// Measures the proposed canonical history as one JSON array, without cloning it.
pub fn history_bytes(
    history: &[crate::CanonicalItem],
    input: &[crate::CanonicalItem],
) -> Result<u64, serde_json::Error> {
    struct Joined<'a>(&'a [crate::CanonicalItem], &'a [crate::CanonicalItem]);
    impl Serialize for Joined<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(self.0.len().checked_add(self.1.len()))?;
            for item in self.0.iter().chain(self.1) {
                seq.serialize_element(item)?;
            }
            seq.end()
        }
    }
    serialized_bytes(&Joined(history, input))
}
