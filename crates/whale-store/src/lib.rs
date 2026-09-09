//! Transactional session storage and cancellation-safe execution journals.
mod backend;
mod configuration;
mod retention;
mod runtime;
pub use configuration::PersistedSessionConfigurationV2;
pub use retention::{RetentionMetadata, RetentionReport};
#[cfg(feature = "sqlite")]
mod sqlite;
mod state;
pub use backend::{MemoryStore, SessionStore};
pub use runtime::{SessionJournal, StoreRuntime};
#[cfg(feature = "sqlite")]
pub use sqlite::SQLiteStore;
pub use state::*;
use thiserror::Error;
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum StoreError {
    #[error("Session admission limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("Invalid store operation: {0}")]
    Invalid(String),
    #[error("Recovery record not found")]
    NotFound,
    #[error("Invalid recovery credentials")]
    Unauthorized,
    #[error("Store revision or identity conflict")]
    Conflict,
    #[error("Recovery session is attached")]
    Active,
    #[error("Unknown executions require acknowledgment")]
    UnknownExecutions,
    #[error("Stale session lease")]
    StaleLease,
    #[error("Recovery record was forgotten")]
    Forgotten,
    #[error("Store I/O failed: {0}")]
    Io(String),
    #[error("Session journal is poisoned: {0}")]
    Poisoned(String),
}
pub type Result<T> = std::result::Result<T, StoreError>;
