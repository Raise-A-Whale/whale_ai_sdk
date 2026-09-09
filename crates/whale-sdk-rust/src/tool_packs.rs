//! Session-scoped host resource factories and their frozen Agent manifests.

use crate::{CancellationSignal, HostTool};
use async_trait::async_trait;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolPackTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub supports_parallel: bool,
    pub require_approval: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolPackManifest {
    pub id: String,
    pub tools: Vec<ToolPackTool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionBindKind {
    Ephemeral,
    PersistentCreate { recovery_id: String },
    PersistentAttach { recovery_id: String },
}

#[derive(Clone)]
pub struct SessionBindContext {
    session_id: String,
    agent_name: String,
    kind: SessionBindKind,
    session_cancelled: CancellationSignal,
}

impl SessionBindContext {
    pub(crate) fn new(
        session_id: String,
        agent_name: String,
        kind: SessionBindKind,
        session_cancelled: CancellationSignal,
    ) -> Self {
        Self {
            session_id,
            agent_name,
            kind,
            session_cancelled,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn agent_name(&self) -> &str {
        &self.agent_name
    }

    pub fn kind(&self) -> &SessionBindKind {
        &self.kind
    }

    pub fn session_cancelled(&self) -> CancellationSignal {
        self.session_cancelled.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ToolPackError {
    message: String,
}

impl ToolPackError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[async_trait]
pub trait ToolPack: Send + Sync {
    fn manifest(&self) -> ToolPackManifest;

    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError>;
}

#[async_trait]
pub trait BoundToolPack: Send + Sync {
    fn tools(&self) -> Vec<Arc<dyn HostTool>>;

    async fn close(&mut self) -> Result<(), ToolPackError>;

    /// Synchronously fences externally owned work when awaited close cannot finish.
    fn emergency_close(&mut self) {}
}

pub(crate) struct PackPlan {
    pub(crate) manifest: ToolPackManifest,
    pub(crate) factory: Arc<dyn ToolPack>,
}

pub(crate) struct BoundPackLease {
    id: String,
    bound: Option<Box<dyn BoundToolPack>>,
    terminal: bool,
}

impl BoundPackLease {
    pub(crate) fn new(id: String, bound: Box<dyn BoundToolPack>) -> Self {
        Self {
            id,
            bound: Some(bound),
            terminal: false,
        }
    }

    fn tools(&self) -> Result<Vec<Arc<dyn HostTool>>, String> {
        catch_unwind(AssertUnwindSafe(|| {
            self.bound
                .as_ref()
                .expect("live lease retains its bound pack")
                .tools()
        }))
        .map_err(|_| format!("ToolPack {} tools panicked", self.id))
    }

    async fn close(mut self) -> Option<String> {
        let outcome = {
            let bound = self
                .bound
                .as_mut()
                .expect("live lease retains its bound pack");
            AssertUnwindSafe(bound.close()).catch_unwind().await
        };
        self.terminal = true;
        let bound = self.bound.take();
        let drop_outcome = catch_unwind(AssertUnwindSafe(|| drop(bound)));
        let mut failures = Vec::new();
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failures.push(format!("ToolPack {} close failed: {}", self.id, error))
            }
            Err(_) => failures.push(format!("ToolPack {} close panicked", self.id)),
        }
        if drop_outcome.is_err() {
            failures.push(format!("ToolPack {} Drop panicked", self.id));
        }
        (!failures.is_empty()).then(|| failures.join("; "))
    }
}

impl Drop for BoundPackLease {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(mut bound) = self.bound.take() {
            let _ = catch_unwind(AssertUnwindSafe(move || {
                bound.emergency_close();
                drop(bound);
            }));
        }
    }
}

#[derive(Default)]
pub(crate) struct SessionPackOwner {
    packs: Vec<BoundPackLease>,
    owned_names: HashSet<String>,
}

impl SessionPackOwner {
    pub(crate) fn new(owned_names: impl IntoIterator<Item = String>) -> Self {
        Self {
            packs: Vec::new(),
            owned_names: owned_names.into_iter().collect(),
        }
    }

    pub(crate) fn push(&mut self, lease: BoundPackLease) {
        self.packs.push(lease);
    }

    pub(crate) fn owned_names(&self) -> &HashSet<String> {
        &self.owned_names
    }

    pub(crate) fn last_tools(&self) -> Result<Vec<Arc<dyn HostTool>>, String> {
        self.packs
            .last()
            .expect("a lease is pushed before bound tools are inspected")
            .tools()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.packs.len()
    }

    pub(crate) async fn close_reverse(mut self) -> Vec<String> {
        let mut failures = Vec::new();
        while let Some(lease) = self.packs.pop() {
            if let Some(failure) = lease.close().await {
                failures.push(failure);
            }
        }
        failures
    }
}

impl Drop for SessionPackOwner {
    fn drop(&mut self) {
        while let Some(lease) = self.packs.pop() {
            drop(lease);
        }
    }
}
