//! Thread session state and context management.

use std::sync::Arc;
use uuid::Uuid;
use whale_adapters::{ProtocolAdapter, SamplingOptions, ToolDefinition};
use whale_protocol::canonical::CanonicalItem;

use crate::coordinator::ToolRegistry;

/// A stateful conversation thread session holding history, adapter, tools, and options.
pub struct ThreadSession {
    /// Unique thread identifier.
    id: String,
    /// System prompt instruction.
    system_prompt: Option<String>,
    /// Append-only history of canonical items.
    history: Vec<CanonicalItem>,
    /// Registered tools available to this session.
    tools: Arc<ToolRegistry>,
    /// Model protocol adapter.
    adapter: Arc<dyn ProtocolAdapter>,
    /// Sampling and execution options.
    sampling_options: SamplingOptions,
}

impl ThreadSession {
    /// Creates a new ThreadSession.
    pub fn new(
        adapter: Arc<dyn ProtocolAdapter>,
        tools: Arc<ToolRegistry>,
        sampling_options: SamplingOptions,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            system_prompt: None,
            history: Vec::new(),
            tools,
            adapter,
            sampling_options,
        }
    }

    /// Creates a new ThreadSession with custom ID and system prompt.
    pub fn with_id_and_prompt(
        id: impl Into<String>,
        system_prompt: Option<String>,
        adapter: Arc<dyn ProtocolAdapter>,
        tools: Arc<ToolRegistry>,
        sampling_options: SamplingOptions,
    ) -> Self {
        Self {
            id: id.into(),
            system_prompt,
            history: Vec::new(),
            tools,
            adapter,
            sampling_options,
        }
    }

    /// Returns session thread ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns system prompt.
    pub fn system_prompt(&self) -> Option<&str> {
        self.system_prompt.as_deref()
    }

    /// Sets system prompt.
    pub fn set_system_prompt(&mut self, prompt: Option<String>) {
        self.system_prompt = prompt;
    }

    /// Returns reference to canonical history.
    pub fn history(&self) -> &[CanonicalItem] {
        &self.history
    }

    /// Returns a clone of canonical history.
    pub fn clone_history(&self) -> Vec<CanonicalItem> {
        self.history.clone()
    }

    /// Appends a single item to history.
    pub fn append_item(&mut self, item: CanonicalItem) {
        self.history.push(item);
    }

    /// Appends multiple items to history.
    pub fn append_items(&mut self, items: impl IntoIterator<Item = CanonicalItem>) {
        self.history.extend(items);
    }

    /// Returns the tool registry.
    pub fn tools(&self) -> &Arc<ToolRegistry> {
        &self.tools
    }

    /// Returns all tool definitions registered for this session.
    pub fn get_tool_definitions(&self) -> Vec<ToolDefinition> {
        self.tools.get_tool_definitions()
    }

    /// Returns the protocol adapter.
    pub fn adapter(&self) -> &Arc<dyn ProtocolAdapter> {
        &self.adapter
    }

    /// Sets or replaces the protocol adapter.
    pub fn set_adapter(&mut self, adapter: Arc<dyn ProtocolAdapter>) {
        self.adapter = adapter;
    }

    /// Returns sampling options.
    pub fn sampling_options(&self) -> &SamplingOptions {
        &self.sampling_options
    }

    /// Returns mutable sampling options.
    pub fn sampling_options_mut(&mut self) -> &mut SamplingOptions {
        &mut self.sampling_options
    }
}
