//! Thread session state and context management.

use crate::CoreError;
use std::sync::Arc;
use uuid::Uuid;
use whale_adapters::{ProtocolAdapter, SamplingOptions, ToolDefinition};
use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput};
use whale_protocol::retention::{history_bytes, serialized_bytes, SessionLimits};

use crate::context::{ContextPolicy, FullHistoryContext};
use crate::coordinator::ToolRegistry;
use crate::{http_provider::HttpModelProvider, model::ModelProvider};

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
    adapter: Option<Arc<dyn ProtocolAdapter>>,
    provider: Arc<dyn ModelProvider>,
    /// Sampling and execution options.
    sampling_options: SamplingOptions,
    agent_name: Option<String>,
    context_policy: Arc<dyn ContextPolicy>,
    journal: Option<whale_store::SessionJournal>,
    limits: Option<SessionLimits>,
    accepted_turns: u64,
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
            provider: Arc::new(HttpModelProvider::new(adapter.clone())),
            adapter: Some(adapter),
            sampling_options,
            agent_name: None,
            context_policy: Arc::new(FullHistoryContext),
            journal: None,
            limits: None,
            accepted_turns: 0,
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
            provider: Arc::new(HttpModelProvider::new(adapter.clone())),
            adapter: Some(adapter),
            sampling_options,
            agent_name: None,
            context_policy: Arc::new(FullHistoryContext),
            journal: None,
            limits: None,
            accepted_turns: 0,
        }
    }

    /// Creates a session using a native model implementation, with no fake HTTP adapter.
    pub fn with_id_prompt_and_provider(
        id: impl Into<String>,
        system_prompt: Option<String>,
        provider: Arc<dyn ModelProvider>,
        tools: Arc<ToolRegistry>,
        sampling_options: SamplingOptions,
    ) -> Self {
        Self {
            id: id.into(),
            system_prompt,
            history: Vec::new(),
            tools,
            adapter: None,
            provider,
            sampling_options,
            agent_name: None,
            context_policy: Arc::new(FullHistoryContext),
            journal: None,
            limits: None,
            accepted_turns: 0,
        }
    }
    pub fn from_provider(
        provider: Arc<dyn ModelProvider>,
        tools: Arc<ToolRegistry>,
        options: SamplingOptions,
    ) -> Self {
        Self::with_id_prompt_and_provider(
            Uuid::new_v4().to_string(),
            None,
            provider,
            tools,
            options,
        )
    }
    pub fn provider(&self) -> &Arc<dyn ModelProvider> {
        &self.provider
    }
    pub(crate) fn projected_view(
        &self,
        projection: &whale_protocol::contexts::ModelContext,
    ) -> Self {
        Self {
            id: self.id.clone(),
            system_prompt: projection.system_prompt.clone(),
            history: projection.items.clone(),
            tools: self.tools.clone(),
            adapter: self.adapter.clone(),
            provider: self.provider.clone(),
            sampling_options: self.sampling_options.clone(),
            agent_name: self.agent_name.clone(),
            context_policy: self.context_policy.clone(),
            // A model projection is not authorized to mutate durable runtime state.
            journal: None,
            limits: self.limits.clone(),
            accepted_turns: self.accepted_turns,
        }
    }

    pub fn agent_name(&self) -> Option<&str> {
        self.agent_name.as_deref()
    }

    pub fn set_limits(&mut self, limits: Option<SessionLimits>) -> Result<(), CoreError> {
        if let Some(limits) = &limits {
            limits.validate().map_err(CoreError::InvalidConfiguration)?;
        }
        self.limits = limits;
        Ok(())
    }
    pub fn limits(&self) -> Option<&SessionLimits> {
        self.limits.as_ref()
    }
    pub fn accepted_turns(&self) -> u64 {
        self.accepted_turns
    }
    /// Runtime owners restore the complete accepted-ID count, including runs
    /// cancelled before Core execution and archived durable runs.
    pub fn restore_accepted_turns(&mut self, count: u64) {
        self.accepted_turns = count;
    }
    pub fn check_run_admission(&self, input: &[CanonicalItem]) -> Result<(), CoreError> {
        if let Some(max) = self.limits.as_ref().and_then(|l| l.max_accepted_turns) {
            if self.accepted_turns >= max {
                return Err(CoreError::LimitExceeded(format!(
                    "max_accepted_turns ({max}) reached"
                )));
            }
        }
        self.check_history_with_input(input)
    }
    pub(crate) fn accept_turn(&mut self, input: &[CanonicalItem]) -> Result<(), CoreError> {
        self.check_run_admission(input)?;
        self.accepted_turns = self
            .accepted_turns
            .checked_add(1)
            .ok_or_else(|| CoreError::LimitExceeded("accepted turn counter overflow".into()))?;
        Ok(())
    }
    fn check_history_with_input(&self, input: &[CanonicalItem]) -> Result<(), CoreError> {
        if let Some(max) = self.limits.as_ref().and_then(|l| l.max_history_bytes) {
            if history_bytes(&self.history, input)? > max {
                return Err(CoreError::LimitExceeded(format!(
                    "max_history_bytes ({max}) exceeded"
                )));
            }
        }
        Ok(())
    }
    pub(crate) fn check_history_limit(&self) -> Result<(), CoreError> {
        self.check_history_with_input(&[])
    }
    pub(crate) fn check_model_request_limit(
        &self,
        request: &crate::model::ModelRequest,
    ) -> Result<(), CoreError> {
        if let Some(max) = self.limits.as_ref().and_then(|l| l.max_model_request_bytes) {
            if serialized_bytes(request)? > max {
                return Err(CoreError::LimitExceeded(format!(
                    "max_model_request_bytes ({max}) exceeded"
                )));
            }
        }
        Ok(())
    }
    pub fn set_agent_name(&mut self, name: Option<String>) {
        self.agent_name = name;
    }
    pub fn context_policy(&self) -> &Arc<dyn ContextPolicy> {
        &self.context_policy
    }
    pub fn set_context_policy(&mut self, policy: Arc<dyn ContextPolicy>) {
        self.context_policy = policy;
    }

    pub fn journal(&self) -> Option<&whale_store::SessionJournal> {
        self.journal.as_ref()
    }

    pub fn set_journal(&mut self, journal: Option<whale_store::SessionJournal>) {
        self.journal = journal;
    }

    /// Rehydrates committed history after attachment or durable settlement.
    pub fn replace_history(&mut self, history: Vec<CanonicalItem>) {
        self.history = history;
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

    /// Closes only calls from this turn whose output was never committed.
    /// Existing results and earlier-turn audit records are left intact.
    pub(crate) fn close_unanswered_tool_calls(
        &mut self,
        history_start: usize,
    ) -> Vec<CanonicalItem> {
        let mut unanswered = Vec::new();
        for item in &self.history[history_start..] {
            match item {
                CanonicalItem::ToolCall { call_id, .. } => unanswered.push(call_id.clone()),
                CanonicalItem::ToolResult { call_id, .. } => {
                    if let Some(index) = unanswered.iter().position(|pending| pending == call_id) {
                        unanswered.remove(index);
                    }
                }
                _ => {}
            }
        }
        let results: Vec<_> = unanswered.into_iter().map(|call_id| CanonicalItem::tool_result(
            call_id,
            CanonicalToolOutput::text("No tool result was recorded before the run ended. The tool may have run or may still be running; execution and external side effects are unknown. The call was not retried."),
            true,
        )).collect();
        self.history.extend(results.iter().cloned());
        results
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
    pub fn adapter(&self) -> Option<&Arc<dyn ProtocolAdapter>> {
        self.adapter.as_ref()
    }

    /// Sets or replaces the protocol adapter.
    pub fn set_adapter(&mut self, adapter: Arc<dyn ProtocolAdapter>) {
        self.provider = Arc::new(HttpModelProvider::new(adapter.clone()));
        self.adapter = Some(adapter);
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
