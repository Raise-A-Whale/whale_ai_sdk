//! Turn execution engine coordinating LLM calls, streaming SSE, tool execution, and session updates.

use crate::http_provider::HttpModelProvider;
use crate::model::{
    validate_model_request, ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
};
use futures::{Stream, StreamExt};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;
use whale_adapters::AdapterError;
use whale_protocol::canonical::{CanonicalContent, CanonicalItem, MessagePhase};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};

use crate::context::validate_model_context;
use crate::coordinator::ToolExecutionCoordinator;
use crate::error::CoreError;
use crate::execution::{CancelOnDrop, CancellationToken};
use crate::session::ThreadSession;
use whale_protocol::contexts::{ContextBuildRequest, RunContextInfo, ToolExecutionRecord};

/// Result summary of a completed agent turn.
#[derive(Debug, Clone)]
pub struct RunTurnResult {
    /// Turn identifier.
    pub turn_id: String,
    /// Total steps taken in this turn.
    pub steps_taken: usize,
    /// Final cumulative usage metrics.
    pub total_usage: UsageMetrics,
    /// Newly completed items generated during the turn.
    pub generated_items: Vec<CanonicalItem>,
}

/// Progress retained independently of a run future, including cancelled or failed runs.
#[derive(Default)]
pub struct RunProgress {
    usage: std::sync::Mutex<UsageMetrics>,
    tool_executions: std::sync::Mutex<Vec<ToolExecutionRecord>>,
}
impl RunProgress {
    pub fn tool_executions(&self) -> Vec<ToolExecutionRecord> {
        self.tool_executions.lock().unwrap().clone()
    }
    pub(crate) fn record_execution(&self, record: ToolExecutionRecord) {
        self.tool_executions.lock().unwrap().push(record);
    }
    pub fn usage(&self) -> UsageMetrics {
        self.usage.lock().unwrap().clone()
    }
}

/// Agent engine that executes conversation turns.
pub struct AgentEngine {
    legacy_http_client: Option<Client>,
    coordinator: Arc<ToolExecutionCoordinator>,
    /// Optional stream provider override (useful for mocking without a real network port).
    stream_provider: Option<
        Arc<
            dyn Fn(
                    &ThreadSession,
                    usize,
                ) -> Result<
                    Pin<Box<dyn Stream<Item = Result<AgentStreamEvent, AdapterError>> + Send>>,
                    CoreError,
                > + Send
                + Sync,
        >,
    >,
}

impl AgentEngine {
    /// Creates a new AgentEngine.
    pub fn new(coordinator: Arc<ToolExecutionCoordinator>) -> Self {
        Self {
            legacy_http_client: None,
            coordinator,
            stream_provider: None,
        }
    }

    /// Creates a new AgentEngine with custom reqwest client.
    pub fn with_client(http_client: Client, coordinator: Arc<ToolExecutionCoordinator>) -> Self {
        Self {
            legacy_http_client: Some(http_client),
            coordinator,
            stream_provider: None,
        }
    }

    /// Sets a custom stream provider callback (used for in-memory testing or custom transports).
    pub fn with_stream_provider(
        mut self,
        provider: Arc<
            dyn Fn(
                    &ThreadSession,
                    usize,
                ) -> Result<
                    Pin<Box<dyn Stream<Item = Result<AgentStreamEvent, AdapterError>> + Send>>,
                    CoreError,
                > + Send
                + Sync,
        >,
    ) -> Self {
        self.stream_provider = Some(provider);
        self
    }

    /// Returns reference to coordinator.
    pub fn coordinator(&self) -> &Arc<ToolExecutionCoordinator> {
        &self.coordinator
    }

    /// Runs an agent turn.
    ///
    /// Turn loop:
    /// - If step == 0 and user_input is Some, append UserMessage and emit ItemCompleted.
    /// - For step in 0..max_steps:
    ///   1. Build and validate the model-only ContextPolicy projection.
    ///   2. Invoke the selected ModelProvider with owned request data.
    ///   3. Consume model-step events through a strict completion boundary.
    ///   4. Forward events to `event_tx` and accumulate items/deltas.
    ///   5. When LLM finishes step:
    ///      - If no ToolCalls generated, assistant completed reply, finish turn!
    ///      - If ToolCalls present, execute them via coordinator (with parallel/exclusive barrier & HITL).
    ///      - Append ToolResults to history and emit ItemCompleted.
    ///      - Continue to next step.
    pub async fn run_turn(
        &self,
        session: &mut ThreadSession,
        user_input: Option<&str>,
        max_steps: usize,
        event_tx: tokio::sync::mpsc::Sender<AgentStreamEvent>,
    ) -> Result<RunTurnResult, CoreError> {
        self.run_turn_with_items(
            session,
            &Uuid::new_v4().to_string(),
            user_input
                .map(CanonicalItem::user_text)
                .into_iter()
                .collect(),
            max_steps,
            event_tx,
        )
        .await
    }

    /// Runs a turn retaining every canonical input item and the caller's stable identity.
    pub async fn run_turn_with_items(
        &self,
        session: &mut ThreadSession,
        turn_id: &str,
        input_items: Vec<CanonicalItem>,
        max_steps: usize,
        event_tx: tokio::sync::mpsc::Sender<AgentStreamEvent>,
    ) -> Result<RunTurnResult, CoreError> {
        self.run_turn_with_progress(
            session,
            turn_id,
            input_items,
            max_steps,
            event_tx,
            Arc::new(RunProgress::default()),
        )
        .await
    }

    /// Runs with a queryable usage accumulator surviving execution cancellation.
    pub async fn run_turn_with_progress(
        &self,
        session: &mut ThreadSession,
        turn_id: &str,
        input_items: Vec<CanonicalItem>,
        max_steps: usize,
        event_tx: tokio::sync::mpsc::Sender<AgentStreamEvent>,
        progress: Arc<RunProgress>,
    ) -> Result<RunTurnResult, CoreError> {
        let context = RunContextInfo {
            agent_name: session.agent_name().map(str::to_owned),
            thread_id: session.id().into(),
            turn_id: turn_id.into(),
            deadline_unix_ms: None,
        };
        self.run_turn_with_context(
            session,
            context,
            CancellationToken::new(),
            input_items,
            max_steps,
            event_tx,
            progress,
        )
        .await
    }

    /// Runs with cooperative cancellation. When a direct event consumer is
    /// backpressured, cancellation makes final event delivery best-effort;
    /// committed history and RunProgress remain available to the caller.
    /// The daemon publishes its own authoritative terminal snapshot.
    pub async fn run_turn_with_context(
        &self,
        session: &mut ThreadSession,
        context: RunContextInfo,
        cancellation: CancellationToken,
        input_items: Vec<CanonicalItem>,
        max_steps: usize,
        event_tx: tokio::sync::mpsc::Sender<AgentStreamEvent>,
        progress: Arc<RunProgress>,
    ) -> Result<RunTurnResult, CoreError> {
        let _cancel = CancelOnDrop(cancellation.clone());
        let turn_id = context.turn_id.as_str();
        let history_start = session.history().len();
        let mut history_guard = TurnHistoryGuard {
            session,
            history_start,
            completed: false,
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(CoreError::Internal("Run cancelled".into())),
            result = self.run_inner(
                history_guard.session, turn_id.to_owned(), input_items, max_steps,
                event_tx.clone(), progress, context.clone(), cancellation.clone(),
            ) => result,
        };
        if result.is_ok() {
            history_guard.completed = true;
        } else if history_guard.session.journal().is_none() {
            for item in history_guard
                .session
                .close_unanswered_tool_calls(history_start)
            {
                send_final_event(
                    &event_tx,
                    &cancellation,
                    AgentStreamEvent::ItemCompleted {
                        turn_id: turn_id.to_owned(),
                        item,
                    },
                )
                .await;
            }
        }
        let terminal = match &result {
            Ok(result) => AgentStreamEvent::TurnCompleted {
                turn_id: turn_id.to_owned(),
                thread_id: history_guard.session.id().to_owned(),
                usage: result.total_usage.clone(),
            },
            Err(error) => AgentStreamEvent::TurnFailed {
                turn_id: turn_id.to_owned(),
                thread_id: history_guard.session.id().to_owned(),
                error_code: match error {
                    CoreError::LimitExceeded(_) => "SESSION_LIMIT_EXCEEDED",
                    CoreError::Store(_) => "STORE_FAILED",
                    CoreError::MaxStepsExceeded(_) => "MAX_STEPS_EXCEEDED",
                    _ => "RUN_FAILED",
                }
                .into(),
                error_message: error.to_string(),
            },
        };
        send_final_event(&event_tx, &cancellation, terminal).await;
        result
    }

    async fn run_inner(
        &self,
        session: &mut ThreadSession,
        turn_id: String,
        input_items: Vec<CanonicalItem>,
        max_steps: usize,
        event_tx: tokio::sync::mpsc::Sender<AgentStreamEvent>,
        progress: Arc<RunProgress>,
        context: RunContextInfo,
        cancellation: CancellationToken,
    ) -> Result<RunTurnResult, CoreError> {
        if max_steps == 0 {
            return Err(CoreError::MaxStepsExceeded(0));
        }
        session.accept_turn(&input_items)?;
        let thread_id = session.id().to_string();
        info!("Starting agent turn: {} (thread: {})", turn_id, thread_id);

        // Emit TurnStarted event
        let _ = event_tx
            .send(AgentStreamEvent::TurnStarted {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
            })
            .await;

        let mut turn_generated_items = Vec::new();
        let mut total_usage = UsageMetrics::default();

        // 1. Initial user message if provided
        for user_item in input_items {
            session.append_item(user_item.clone());
            turn_generated_items.push(user_item.clone());

            let _ = event_tx
                .send(AgentStreamEvent::ItemCompleted {
                    turn_id: turn_id.clone(),
                    item: user_item,
                })
                .await;
        }

        let mut steps_taken = 0;

        for step in 0..max_steps {
            session.check_history_limit()?;
            steps_taken = step + 1;
            debug!("Starting turn step {}/{}", steps_taken, max_steps);

            let request = ContextBuildRequest {
                context: context.clone(),
                step_index: step,
                model: session.sampling_options().model.clone(),
                system_prompt: session.system_prompt().map(str::to_owned),
                history: session.clone_history(),
            };
            let model_context = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(CoreError::Internal("Run cancelled during model context construction".into())),
                result = session.context_policy().build(request, cancellation.clone()) => result.map_err(|error| CoreError::Internal(format!("Context policy failed: {error}")))?,
            };
            validate_model_context(&model_context)
                .map_err(|error| CoreError::Internal(format!("Invalid model context: {error}")))?;
            let provider: Arc<dyn ModelProvider> =
                match (&self.legacy_http_client, session.adapter()) {
                    (Some(client), Some(adapter)) => Arc::new(HttpModelProvider::with_client(
                        adapter.clone(),
                        client.clone(),
                    )),
                    _ => session.provider().clone(),
                };
            let request = ModelRequest {
                context: context.clone(),
                step_index: step,
                step_id: Uuid::new_v4().to_string(),
                model_context,
                tools: session.get_tool_definitions(),
                options: session.sampling_options().clone(),
            };
            let capabilities = provider.capabilities(&request.options.model)?;
            validate_model_request(&request, &capabilities)?;
            session.check_model_request_limit(&request)?;
            let step_id = request.step_id.clone();
            if let Some(journal) = session.journal() {
                journal
                    .commit(
                        &turn_id,
                        whale_store::RunMutation::ModelInput {
                            step_id: step_id.clone(),
                            step_index: step,
                            request: serde_json::to_value(&request)?,
                        },
                    )
                    .await?;
            }
            let model_cancellation = CancellationToken::new();
            let mut event_stream = if let Some(legacy) = &self.stream_provider {
                let view = session.projected_view(&request.model_context);
                let source = legacy(&view, step)?;
                InvocationStream::new(crate::model::adapt_stream(source), model_cancellation)
            } else {
                let source = CreationFuture::new(
                    provider.stream(request, model_cancellation.clone()),
                    model_cancellation.clone(),
                )
                .await?;
                InvocationStream::new(source, model_cancellation)
            };
            let mut step_state = ModelStepState::new(session.history());

            let mut step_completed_items: Vec<CanonicalItem> = Vec::new();
            let mut step_usage = UsageMetrics::default();
            let mut active_text_deltas: HashMap<String, (String, MessagePhase)> = HashMap::new();
            let mut active_reasoning_deltas: HashMap<String, (String, Option<String>)> =
                HashMap::new();
            let mut active_tool_deltas: HashMap<String, (String, String, Option<String>, String)> =
                HashMap::new();

            while let Some(event_res) = event_stream.next().await {
                let model_event = event_res?;
                step_state.observe(&model_event)?;
                if let ModelEvent::ItemCompleted { item } = &model_event {
                    if matches!(
                        item,
                        CanonicalItem::UserMessage { .. } | CanonicalItem::ToolResult { .. }
                    ) {
                        return Err(
                            ModelError::Stream("Provider emitted a non-model item".into()).into(),
                        );
                    }
                    whale_adapters::capabilities::validate_items(
                        std::slice::from_ref(item),
                        &capabilities,
                    )
                    .map_err(|error| ModelError::Stream(error.to_string()))?;
                }
                let event = model_event.into_agent(&turn_id, &thread_id);
                // Track accumulated state for deltas
                match &event {
                    AgentStreamEvent::ItemStarted {
                        item_id,
                        item_type,
                        phase,
                        ..
                    } => {
                        if item_type == "assistant_message" {
                            active_text_deltas.insert(
                                item_id.clone(),
                                (String::new(), phase.unwrap_or(MessagePhase::FinalAnswer)),
                            );
                        } else if item_type == "reasoning" {
                            active_reasoning_deltas.insert(item_id.clone(), (String::new(), None));
                        }
                    }
                    AgentStreamEvent::TextDelta { item_id, delta, .. } => {
                        if let Some((text, _)) = active_text_deltas.get_mut(item_id) {
                            text.push_str(delta);
                        } else {
                            active_text_deltas.insert(
                                item_id.clone(),
                                (delta.clone(), MessagePhase::FinalAnswer),
                            );
                        }
                    }
                    AgentStreamEvent::ReasoningDelta { item_id, delta, .. } => {
                        if let Some((thinking, _)) = active_reasoning_deltas.get_mut(item_id) {
                            thinking.push_str(delta);
                        } else {
                            active_reasoning_deltas.insert(item_id.clone(), (delta.clone(), None));
                        }
                    }
                    AgentStreamEvent::ReasoningSignature {
                        item_id, signature, ..
                    } => {
                        if let Some((_, sig)) = active_reasoning_deltas.get_mut(item_id) {
                            *sig = Some(signature.clone());
                        }
                    }
                    AgentStreamEvent::ToolCallDelta {
                        item_id,
                        call_id,
                        delta,
                        ..
                    } => {
                        if let Some((_, _, _, raw_args)) = active_tool_deltas.get_mut(item_id) {
                            raw_args.push_str(delta);
                        } else {
                            active_tool_deltas.insert(
                                item_id.clone(),
                                (item_id.clone(), call_id.clone(), None, delta.clone()),
                            );
                        }
                    }
                    AgentStreamEvent::ItemCompleted { item, .. } => {
                        if let Some(journal) = session.journal() {
                            journal
                                .commit(
                                    &turn_id,
                                    whale_store::RunMutation::ModelItem {
                                        step_id: step_id.clone(),
                                        item: item.clone(),
                                    },
                                )
                                .await?;
                        }
                        step_completed_items.push(item.clone());
                        session.append_item(item.clone());
                        turn_generated_items.push(item.clone());
                    }
                    AgentStreamEvent::TurnCompleted { usage, .. } => {
                        // Check the entire step before publishing any cumulative field.
                        // On failure the last representable usage remains available, and
                        // this step never reaches tool dispatch or durable completion.
                        let cumulative = checked_usage(&total_usage, usage)?;
                        step_usage = usage.clone();
                        total_usage = cumulative;
                        *progress.usage.lock().unwrap() = total_usage.clone();
                    }
                    _ => {}
                }

                if matches!(event, AgentStreamEvent::TurnCompleted { .. }) {
                    continue;
                }

                // Forward event to caller
                if event_tx.send(event).await.is_err() {
                    return Err(CoreError::EventChannelClosed);
                }
            }

            drop(event_stream);
            if !step_state.finished {
                return Err(
                    ModelError::Stream("Unexpected EOF before model StepFinished".into()).into(),
                );
            }

            // If adapter did not emit ItemCompleted for deltas, assemble fallback items
            if step_completed_items.is_empty() {
                for (id, (text, phase)) in active_text_deltas {
                    if !text.is_empty() {
                        let item = CanonicalItem::AssistantMessage {
                            id: id.clone(),
                            content: vec![CanonicalContent::text(text)],
                            phase,
                        };
                        if let Some(journal) = session.journal() {
                            journal
                                .commit(
                                    &turn_id,
                                    whale_store::RunMutation::ModelItem {
                                        step_id: step_id.clone(),
                                        item: item.clone(),
                                    },
                                )
                                .await?;
                        }
                        step_completed_items.push(item.clone());
                        session.append_item(item.clone());
                        turn_generated_items.push(item.clone());
                        let _ = event_tx
                            .send(AgentStreamEvent::ItemCompleted {
                                turn_id: turn_id.clone(),
                                item,
                            })
                            .await;
                    }
                }
            }

            // Inspect completed items in this step for tool calls
            if let Some(journal) = session.journal() {
                journal
                    .commit(
                        &turn_id,
                        whale_store::RunMutation::ModelStepFinished {
                            step_id,
                            usage: step_usage,
                        },
                    )
                    .await?;
            }
            let tool_calls: Vec<CanonicalItem> = step_completed_items
                .iter()
                .filter(|item| matches!(item, CanonicalItem::ToolCall { .. }))
                .cloned()
                .collect();

            session.check_history_limit()?;

            if tool_calls.is_empty() {
                info!(
                    "No tool calls in step {}; turn completed successfully.",
                    steps_taken
                );
                break;
            }

            // Execute tool calls via coordinator
            info!(
                "Executing {} tool call(s) for step {}",
                tool_calls.len(),
                steps_taken
            );
            let results = self
                .coordinator
                .execute_calls_with_journal(
                    context.clone(),
                    cancellation.clone(),
                    progress.clone(),
                    tool_calls,
                    Some(Arc::clone(session.tools())),
                    Some(event_tx.clone()),
                    session.journal().cloned(),
                )
                .await?;

            if let Some(journal) = session.journal() {
                journal
                    .commit(
                        &turn_id,
                        whale_store::RunMutation::ToolBatch {
                            call_ids: results
                                .iter()
                                .filter_map(|item| match item {
                                    CanonicalItem::ToolResult { call_id, .. } => {
                                        Some(call_id.clone())
                                    }
                                    _ => None,
                                })
                                .collect(),
                        },
                    )
                    .await?;
            }

            for res_item in results {
                session.append_item(res_item.clone());
                turn_generated_items.push(res_item.clone());

                let _ = event_tx
                    .send(AgentStreamEvent::ItemCompleted {
                        turn_id: turn_id.clone(),
                        item: res_item,
                    })
                    .await;
            }

            if step + 1 >= max_steps {
                warn!("Reached max steps ({}); halting turn loop", max_steps);
                return Err(CoreError::MaxStepsExceeded(max_steps));
            }
        }

        Ok(RunTurnResult {
            turn_id,
            steps_taken,
            total_usage,
            generated_items: turn_generated_items,
        })
    }
}

fn checked_usage(total: &UsageMetrics, step: &UsageMetrics) -> Result<UsageMetrics, ModelError> {
    let add = |left: u64, right: u64, field: &str| {
        left.checked_add(right)
            .ok_or_else(|| ModelError::Stream(format!("Cumulative model usage overflow: {field}")))
    };
    Ok(UsageMetrics {
        input_tokens: add(total.input_tokens, step.input_tokens, "input_tokens")?,
        output_tokens: add(total.output_tokens, step.output_tokens, "output_tokens")?,
        reasoning_tokens: add(
            total.reasoning_tokens,
            step.reasoning_tokens,
            "reasoning_tokens",
        )?,
        cache_creation_input_tokens: add(
            total.cache_creation_input_tokens,
            step.cache_creation_input_tokens,
            "cache_creation_input_tokens",
        )?,
        cache_read_input_tokens: add(
            total.cache_read_input_tokens,
            step.cache_read_input_tokens,
            "cache_read_input_tokens",
        )?,
    })
}

async fn send_final_event(
    tx: &tokio::sync::mpsc::Sender<AgentStreamEvent>,
    cancellation: &CancellationToken,
    event: AgentStreamEvent,
) {
    // Reserve before moving the event so cancellation can still use an available
    // slot without waiting for an abandoned consumer.
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => { let _ = tx.try_send(event); return; },
        permit = tx.reserve() => permit,
    };
    if let Ok(permit) = permit {
        permit.send(event);
    }
}

/// Dropping an engine future must leave legal tool-call/output history for the next turn.
struct TurnHistoryGuard<'a> {
    session: &'a mut ThreadSession,
    history_start: usize,
    completed: bool,
}
impl Drop for TurnHistoryGuard<'_> {
    fn drop(&mut self) {
        if !self.completed && self.session.journal().is_none() {
            self.session.close_unanswered_tool_calls(self.history_start);
        }
    }
}

/// Signal cancellation before dropping provider-owned async state, including aborts.
struct CreationFuture<F: std::future::Future> {
    inner: Pin<Box<F>>,
    cancellation: CancellationToken,
    completed: bool,
}
impl<F: std::future::Future> CreationFuture<F> {
    fn new(inner: F, cancellation: CancellationToken) -> Self {
        Self {
            inner: Box::pin(inner),
            cancellation,
            completed: false,
        }
    }
}
impl<F> std::future::Future for CreationFuture<F>
where
    F: std::future::Future<Output = Result<ModelEventStream, ModelError>>,
{
    type Output = F::Output;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll(cx);
        // Only a successful stream can transfer cancellation ownership to
        // InvocationStream. Failed construction must cancel retained tokens too.
        if matches!(&result, std::task::Poll::Ready(Ok(_))) {
            this.completed = true;
        }
        result
    }
}
impl<F: std::future::Future> Drop for CreationFuture<F> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.cancel();
        }
    }
}
struct InvocationStream {
    inner: ModelEventStream,
    cancellation: CancellationToken,
}
impl InvocationStream {
    fn new(inner: ModelEventStream, cancellation: CancellationToken) -> Self {
        Self {
            inner,
            cancellation,
        }
    }
}
impl Stream for InvocationStream {
    type Item = Result<ModelEvent, ModelError>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}
impl Drop for InvocationStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Validate a whole model step before allowing any proposed calls to execute.
struct ModelStepState {
    finished: bool,
    item_ids: HashSet<String>,
    call_ids: HashSet<String>,
    pending: HashMap<String, String>,
}
impl ModelStepState {
    fn new(history: &[CanonicalItem]) -> Self {
        Self {
            finished: false,
            item_ids: history.iter().map(|item| item.id().to_string()).collect(),
            call_ids: history
                .iter()
                .filter_map(|item| match item {
                    CanonicalItem::ToolCall { call_id, .. } => Some(call_id.clone()),
                    _ => None,
                })
                .collect(),
            pending: HashMap::new(),
        }
    }
    fn observe(&mut self, event: &ModelEvent) -> Result<(), ModelError> {
        let invalid = |message: &str| ModelError::Stream(message.into());
        if self.finished {
            return Err(invalid("Event after model step completion"));
        }
        match event {
            ModelEvent::StepFinished { .. } => {
                if self
                    .pending
                    .values()
                    .any(|kind| kind != "assistant_message")
                {
                    return Err(invalid(
                        "Model step ended with unfinished tool or reasoning items",
                    ));
                }
                self.finished = true;
            }
            ModelEvent::ItemCompleted { item } => {
                if matches!(
                    item,
                    CanonicalItem::UserMessage { .. } | CanonicalItem::ToolResult { .. }
                ) {
                    return Err(invalid("Provider emitted a non-model item"));
                }
                if !self.item_ids.insert(item.id().into()) {
                    return Err(invalid("Duplicate model item ID"));
                }
                if let CanonicalItem::ToolCall { call_id, name, .. } = item {
                    if call_id.trim().is_empty() || name.trim().is_empty() {
                        return Err(invalid("Model tool call requires call ID and name"));
                    }
                    if !self.call_ids.insert(call_id.clone()) {
                        return Err(invalid("Duplicate model tool call ID"));
                    }
                }
                self.pending.remove(item.id());
            }
            ModelEvent::ItemStarted {
                item_id, item_type, ..
            } => {
                if !matches!(
                    item_type.as_str(),
                    "assistant_message" | "reasoning" | "tool_call"
                ) {
                    return Err(invalid("Unknown model item type"));
                }
                if self.item_ids.contains(item_id) || self.pending.contains_key(item_id) {
                    return Err(invalid("Duplicate started model item ID"));
                }
                self.pending.insert(item_id.clone(), item_type.clone());
            }
            ModelEvent::TextDelta { item_id, .. }
            | ModelEvent::ReasoningDelta { item_id, .. }
            | ModelEvent::ReasoningSignature { item_id, .. }
            | ModelEvent::ToolCallDelta { item_id, .. } => {
                if self.item_ids.contains(item_id) {
                    return Err(invalid("Delta after model item completion"));
                }
                let kind = match event {
                    ModelEvent::TextDelta { .. } => "assistant_message",
                    ModelEvent::ToolCallDelta { .. } => "tool_call",
                    _ => "reasoning",
                };
                let previous = self
                    .pending
                    .entry(item_id.clone())
                    .or_insert_with(|| kind.into());
                if previous != kind {
                    return Err(invalid("Model item delta type changed"));
                }
            }
        }
        Ok(())
    }
}
