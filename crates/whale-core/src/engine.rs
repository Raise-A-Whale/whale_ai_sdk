//! Turn execution engine coordinating LLM calls, streaming SSE, tool execution, and session updates.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use futures::{Stream, StreamExt};
use reqwest::Client;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use whale_adapters::AdapterError;
use whale_protocol::canonical::{
    CanonicalContent, CanonicalItem, MessagePhase,
};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};

use crate::coordinator::ToolExecutionCoordinator;
use crate::error::CoreError;
use crate::session::ThreadSession;

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

/// Agent engine that executes conversation turns.
pub struct AgentEngine {
    http_client: Client,
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
            http_client: Client::builder().build().unwrap_or_default(),
            coordinator,
            stream_provider: None,
        }
    }

    /// Creates a new AgentEngine with custom reqwest client.
    pub fn with_client(http_client: Client, coordinator: Arc<ToolExecutionCoordinator>) -> Self {
        Self {
            http_client,
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
    ///   1. Serialize request via `session.adapter.serialize_request`.
    ///   2. Send HTTP POST to provider endpoint.
    ///   3. Parse SSE stream into `AgentStreamEvent`s.
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
        let turn_id = Uuid::new_v4().to_string();
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
        if let Some(input) = user_input {
            let user_item = CanonicalItem::user_text(input);
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
            steps_taken = step + 1;
            debug!("Starting turn step {}/{}", steps_taken, max_steps);

            let mut event_stream: Pin<
                Box<dyn Stream<Item = Result<AgentStreamEvent, AdapterError>> + Send>,
            > = if let Some(ref provider) = self.stream_provider {
                provider(session, step)?
            } else {
                let tool_defs = session.get_tool_definitions();
                let adapter = Arc::clone(session.adapter());
                let (body, headers) = adapter.serialize_request(
                    session.system_prompt(),
                    session.history(),
                    &tool_defs,
                    session.sampling_options(),
                )?;

                let endpoint_url = adapter.endpoint_url();
                debug!("Dispatching request to {}: {}", adapter.provider_name(), endpoint_url);

                let response = self
                    .http_client
                    .post(&endpoint_url)
                    .headers(headers)
                    .json(&body)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    let status = response.status();
                    let err_text = response.text().await.unwrap_or_default();
                    error!("Provider API error ({}): {}", status, err_text);
                    let _ = event_tx
                        .send(AgentStreamEvent::TurnFailed {
                            turn_id: turn_id.clone(),
                            thread_id: thread_id.clone(),
                            error_code: status.as_str().to_string(),
                            error_message: err_text.clone(),
                        })
                        .await;
                    return Err(CoreError::Internal(format!(
                        "API error {}: {}",
                        status, err_text
                    )));
                }

                let byte_stream = Box::pin(response.bytes_stream());
                adapter.parse_stream(byte_stream)
            };

            let mut step_completed_items: Vec<CanonicalItem> = Vec::new();
            let mut active_text_deltas: HashMap<String, (String, MessagePhase)> = HashMap::new();
            let mut active_reasoning_deltas: HashMap<String, (String, Option<String>)> = HashMap::new();
            let mut active_tool_deltas: HashMap<String, (String, String, Option<String>, String)> =
                HashMap::new();

            while let Some(event_res) = event_stream.next().await {
                let event = match event_res {
                    Ok(ev) => ev,
                    Err(e) => {
                        warn!("Stream parse error: {}", e);
                        continue;
                    }
                };

                // Track accumulated state for deltas
                match &event {
                    AgentStreamEvent::ItemStarted {
                        item_id,
                        item_type,
                        phase,
                        ..
                    } => {
                        if item_type == "assistant_message" {
                            active_text_deltas
                                .insert(item_id.clone(), (String::new(), phase.unwrap_or(MessagePhase::FinalAnswer)));
                        } else if item_type == "reasoning" {
                            active_reasoning_deltas.insert(item_id.clone(), (String::new(), None));
                        }
                    }
                    AgentStreamEvent::TextDelta { item_id, delta, .. } => {
                        if let Some((text, _)) = active_text_deltas.get_mut(item_id) {
                            text.push_str(delta);
                        } else {
                            active_text_deltas
                                .insert(item_id.clone(), (delta.clone(), MessagePhase::FinalAnswer));
                        }
                    }
                    AgentStreamEvent::ReasoningDelta { item_id, delta, .. } => {
                        if let Some((thinking, _)) = active_reasoning_deltas.get_mut(item_id) {
                            thinking.push_str(delta);
                        } else {
                            active_reasoning_deltas.insert(item_id.clone(), (delta.clone(), None));
                        }
                    }
                    AgentStreamEvent::ReasoningSignature { item_id, signature, .. } => {
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
                        step_completed_items.push(item.clone());
                        session.append_item(item.clone());
                        turn_generated_items.push(item.clone());
                    }
                    AgentStreamEvent::TurnCompleted { usage, .. } => {
                        total_usage.input_tokens += usage.input_tokens;
                        total_usage.output_tokens += usage.output_tokens;
                        total_usage.reasoning_tokens += usage.reasoning_tokens;
                        total_usage.cache_creation_input_tokens += usage.cache_creation_input_tokens;
                        total_usage.cache_read_input_tokens += usage.cache_read_input_tokens;
                    }
                    _ => {}
                }

                // Forward event to caller
                if event_tx.send(event).await.is_err() {
                    return Err(CoreError::EventChannelClosed);
                }
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
            let tool_calls: Vec<CanonicalItem> = step_completed_items
                .iter()
                .filter(|item| matches!(item, CanonicalItem::ToolCall { .. }))
                .cloned()
                .collect();

            if tool_calls.is_empty() {
                info!("No tool calls in step {}; turn completed successfully.", steps_taken);
                break;
            }

            // Execute tool calls via coordinator
            info!("Executing {} tool call(s) for step {}", tool_calls.len(), steps_taken);
            let results = self
                .coordinator
                .execute_calls_with_override_registry(
                    &turn_id,
                    tool_calls,
                    Some(Arc::clone(session.tools())),
                    Some(event_tx.clone()),
                )
                .await;

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
                let _ = event_tx
                    .send(AgentStreamEvent::TurnFailed {
                        turn_id: turn_id.clone(),
                        thread_id: thread_id.clone(),
                        error_code: "MAX_STEPS_EXCEEDED".to_string(),
                        error_message: format!("Turn exceeded maximum steps limit of {}", max_steps),
                    })
                    .await;
                return Err(CoreError::MaxStepsExceeded(max_steps));
            }
        }

        // Emit TurnCompleted event
        let _ = event_tx
            .send(AgentStreamEvent::TurnCompleted {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
                usage: total_usage.clone(),
            })
            .await;

        Ok(RunTurnResult {
            turn_id,
            steps_taken,
            total_usage,
            generated_items: turn_generated_items,
        })
    }
}
