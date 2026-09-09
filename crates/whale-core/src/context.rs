//! Model-context projection policies. The session owns the audit history; a
//! policy receives a snapshot and returns a separate model-visible projection.

use std::collections::BTreeSet;

use async_trait::async_trait;
use whale_protocol::canonical::CanonicalItem;
use whale_protocol::contexts::{ContextBuildRequest, ModelContext};

use crate::execution::CancellationToken;

/// Builds the next model request without executing tools or mutating a session.
/// Implementations should cooperate with cancellation. The engine validates every
/// returned projection, including results from application-provided policies.
#[async_trait]
pub trait ContextPolicy: Send + Sync {
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelContext, String>;
}

/// Passes through the complete validated history and system prompt.
#[derive(Debug, Default)]
pub struct FullHistoryContext;

/// Retains the most recent complete user turns. One user turn can contain many
/// model requests and tool batches. It is never cut at a tool or model boundary.
#[derive(Debug, Clone)]
pub struct RecentTurnsContext {
    max_turns: usize,
}

impl RecentTurnsContext {
    pub fn new(max_turns: usize) -> Result<Self, String> {
        if max_turns == 0 {
            return Err("Recent-turn context policy requires max_turns > 0".into());
        }
        Ok(Self { max_turns })
    }
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), String> {
    if cancellation.is_cancelled() {
        Err("Context construction cancelled".into())
    } else {
        Ok(())
    }
}

#[async_trait]
impl ContextPolicy for FullHistoryContext {
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelContext, String> {
        check_cancelled(&cancellation)?;
        let context = ModelContext {
            system_prompt: request.system_prompt,
            items: request.history,
        };
        validate_model_context(&context)?;
        check_cancelled(&cancellation)?;
        Ok(context)
    }
}

#[async_trait]
impl ContextPolicy for RecentTurnsContext {
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelContext, String> {
        check_cancelled(&cancellation)?;
        // Validate before pruning: discarding an old turn must not hide a
        // malformed call/result group. Interrupted-call repair belongs to the
        // audit-history owner and must be explicit there.
        validate_items(&request.history)?;
        let user_starts: Vec<_> = request
            .history
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                matches!(item, CanonicalItem::UserMessage { .. }).then_some(index)
            })
            .collect();
        let start = if user_starts.len() > self.max_turns {
            user_starts[user_starts.len() - self.max_turns]
        } else {
            // Preserve imported preamble when no user turn is being removed.
            0
        };
        let context = ModelContext {
            system_prompt: request.system_prompt,
            items: request.history.into_iter().skip(start).collect(),
        };
        validate_model_context(&context)?;
        check_cancelled(&cancellation)?;
        Ok(context)
    }
}

/// Validates the model-visible conversation's tool structure. Call IDs must be
/// unique throughout the projection. Each contiguous call batch must receive
/// exactly one result per call before another message or batch begins. Results
/// may arrive in any order; validation never reorders or repairs the input.
///
/// Item IDs and tool-output payloads are intentionally not rewritten or compared:
/// a host policy may create new summary items, and error results still answer calls.
pub fn validate_model_context(context: &ModelContext) -> Result<(), String> {
    validate_items(&context.items)
}

fn validate_items(items: &[CanonicalItem]) -> Result<(), String> {
    let mut seen_calls = BTreeSet::new();
    let mut pending = BTreeSet::new();
    let mut results_started = false;

    for (index, item) in items.iter().enumerate() {
        match item {
            CanonicalItem::ToolCall { call_id, .. } => {
                if call_id.is_empty() {
                    return Err(format!("Context item {index} has an empty tool call_id"));
                }
                if !seen_calls.insert(call_id.as_str()) {
                    return Err(format!(
                        "Context contains duplicate tool call_id '{call_id}'"
                    ));
                }
                if results_started && !pending.is_empty() {
                    return Err(format!(
                        "Context starts tool call '{call_id}' before the preceding call batch is fully answered"
                    ));
                }
                pending.insert(call_id.as_str());
                results_started = false;
            }
            CanonicalItem::ToolResult { call_id, .. } => {
                if !pending.remove(call_id.as_str()) {
                    return Err(format!(
                        "Context item {index} has an orphan or duplicate tool result for '{call_id}'"
                    ));
                }
                results_started = !pending.is_empty();
            }
            _ => {
                if !pending.is_empty() {
                    return Err(format!(
                        "Context item {index} interrupts an unanswered tool-call batch: {}",
                        pending.iter().copied().collect::<Vec<_>>().join(", ")
                    ));
                }
                results_started = false;
            }
        }
    }

    if !pending.is_empty() {
        return Err(format!(
            "Context contains unanswered tool calls: {}",
            pending.iter().copied().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(())
}
