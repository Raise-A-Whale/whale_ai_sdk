use crate::{Result, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use whale_protocol::contexts::ToolExecutionRecord;
use whale_protocol::events::UsageMetrics;
pub use whale_protocol::recovery::{ModelInputRecord, UnknownExecution};
use whale_protocol::recovery::{RecoveredRun, RecoverySnapshot};
use whale_protocol::rpc::{RunTurnResult, TurnStatus};
use whale_protocol::runs::{RunFailure, RunSnapshot, RunStatus, StartTurnParams};
use whale_protocol::{CanonicalItem, CanonicalToolOutput};
#[derive(Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub schema_version: u32,
    #[serde(default)]
    pub created_at_ms: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: Option<u64>,
    #[serde(default)]
    pub detached_since_ms: Option<u64>,
    #[serde(default)]
    pub retired_at_ms: Option<u64>,
    #[serde(default)]
    pub retirement_reason: Option<String>,
    pub recovery_id: String,
    pub secret_digest: String,
    pub revision: u64,
    pub epoch: u64,
    pub owner: Option<String>,
    pub thread_id: Option<String>,
    pub historical_thread_ids: Vec<String>,
    pub forgotten: bool,
    pub configuration: Value,
    pub history: Vec<CanonicalItem>,
    pub runs: BTreeMap<String, StoredRun>,
    pub unknown_executions: Vec<UnknownExecution>,
}
impl std::fmt::Debug for SessionRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRecord")
            .field("recovery_id", &self.recovery_id)
            .field("revision", &self.revision)
            .field("epoch", &self.epoch)
            .field("forgotten", &self.forgotten)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRun {
    pub params: StartTurnParams,
    pub snapshot: RunSnapshot,
    pub effective_options: Value,
    pub history_start: usize,
    pub model_inputs: Vec<ModelInputRecord>,
    pub calls: Vec<StoredCall>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCall {
    pub call_id: String,
    pub step_id: String,
    pub tool_name: String,
    pub result_item_id: String,
    pub execution_id: String,
    pub intent: Option<ToolExecutionRecord>,
    pub outcome: Option<CanonicalItem>,
    pub committed: bool,
}
#[derive(Debug, Clone)]
pub enum RunMutation {
    ModelInput {
        step_id: String,
        step_index: usize,
        request: Value,
    },
    ModelItem {
        step_id: String,
        item: CanonicalItem,
    },
    ModelStepFinished {
        step_id: String,
        usage: UsageMetrics,
    },
    DispatchIntent {
        call_id: String,
        execution: ToolExecutionRecord,
    },
    ToolOutcome {
        call_id: String,
        result: CanonicalItem,
    },
    ToolBatch {
        call_ids: Vec<String>,
    },
}
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    pub revision: u64,
    pub result_item: Option<CanonicalItem>,
}
#[derive(Debug, Clone)]
pub struct FinalizedRun {
    pub snapshot: RunSnapshot,
    pub history: Vec<CanonicalItem>,
}
impl SessionRecord {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.schema_version, 1 | 2)
            || uuid::Uuid::parse_str(&self.recovery_id).is_err()
            || self.secret_digest.len() != 64
            || !self.secret_digest.bytes().all(|b| b.is_ascii_hexdigit())
            || self.revision == 0
            || self.owner.is_some() != self.thread_id.is_some()
            || !self.configuration.is_object()
            || self.epoch == 0
        {
            return Err(StoreError::Invalid("Malformed session record".into()));
        }
        if self.schema_version == 2
            && (self.created_at_ms.is_none()
                || self.updated_at_ms < self.created_at_ms
                || (self.owner.is_some() && self.detached_since_ms.is_some())
                || (!self.forgotten && self.owner.is_none() && self.detached_since_ms.is_none())
                || (self.forgotten != self.retired_at_ms.is_some())
                || (self.forgotten != self.retirement_reason.is_some()))
        {
            return Err(StoreError::Invalid("Malformed retention metadata".into()));
        }
        if self.forgotten
            && (self.owner.is_some()
                || !self.history.is_empty()
                || !self.runs.is_empty()
                || !self.unknown_executions.is_empty())
        {
            return Err(StoreError::Invalid("Malformed tombstone".into()));
        }
        if self.owner.as_ref().is_some_and(|v| v.trim().is_empty())
            || self
                .thread_id
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || !self.historical_thread_ids.contains(v))
            || (self.active() && self.owner.is_none())
        {
            return Err(StoreError::Invalid("Malformed live attachment".into()));
        }
        let mut attachment_ids = HashSet::new();
        if self
            .historical_thread_ids
            .iter()
            .any(|id| id.is_empty() || !attachment_ids.insert(id))
        {
            return Err(StoreError::Invalid("Duplicate attachment identity".into()));
        }
        let mut ids = HashSet::new();
        if self.history.iter().any(|item| !ids.insert(item.id())) {
            return Err(StoreError::Invalid("Duplicate history item ID".into()));
        }
        for (id, run) in &self.runs {
            if id != &run.params.turn_id
                || id != &run.snapshot.turn_id
                || run.params.thread_id != run.snapshot.thread_id
                || run.history_start > self.history.len()
                || (run.snapshot.status.is_terminal() && run.snapshot.result.is_none())
            {
                return Err(StoreError::Invalid("Malformed run record".into()));
            }
        }
        let mut call_ids = HashSet::new();
        let mut execution_ids = HashSet::new();
        let mut result_ids = HashSet::new();
        for run in self.runs.values() {
            let mut steps = HashSet::new();
            for (index, step) in run.model_inputs.iter().enumerate() {
                if step.step_index != index
                    || step.step_id.is_empty()
                    || !steps.insert(&step.step_id)
                    || !step.request.is_object()
                    || step.completed != step.usage.is_some()
                    || step.history_revision > self.revision
                {
                    return Err(StoreError::Invalid("Malformed model input ledger".into()));
                }
            }
            for call in &run.calls {
                if call.call_id.is_empty() || !call_ids.insert(&call.call_id) || !execution_ids.insert(&call.execution_id) || !result_ids.insert(&call.result_item_id) || !steps.contains(&call.step_id) || !self.history.iter().any(|item|matches!(item,CanonicalItem::ToolCall{call_id,name,..} if call_id==&call.call_id && name==&call.tool_name)) {return Err(StoreError::Invalid("Malformed tool ledger identity".into()))}
                if call
                    .intent
                    .as_ref()
                    .is_some_and(|intent| intent.call_id != call.call_id)
                    || (call.committed && call.outcome.is_none())
                {
                    return Err(StoreError::Invalid("Malformed tool ledger state".into()));
                }
                if let Some(outcome) = &call.outcome {
                    if !matches!(outcome,CanonicalItem::ToolResult{id,call_id,..} if id==&call.result_item_id && call_id==&call.call_id)
                        || (call.committed && !self.history.contains(outcome))
                    {
                        return Err(StoreError::Invalid("Malformed stored tool result".into()));
                    }
                }
            }
            if let Some(result) = &run.snapshot.result {
                let status = match run.snapshot.status {
                    RunStatus::Completed => TurnStatus::Completed,
                    RunStatus::Cancelled => TurnStatus::Interrupted,
                    _ => TurnStatus::Failed,
                };
                if result.thread_id != run.snapshot.thread_id
                    || result.turn_id != run.snapshot.turn_id
                    || result.status != status
                    || result.items != run.snapshot.items
                    || result.usage != run.snapshot.usage
                {
                    return Err(StoreError::Invalid("Malformed terminal result".into()));
                }
            }
        }
        let mut unknown_ids = HashSet::new();
        for unknown in &self.unknown_executions {
            if !unknown_ids.insert(&unknown.execution_id)
                || !self.runs.get(&unknown.turn_id).is_some_and(|run| {
                    run.calls.iter().any(|call| {
                        call.execution_id == unknown.execution_id
                            && call.call_id == unknown.call_id
                            && call.intent.is_some()
                            && call.outcome.is_some()
                    })
                })
            {
                return Err(StoreError::Invalid("Malformed unknown execution".into()));
            }
        }
        Ok(())
    }
    pub fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            recovery_id: self.recovery_id.clone(),
            revision: self.revision,
            epoch: self.epoch,
            attached: self.owner.is_some(),
            configuration: self.configuration.clone(),
            history: self.history.clone(),
            runs: self
                .runs
                .values()
                .map(|run| RecoveredRun {
                    snapshot: run.snapshot.clone(),
                    model_inputs: run.model_inputs.clone(),
                    effective_options: run.effective_options.clone(),
                })
                .collect(),
            unknown_executions: self.unknown_executions.clone(),
        }
    }
    pub(crate) fn unresolved(&self) -> bool {
        self.unknown_executions.iter().any(|u| !u.acknowledged)
    }
    pub(crate) fn active(&self) -> bool {
        self.runs.values().any(|r| !r.snapshot.status.is_terminal())
    }
    pub(crate) fn begin(
        &mut self,
        params: StartTurnParams,
        snapshot: RunSnapshot,
        effective_options: Value,
    ) -> Result<bool> {
        if let Some(old) = self.runs.get(&params.turn_id) {
            return if old.params == params && old.effective_options == effective_options {
                Ok(false)
            } else {
                Err(StoreError::Conflict)
            };
        }
        if self.active() {
            return Err(StoreError::Active);
        }
        if self.unresolved() {
            return Err(StoreError::UnknownExecutions);
        }
        if self.thread_id.as_deref() != Some(&params.thread_id)
            || params.turn_id.is_empty()
            || params.max_steps == 0
            || params.timeout_ms == Some(0)
            || snapshot.thread_id != params.thread_id
            || snapshot.turn_id != params.turn_id
            || snapshot.status.is_terminal()
            || snapshot.result.is_some()
        {
            return Err(StoreError::Invalid("Invalid run acceptance".into()));
        }
        if let Some(value) = self
            .configuration
            .pointer("/session/limits")
            .filter(|v| !v.is_null())
        {
            let limits: whale_protocol::retention::SessionLimits =
                serde_json::from_value(value.clone()).map_err(|error| {
                    StoreError::Invalid(format!("Invalid session limits: {error}"))
                })?;
            limits.validate().map_err(StoreError::Invalid)?;
            if limits
                .max_accepted_turns
                .is_some_and(|max| self.runs.len() as u64 >= max)
            {
                return Err(StoreError::LimitExceeded("max_accepted_turns".into()));
            }
            if let Some(max) = limits.max_history_bytes {
                let bytes =
                    whale_protocol::retention::history_bytes(&self.history, &params.input_items)
                        .map_err(|e| StoreError::Invalid(e.to_string()))?;
                if bytes > max {
                    return Err(StoreError::LimitExceeded("max_history_bytes".into()));
                }
            }
        }
        let mut ids = self.history.iter().map(|i| i.id()).collect::<HashSet<_>>();
        if params.input_items.iter().any(|i| !ids.insert(i.id())) {
            return Err(StoreError::Invalid("Duplicate input item ID".into()));
        }
        let history_start = self.history.len();
        self.history.extend(params.input_items.clone());
        let mut snapshot = snapshot;
        snapshot.items = params.input_items.clone();
        self.runs.insert(
            params.turn_id.clone(),
            StoredRun {
                params,
                snapshot,
                effective_options,
                history_start,
                model_inputs: vec![],
                calls: vec![],
            },
        );
        Ok(true)
    }
    pub(crate) fn mutate(&mut self, turn: &str, op: RunMutation) -> Result<Option<CanonicalItem>> {
        if matches!(
            &op,
            RunMutation::ModelInput { .. } | RunMutation::DispatchIntent { .. }
        ) {
            if let Some(value) = self
                .configuration
                .pointer("/session/limits")
                .filter(|v| !v.is_null())
            {
                let limits: whale_protocol::retention::SessionLimits =
                    serde_json::from_value(value.clone())
                        .map_err(|e| StoreError::Invalid(e.to_string()))?;
                limits.validate().map_err(StoreError::Invalid)?;
                if let Some(max) = limits.max_history_bytes {
                    let size = whale_protocol::retention::history_bytes(&self.history, &[])
                        .map_err(|e| StoreError::Invalid(e.to_string()))?;
                    if size > max {
                        return Err(StoreError::LimitExceeded("max_history_bytes".into()));
                    }
                }
                if let (Some(max), RunMutation::ModelInput { request, .. }) =
                    (limits.max_model_request_bytes, &op)
                {
                    let size = whale_protocol::retention::serialized_bytes(request)
                        .map_err(|e| StoreError::Invalid(e.to_string()))?;
                    if size > max {
                        return Err(StoreError::LimitExceeded("max_model_request_bytes".into()));
                    }
                }
            }
        }
        let run = self.runs.get_mut(turn).ok_or(StoreError::NotFound)?;
        if run.snapshot.status.is_terminal() {
            return Err(StoreError::Invalid("Run is terminal".into()));
        }
        let mut returned = None;
        match op {
            RunMutation::ModelInput {
                step_id,
                step_index,
                request,
            } => {
                if step_id.is_empty()
                    || step_index != run.model_inputs.len()
                    || !request.is_object()
                    || run.model_inputs.last().is_some_and(|s| !s.completed)
                    || run.calls.iter().any(|c| !c.committed)
                    || run.model_inputs.iter().any(|s| s.step_id == step_id)
                {
                    return Err(StoreError::Invalid("Invalid model step order".into()));
                }
                run.model_inputs.push(ModelInputRecord {
                    step_id,
                    step_index,
                    history_revision: self.revision,
                    request,
                    completed: false,
                    usage: None,
                });
            }
            RunMutation::ModelItem { step_id, item } => {
                let step = run
                    .model_inputs
                    .last()
                    .ok_or_else(|| StoreError::Invalid("Missing model input".into()))?;
                if step.step_id != step_id
                    || step.completed
                    || matches!(
                        item,
                        CanonicalItem::UserMessage { .. } | CanonicalItem::ToolResult { .. }
                    )
                    || self.history.iter().any(|i| i.id() == item.id())
                {
                    return Err(StoreError::Invalid("Invalid model item".into()));
                }
                if let CanonicalItem::ToolCall { call_id, name, .. } = &item {
                    if call_id.is_empty()
                        || self.history.iter().any(
                            |i| matches!(i,CanonicalItem::ToolCall{call_id:old,..} if old==call_id),
                        )
                    {
                        return Err(StoreError::Invalid("Duplicate model call ID".into()));
                    }
                    run.calls.push(StoredCall {
                        call_id: call_id.clone(),
                        step_id,
                        tool_name: name.clone(),
                        result_item_id: uuid::Uuid::new_v4().to_string(),
                        execution_id: uuid::Uuid::new_v4().to_string(),
                        intent: None,
                        outcome: None,
                        committed: false,
                    });
                }
                self.history.push(item);
            }
            RunMutation::ModelStepFinished { step_id, usage } => {
                checked_usage(
                    run.model_inputs
                        .iter()
                        .filter_map(|step| step.usage.as_ref())
                        .chain(std::iter::once(&usage)),
                )?;
                let step = run
                    .model_inputs
                    .last_mut()
                    .ok_or_else(|| StoreError::Invalid("Missing model input".into()))?;
                if step.step_id != step_id || step.completed {
                    return Err(StoreError::Invalid("Invalid model completion".into()));
                }
                step.completed = true;
                step.usage = Some(usage);
            }
            RunMutation::DispatchIntent { call_id, execution } => {
                let call = run
                    .calls
                    .iter_mut()
                    .find(|c| c.call_id == call_id)
                    .ok_or_else(|| StoreError::Invalid("Unknown tool call".into()))?;
                if execution.call_id != call_id
                    || call.intent.is_some()
                    || call.outcome.is_some()
                    || !run
                        .model_inputs
                        .iter()
                        .any(|s| s.step_id == call.step_id && s.completed)
                {
                    return Err(StoreError::Invalid(
                        "Tool dispatch cannot be repeated or precede step completion".into(),
                    ));
                }
                call.intent = Some(execution);
            }
            RunMutation::ToolOutcome { call_id, result } => {
                let call = run
                    .calls
                    .iter_mut()
                    .find(|c| c.call_id == call_id)
                    .ok_or_else(|| StoreError::Invalid("Unknown tool call".into()))?;
                if !run
                    .model_inputs
                    .iter()
                    .any(|s| s.step_id == call.step_id && s.completed)
                {
                    return Err(StoreError::Invalid(
                        "Tool outcome precedes model step completion".into(),
                    ));
                }
                let CanonicalItem::ToolResult {
                    id: _,
                    call_id: result_call,
                    output,
                    is_error,
                } = result
                else {
                    return Err(StoreError::Invalid("Expected tool result".into()));
                };
                if result_call != call_id {
                    return Err(StoreError::Invalid("Wrong outcome identity".into()));
                }
                let item = CanonicalItem::ToolResult {
                    id: call.result_item_id.clone(),
                    call_id,
                    output,
                    is_error,
                };
                if call.outcome.as_ref().is_some_and(|old| old != &item) {
                    return Err(StoreError::Conflict);
                }
                call.outcome = Some(item.clone());
                returned = Some(item);
            }
            RunMutation::ToolBatch { call_ids } => {
                let calls = run
                    .calls
                    .iter()
                    .filter(|c| !c.committed)
                    .collect::<Vec<_>>();
                if calls.iter().map(|c| c.call_id.clone()).collect::<Vec<_>>() != call_ids
                    || calls.iter().any(|c| c.outcome.is_none())
                {
                    return Err(StoreError::Invalid(
                        "Incomplete or out-of-order tool batch".into(),
                    ));
                }
                for call in run.calls.iter_mut().filter(|c| !c.committed) {
                    self.history.push(call.outcome.clone().unwrap());
                    call.committed = true;
                }
            }
        }
        run.snapshot.items = self.history[run.history_start..].to_vec();
        Ok(returned)
    }
    pub(crate) fn finalize(
        &mut self,
        turn: &str,
        mut candidate: RunSnapshot,
    ) -> Result<FinalizedRun> {
        let run = self.runs.get_mut(turn).ok_or(StoreError::NotFound)?;
        if run.snapshot.status.is_terminal() {
            return Ok(FinalizedRun {
                snapshot: run.snapshot.clone(),
                history: self.history.clone(),
            });
        }
        if candidate.thread_id != run.params.thread_id
            || candidate.turn_id != turn
            || !candidate.status.is_terminal()
        {
            return Err(StoreError::Invalid(
                "Invalid terminal identity/status".into(),
            ));
        }
        let mut incomplete = false;
        for call in &mut run.calls {
            if call.outcome.is_none() {
                incomplete = true;
                let unknown = call.intent.is_some();
                let reason = if unknown {
                    "Tool execution was dispatched but no result was durably recorded. External side effects are unknown; the call was not retried."
                } else {
                    "Tool was not dispatched before the run ended; the call was not retried."
                };
                call.outcome = Some(CanonicalItem::ToolResult {
                    id: call.result_item_id.clone(),
                    call_id: call.call_id.clone(),
                    output: CanonicalToolOutput::text(reason),
                    is_error: true,
                });
                if unknown {
                    self.unknown_executions.push(UnknownExecution {
                        execution_id: call.execution_id.clone(),
                        turn_id: turn.into(),
                        call_id: call.call_id.clone(),
                        tool_name: call.tool_name.clone(),
                        original_arguments: call
                            .intent
                            .as_ref()
                            .map(|i| i.original_arguments.clone()),
                        arguments: call.intent.as_ref().map(|i| i.arguments.clone()),
                        reason: reason.into(),
                        acknowledged: false,
                    });
                }
            }
            if !call.committed {
                self.history.push(call.outcome.clone().unwrap());
                call.committed = true;
            }
        }
        if incomplete && candidate.status == RunStatus::Completed {
            candidate.status = RunStatus::Failed;
            candidate.error = Some(RunFailure {
                code: "INCOMPLETE_EXECUTION".into(),
                message: "Completed candidate contains unfinished calls".into(),
            });
        }
        candidate.items = self.history[run.history_start..].to_vec();
        candidate.pending_approvals.clear();
        candidate.tool_executions = run.calls.iter().filter_map(|c| c.intent.clone()).collect();
        // The stored model-step usage is authoritative; interrupted partial streams
        // have no known usage and are not invented during recovery.
        candidate.usage = checked_usage(
            run.model_inputs
                .iter()
                .filter_map(|step| step.usage.as_ref()),
        )?;
        candidate.last_seq = candidate
            .last_seq
            .max(run.snapshot.last_seq.saturating_add(1));
        candidate.result = Some(RunTurnResult {
            thread_id: candidate.thread_id.clone(),
            turn_id: turn.into(),
            status: match candidate.status {
                RunStatus::Completed => TurnStatus::Completed,
                RunStatus::Cancelled => TurnStatus::Interrupted,
                _ => TurnStatus::Failed,
            },
            items: candidate.items.clone(),
            usage: candidate.usage.clone(),
        });
        run.snapshot = candidate.clone();
        Ok(FinalizedRun {
            snapshot: candidate,
            history: self.history.clone(),
        })
    }
    pub(crate) fn recover(&mut self) -> Result<bool> {
        if self.forgotten {
            return Ok(false);
        }
        let active = self
            .runs
            .iter()
            .filter(|(_, r)| !r.snapshot.status.is_terminal())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let changed = self.owner.is_some() || !active.is_empty();
        for turn in active {
            let mut candidate = self.runs[&turn].snapshot.clone();
            candidate.status = RunStatus::Failed;
            candidate.error = Some(RunFailure {
                code: "RECOVERY_INTERRUPTED".into(),
                message:
                    "Daemon stopped before the run was durably finalized; execution was not resumed"
                        .into(),
            });
            self.finalize(&turn, candidate)?;
        }
        self.owner = None;
        self.thread_id = None;
        Ok(changed)
    }
}

fn checked_usage<'a>(usages: impl Iterator<Item = &'a UsageMetrics>) -> Result<UsageMetrics> {
    let mut total = UsageMetrics::default();
    for usage in usages {
        macro_rules! add {
            ($field:ident) => {
                total.$field = total
                    .$field
                    .checked_add(usage.$field)
                    .ok_or_else(|| StoreError::Invalid("Model usage exceeds u64 range".into()))?;
            };
        }
        add!(input_tokens);
        add!(output_tokens);
        add!(reasoning_tokens);
        add!(cache_creation_input_tokens);
        add!(cache_read_input_tokens);
    }
    Ok(total)
}
