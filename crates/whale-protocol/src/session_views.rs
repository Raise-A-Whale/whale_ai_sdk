//! Authoritative live Session snapshots and bounded replay contracts.
//!
//! Session cursors are scoped to one live attachment. They are independent of
//! per-Run sequence numbers and durable Store revisions.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashSet, fmt};

use crate::{
    canonical::{CanonicalItem, MessagePhase},
    contexts::ToolExecutionRecord,
    events::{AgentStreamEvent, UsageMetrics},
    runs::{PendingApproval, RunEvent, RunEventPayload, RunFailure, RunSnapshot, RunStatus},
};

pub const CAPABILITY_SESSION_VIEWS: &str = "session_views.v1";
pub const CAPABILITY_SESSION_EVENT_REPLAY: &str = "session_event_replay.v1";

pub const METHOD_SESSION_GET: &str = "session.get";
pub const METHOD_SESSION_SUBSCRIBE: &str = "session.subscribe";
pub const METHOD_SESSION_EVENT: &str = "session.event";

/// Maximum number of retained events returned by one replay page.
pub const MAX_SESSION_REPLAY_PAGE_LIMIT: u32 = 256;
/// Maximum canonical history items returned by one Session snapshot.
pub const MAX_SESSION_HISTORY_LIMIT: u32 = 1024;

/// Position in the event stream of one exact live Session attachment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionCursor {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

impl SessionCursor {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.stream_id, "stream_id")
    }

    /// Returns the next cursor without wrapping sequence arithmetic.
    pub fn checked_next(&self) -> Result<Self, String> {
        self.validate()?;
        Ok(Self {
            thread_id: self.thread_id.clone(),
            stream_id: self.stream_id.clone(),
            seq: self
                .seq
                .checked_add(1)
                .ok_or_else(|| "Session cursor sequence is exhausted".to_string())?,
        })
    }

    fn same_stream(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id && self.stream_id == other.stream_id
    }
}

/// Safe, application-facing Session metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSummary {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    /// Visible projection revision; independent of Store recovery revisions.
    pub revision: u64,
}

impl SessionSummary {
    pub fn new(thread_id: impl Into<String>, created_at_ms: u64) -> Self {
        Self {
            thread_id: thread_id.into(),
            agent_name: None,
            metadata: Map::new(),
            created_at_ms,
            updated_at_ms: created_at_ms,
            revision: 0,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        if self
            .agent_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty() || name.trim() != name)
        {
            return Err("agent_name must be nonempty and unpadded when present".into());
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err("updated_at_ms must not precede created_at_ms".into());
        }
        Ok(())
    }
}

/// Partial item state required to repaint an active Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InProgressItemProjection {
    pub item_id: String,
    pub item_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<MessagePhase>,
    #[serde(default)]
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default)]
    pub raw_arguments: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_signature: Option<String>,
}

impl InProgressItemProjection {
    pub fn new(item_id: impl Into<String>, item_type: impl Into<String>) -> Self {
        Self {
            item_id: item_id.into(),
            item_type: item_type.into(),
            phase: None,
            text: String::new(),
            call_id: None,
            raw_arguments: String::new(),
            reasoning_signature: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.item_id, "item_id")?;
        validate_identity(&self.item_type, "item_type")?;
        validate_optional_identity(self.call_id.as_deref(), "call_id")?;
        validate_optional_identity(self.reasoning_signature.as_deref(), "reasoning_signature")
    }
}

/// Authoritative state for the currently active Run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionRunView {
    pub snapshot: RunSnapshot,
    #[serde(default)]
    pub in_progress_items: Vec<InProgressItemProjection>,
    pub accepted_at_ms: u64,
    pub updated_at_ms: u64,
}

impl SessionRunView {
    pub fn new(snapshot: RunSnapshot, accepted_at_ms: u64) -> Self {
        Self {
            snapshot,
            in_progress_items: Vec::new(),
            accepted_at_ms,
            updated_at_ms: accepted_at_ms,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.snapshot.thread_id, "run thread_id")?;
        validate_identity(&self.snapshot.turn_id, "turn_id")?;
        if self.snapshot.status.is_terminal() {
            return Err("An active Session Run view must not be terminal".into());
        }
        if self.updated_at_ms < self.accepted_at_ms {
            return Err("Run updated_at_ms must not precede accepted_at_ms".into());
        }
        let mut items = HashSet::new();
        for item in &self.in_progress_items {
            item.validate()?;
            if !items.insert(item.item_id.as_str()) {
                return Err("In-progress item identities must be unique".into());
            }
        }
        Ok(())
    }
}

/// Lightweight terminal Run state retained by the Session projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionRunSummary {
    pub turn_id: String,
    pub status: RunStatus,
    pub usage: UsageMetrics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RunFailure>,
    pub accepted_at_ms: u64,
    pub completed_at_ms: u64,
}

impl SessionRunSummary {
    pub fn new(
        turn_id: impl Into<String>,
        status: RunStatus,
        usage: UsageMetrics,
        accepted_at_ms: u64,
        completed_at_ms: u64,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            status,
            usage,
            error: None,
            accepted_at_ms,
            completed_at_ms,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.turn_id, "turn_id")?;
        if !self.status.is_terminal() {
            return Err("A Session Run summary must be terminal".into());
        }
        if self.completed_at_ms < self.accepted_at_ms {
            return Err("completed_at_ms must not precede accepted_at_ms".into());
        }
        Ok(())
    }
}

/// Bounded tail of canonical Session history with absolute item indexes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionHistoryWindow {
    pub items: Vec<CanonicalItem>,
    pub start_index: u64,
    pub total_items: u64,
    pub capacity: u32,
}

impl SessionHistoryWindow {
    pub fn new(capacity: u32) -> Result<Self, String> {
        validate_history_capacity(capacity)?;
        Ok(Self {
            items: Vec::new(),
            start_index: 0,
            total_items: 0,
            capacity,
        })
    }

    pub fn from_history(history: &[CanonicalItem], capacity: u32) -> Result<Self, String> {
        validate_history_capacity(capacity)?;
        let mut identities = HashSet::new();
        for item in history {
            validate_identity(item.id(), "history item_id")?;
            if !identities.insert(item.id()) {
                return Err("Canonical history item identities must be unique".into());
            }
        }
        let total_items = u64::try_from(history.len())
            .map_err(|_| "Canonical history length exceeds u64".to_string())?;
        let keep = history.len().min(capacity as usize);
        let start = history.len() - keep;
        Ok(Self {
            items: history[start..].to_vec(),
            start_index: u64::try_from(start)
                .map_err(|_| "Canonical history index exceeds u64".to_string())?,
            total_items,
            capacity,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_history_capacity(self.capacity)?;
        let item_count = u64::try_from(self.items.len())
            .map_err(|_| "History window length exceeds u64".to_string())?;
        let expected_count = self.total_items.min(u64::from(self.capacity));
        if item_count != expected_count
            || self
                .start_index
                .checked_add(item_count)
                .is_none_or(|end| end != self.total_items)
        {
            return Err("History window indexes and capacity are inconsistent".into());
        }
        let mut identities = HashSet::new();
        for item in &self.items {
            validate_identity(item.id(), "history item_id")?;
            if !identities.insert(item.id()) {
                return Err("History window item identities must be unique".into());
            }
        }
        Ok(())
    }

    fn append_unless_known(
        &mut self,
        item: CanonicalItem,
        already_known: bool,
    ) -> Result<bool, String> {
        if already_known || self.items.iter().any(|old| old.id() == item.id()) {
            return Ok(false);
        }
        validate_identity(item.id(), "history item_id")?;
        self.total_items = self
            .total_items
            .checked_add(1)
            .ok_or_else(|| "Canonical history item count is exhausted".to_string())?;
        self.items.push(item);
        let capacity = self.capacity as usize;
        if self.items.len() > capacity {
            let remove = self.items.len() - capacity;
            self.items.drain(..remove);
        }
        let retained = u64::try_from(self.items.len())
            .map_err(|_| "History window length exceeds u64".to_string())?;
        self.start_index = self
            .total_items
            .checked_sub(retained)
            .ok_or_else(|| "History window indexes underflowed".to_string())?;
        Ok(true)
    }
}

/// Stable reducer failures for applying Session events to a local projection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionProjectionError {
    /// The snapshot, event, or reducer state violates an internal projection invariant.
    InvalidState { message: String },
    /// The event belongs to another live Session attachment.
    StreamMismatch {
        expected_thread: String,
        expected_stream: String,
        actual_thread: String,
        actual_stream: String,
    },
    /// The event skipped one or more Session cursors.
    SequenceGap {
        expected: SessionCursor,
        actual: SessionCursor,
    },
}

impl SessionProjectionError {
    fn invalid_state(message: String) -> Self {
        Self::InvalidState { message }
    }
}

impl fmt::Display for SessionProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState { message } => {
                write!(formatter, "invalid Session projection: {message}")
            }
            Self::StreamMismatch {
                expected_thread,
                expected_stream,
                actual_thread,
                actual_stream,
            } => write!(
                formatter,
                "Session projection stream mismatch: expected {expected_thread}/{expected_stream}, got {actual_thread}/{actual_stream}"
            ),
            Self::SequenceGap { expected, actual } => write!(
                formatter,
                "Session projection sequence gap: expected {}/{}/{}, got {}/{}/{}",
                expected.thread_id,
                expected.stream_id,
                expected.seq,
                actual.thread_id,
                actual.stream_id,
                actual.seq
            ),
        }
    }
}

impl std::error::Error for SessionProjectionError {}

/// Complete authoritative read model for one live Session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSnapshot {
    pub summary: SessionSummary,
    pub history: SessionHistoryWindow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_run: Option<SessionRunView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<SessionRunSummary>,
    pub cursor: SessionCursor,
}

impl SessionSnapshot {
    pub fn new(
        summary: SessionSummary,
        history: SessionHistoryWindow,
        cursor: SessionCursor,
    ) -> Self {
        Self {
            summary,
            history,
            active_run: None,
            last_run: None,
            cursor,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.summary.validate()?;
        self.cursor.validate()?;
        if self.summary.thread_id != self.cursor.thread_id {
            return Err("Session snapshot cursor belongs to another thread".into());
        }
        if self.summary.revision != self.cursor.seq {
            return Err("Session revision and cursor sequence must match".into());
        }
        self.history.validate()?;
        if let Some(run) = &self.active_run {
            run.validate()?;
            if run.snapshot.thread_id != self.summary.thread_id {
                return Err("Active Run belongs to another thread".into());
            }
        }
        if let Some(run) = &self.last_run {
            run.validate()?;
        }
        Ok(())
    }

    /// Applies one ordered Session event to this snapshot. Events at already-applied
    /// cursors are ignored; an out-of-order event is reported as a replay gap.
    pub fn apply(
        &mut self,
        envelope: &SessionEventEnvelope,
    ) -> Result<bool, SessionProjectionError> {
        self.validate()
            .map_err(SessionProjectionError::invalid_state)?;
        envelope
            .validate()
            .map_err(SessionProjectionError::invalid_state)?;
        if envelope.thread_id != self.summary.thread_id
            || envelope.cursor.stream_id != self.cursor.stream_id
        {
            return Err(SessionProjectionError::StreamMismatch {
                expected_thread: self.summary.thread_id.clone(),
                expected_stream: self.cursor.stream_id.clone(),
                actual_thread: envelope.thread_id.clone(),
                actual_stream: envelope.cursor.stream_id.clone(),
            });
        }
        if envelope.cursor.seq <= self.cursor.seq {
            return Ok(false);
        }
        let expected = self
            .cursor
            .checked_next()
            .map_err(SessionProjectionError::invalid_state)?;
        if envelope.cursor != expected {
            return Err(SessionProjectionError::SequenceGap {
                expected,
                actual: envelope.cursor.clone(),
            });
        }

        let mut next = self.clone();
        next.apply_payload(envelope)
            .map_err(SessionProjectionError::invalid_state)?;
        next.summary.updated_at_ms = next.summary.updated_at_ms.max(envelope.occurred_at_ms);
        next.summary.revision = envelope.cursor.seq;
        next.cursor = envelope.cursor.clone();
        next.validate()
            .map_err(SessionProjectionError::invalid_state)?;
        *self = next;
        Ok(true)
    }

    fn apply_payload(&mut self, envelope: &SessionEventEnvelope) -> Result<(), String> {
        match &envelope.payload {
            SessionEventPayload::RunChanged { run } => {
                self.active_run = Some(run.clone());
            }
            SessionEventPayload::RunEvent { event } => match &event.payload {
                RunEventPayload::Stream { event: stream } => {
                    self.apply_stream_event(event, stream, envelope.occurred_at_ms)?;
                }
                RunEventPayload::Finished { snapshot } => {
                    if !snapshot.status.is_terminal()
                        || snapshot.thread_id != self.summary.thread_id
                        || snapshot.turn_id != event.turn_id
                        || snapshot.last_seq != event.seq
                    {
                        return Err("Invalid terminal Run event in Session projection".into());
                    }
                    let active = self.active_run.as_ref().ok_or_else(|| {
                        "Terminal Run event has no matching active Run projection".to_string()
                    })?;
                    if active.snapshot.turn_id != event.turn_id {
                        return Err("Terminal Run event belongs to another active Run".into());
                    }
                    let known_items = active
                        .snapshot
                        .items
                        .iter()
                        .map(|item| item.id())
                        .collect::<HashSet<_>>();
                    for item in &snapshot.items {
                        self.history
                            .append_unless_known(item.clone(), known_items.contains(item.id()))?;
                    }
                    let mut summary = SessionRunSummary::new(
                        event.turn_id.clone(),
                        snapshot.status,
                        snapshot.usage.clone(),
                        active.accepted_at_ms,
                        envelope.occurred_at_ms.max(active.accepted_at_ms),
                    );
                    summary.error = snapshot.error.clone();
                    self.last_run = Some(summary);
                    self.active_run = None;
                }
            },
        }
        Ok(())
    }

    fn apply_stream_event(
        &mut self,
        event: &RunEvent,
        stream: &AgentStreamEvent,
        occurred_at_ms: u64,
    ) -> Result<(), String> {
        let run = self
            .active_run
            .as_mut()
            .ok_or_else(|| "Run event has no active Run projection".to_string())?;
        if run.snapshot.turn_id != event.turn_id {
            return Err("Run event belongs to another active Run".into());
        }
        if stream_turn_id(stream) != event.turn_id {
            return Err("Stream event belongs to another Run".into());
        }
        run.snapshot.last_seq = run.snapshot.last_seq.max(event.seq);
        run.updated_at_ms = run.updated_at_ms.max(occurred_at_ms);

        match stream {
            AgentStreamEvent::ToolProgress { .. }
            | AgentStreamEvent::TurnCompleted { .. }
            | AgentStreamEvent::TurnFailed { .. } => {}
            AgentStreamEvent::ToolExecutionStarted {
                call_id,
                original_arguments,
                arguments,
                ..
            } => {
                if let Some(old) = run
                    .snapshot
                    .tool_executions
                    .iter_mut()
                    .find(|record| record.call_id == *call_id)
                {
                    old.original_arguments = original_arguments.clone();
                    old.arguments = arguments.clone();
                } else {
                    run.snapshot.tool_executions.push(ToolExecutionRecord {
                        call_id: call_id.clone(),
                        original_arguments: original_arguments.clone(),
                        arguments: arguments.clone(),
                    });
                }
            }
            AgentStreamEvent::TurnStarted { thread_id, .. } => {
                if thread_id != &self.summary.thread_id {
                    return Err("Turn start belongs to another thread".into());
                }
                run.snapshot.status = RunStatus::Running;
            }
            AgentStreamEvent::ItemStarted {
                item_id,
                item_type,
                phase,
                ..
            } => {
                if !self.history.items.iter().any(|item| item.id() == item_id)
                    && !run
                        .in_progress_items
                        .iter()
                        .any(|item| item.item_id == *item_id)
                {
                    let mut draft = InProgressItemProjection::new(item_id, item_type);
                    draft.phase = *phase;
                    run.in_progress_items.push(draft);
                }
            }
            AgentStreamEvent::TextDelta { item_id, delta, .. }
            | AgentStreamEvent::ReasoningDelta { item_id, delta, .. } => {
                draft_mut(run, item_id)?.text.push_str(delta);
            }
            AgentStreamEvent::ReasoningSignature {
                item_id, signature, ..
            } => {
                draft_mut(run, item_id)?.reasoning_signature = Some(signature.clone());
            }
            AgentStreamEvent::ToolCallDelta {
                item_id,
                call_id,
                delta,
                ..
            } => {
                let draft = draft_mut(run, item_id)?;
                draft.call_id = Some(call_id.clone());
                draft.raw_arguments.push_str(delta);
            }
            AgentStreamEvent::ItemCompleted { item, .. } => {
                run.in_progress_items
                    .retain(|draft| draft.item_id != item.id());
                let already_known = run.snapshot.items.iter().any(|old| old.id() == item.id());
                append_canonical_once(&mut run.snapshot.items, item.clone());
                self.history
                    .append_unless_known(item.clone(), already_known)?;
            }
            AgentStreamEvent::ApprovalRequested {
                request_id,
                tool_call,
                reason,
                ..
            } => {
                run.snapshot.status = RunStatus::WaitingApproval;
                if !run
                    .snapshot
                    .pending_approvals
                    .iter()
                    .any(|approval| approval.request_id == *request_id)
                {
                    run.snapshot.pending_approvals.push(PendingApproval {
                        request_id: request_id.clone(),
                        tool_call: tool_call.clone(),
                        reason: reason.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// One visible Session mutation in global Session order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionEventEnvelope {
    pub thread_id: String,
    pub cursor: SessionCursor,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub payload: SessionEventPayload,
}

impl SessionEventEnvelope {
    pub fn new(
        thread_id: impl Into<String>,
        cursor: SessionCursor,
        occurred_at_ms: u64,
        payload: SessionEventPayload,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            cursor,
            occurred_at_ms,
            payload,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        self.cursor.validate()?;
        if self.cursor.thread_id != self.thread_id {
            return Err("Session event cursor belongs to another thread".into());
        }
        if self.cursor.seq == 0 {
            return Err("Session event sequence must be positive".into());
        }
        match &self.payload {
            SessionEventPayload::RunChanged { run } => {
                run.validate()?;
                if run.snapshot.thread_id != self.thread_id {
                    return Err("Changed Run belongs to another thread".into());
                }
            }
            SessionEventPayload::RunEvent { event } => {
                validate_identity(&event.turn_id, "turn_id")?;
                if event.thread_id != self.thread_id || event.seq == 0 {
                    return Err("Run event belongs to another thread".into());
                }
            }
        }
        Ok(())
    }
}

/// Closed V1 Session event wire family. New event categories require a new wire version.
///
/// `#[non_exhaustive]` preserves Rust source compatibility; it does not permit adding V1 wire
/// variants that an existing client cannot deserialize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionEventPayload {
    RunChanged { run: SessionRunView },
    RunEvent { event: RunEvent },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSessionParams {
    pub thread_id: String,
    pub history_limit: u32,
}

impl GetSessionParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_history_capacity(self.history_limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeSessionParams {
    pub thread_id: String,
    pub after: SessionCursor,
    /// Fixed high watermark for subsequent pages. Omit on the first request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<SessionCursor>,
    pub limit: u32,
}

impl SubscribeSessionParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        self.after.validate()?;
        if self.after.thread_id != self.thread_id {
            return Err("Replay cursor belongs to another thread".into());
        }
        if let Some(through) = &self.through {
            through.validate()?;
            if !self.after.same_stream(through) {
                return Err("Replay window cursors belong to different streams".into());
            }
            if self.after.seq > through.seq {
                return Err("Replay cursor is ahead of the fixed replay window".into());
            }
        }
        if self.limit == 0 || self.limit > MAX_SESSION_REPLAY_PAGE_LIMIT {
            return Err(format!(
                "Replay page limit must be between 1 and {MAX_SESSION_REPLAY_PAGE_LIMIT}"
            ));
        }
        Ok(())
    }
}

/// Expected replay discontinuities that require a fresh Session snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplayGapReason {
    Retention,
    StreamReset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayGap {
    pub reason: ReplayGapReason,
    pub requested: SessionCursor,
    /// Oldest `after` cursor from which every later event remains replayable.
    pub replay_floor: SessionCursor,
    pub current: SessionCursor,
    pub session_revision: u64,
}

impl ReplayGap {
    pub fn validate(&self) -> Result<(), String> {
        self.requested.validate()?;
        self.replay_floor.validate()?;
        self.current.validate()?;
        if self.requested.thread_id != self.current.thread_id {
            return Err("Replay gap cursors belong to different threads".into());
        }
        if !self.replay_floor.same_stream(&self.current) || self.replay_floor.seq > self.current.seq
        {
            return Err("Replay floor is outside the current stream".into());
        }
        match self.reason {
            ReplayGapReason::Retention => {
                if !self.requested.same_stream(&self.current)
                    || self.requested.seq >= self.replay_floor.seq
                {
                    return Err("Retention gap requires a cursor below the replay floor".into());
                }
            }
            ReplayGapReason::StreamReset => {
                if self.requested.stream_id == self.current.stream_id {
                    return Err("Stream reset requires a different stream identity".into());
                }
            }
        }
        Ok(())
    }
}

/// Bounded replay page and atomic high-watermark returned by `session.subscribe`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeSessionResult {
    #[serde(default)]
    pub events: Vec<SessionEventEnvelope>,
    /// Last cursor included in this page, or the requested `after` when empty.
    pub resume_after: SessionCursor,
    /// Fixed inclusive high watermark for this complete pagination window.
    pub through: SessionCursor,
    pub has_more: bool,
    pub gap: Option<ReplayGap>,
}

impl SubscribeSessionResult {
    pub fn validate_for(&self, params: &SubscribeSessionParams) -> Result<(), String> {
        params.validate()?;
        self.resume_after.validate()?;
        self.through.validate()?;
        if self.resume_after.thread_id != params.thread_id
            || self.through.thread_id != params.thread_id
        {
            return Err("Replay result belongs to another thread".into());
        }
        if let Some(gap) = &self.gap {
            gap.validate()?;
            if gap.requested != params.after {
                return Err("Replay gap does not match the requested cursor".into());
            }
            match gap.reason {
                ReplayGapReason::Retention => match &params.through {
                    Some(through) if &self.through != through => {
                        return Err("Replay response changed the fixed high watermark".into());
                    }
                    None if self.through != gap.current => {
                        return Err(
                            "Initial replay response did not freeze the current cursor".into()
                        );
                    }
                    _ => {}
                },
                ReplayGapReason::StreamReset if self.through != gap.current => {
                    return Err("Stream-reset response must expose the new current cursor".into());
                }
                ReplayGapReason::StreamReset => {}
            }
            if !self.events.is_empty() || self.has_more || self.resume_after != params.after {
                return Err("A replay gap cannot contain an applicable event page".into());
            }
            return Ok(());
        }

        if let Some(through) = &params.through {
            if &self.through != through {
                return Err("Replay response changed the fixed high watermark".into());
            }
        }

        if !params.after.same_stream(&self.through) {
            return Err("A changed stream requires a stream_reset gap".into());
        }
        if params.after.seq > self.through.seq {
            return Err("Replay cursor is ahead of the fixed high watermark".into());
        }
        if !self.resume_after.same_stream(&self.through) {
            return Err("Replay resume cursor belongs to another stream".into());
        }
        if self.events.len() as u64 > u64::from(params.limit) {
            return Err("Replay page exceeds the requested limit".into());
        }

        let mut expected = params.after.clone();
        for event in &self.events {
            event.validate()?;
            expected = expected.checked_next()?;
            if event.cursor != expected || event.cursor.seq > self.through.seq {
                return Err("Replay events must be contiguous and in Session cursor order".into());
            }
        }
        if self.resume_after != expected {
            return Err("Replay resume cursor must identify the last returned event".into());
        }
        let expected_more = self.resume_after.seq < self.through.seq;
        if self.has_more != expected_more {
            return Err("Replay has_more does not match the high watermark".into());
        }
        Ok(())
    }
}

fn draft_mut<'a>(
    run: &'a mut SessionRunView,
    item_id: &str,
) -> Result<&'a mut InProgressItemProjection, String> {
    run.in_progress_items
        .iter_mut()
        .find(|item| item.item_id == item_id)
        .ok_or_else(|| format!("No in-progress projection exists for item {item_id}"))
}

fn append_canonical_once(history: &mut Vec<CanonicalItem>, item: CanonicalItem) {
    if !history.iter().any(|old| old.id() == item.id()) {
        history.push(item);
    }
}

fn stream_turn_id(event: &AgentStreamEvent) -> &str {
    match event {
        AgentStreamEvent::ToolProgress { turn_id, .. }
        | AgentStreamEvent::ToolExecutionStarted { turn_id, .. }
        | AgentStreamEvent::TurnStarted { turn_id, .. }
        | AgentStreamEvent::ItemStarted { turn_id, .. }
        | AgentStreamEvent::TextDelta { turn_id, .. }
        | AgentStreamEvent::ReasoningDelta { turn_id, .. }
        | AgentStreamEvent::ReasoningSignature { turn_id, .. }
        | AgentStreamEvent::ToolCallDelta { turn_id, .. }
        | AgentStreamEvent::ItemCompleted { turn_id, .. }
        | AgentStreamEvent::ApprovalRequested { turn_id, .. }
        | AgentStreamEvent::TurnCompleted { turn_id, .. }
        | AgentStreamEvent::TurnFailed { turn_id, .. } => turn_id,
    }
}

fn validate_identity(value: &str, label: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.trim() != value {
        return Err(format!("{label} must be nonempty and unpadded"));
    }
    Ok(())
}

fn validate_optional_identity(value: Option<&str>, label: &str) -> Result<(), String> {
    if let Some(value) = value {
        validate_identity(value, label)?;
    }
    Ok(())
}

fn validate_history_capacity(capacity: u32) -> Result<(), String> {
    if capacity == 0 || capacity > MAX_SESSION_HISTORY_LIMIT {
        return Err(format!(
            "Session history limit must be between 1 and {MAX_SESSION_HISTORY_LIMIT}"
        ));
    }
    Ok(())
}
