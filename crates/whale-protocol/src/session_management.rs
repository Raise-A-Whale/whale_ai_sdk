//! Stage 2.2 connection-scoped Session management contracts.
//!
//! V2 cursors are scoped to one live attachment (or its retained tombstone).
//! They are independent of V1 Session cursors, per-Run sequences, list/history
//! page cursors, and durable Store recovery revisions.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashSet, fmt};

use crate::{
    retention::serialized_bytes,
    runs::{RunEvent, RunStatus},
    session_views::{
        SessionCursor, SessionEventEnvelope, SessionEventPayload, SessionHistoryWindow,
        SessionRunSummary, SessionRunView, SessionSnapshot, SessionSummary,
        MAX_SESSION_HISTORY_LIMIT, MAX_SESSION_REPLAY_PAGE_LIMIT,
    },
    CanonicalItem,
};

pub const CAPABILITY_SESSION_CATALOG: &str = "session_catalog.v1";
pub const CAPABILITY_SESSION_HISTORY: &str = "session_history.v1";
pub const CAPABILITY_SESSION_METADATA_CAS: &str = "session_metadata_cas.v1";
pub const CAPABILITY_SESSION_LIFECYCLE_REPLAY: &str = "session_lifecycle_replay.v1";

pub const METHOD_SESSION_GET_V2: &str = "session.get.v2";
pub const METHOD_SESSION_SUBSCRIBE_V2: &str = "session.subscribe.v2";
pub const METHOD_SESSION_EVENT_V2: &str = "session.event.v2";
pub const METHOD_SESSION_LIST: &str = "session.list";
pub const METHOD_SESSION_HISTORY: &str = "session.history";
pub const METHOD_SESSION_METADATA_REPLACE: &str = "session.metadata.replace";

pub const SESSION_MANAGEMENT_STATE: i64 = -32040;
pub const SESSION_REVISION_CONFLICT: i64 = -32041;
pub const SESSION_CURSOR_REJECTED: i64 = -32042;
pub const SESSION_HISTORY_GAP: i64 = -32043;

pub const DEFAULT_SESSION_LIST_PAGE_LIMIT: u32 = 64;
pub const MAX_SESSION_LIST_PAGE_LIMIT: u32 = 256;
pub const DEFAULT_SESSION_HISTORY_PAGE_LIMIT: u32 = 128;
pub const MAX_SESSION_HISTORY_PAGE_LIMIT: u32 = 256;
pub const DEFAULT_SESSION_V2_REPLAY_PAGE_LIMIT: u32 = 128;
pub const DEFAULT_SESSION_V2_SUBSCRIBER_OUTPUT: u32 = 64;
pub const MAX_SESSION_V2_SUBSCRIBER_OUTPUT: u32 = 4096;
pub const MAX_SESSION_MANAGEMENT_PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_SESSION_CURSOR_TOKEN_BYTES: usize = 4096;
pub const MAX_SESSION_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_SESSION_METADATA_KEYS: usize = 256;
pub const MAX_SESSION_METADATA_DEPTH: usize = 16;
pub const MAX_SESSION_V2_EVENT_JOURNAL_EVENTS: usize = 1024;
pub const MAX_SESSION_V2_EVENT_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SESSION_MANAGEMENT_HISTORY_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_CLOSED_SESSION_TOMBSTONES_PER_OWNER: usize = 128;
pub const MAX_CLOSED_SESSION_TOMBSTONE_BYTES_PER_OWNER: usize = 64 * 1024 * 1024;
pub const CLOSED_SESSION_TOMBSTONE_TTL_MS: u64 = 10 * 60 * 1000;
pub const SESSION_MANAGEMENT_CURSOR_TTL_MS: u64 = 5 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionLifecycleState {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionPersistenceV2 {
    Ephemeral,
    Persistent { recovery_id: String },
}

impl SessionPersistenceV2 {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Ephemeral => Ok(()),
            Self::Persistent { recovery_id } => uuid::Uuid::parse_str(recovery_id)
                .map(|_| ())
                .map_err(|_| "Persistent Session recovery_id must be a UUID".into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCursorV2 {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

impl SessionCursorV2 {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.stream_id, "stream_id")
    }

    pub fn checked_next(&self) -> Result<Self, String> {
        self.validate()?;
        Ok(Self {
            thread_id: self.thread_id.clone(),
            stream_id: self.stream_id.clone(),
            seq: self
                .seq
                .checked_add(1)
                .ok_or_else(|| "V2 Session cursor sequence is exhausted".to_string())?,
        })
    }

    fn same_stream(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id && self.stream_id == other.stream_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSummaryV2 {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub view_revision: u64,
}

impl SessionSummaryV2 {
    pub fn new(thread_id: impl Into<String>, created_at_ms: u64) -> Self {
        Self {
            thread_id: thread_id.into(),
            agent_name: None,
            metadata: Map::new(),
            created_at_ms,
            updated_at_ms: created_at_ms,
            view_revision: 0,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSnapshotV2 {
    pub summary: SessionSummaryV2,
    pub lifecycle: SessionLifecycleState,
    pub persistence: SessionPersistenceV2,
    pub history: SessionHistoryWindow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_run: Option<SessionRunView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<SessionRunSummary>,
    pub cursor: SessionCursorV2,
}

impl SessionSnapshotV2 {
    pub fn new(
        summary: SessionSummaryV2,
        lifecycle: SessionLifecycleState,
        persistence: SessionPersistenceV2,
        history: SessionHistoryWindow,
        cursor: SessionCursorV2,
    ) -> Self {
        Self {
            summary,
            lifecycle,
            persistence,
            history,
            active_run: None,
            last_run: None,
            cursor,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.summary.validate()?;
        self.persistence.validate()?;
        self.history.validate()?;
        self.cursor.validate()?;
        if self.summary.thread_id != self.cursor.thread_id {
            return Err("V2 Session snapshot cursor belongs to another thread".into());
        }
        if self.summary.view_revision != self.cursor.seq {
            return Err("V2 Session view revision and cursor sequence must match".into());
        }
        if let Some(run) = &self.active_run {
            run.validate()?;
            if run.snapshot.thread_id != self.summary.thread_id {
                return Err("Active Run belongs to another V2 Session".into());
            }
            if self.lifecycle == SessionLifecycleState::Closed {
                return Err("A Closed V2 Session cannot retain an active Run".into());
            }
        }
        if let Some(run) = &self.last_run {
            run.validate()?;
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        envelope: &SessionEventEnvelopeV2,
    ) -> Result<bool, SessionProjectionErrorV2> {
        self.validate()
            .map_err(SessionProjectionErrorV2::invalid_state)?;
        envelope
            .validate()
            .map_err(SessionProjectionErrorV2::invalid_state)?;
        if envelope.thread_id != self.summary.thread_id
            || !self.cursor.same_stream(&envelope.cursor)
        {
            return Err(SessionProjectionErrorV2::StreamMismatch {
                expected: self.cursor.clone(),
                actual: envelope.cursor.clone(),
            });
        }
        if envelope.cursor.seq <= self.cursor.seq {
            return Ok(false);
        }
        if self.lifecycle == SessionLifecycleState::Closed {
            return Err(SessionProjectionErrorV2::invalid_state(
                "A Closed V2 Session cannot apply another event",
            ));
        }
        let expected = self
            .cursor
            .checked_next()
            .map_err(SessionProjectionErrorV2::invalid_state)?;
        if envelope.cursor != expected {
            return Err(SessionProjectionErrorV2::SequenceGap {
                expected,
                actual: envelope.cursor.clone(),
            });
        }

        let mut next = self.clone();
        next.apply_payload(envelope)
            .map_err(SessionProjectionErrorV2::invalid_state)?;
        next.summary.updated_at_ms = next.summary.updated_at_ms.max(envelope.occurred_at_ms);
        next.summary.view_revision = envelope.cursor.seq;
        next.cursor = envelope.cursor.clone();
        next.validate()
            .map_err(SessionProjectionErrorV2::invalid_state)?;
        *self = next;
        Ok(true)
    }

    fn apply_payload(&mut self, envelope: &SessionEventEnvelopeV2) -> Result<(), String> {
        match &envelope.payload {
            SessionEventPayloadV2::RunChanged { run } => {
                if self.lifecycle == SessionLifecycleState::Closing {
                    let current = self.active_run.as_ref().ok_or_else(|| {
                        "Closing V2 Session cannot accept a new active Run".to_string()
                    })?;
                    if current.snapshot.turn_id != run.snapshot.turn_id {
                        return Err("Closing V2 Session Run identity changed".into());
                    }
                }
                self.apply_v1_payload(
                    envelope,
                    SessionEventPayload::RunChanged { run: run.clone() },
                )
            }
            SessionEventPayloadV2::RunEvent { event } => self.apply_v1_payload(
                envelope,
                SessionEventPayload::RunEvent {
                    event: event.clone(),
                },
            ),
            SessionEventPayloadV2::MetadataChanged { metadata } => {
                if self.lifecycle != SessionLifecycleState::Open {
                    return Err("Session metadata can change only while Open".into());
                }
                validate_session_metadata_replacement(metadata)?;
                self.summary.metadata = metadata.clone();
                Ok(())
            }
            SessionEventPayloadV2::LifecycleChanged { lifecycle } => {
                match (self.lifecycle, lifecycle) {
                    (SessionLifecycleState::Open, SessionLifecycleState::Closing) => {
                        self.lifecycle = SessionLifecycleState::Closing;
                    }
                    (SessionLifecycleState::Closing, SessionLifecycleState::Closed) => {
                        if self.active_run.is_some() {
                            return Err("V2 Session cannot become Closed with an active Run".into());
                        }
                        self.lifecycle = SessionLifecycleState::Closed;
                    }
                    _ => return Err("Invalid V2 Session lifecycle transition".into()),
                }
                Ok(())
            }
        }
    }

    fn apply_v1_payload(
        &mut self,
        envelope: &SessionEventEnvelopeV2,
        payload: SessionEventPayload,
    ) -> Result<(), String> {
        let mut summary =
            SessionSummary::new(self.summary.thread_id.clone(), self.summary.created_at_ms);
        summary.agent_name = self.summary.agent_name.clone();
        summary.metadata = self.summary.metadata.clone();
        summary.updated_at_ms = self.summary.updated_at_ms;
        summary.revision = self.cursor.seq;
        let cursor = SessionCursor {
            thread_id: self.cursor.thread_id.clone(),
            stream_id: self.cursor.stream_id.clone(),
            seq: self.cursor.seq,
        };
        let mut snapshot = SessionSnapshot::new(summary, self.history.clone(), cursor);
        snapshot.active_run = self.active_run.clone();
        snapshot.last_run = self.last_run.clone();
        let legacy = SessionEventEnvelope::new(
            envelope.thread_id.clone(),
            SessionCursor {
                thread_id: envelope.cursor.thread_id.clone(),
                stream_id: envelope.cursor.stream_id.clone(),
                seq: envelope.cursor.seq,
            },
            envelope.occurred_at_ms,
            payload,
        );
        snapshot.apply(&legacy).map_err(|error| error.to_string())?;
        self.history = snapshot.history;
        self.active_run = snapshot.active_run;
        self.last_run = snapshot.last_run;
        Ok(())
    }
}

/// Closed V2 Session-management event wire family.
///
/// `#[non_exhaustive]` protects external Rust matches only; serde cannot skip an
/// unknown enum variant. Future event categories require a new capability and
/// wire family rather than another V2 variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionEventPayloadV2 {
    RunChanged { run: SessionRunView },
    RunEvent { event: RunEvent },
    MetadataChanged { metadata: Map<String, Value> },
    LifecycleChanged { lifecycle: SessionLifecycleState },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionEventEnvelopeV2 {
    pub thread_id: String,
    pub cursor: SessionCursorV2,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub payload: SessionEventPayloadV2,
}

impl SessionEventEnvelopeV2 {
    pub fn new(
        thread_id: impl Into<String>,
        cursor: SessionCursorV2,
        occurred_at_ms: u64,
        payload: SessionEventPayloadV2,
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
            return Err("V2 Session event cursor belongs to another thread".into());
        }
        if self.cursor.seq == 0 {
            return Err("V2 Session event sequence must be positive".into());
        }
        match &self.payload {
            SessionEventPayloadV2::RunChanged { run } => {
                run.validate()?;
                if run.snapshot.thread_id != self.thread_id {
                    return Err("Changed Run belongs to another V2 Session".into());
                }
            }
            SessionEventPayloadV2::RunEvent { event } => {
                let legacy = SessionEventEnvelope::new(
                    self.thread_id.clone(),
                    SessionCursor {
                        thread_id: self.cursor.thread_id.clone(),
                        stream_id: self.cursor.stream_id.clone(),
                        seq: self.cursor.seq,
                    },
                    self.occurred_at_ms,
                    SessionEventPayload::RunEvent {
                        event: event.clone(),
                    },
                );
                legacy.validate()?;
            }
            SessionEventPayloadV2::MetadataChanged { metadata } => {
                validate_session_metadata_replacement(metadata)?;
            }
            SessionEventPayloadV2::LifecycleChanged { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionProjectionErrorV2 {
    InvalidState {
        message: String,
    },
    StreamMismatch {
        expected: SessionCursorV2,
        actual: SessionCursorV2,
    },
    SequenceGap {
        expected: SessionCursorV2,
        actual: SessionCursorV2,
    },
}

impl SessionProjectionErrorV2 {
    fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState {
            message: message.into(),
        }
    }
}

impl fmt::Display for SessionProjectionErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState { message } => {
                write!(formatter, "Invalid V2 Session projection: {message}")
            }
            Self::StreamMismatch { expected, actual } => write!(
                formatter,
                "V2 Session projection stream mismatch: expected {}/{}, got {}/{}",
                expected.thread_id, expected.stream_id, actual.thread_id, actual.stream_id
            ),
            Self::SequenceGap { expected, actual } => write!(
                formatter,
                "V2 Session projection sequence gap: expected {}/{}/{}, got {}/{}/{}",
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

impl std::error::Error for SessionProjectionErrorV2 {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSessionV2Params {
    pub thread_id: String,
    pub history_limit: u32,
}

impl GetSessionV2Params {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_bounded_limit(
            self.history_limit,
            MAX_SESSION_HISTORY_LIMIT,
            "V2 Session history limit",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeSessionV2Params {
    pub thread_id: String,
    pub after: SessionCursorV2,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<SessionCursorV2>,
    pub limit: u32,
}

impl SubscribeSessionV2Params {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        self.after.validate()?;
        if self.after.thread_id != self.thread_id {
            return Err("V2 replay cursor belongs to another thread".into());
        }
        if let Some(through) = &self.through {
            through.validate()?;
            if !self.after.same_stream(through) {
                return Err("V2 replay window cursors belong to different streams".into());
            }
            if self.after.seq > through.seq {
                return Err("V2 replay cursor is ahead of the fixed window".into());
            }
        }
        validate_bounded_limit(
            self.limit,
            MAX_SESSION_REPLAY_PAGE_LIMIT,
            "V2 replay page limit",
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplayGapReasonV2 {
    Retention,
    StreamReset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayGapV2 {
    pub reason: ReplayGapReasonV2,
    pub requested: SessionCursorV2,
    pub replay_floor: SessionCursorV2,
    pub current: SessionCursorV2,
    pub view_revision: u64,
}

impl ReplayGapV2 {
    pub fn validate(&self) -> Result<(), String> {
        self.requested.validate()?;
        self.replay_floor.validate()?;
        self.current.validate()?;
        if self.requested.thread_id != self.current.thread_id {
            return Err("V2 replay gap cursors belong to different threads".into());
        }
        if !self.replay_floor.same_stream(&self.current) || self.replay_floor.seq > self.current.seq
        {
            return Err("V2 replay floor is outside the current stream".into());
        }
        if self.view_revision != self.current.seq {
            return Err("V2 replay gap revision must match current cursor".into());
        }
        match self.reason {
            ReplayGapReasonV2::Retention => {
                if !self.requested.same_stream(&self.current)
                    || self.requested.seq >= self.replay_floor.seq
                {
                    return Err("V2 retention gap requires a cursor below the floor".into());
                }
            }
            ReplayGapReasonV2::StreamReset => {
                if self.requested.stream_id == self.current.stream_id {
                    return Err("V2 stream reset requires a new stream identity".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeSessionV2Result {
    #[serde(default)]
    pub events: Vec<SessionEventEnvelopeV2>,
    pub resume_after: SessionCursorV2,
    pub through: SessionCursorV2,
    pub lifecycle: SessionLifecycleState,
    pub has_more: bool,
    #[serde(default)]
    pub gap: Option<ReplayGapV2>,
}

impl SubscribeSessionV2Result {
    pub fn validate_for(&self, params: &SubscribeSessionV2Params) -> Result<(), String> {
        params.validate()?;
        self.resume_after.validate()?;
        self.through.validate()?;
        if self.resume_after.thread_id != params.thread_id
            || self.through.thread_id != params.thread_id
        {
            return Err("V2 replay result belongs to another thread".into());
        }
        if let Some(gap) = &self.gap {
            gap.validate()?;
            if gap.requested != params.after {
                return Err("V2 replay gap does not match the request".into());
            }
            match gap.reason {
                ReplayGapReasonV2::Retention => match &params.through {
                    Some(through) if &self.through != through => {
                        return Err("V2 replay changed the fixed high watermark".into())
                    }
                    None if self.through != gap.current => {
                        return Err("Initial V2 gap did not expose current cursor".into())
                    }
                    _ => {}
                },
                ReplayGapReasonV2::StreamReset if self.through != gap.current => {
                    return Err("V2 stream reset must expose current cursor".into())
                }
                ReplayGapReasonV2::StreamReset => {}
            }
            if !self.events.is_empty() || self.has_more || self.resume_after != params.after {
                return Err("V2 replay gap cannot contain an event page".into());
            }
            return Ok(());
        }
        if let Some(through) = &params.through {
            if through != &self.through {
                return Err("V2 replay changed the fixed high watermark".into());
            }
        }
        if !params.after.same_stream(&self.through) || !self.resume_after.same_stream(&self.through)
        {
            return Err("V2 replay result changed stream without a gap".into());
        }
        if params.after.seq > self.through.seq {
            return Err("V2 replay cursor is ahead of the high watermark".into());
        }
        if self.events.len() > params.limit as usize {
            return Err("V2 replay page exceeds the requested limit".into());
        }
        let mut expected = params.after.clone();
        for event in &self.events {
            event.validate()?;
            expected = expected.checked_next()?;
            if event.cursor != expected || event.cursor.seq > self.through.seq {
                return Err("V2 replay events must be contiguous and ordered".into());
            }
        }
        if self.resume_after != expected {
            return Err("V2 replay resume cursor is not the last returned event".into());
        }
        if self.has_more != (self.resume_after.seq < self.through.seq) {
            return Err("V2 replay has_more disagrees with the fixed window".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionListCursor(String);

impl SessionListCursor {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_token(&value, "Session list cursor")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_token(&self.0, "Session list cursor")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionHistoryPageCursor(String);

impl SessionHistoryPageCursor {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_token(&value, "Session history page cursor")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_token(&self.0, "Session history page cursor")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionRunHeadline {
    pub turn_id: String,
    pub status: RunStatus,
    pub accepted_at_ms: u64,
    pub updated_at_ms: u64,
}

impl SessionRunHeadline {
    pub fn new(
        turn_id: impl Into<String>,
        status: RunStatus,
        accepted_at_ms: u64,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            status,
            accepted_at_ms,
            updated_at_ms,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.turn_id, "turn_id")?;
        if self.status.is_terminal() {
            return Err("Active Run headline cannot be terminal".into());
        }
        if self.updated_at_ms < self.accepted_at_ms {
            return Err("Run headline update precedes acceptance".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionListEntry {
    pub summary: SessionSummaryV2,
    pub lifecycle: SessionLifecycleState,
    pub persistence: SessionPersistenceV2,
    pub cursor: SessionCursorV2,
    pub history_total_items: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_run: Option<SessionRunHeadline>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<SessionRunSummary>,
}

impl SessionListEntry {
    pub fn new(
        summary: SessionSummaryV2,
        lifecycle: SessionLifecycleState,
        persistence: SessionPersistenceV2,
        cursor: SessionCursorV2,
        history_total_items: u64,
    ) -> Self {
        Self {
            summary,
            lifecycle,
            persistence,
            cursor,
            history_total_items,
            active_run: None,
            last_run: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.summary.validate()?;
        self.persistence.validate()?;
        self.cursor.validate()?;
        if self.summary.thread_id != self.cursor.thread_id
            || self.summary.view_revision != self.cursor.seq
        {
            return Err("Session list entry summary and cursor disagree".into());
        }
        if let Some(run) = &self.active_run {
            run.validate()?;
            if self.lifecycle == SessionLifecycleState::Closed {
                return Err("Closed Session list entry cannot have an active Run".into());
            }
        }
        if let Some(run) = &self.last_run {
            run.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSessionsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SessionListCursor>,
    pub limit: u32,
}

impl ListSessionsParams {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        validate_bounded_limit(
            self.limit,
            MAX_SESSION_LIST_PAGE_LIMIT,
            "Session list page limit",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListSessionsResult {
    #[serde(default)]
    pub sessions: Vec<SessionListEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<SessionListCursor>,
}

impl ListSessionsResult {
    pub fn validate_for(&self, params: &ListSessionsParams) -> Result<(), String> {
        params.validate()?;
        validate_management_page_bytes(self, "Session list response")?;
        if self.sessions.len() > params.limit as usize {
            return Err("Session list response exceeds the requested limit".into());
        }
        let mut threads = HashSet::new();
        for session in &self.sessions {
            session.validate()?;
            if !threads.insert(session.summary.thread_id.as_str()) {
                return Err("Session list response contains duplicate threads".into());
            }
        }
        if let Some(cursor) = &self.next_cursor {
            cursor.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionHistoryAnchor {
    pub thread_id: String,
    pub stream_id: String,
    pub index: u64,
}

impl SessionHistoryAnchor {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.stream_id, "stream_id")
    }

    fn same_stream(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id && self.stream_id == other.stream_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSessionHistoryParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<SessionHistoryAnchor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SessionHistoryPageCursor>,
    pub limit: u32,
}

impl GetSessionHistoryParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        if self.before.is_some() && self.cursor.is_some() {
            return Err("History before anchor and page cursor are mutually exclusive".into());
        }
        if let Some(before) = &self.before {
            before.validate()?;
            if before.thread_id != self.thread_id {
                return Err("History anchor belongs to another thread".into());
            }
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        validate_bounded_limit(
            self.limit,
            MAX_SESSION_HISTORY_PAGE_LIMIT,
            "Session history page limit",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetSessionHistoryResult {
    #[serde(default)]
    pub items: Vec<CanonicalItem>,
    pub start_index: u64,
    pub end_index: u64,
    pub through: SessionHistoryAnchor,
    pub current_end: SessionHistoryAnchor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<SessionHistoryPageCursor>,
}

impl GetSessionHistoryResult {
    pub fn validate_for(&self, params: &GetSessionHistoryParams) -> Result<(), String> {
        params.validate()?;
        validate_management_page_bytes(self, "Session history response")?;
        self.through.validate()?;
        self.current_end.validate()?;
        if self.through.thread_id != params.thread_id
            || self.current_end.thread_id != params.thread_id
            || !self.through.same_stream(&self.current_end)
        {
            return Err("History result belongs to another Session stream".into());
        }
        if self.start_index > self.end_index
            || self.end_index > self.through.index
            || self.through.index > self.current_end.index
        {
            return Err("History result indexes are outside the fixed window".into());
        }
        let count = self
            .end_index
            .checked_sub(self.start_index)
            .ok_or_else(|| "History result index underflow".to_string())?;
        if count != self.items.len() as u64 || self.items.len() > params.limit as usize {
            return Err("History result items do not match its absolute range".into());
        }
        let mut item_ids = HashSet::new();
        for item in &self.items {
            validate_identity(item.id(), "history item_id")?;
            if !item_ids.insert(item.id()) {
                return Err("History result contains duplicate canonical item identities".into());
            }
        }
        if params.cursor.is_none() {
            if self.end_index != self.through.index {
                return Err("Initial backward history page must end at through".into());
            }
            let expected_through = params
                .before
                .as_ref()
                .map(|before| before.index.min(self.current_end.index))
                .unwrap_or(self.current_end.index);
            if self.through.index != expected_through {
                return Err("Initial history page froze the wrong high watermark".into());
            }
            if let Some(before) = &params.before {
                if !before.same_stream(&self.through) {
                    return Err("History before anchor belongs to another stream".into());
                }
            }
        }
        if let Some(cursor) = &self.next_cursor {
            cursor.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceSessionMetadataParams {
    pub thread_id: String,
    pub expected_view_revision: u64,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl ReplaceSessionMetadataParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_session_metadata_replacement(&self.metadata)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceSessionMetadataResult {
    pub summary: SessionSummaryV2,
    pub cursor: SessionCursorV2,
    pub changed: bool,
}

impl ReplaceSessionMetadataResult {
    pub fn validate_for(&self, params: &ReplaceSessionMetadataParams) -> Result<(), String> {
        params.validate()?;
        self.summary.validate()?;
        self.cursor.validate()?;
        if self.summary.thread_id != params.thread_id
            || self.cursor.thread_id != params.thread_id
            || self.summary.view_revision != self.cursor.seq
        {
            return Err("Metadata result belongs to another Session revision".into());
        }
        if self.summary.metadata != params.metadata {
            return Err("Metadata result does not contain the requested replacement".into());
        }
        let expected = if self.changed {
            params
                .expected_view_revision
                .checked_add(1)
                .ok_or_else(|| "Metadata revision is exhausted".to_string())?
        } else {
            params.expected_view_revision
        };
        if self.cursor.seq != expected {
            return Err("Metadata result revision does not match changed state".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionManagementErrorData {
    Unavailable,
    SessionNotOpen {
        lifecycle: SessionLifecycleState,
    },
    RevisionConflict {
        expected_view_revision: u64,
        current_view_revision: u64,
        current: SessionCursorV2,
    },
    ListCursorInvalid,
    ListCursorExpired,
    HistoryCursorInvalid,
    HistoryStreamReset {
        requested: SessionHistoryAnchor,
        current: SessionHistoryAnchor,
    },
    HistoryGap {
        requested: SessionHistoryAnchor,
        floor: SessionHistoryAnchor,
        current_end: SessionHistoryAnchor,
    },
    TombstoneExpired {
        thread_id: String,
    },
    ResourceLimit {
        resource: String,
        actual: u64,
        limit: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_index: Option<u64>,
    },
    StorageFailure {
        outcome_unknown: bool,
    },
}

impl SessionManagementErrorData {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Unavailable
            | Self::ListCursorInvalid
            | Self::ListCursorExpired
            | Self::HistoryCursorInvalid
            | Self::StorageFailure { .. } => Ok(()),
            Self::SessionNotOpen { lifecycle } => {
                if *lifecycle == SessionLifecycleState::Open {
                    Err("SessionNotOpen cannot report Open".into())
                } else {
                    Ok(())
                }
            }
            Self::RevisionConflict {
                expected_view_revision,
                current_view_revision,
                current,
            } => {
                current.validate()?;
                if current.seq != *current_view_revision {
                    return Err("Revision conflict cursor and revision disagree".into());
                }
                if expected_view_revision == current_view_revision {
                    return Err("Revision conflict requires different revisions".into());
                }
                Ok(())
            }
            Self::HistoryStreamReset { requested, current } => {
                requested.validate()?;
                current.validate()?;
                if requested.thread_id != current.thread_id
                    || requested.stream_id == current.stream_id
                {
                    return Err("History stream reset requires a new stream".into());
                }
                Ok(())
            }
            Self::HistoryGap {
                requested,
                floor,
                current_end,
            } => {
                requested.validate()?;
                floor.validate()?;
                current_end.validate()?;
                if !requested.same_stream(floor)
                    || !floor.same_stream(current_end)
                    || requested.index >= floor.index
                    || floor.index > current_end.index
                {
                    return Err("History gap boundaries are inconsistent".into());
                }
                Ok(())
            }
            Self::TombstoneExpired { thread_id } => validate_identity(thread_id, "thread_id"),
            Self::ResourceLimit {
                resource,
                actual,
                limit,
                ..
            } => {
                validate_identity(resource, "resource")?;
                if actual <= limit {
                    return Err("Resource limit error requires actual above limit".into());
                }
                Ok(())
            }
        }
    }
}

fn validate_bounded_limit(value: u32, maximum: u32, name: &str) -> Result<(), String> {
    if value == 0 || value > maximum {
        return Err(format!("{name} must be between 1 and {maximum}"));
    }
    Ok(())
}

fn validate_management_page_bytes<T: Serialize + ?Sized>(
    value: &T,
    name: &str,
) -> Result<(), String> {
    let bytes = serialized_bytes(value).map_err(|error| error.to_string())?;
    if bytes > MAX_SESSION_MANAGEMENT_PAGE_BYTES as u64 {
        return Err(format!(
            "{name} exceeds {MAX_SESSION_MANAGEMENT_PAGE_BYTES} serialized bytes"
        ));
    }
    Ok(())
}

fn validate_identity(value: &str, name: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(format!("{name} must be nonempty, unpadded, and printable"));
    }
    Ok(())
}

fn validate_token(value: &str, name: &str) -> Result<(), String> {
    validate_identity(value, name)?;
    if value.len() > MAX_SESSION_CURSOR_TOKEN_BYTES {
        return Err(format!(
            "{name} exceeds {MAX_SESSION_CURSOR_TOKEN_BYTES} encoded bytes"
        ));
    }
    Ok(())
}

/// Validate a newly requested metadata replacement.
///
/// Persisted legacy metadata remains readable even when it exceeds these write
/// limits; callers apply this helper only to new replacement values.
pub fn validate_session_metadata_replacement(metadata: &Map<String, Value>) -> Result<(), String> {
    if metadata.len() > MAX_SESSION_METADATA_KEYS {
        return Err(format!(
            "Session metadata exceeds {MAX_SESSION_METADATA_KEYS} top-level keys"
        ));
    }
    let bytes = serialized_bytes(metadata).map_err(|error| error.to_string())?;
    if bytes > MAX_SESSION_METADATA_BYTES as u64 {
        return Err(format!(
            "Session metadata exceeds {MAX_SESSION_METADATA_BYTES} serialized bytes"
        ));
    }
    let depth = metadata
        .values()
        .map(|value| value_depth(value, 1))
        .max()
        .unwrap_or(1);
    if depth > MAX_SESSION_METADATA_DEPTH {
        return Err(format!(
            "Session metadata exceeds nesting depth {MAX_SESSION_METADATA_DEPTH}"
        ));
    }
    Ok(())
}

fn value_depth(value: &Value, parent_depth: usize) -> usize {
    match value {
        Value::Array(values) => values
            .iter()
            .map(|value| value_depth(value, parent_depth.saturating_add(1)))
            .max()
            .unwrap_or_else(|| parent_depth.saturating_add(1)),
        Value::Object(values) => values
            .values()
            .map(|value| value_depth(value, parent_depth.saturating_add(1)))
            .max()
            .unwrap_or_else(|| parent_depth.saturating_add(1)),
        _ => parent_depth,
    }
}
