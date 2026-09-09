//! Optional, session-scoped Interaction snapshots and bounded replay contracts.

use crate::{
    recovery::{AttachRecoveryParams, CreatePersistentSessionParams},
    retention::serialized_bytes,
    rpc::StartThreadParams,
};
use serde::{
    de::{DeserializeOwned, Error as DeError, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    fmt,
};
use uuid::Uuid;

pub const CAPABILITY_INTERACTIONS: &str = "interactions.v1";

pub const METHOD_SESSION_INTERACTIONS_GET: &str = "session.interactions.get";
pub const METHOD_SESSION_INTERACTIONS_SUBSCRIBE: &str = "session.interactions.subscribe";
pub const METHOD_SESSION_INTERACTION_EVENT: &str = "session.interaction_event";
pub const METHOD_TURN_INTERACTIONS_GET: &str = "turn.interactions.get";
pub const METHOD_TURN_REQUEST_INTERACTION: &str = "turn.request_interaction";
pub const METHOD_TURN_RESPOND_INTERACTION: &str = "turn.respond_interaction";

pub const INTERACTION_NOT_FOUND: i64 = -32050;
pub const INTERACTION_CONFLICT: i64 = -32051;
pub const INTERACTION_RESPONSE_INVALID: i64 = -32052;
pub const INTERACTION_UNAVAILABLE: i64 = -32053;

pub const MAX_INTERACTION_ID_BYTES: usize = 128;
pub const MAX_INTERACTION_KIND_BYTES: usize = 128;
pub const MAX_INTERACTION_TITLE_BYTES: usize = 512;
pub const MAX_INTERACTION_PAYLOAD_BYTES: usize = 65_536;
pub const MAX_INTERACTION_SCHEMA_BYTES: usize = 65_536;
pub const MAX_INTERACTION_RESPONSE_BYTES: usize = 65_536;
pub const MAX_PENDING_INTERACTIONS_PER_RUN: usize = 32;
pub const DEFAULT_INTERACTION_REPLAY_PAGE_LIMIT: u32 = 128;
pub const MAX_INTERACTION_REPLAY_PAGE_LIMIT: u32 = 256;
pub const DEFAULT_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY: usize = 64;
pub const MAX_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY: usize = 4_096;
pub const MAX_INTERACTION_JOURNAL_EVENTS: usize = 1_024;
pub const MAX_INTERACTION_JOURNAL_BYTES: usize = 4 * 1_024 * 1_024;

pub const KIND_TOOL_APPROVAL: &str = "whale.tool_approval";
pub const KIND_CLARIFICATION: &str = "whale.clarification";
pub const KIND_FORM: &str = "whale.form";
pub const KIND_AUTH: &str = "whale.auth";
pub const KIND_PERMISSION_FILE: &str = "whale.permission.file";
pub const KIND_PERMISSION_NETWORK: &str = "whale.permission.network";
pub const KIND_REVIEW: &str = "whale.review";

pub const INTERACTION_REMOVAL_RESOLVED: &str = "resolved";
pub const INTERACTION_REMOVAL_CANCELLED: &str = "cancelled";
pub const INTERACTION_REMOVAL_RUN_FINISHED: &str = "run_finished";
pub const INTERACTION_REMOVAL_SESSION_CLOSED: &str = "session_closed";
pub const INTERACTION_REMOVAL_CONNECTION_CLOSED: &str = "connection_closed";
pub const INTERACTION_REMOVAL_ORIGIN_FINISHED: &str = "origin_finished";
pub const INTERACTION_REMOVAL_PUBLICATION_FAILED: &str = "publication_failed";

const BUILTIN_KINDS: &[&str] = &[
    KIND_TOOL_APPROVAL,
    KIND_CLARIFICATION,
    KIND_FORM,
    KIND_AUTH,
    KIND_PERMISSION_FILE,
    KIND_PERMISSION_NETWORK,
    KIND_REVIEW,
];

const START_THREAD_KEYS: &[&str] = &[
    "limits",
    "provider_ref",
    "agent_name",
    "context_policy",
    "provider_config",
    "options",
    "session_id",
    "provider",
    "model",
    "system_prompt",
    "tools",
    "metadata",
    "interactions_enabled",
];
const CREATE_PERSISTENT_KEYS: &[&str] = &["key", "session", "run_defaults", "interactions_enabled"];
const ATTACH_RECOVERY_KEYS: &[&str] = &[
    "key",
    "expected_revision",
    "session",
    "run_defaults",
    "interactions_enabled",
];
const REQUEST_INTERACTION_KEYS: &[&str] = &[
    "thread_id",
    "turn_id",
    "host_call_id",
    "request_id",
    "kind",
    "title",
    "payload",
    "response_schema",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionRequest {
    pub kind: String,
    pub title: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
}

impl InteractionRequest {
    pub fn new(
        kind: impl Into<String>,
        title: impl Into<String>,
        payload: Value,
        response_schema: Option<Value>,
    ) -> Result<Self, String> {
        let value = Self {
            kind: kind.into(),
            title: title.into(),
            payload,
            response_schema,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_kind(&self.kind)?;
        validate_bounded_text(
            &self.title,
            "Interaction title",
            MAX_INTERACTION_TITLE_BYTES,
        )?;
        validate_json_size(
            &self.payload,
            "Interaction payload",
            MAX_INTERACTION_PAYLOAD_BYTES,
        )?;
        if let Some(schema) = &self.response_schema {
            if !schema.is_object() {
                return Err("Interaction response_schema must be a JSON object".into());
            }
            validate_json_size(
                schema,
                "Interaction response_schema",
                MAX_INTERACTION_SCHEMA_BYTES,
            )?;
            validate_local_schema_references(schema)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PendingInteraction {
    pub request_id: String,
    pub turn_id: String,
    #[serde(flatten)]
    pub request: InteractionRequest,
}

impl PendingInteraction {
    pub fn new(
        request_id: impl Into<String>,
        turn_id: impl Into<String>,
        request: InteractionRequest,
    ) -> Result<Self, String> {
        let value = Self {
            request_id: request_id.into(),
            turn_id: turn_id.into(),
            request,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.request_id, "request_id")?;
        validate_identity(&self.turn_id, "turn_id")?;
        self.request.validate()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionResponse {
    pub request_id: String,
    pub response: Value,
}

impl fmt::Debug for InteractionResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InteractionResponse")
            .field("request_id", &self.request_id)
            .field("response", &"[REDACTED]")
            .finish()
    }
}

impl InteractionResponse {
    pub fn new(request_id: impl Into<String>, response: Value) -> Result<Self, String> {
        let value = Self {
            request_id: request_id.into(),
            response,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.request_id, "request_id")?;
        validate_json_size(
            &self.response,
            "Interaction response",
            MAX_INTERACTION_RESPONSE_BYTES,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InteractionCursor {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

impl InteractionCursor {
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
                .ok_or_else(|| "Interaction cursor sequence is exhausted".to_string())?,
        })
    }

    fn same_stream(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id && self.stream_id == other.stream_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InteractionProjectionError {
    InvalidState {
        message: String,
    },
    StreamMismatch {
        expected_thread: String,
        expected_stream: String,
        actual_thread: String,
        actual_stream: String,
    },
    SequenceGap {
        expected: InteractionCursor,
        actual: InteractionCursor,
    },
}

impl InteractionProjectionError {
    fn invalid_state(message: String) -> Self {
        Self::InvalidState { message }
    }
}

impl fmt::Display for InteractionProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState { message } => {
                write!(formatter, "invalid Interaction projection: {message}")
            }
            Self::StreamMismatch {
                expected_thread,
                expected_stream,
                actual_thread,
                actual_stream,
            } => write!(
                formatter,
                "Interaction projection stream mismatch: expected {expected_thread}/{expected_stream}, got {actual_thread}/{actual_stream}"
            ),
            Self::SequenceGap { expected, actual } => write!(
                formatter,
                "Interaction projection sequence gap: expected {}/{}/{}, got {}/{}/{}",
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

impl std::error::Error for InteractionProjectionError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionSnapshot {
    pub thread_id: String,
    pub cursor: InteractionCursor,
    pub pending: Vec<PendingInteraction>,
}

impl InteractionSnapshot {
    pub fn new(
        thread_id: impl Into<String>,
        cursor: InteractionCursor,
        pending: Vec<PendingInteraction>,
    ) -> Result<Self, String> {
        let value = Self {
            thread_id: thread_id.into(),
            cursor,
            pending,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        self.cursor.validate()?;
        if self.cursor.thread_id != self.thread_id {
            return Err("Interaction snapshot cursor belongs to another thread".into());
        }
        validate_pending_set(&self.pending, None)
    }

    pub fn apply(
        &mut self,
        envelope: &InteractionEventEnvelope,
    ) -> Result<bool, InteractionProjectionError> {
        self.validate()
            .map_err(InteractionProjectionError::invalid_state)?;
        envelope
            .validate()
            .map_err(InteractionProjectionError::invalid_state)?;
        if envelope.thread_id != self.thread_id
            || envelope.cursor.stream_id != self.cursor.stream_id
        {
            return Err(InteractionProjectionError::StreamMismatch {
                expected_thread: self.thread_id.clone(),
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
            .map_err(InteractionProjectionError::invalid_state)?;
        if envelope.cursor != expected {
            return Err(InteractionProjectionError::SequenceGap {
                expected,
                actual: envelope.cursor.clone(),
            });
        }

        let mut next = self.clone();
        match &envelope.payload {
            InteractionEventPayload::Requested { interaction } => {
                if next.pending.iter().any(|pending| {
                    pending.turn_id == interaction.turn_id
                        && pending.request_id == interaction.request_id
                }) {
                    return Err(InteractionProjectionError::invalid_state(
                        "Requested Interaction is already pending".into(),
                    ));
                }
                next.pending.push(interaction.clone());
            }
            InteractionEventPayload::Removed {
                request_id,
                turn_id,
                ..
            } => {
                let index = next
                    .pending
                    .iter()
                    .position(|pending| {
                        pending.turn_id == *turn_id && pending.request_id == *request_id
                    })
                    .ok_or_else(|| {
                        InteractionProjectionError::invalid_state(
                            "Removed Interaction is not pending".into(),
                        )
                    })?;
                next.pending.remove(index);
            }
        }
        next.cursor = envelope.cursor.clone();
        next.validate()
            .map_err(InteractionProjectionError::invalid_state)?;
        *self = next;
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TurnInteractionSnapshot {
    pub thread_id: String,
    pub turn_id: String,
    pub cursor: InteractionCursor,
    pub pending: Vec<PendingInteraction>,
}

impl TurnInteractionSnapshot {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        cursor: InteractionCursor,
        pending: Vec<PendingInteraction>,
    ) -> Result<Self, String> {
        let value = Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            cursor,
            pending,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.turn_id, "turn_id")?;
        self.cursor.validate()?;
        if self.cursor.thread_id != self.thread_id {
            return Err("Turn Interaction snapshot cursor belongs to another thread".into());
        }
        validate_pending_set(&self.pending, Some(&self.turn_id))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionEventEnvelope {
    pub thread_id: String,
    pub cursor: InteractionCursor,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub payload: InteractionEventPayload,
}

impl InteractionEventEnvelope {
    pub fn new(
        thread_id: impl Into<String>,
        cursor: InteractionCursor,
        occurred_at_ms: u64,
        payload: InteractionEventPayload,
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
            return Err("Interaction event cursor belongs to another thread".into());
        }
        if self.cursor.seq == 0 {
            return Err("Interaction event sequence must be positive".into());
        }
        self.payload.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum InteractionEventPayload {
    Requested {
        interaction: PendingInteraction,
    },
    Removed {
        request_id: String,
        turn_id: String,
        cause: String,
    },
}

impl InteractionEventPayload {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Requested { interaction } => interaction.validate(),
            Self::Removed {
                request_id,
                turn_id,
                cause,
            } => {
                validate_identity(request_id, "request_id")?;
                validate_identity(turn_id, "turn_id")?;
                validate_bounded_text(
                    cause,
                    "Interaction removal cause",
                    MAX_INTERACTION_KIND_BYTES,
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSessionInteractionsParams {
    pub thread_id: String,
}

impl GetSessionInteractionsParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetTurnInteractionsParams {
    pub thread_id: String,
    pub turn_id: String,
}

impl GetTurnInteractionsParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.turn_id, "turn_id")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeInteractionsParams {
    pub thread_id: String,
    pub after: InteractionCursor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<InteractionCursor>,
    pub limit: u32,
}

impl SubscribeInteractionsParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        self.after.validate()?;
        if self.after.thread_id != self.thread_id {
            return Err("Interaction replay cursor belongs to another thread".into());
        }
        if let Some(through) = &self.through {
            through.validate()?;
            if !self.after.same_stream(through) {
                return Err("Interaction replay window cursors belong to different streams".into());
            }
            if self.after.seq > through.seq {
                return Err("Interaction replay cursor is ahead of the fixed replay window".into());
            }
        }
        validate_interaction_replay_page_limit(self.limit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InteractionReplayGapReason {
    Retention,
    StreamReset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionReplayGap {
    pub reason: InteractionReplayGapReason,
    pub requested: InteractionCursor,
    pub replay_floor: InteractionCursor,
    pub current: InteractionCursor,
}

impl InteractionReplayGap {
    pub fn validate(&self) -> Result<(), String> {
        self.requested.validate()?;
        self.replay_floor.validate()?;
        self.current.validate()?;
        if self.requested.thread_id != self.current.thread_id {
            return Err("Interaction replay gap cursors belong to different threads".into());
        }
        if !self.replay_floor.same_stream(&self.current) || self.replay_floor.seq > self.current.seq
        {
            return Err("Interaction replay floor is outside the current stream".into());
        }
        match self.reason {
            InteractionReplayGapReason::Retention => {
                if !self.requested.same_stream(&self.current)
                    || self.requested.seq >= self.replay_floor.seq
                {
                    return Err(
                        "Interaction retention gap requires a cursor below the replay floor".into(),
                    );
                }
            }
            InteractionReplayGapReason::StreamReset => {
                if self.requested.stream_id == self.current.stream_id {
                    return Err(
                        "Interaction stream reset requires a different stream identity".into(),
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeInteractionsResult {
    #[serde(default)]
    pub events: Vec<InteractionEventEnvelope>,
    pub resume_after: InteractionCursor,
    pub through: InteractionCursor,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap: Option<InteractionReplayGap>,
}

impl SubscribeInteractionsResult {
    pub fn validate_for(&self, params: &SubscribeInteractionsParams) -> Result<(), String> {
        params.validate()?;
        self.resume_after.validate()?;
        self.through.validate()?;
        if self.resume_after.thread_id != params.thread_id
            || self.through.thread_id != params.thread_id
        {
            return Err("Interaction replay result belongs to another thread".into());
        }

        if let Some(gap) = &self.gap {
            gap.validate()?;
            if gap.requested != params.after {
                return Err("Interaction replay gap does not match the requested cursor".into());
            }
            match gap.reason {
                InteractionReplayGapReason::Retention => match &params.through {
                    Some(through) if &self.through != through => {
                        return Err(
                            "Interaction replay response changed the fixed high watermark".into(),
                        );
                    }
                    None if self.through != gap.current => {
                        return Err(
                            "Initial Interaction replay did not freeze the current cursor".into(),
                        );
                    }
                    _ => {}
                },
                InteractionReplayGapReason::StreamReset if self.through != gap.current => {
                    return Err(
                        "Interaction stream-reset response must expose the new current cursor"
                            .into(),
                    );
                }
                InteractionReplayGapReason::StreamReset => {}
            }
            if !self.events.is_empty() || self.has_more || self.resume_after != params.after {
                return Err("An Interaction replay gap cannot contain an event page".into());
            }
            return Ok(());
        }

        if let Some(through) = &params.through {
            if &self.through != through {
                return Err("Interaction replay response changed the fixed high watermark".into());
            }
        }
        if !params.after.same_stream(&self.through) {
            return Err("A changed Interaction stream requires a stream_reset gap".into());
        }
        if params.after.seq > self.through.seq {
            return Err("Interaction replay cursor is ahead of the fixed high watermark".into());
        }
        if !self.resume_after.same_stream(&self.through) {
            return Err("Interaction replay resume cursor belongs to another stream".into());
        }
        if self.events.len() as u64 > u64::from(params.limit) {
            return Err("Interaction replay page exceeds the requested limit".into());
        }

        let mut expected = params.after.clone();
        for event in &self.events {
            event.validate()?;
            expected = expected.checked_next()?;
            if event.cursor != expected || event.cursor.seq > self.through.seq {
                return Err(
                    "Interaction replay events must be contiguous and in cursor order".into(),
                );
            }
        }
        if self.resume_after != expected {
            return Err(
                "Interaction replay resume cursor must identify the last returned event".into(),
            );
        }
        let expected_more = self.resume_after.seq < self.through.seq;
        if self.has_more != expected_more {
            return Err("Interaction replay has_more does not match the high watermark".into());
        }
        if self.has_more && self.events.is_empty() {
            return Err("Interaction replay cannot make no progress while has_more is true".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct RequestInteractionParams {
    pub thread_id: String,
    pub turn_id: String,
    pub host_call_id: String,
    pub request_id: String,
    #[serde(flatten)]
    pub request: InteractionRequest,
}

impl RequestInteractionParams {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        host_call_id: impl Into<String>,
        request_id: impl Into<String>,
        request: InteractionRequest,
    ) -> Result<Self, String> {
        let value = Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            host_call_id: host_call_id.into(),
            request_id: request_id.into(),
            request,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.turn_id, "turn_id")?;
        validate_identity(&self.host_call_id, "host_call_id")?;
        validate_identity(&self.request_id, "request_id")?;
        if self.request_id.len() != 36 || Uuid::parse_str(&self.request_id).is_err() {
            return Err("Nested Interaction request_id must be a UUID".into());
        }
        self.request.validate()
    }
}

impl<'de> Deserialize<'de> for RequestInteractionParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            thread_id: String,
            turn_id: String,
            host_call_id: String,
            request_id: String,
            #[serde(flatten)]
            request: InteractionRequest,
        }

        let map = checked_map(deserializer, REQUEST_INTERACTION_KEYS)?;
        let wire: Wire = decode_object(map)?;
        Ok(Self {
            thread_id: wire.thread_id,
            turn_id: wire.turn_id,
            host_call_id: wire.host_call_id,
            request_id: wire.request_id,
            request: wire.request,
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RespondInteractionParams {
    pub thread_id: String,
    pub turn_id: String,
    pub request_id: String,
    pub response: Value,
}

impl fmt::Debug for RespondInteractionParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RespondInteractionParams")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("request_id", &self.request_id)
            .field("response", &"[REDACTED]")
            .finish()
    }
}

impl RespondInteractionParams {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        request_id: impl Into<String>,
        response: Value,
    ) -> Result<Self, String> {
        let value = Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            request_id: request_id.into(),
            response,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.thread_id, "thread_id")?;
        validate_identity(&self.turn_id, "turn_id")?;
        InteractionResponse {
            request_id: self.request_id.clone(),
            response: self.response.clone(),
        }
        .validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RespondInteractionResult {
    pub request_id: String,
    pub resolved: bool,
}

impl RespondInteractionResult {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity(&self.request_id, "request_id")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct StartThreadWithInteractionsParams {
    #[serde(flatten)]
    pub session: StartThreadParams,
    pub interactions_enabled: bool,
}

impl StartThreadWithInteractionsParams {
    pub fn new(session: StartThreadParams, interactions_enabled: bool) -> Self {
        Self {
            session,
            interactions_enabled,
        }
    }
}

impl<'de> Deserialize<'de> for StartThreadWithInteractionsParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = checked_map(deserializer, START_THREAD_KEYS)?;
        let interactions_enabled = take_bool(&mut map, "interactions_enabled")?;
        let session = decode_object(map)?;
        Ok(Self {
            session,
            interactions_enabled,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct CreatePersistentSessionWithInteractionsParams {
    #[serde(flatten)]
    pub persistent: CreatePersistentSessionParams,
    pub interactions_enabled: bool,
}

impl CreatePersistentSessionWithInteractionsParams {
    pub fn new(persistent: CreatePersistentSessionParams, interactions_enabled: bool) -> Self {
        Self {
            persistent,
            interactions_enabled,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.persistent.validate()
    }
}

impl<'de> Deserialize<'de> for CreatePersistentSessionWithInteractionsParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = checked_map(deserializer, CREATE_PERSISTENT_KEYS)?;
        let interactions_enabled = take_bool(&mut map, "interactions_enabled")?;
        let persistent = decode_object(map)?;
        Ok(Self {
            persistent,
            interactions_enabled,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct AttachRecoveryWithInteractionsParams {
    #[serde(flatten)]
    pub recovery: AttachRecoveryParams,
    pub interactions_enabled: bool,
}

impl AttachRecoveryWithInteractionsParams {
    pub fn new(recovery: AttachRecoveryParams, interactions_enabled: bool) -> Self {
        Self {
            recovery,
            interactions_enabled,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.recovery.validate()
    }
}

impl<'de> Deserialize<'de> for AttachRecoveryWithInteractionsParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = checked_map(deserializer, ATTACH_RECOVERY_KEYS)?;
        let interactions_enabled = take_bool(&mut map, "interactions_enabled")?;
        let recovery = decode_object(map)?;
        Ok(Self {
            recovery,
            interactions_enabled,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionEventRetention {
    Retain,
    AdvanceReplayFloorWithoutRetaining,
}

pub const fn interaction_event_retention(
    serialized_event_bytes: usize,
) -> InteractionEventRetention {
    if serialized_event_bytes > MAX_INTERACTION_JOURNAL_BYTES {
        InteractionEventRetention::AdvanceReplayFloorWithoutRetaining
    } else {
        InteractionEventRetention::Retain
    }
}

pub fn validate_interaction_replay_page_limit(limit: u32) -> Result<(), String> {
    if limit == 0 || limit > MAX_INTERACTION_REPLAY_PAGE_LIMIT {
        return Err(format!(
            "Interaction replay page limit must be between 1 and {MAX_INTERACTION_REPLAY_PAGE_LIMIT}"
        ));
    }
    Ok(())
}

pub fn validate_interaction_subscriber_output_capacity(capacity: usize) -> Result<(), String> {
    if capacity == 0 || capacity > MAX_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY {
        return Err(format!(
            "Interaction subscriber output capacity must be between 1 and {MAX_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY}"
        ));
    }
    Ok(())
}

pub fn validate_interaction_journal_usage(
    retained_events: usize,
    serialized_event_bytes: usize,
) -> Result<(), String> {
    if retained_events > MAX_INTERACTION_JOURNAL_EVENTS {
        return Err(format!(
            "Interaction journal cannot retain more than {MAX_INTERACTION_JOURNAL_EVENTS} events"
        ));
    }
    if serialized_event_bytes > MAX_INTERACTION_JOURNAL_BYTES {
        return Err(format!(
            "Interaction journal cannot retain more than {MAX_INTERACTION_JOURNAL_BYTES} serialized bytes"
        ));
    }
    Ok(())
}

fn validate_pending_set(
    pending: &[PendingInteraction],
    required_turn_id: Option<&str>,
) -> Result<(), String> {
    let mut identities = HashSet::new();
    let mut per_turn = HashMap::<&str, usize>::new();
    for interaction in pending {
        interaction.validate()?;
        if required_turn_id.is_some_and(|turn_id| interaction.turn_id != turn_id) {
            return Err("Turn Interaction snapshot contains another Run".into());
        }
        if !identities.insert((&interaction.turn_id, &interaction.request_id)) {
            return Err("Interaction snapshot contains a duplicate request identity".into());
        }
        let count = per_turn.entry(interaction.turn_id.as_str()).or_default();
        *count += 1;
        if *count > MAX_PENDING_INTERACTIONS_PER_RUN {
            return Err(format!(
                "A Run cannot contain more than {MAX_PENDING_INTERACTIONS_PER_RUN} pending Interactions"
            ));
        }
    }
    Ok(())
}

fn validate_kind(kind: &str) -> Result<(), String> {
    if kind.is_empty()
        || kind.len() > MAX_INTERACTION_KIND_BYTES
        || kind.trim() != kind
        || !kind.as_bytes()[0].is_ascii_alphanumeric()
        || !kind.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
        })
    {
        return Err("Interaction kind must match [A-Za-z0-9][A-Za-z0-9._:/-]{0,127}".into());
    }
    if (kind == "whale" || kind.starts_with("whale.")) && !BUILTIN_KINDS.contains(&kind) {
        return Err("Unknown whale.* Interaction kinds are reserved".into());
    }
    Ok(())
}

fn validate_identity(value: &str, label: &str) -> Result<(), String> {
    validate_bounded_text(value, label, MAX_INTERACTION_ID_BYTES)
}

fn validate_bounded_text(value: &str, label: &str, max_bytes: usize) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!("{label} must be nonempty and unpadded"));
    }
    if value.len() > max_bytes {
        return Err(format!("{label} exceeds {max_bytes} UTF-8 bytes"));
    }
    Ok(())
}

fn validate_json_size(value: &Value, label: &str, max_bytes: usize) -> Result<(), String> {
    let bytes = serialized_bytes(value).map_err(|error| error.to_string())?;
    let max_bytes = u64::try_from(max_bytes).map_err(|_| "JSON byte limit exceeds u64")?;
    if bytes > max_bytes {
        return Err(format!("{label} exceeds {max_bytes} serialized bytes"));
    }
    Ok(())
}

fn validate_local_schema_references(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "$ref" | "$dynamicRef") {
                    let reference = value
                        .as_str()
                        .ok_or_else(|| format!("Interaction schema {key} must be a string"))?;
                    if !reference.starts_with('#') {
                        return Err(
                            "Interaction response_schema cannot retrieve external references"
                                .into(),
                        );
                    }
                }
                validate_local_schema_references(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_local_schema_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn checked_map<'de, D>(
    deserializer: D,
    allowed: &'static [&'static str],
) -> Result<Map<String, Value>, D::Error>
where
    D: Deserializer<'de>,
{
    struct CheckedMapVisitor {
        allowed: &'static [&'static str],
    }

    impl<'de> Visitor<'de> for CheckedMapVisitor {
        type Value = Map<String, Value>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an Interaction parameter object")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut map = Map::new();
            while let Some((key, value)) = access.next_entry::<String, Value>()? {
                if !self.allowed.contains(&key.as_str()) {
                    return Err(A::Error::unknown_field(&key, self.allowed));
                }
                if map.insert(key.clone(), value).is_some() {
                    return Err(A::Error::custom(format!("duplicate field {key}")));
                }
            }
            Ok(map)
        }
    }

    deserializer.deserialize_map(CheckedMapVisitor { allowed })
}

fn take_bool<E>(map: &mut Map<String, Value>, key: &'static str) -> Result<bool, E>
where
    E: DeError,
{
    let value = map.remove(key).ok_or_else(|| E::missing_field(key))?;
    serde_json::from_value(value).map_err(E::custom)
}

fn decode_object<T, E>(map: Map<String, Value>) -> Result<T, E>
where
    T: DeserializeOwned,
    E: DeError,
{
    serde_json::from_value(Value::Object(map)).map_err(E::custom)
}
