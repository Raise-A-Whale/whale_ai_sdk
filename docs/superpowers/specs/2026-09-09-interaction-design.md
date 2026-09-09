# Rust generic Interaction design

**Status:** Application SDK Stage 4 design, proposed on 2026-09-09
**Scope:** Rust protocol, Core, daemon, Store recovery boundary, and Rust SDK
**Product boundary:** no CLI, TUI, desktop window, renderer, or product policy

## Purpose

An application host needs one suspension contract for every point where an
Agent cannot continue without a person or host policy. Tool approval is one
instance. The same runtime boundary must support clarification, structured
forms, authentication handoff, file or network permission, review, and
integration-defined requests.

The daemon owns live pending state, ordering, replay, ownership, idempotency,
cancellation, and cleanup. The host owns presentation and whether to answer.
The component that requested an interaction owns the response meaning and any
subsequent effect.

This is application substrate. It does not prescribe a terminal prompt, modal,
notification, credential store, filesystem sandbox, network policy, or review
UI.

## Current-state audit

- `whale-core::ApprovalGate` stores only a request ID and one-shot sender. It
  removes the entry on first resolution or waiter drop, so Core cannot expose
  a complete pending request or distinguish a duplicate response from a
  conflict (`crates/whale-core/src/approval.rs:30-113`).
- The tool coordinator validates original arguments before approval and
  validates modified arguments again before execution. A generic compatibility
  wrapper must preserve both checks
  (`crates/whale-core/src/coordinator.rs:332-375`).
- `RunSnapshot.pending_approvals` is the current typed query surface.
  `RunRecord.decisions` already gives the run-scoped typed path same-request
  idempotency and different-request conflict detection
  (`crates/whale-daemon/src/server.rs:1313-1326` and
  `crates/whale-daemon/src/server.rs:1674-1763`).
- Typed pending state is projected from `ApprovalRequested` and cleared at
  terminal reconciliation (`crates/whale-daemon/src/server.rs:1855-1880` and
  `crates/whale-daemon/src/server.rs:1975-2009`).
- Session close and connection EOF cancel owned Runs, join terminal delivery,
  and then remove Session, Run, callback, projection, and persistent attachment
  state (`crates/whale-daemon/src/session_lifecycle.rs:247-425`).
- Store recovery never resumes an active Run. It finalizes it as
  `RECOVERY_INTERRUPTED` and relies on the dispatch-intent journal for uncertain
  external effects (`crates/whale-store/src/state.rs:540-649`).
- Rust exposes typed approval methods on `RunHandle` and `WhaleClient`. Their
  source signatures and wire behavior are compatibility obligations
  (`crates/whale-sdk-rust/src/run.rs:355-377`).

Three existing public structs are also compatibility boundaries:
`AgentDefinition`, `StartThreadParams`, and `RunSnapshot` are constructed with
struct literals by downstream Rust code. Adding a public field would break
source compatibility even if serde supplied a default. This design leaves all
three unchanged.

## Chosen architecture

Add an optional `interactions.v1` capability with a separate, session-scoped
Interaction read model:

```text
InteractionRegistry
  -> InteractionSnapshot (authoritative pending set)
  -> InteractionCursor (one live Session attachment)
  -> bounded Requested/Removed journal
  -> session.interactions.get / session.interactions.subscribe
  -> session.interaction_event notification
  -> Rust InteractionWatch with replay/live merge

turn.interactions.get
  -> authoritative Run-filtered pending set at the same Session cursor

turn.respond_interaction
  -> one daemon-owned response transaction
```

This does not modify `RunSnapshot`, `AgentStreamEvent`, `RunEventPayload`,
`SessionSnapshot`, or the closed Session V1 event family. A missed notification
is recovered from the independent snapshot and bounded journal, without
polling.

Rejected alternatives:

- Adding a field to `RunSnapshot` or `AgentDefinition` would preserve wire
  decode but break Rust struct-literal construction.
- Adding `InteractionRequested` to an existing event enum would extend a
  previously frozen wire family and could make an old decoder reject the whole
  envelope.
- A best-effort notification with no cursor/journal would require polling and
  could leave a UI unaware of a suspended Run after a lost frame.

## Capability and source-compatible Session opt-in

`CAPABILITY_INTERACTIONS` is `"interactions.v1"`. A complete implementation
includes request/query/respond, fixed-window replay, lifecycle cleanup, and
typed approval compatibility. The capability is optional and is not added to
baseline `PROTOCOL_CAPABILITIES`.

`AgentDefinition` and `StartThreadParams` remain unchanged. Rust opts in
through private state on `Agent`:

```rust
impl Agent {
    pub fn with_interactions_enabled(self) -> Self;
    pub fn interactions_enabled(&self) -> bool;
}
```

Session creation serializes an additive wrapper through the existing method:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartThreadWithInteractionsParams {
    #[serde(flatten)]
    pub session: StartThreadParams,
    pub interactions_enabled: bool,
}
```

The daemon detects `interactions_enabled` in the request object, parses the
wrapper when present, and otherwise parses the unchanged `StartThreadParams`
with `false`. Because serde flatten and `deny_unknown_fields` are not a valid
combination, the wrapper uses a checked deserializer that rejects every
top-level key outside the flattened legacy key set plus
`interactions_enabled`. The SDK sends the wrapper only after it has verified
`interactions.v1`; an old daemon therefore never silently ignores the opt-in
field. Interaction snapshot/replay is independent and has no dependency on
`session_views.v1`.

Persistent create and attach use equivalent additive wrappers around their
unchanged parameter structs. Stage 2.2 already owns configuration V2 with the
exact `version/session/run_defaults/metadata` shape. Interaction therefore
introduces configuration V3, adding a separate runtime-features object rather
than altering serialized `StartThreadParams`:

```json
{
  "version": 3,
  "session": {},
  "run_defaults": {},
  "metadata": {},
  "runtime_features": {"interactions_enabled": true}
}
```

Migration is deterministic and idempotent:

- V1 first performs the frozen V1 -> V2 metadata move, then adds
  `runtime_features.interactions_enabled=false` and writes V3 in the same
  checked Store-open repair transaction, advancing the Store revision once;
- V2 preserves `session`, `run_defaults`, and `metadata` byte-for-byte, adds
  the false runtime feature, writes V3, and advances the revision once;
- reopening valid V3 performs no write; unknown versions or malformed feature
  objects fail without rewriting the record.

Persistent create writes V3 directly. Attach normalizes the caller to V3 and
requires exact equality for `session`, `run_defaults`, and
`runtime_features.interactions_enabled`; metadata remains the only ignored
caller field under the Stage 2.2 rule. A legacy attach wrapper means `false`.
Thus a V1/V2 record migrated to false cannot be silently upgraded by attach;
enabling it requires a separately specified future configuration mutation.
The SDK checks only `interactions.v1` before dispatch.

The flag says that the host can observe and answer generic interactions. It
grants no file, network, credential, or tool authority.

Legacy tool approval works without opt-in. A new daemon mirrors it into the
generic registry but still emits the old event and accepts old typed methods.

## Base Interaction types

Add `crates/whale-protocol/src/interactions.rs`.

```rust
pub const CAPABILITY_INTERACTIONS: &str = "interactions.v1";

pub const METHOD_SESSION_INTERACTIONS_GET: &str = "session.interactions.get";
pub const METHOD_SESSION_INTERACTIONS_SUBSCRIBE: &str =
    "session.interactions.subscribe";
pub const METHOD_SESSION_INTERACTION_EVENT: &str = "session.interaction_event";
pub const METHOD_TURN_INTERACTIONS_GET: &str = "turn.interactions.get";
pub const METHOD_TURN_REQUEST_INTERACTION: &str = "turn.request_interaction";
pub const METHOD_TURN_RESPOND_INTERACTION: &str = "turn.respond_interaction";

pub const INTERACTION_NOT_FOUND: i64 = -32050;
pub const INTERACTION_CONFLICT: i64 = -32051;
pub const INTERACTION_RESPONSE_INVALID: i64 = -32052;
pub const INTERACTION_UNAVAILABLE: i64 = -32053;
```

The `-32040..=-32043` range remains reserved by the Session management design.
Malformed JSON-RPC parameters use `-32602`.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionRequest {
    pub kind: String,
    pub title: String,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PendingInteraction {
    pub request_id: String,
    pub turn_id: String,
    #[serde(flatten)]
    pub request: InteractionRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionResponse {
    pub request_id: String,
    pub response: serde_json::Value,
}
```

The Session is implicit in its registry; `turn_id` permits filtering and owner
validation. Responses never appear in a pending value or event.

Limits are:

```rust
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
```

IDs, kind, and title are nonempty, unpadded UTF-8 within their byte limits.
`kind` matches `[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}` and is byte-exact.
`whale.*` is reserved; integrations use stable vendor/application namespaces.

`payload` may be any bounded JSON value. `response_schema`, when present, is a
bounded JSON object compiled as Draft 2020-12. External HTTP/file/custom schema
retrieval is disabled; local `#` references are allowed. Invalid response size
or schema leaves the request pending.

## Snapshot, cursor, replay, and notification

Interaction ordering is independent of Run sequences, Session cursors, and
Store revisions.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InteractionCursor {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InteractionSnapshot {
    pub thread_id: String,
    pub cursor: InteractionCursor,
    pub pending: Vec<PendingInteraction>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TurnInteractionSnapshot {
    pub thread_id: String,
    pub turn_id: String,
    pub cursor: InteractionCursor,
    pub pending: Vec<PendingInteraction>,
}
```

A fresh ordinary or recovered live Session receives a new opaque interaction
`stream_id` and cursor zero. `session.interactions.get` clones the pending set
and cursor atomically. `turn.interactions.get` validates the Run owner and
retention state, then returns only matching entries with the same Session
high-watermark cursor.

The V1 event family is closed at inception:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InteractionEventEnvelope {
    pub thread_id: String,
    pub cursor: InteractionCursor,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub payload: InteractionEventPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum InteractionEventPayload {
    Requested { interaction: PendingInteraction },
    Removed {
        request_id: String,
        turn_id: String,
        cause: String,
    },
}
```

Standard `cause` values are `resolved`, `cancelled`, `run_finished`,
`session_closed`, `connection_closed`, `origin_finished`, and
`publication_failed`. It is an open string so adding a cleanup reason does not
extend the event enum. Removed events contain no response or fingerprint.

Every pending-set mutation assigns a checked next cursor, mutates the
authoritative snapshot, appends the event to the journal, then enqueues the
notification under one short registry lock. I/O occurs later on one
session-scoped notifier. Commit precedes delivery.

Each Session journal retains at most 1,024 envelopes and 4 MiB of serialized
envelope bytes, the same hard bounds as the Session view journal. It maintains
a continuous `replay_floor`; count and byte eviction advance the floor through
the newest evicted sequence. Request payload and response-schema limits are
64 KiB each, and all envelope identifiers and text are separately bounded, so
an ordinary retained Requested event remains below the 4 MiB journal byte cap.
If any serialized envelope nevertheless exceeds 4 MiB, it may be delivered
live, advances `replay_floor` through its sequence, and is never retained. Run
retirement scrubs all events associated with that Run and advances the floor
before `RunExpired` is visible, so replay cannot bypass retention.

`session.interactions.subscribe` copies the Stage 2.1 fixed-window contract:

- input is `after`, optional `through`, and positive bounded `limit`;
- the first page freezes `through` to current;
- later pages must carry the same `through`;
- events satisfy `after < cursor <= through`;
- output is `events`, `resume_after`, `through`, `has_more`, and optional
  `InteractionReplayGap`;
- retention and stream-reset gaps require a fresh snapshot.

`session.interaction_event` carries one committed envelope. The Rust SDK
performs `get -> subscribe barrier -> live/replay merge` and de-duplicates by
cursor. Multiple bounded local watchers are independent. A slow watcher cannot
block a Run, another watcher, or the connection reader.

On response/cancel, `Removed` is committed before the suspended continuation is
released. Session close attempts to publish removals, then closes the notifier
and deletes the registry. Connection EOF cannot promise delivery; stream end
and subsequent Session absence are authoritative.

## Interaction kinds

`kind` stays open. The daemon validates shape/lifetime but never executes an
action because of a kind or payload.

| Kind | Safe request payload | Response purpose |
| --- | --- | --- |
| `whale.tool_approval` | canonical tool call, original arguments, optional reason | approve, reject, or replace arguments |
| `whale.clarification` | prompt, optional choices, free-text flag | answer or selected IDs |
| `whale.form` | field descriptions and safe defaults | schema-validated object |
| `whale.auth` | provider, scopes, authorization URL, display code | opaque credential reference or cancellation |
| `whale.permission.file` | operation and exact display path/scope | allow once or deny |
| `whale.permission.network` | scheme, host, port, method, reason | allow once or deny |
| `whale.review` | subject, safe preview, content digest, allowed actions | approve, changes, reject, optional feedback |

These are conventions rather than daemon switches. The producer binds a
response to one exact suspended continuation and rechecks the real resource
immediately before acting.

## Tool approval compatibility

Tool approval is a typed facade over `whale.tool_approval`. The generic payload
contains the canonical tool call, original arguments, and optional safe reason.
Its response schema accepts exactly:

```json
{"decision":"approve"}
{"decision":"reject","feedback":"optional"}
{"decision":"modify_arguments","arguments":{},"feedback":"optional"}
```

The coordinator still validates initial arguments before suspension and
replacement arguments after resolution.

The daemon mirrors each pending typed approval into its independent Interaction
registry with the same request ID and turn ID. `RunSnapshot.pending_approvals`
and `ApprovalRequested` remain unchanged. These methods delegate to one
transaction:

- `turn.respond_interaction`;
- `turn.resolve_approval`;
- legacy `approval.resolve`; and
- Rust `RunHandle::resolve_approval` and
  `WhaleClient::resolve_approval`.

The generic snapshot/event family is emitted only for a connection whose
Session opted in. Typed state works without opt-in. A response accepted through
one path is idempotent through another if its mapped JSON is equivalent; a
different mapped decision conflicts. New Rust typed methods use the generic
path for an opted-in capable Session and the exact old RPC otherwise.

## Producer paths

Core gains a transport-neutral bridge:

```rust
#[async_trait]
pub trait InteractionBridge: Send + Sync {
    async fn begin(
        &self,
        context: &RunContextInfo,
        request_id: String,
        request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError>;
}

impl whale_core::ToolContext {
    pub async fn request_interaction(
        &self,
        request: InteractionRequest,
    ) -> Result<serde_json::Value, CoreError>;
}
```

The daemon supplies a Run-scoped bridge. Direct Core callers may supply an
in-memory bridge. `ApprovalGate` stays public and maps typed decisions through
the bridge while preserving its no-bridge direct-Core behavior.

Rust host tools use a restricted nested RPC:

```rust
impl whale_sdk_rust::ToolContext {
    pub async fn request_interaction(
        &self,
        request: InteractionRequest,
    ) -> Result<serde_json::Value, SdkError>;
}
```

`turn.request_interaction` parameters contain exact thread, turn, active
reverse-call ID, client-generated UUID request ID, and flattened request. The
daemon accepts it only for the same connection and live callback. The RPC stays
pending and returns `{request_id,response}` after resolution. Completing or
cancelling the outer callback clears requests from that exact origin.

## Response state and idempotency

Each Run owns entries:

```text
Pending(request, origin, response_sender, compiled_schema)
    -> Resolved(hmac_sha256_response_digest)
    -> removed with Run retention or Session close

Pending -> Cleared(cancel | run finish | session/connection close |
                   origin finish | publication failure)
```

`Resolved` stores no response JSON. It stores HMAC-SHA256 over canonical JSON,
using a random 256-bit process-local key and constant-time digest comparison.
Canonical encoding sorts object keys, preserves array order, and uses the
decoded JSON number representation. Object member order therefore does not
create a conflict. This contract uses the `hmac` and `sha2` crates; it is not
the ambiguous `sha256(key || json)` construction.

All request, respond, cancel, and terminal mutations use:

```text
Run interaction table -> Interaction registry -> Run snapshot when typed
                       -> publication queue
```

No transport, Store, host code, or user wait occurs under these locks.

Response processing:

1. Validate the owner-scoped Run before revealing request existence.
2. If resolved, equal fingerprint returns success without another delivery;
   different fingerprint returns `INTERACTION_CONFLICT`.
3. If pending, validate response size/schema without consuming it.
4. Recheck exact pending identity, commit `Removed`, update typed approval state
   when applicable, and store the fingerprint.
5. Release the suspended continuation once; send the RPC acknowledgement only
   after state/event commit.

Concurrent equivalent responders join one logical resolution. Invalid response
leaves pending. If the continuation disappeared, clear and return
`INTERACTION_NOT_FOUND` rather than claiming an unconsumed response.

Cancel and response arbitrate under the same Run interaction lock. Resolution
first means the response wins; cancel can still stop later dispatch. Cancel
first commits Removed/cancelled, wakes producers, and makes later response not
found. Resolved fingerprints survive Run cancellation only while its live
record is retained.

## Rust SDK read and response surface

```rust
impl WhaleThread {
    pub async fn interaction_snapshot(
        &self,
    ) -> Result<InteractionSnapshot, InteractionViewError>;

    pub async fn watch_interactions(
        &self,
        options: InteractionWatchOptions,
    ) -> Result<InteractionWatch, InteractionViewError>;

    pub async fn subscribe_interactions_from(
        &self,
        cursor: InteractionCursor,
        options: InteractionSubscriptionOptions,
    ) -> Result<InteractionEventStream, InteractionViewError>;
}

impl RunHandle {
    pub async fn pending_interactions(
        &self,
    ) -> Result<TurnInteractionSnapshot, SdkError>;

    pub async fn respond_interaction(
        &self,
        request_id: &str,
        response: serde_json::Value,
    ) -> Result<RespondInteractionResult, SdkError>;
}

impl WhaleClient {
    pub async fn respond_interaction(
        &self,
        thread_id: &str,
        turn_id: &str,
        request_id: &str,
        response: serde_json::Value,
    ) -> Result<RespondInteractionResult, SdkError>;
}
```

`InteractionWatch` contains an atomic snapshot and independent event stream.
Options have private checked fields with defaults aligned to Session watch:
subscriber output capacity 64, capped at 4,096, and replay page limit 128,
capped at 256. Zero or an over-limit value fails locally before any business
RPC. Errors distinguish invalid projection, local lag requiring replay, replay
gap requiring resync, closed Session, and transport/RPC failure.

The SDK keeps interaction RPC codes in `SdkError::Rpc`, avoiding a new variant
on its currently exhaustive error enum. New methods check the capability and
private Session opt-in before sending a business RPC.

## Close, retention, and recovery

Run cancel/timeout clears pending entries and publishes Removed before the
terminal snapshot. Session close clears all pending entries, wakes producer and
nested RPC waiters, closes watcher routes, then removes the registry. Connection
EOF performs the same state cleanup even though it cannot deliver final
notifications. A cancelled close waiter does not cancel daemon-owned cleanup.

Run retention scrubs that Run's interaction journal payload before exposing
`RunExpired`; Session close removes the full cursor and journal.

The V3 runtime enablement flag is the only Interaction-related durable value.
Pending requests, responses, fingerprints, and waiters are absent from
`SessionRecord`, SQLite, `RecoverySnapshot`, model input, canonical history,
and tool execution records. Store finalization keeps clearing typed approvals,
and startup recovery creates no Interaction registry content.

After a response permits execution, existing durable tool rules remain
authoritative:

- before `DispatchIntent`, the tool was not dispatched;
- after intent but before outcome, recovery records one `UnknownExecution` and
  never retries automatically;
- after durable outcome, recovery uses the recorded canonical result.

An Interaction response is not evidence that an effect occurred.

## Security boundary

- Only the connection owning the exact Session and Run may query or respond.
  Request IDs and cursors are correlation, not authorization.
- The daemon never treats kind, title, payload, schema annotation, or response
  as executable code or an ambient permission.
- Permission is request-local. The producer revalidates the actual target and
  operation immediately before acting; a response cannot widen the captured
  scope or create a cached grant.
- Request payload/schema are replayable display data. They must exclude
  authorization headers, environment values, recovery keys, cookies, private
  keys, and raw access tokens.
- `whale.auth` should return an opaque credential reference. If raw credentials
  are unavoidable, they remain only in local transport and memory and are
  omitted from Debug, tracing, errors, snapshots, events, and Store.
- Schema compilation cannot fetch external resources. Size/count limits apply
  before insertion into retained state.
- Unknown kinds are opaque. A host may render a safe generic view or decline;
  it must never interpret unknown as allow.
- Logs contain safe IDs, kind, transition, byte counts, and timing only. They
  never include payload, schema, response, credential reference, or
  fingerprint.
- Cancellation cannot undo an effect already dispatched. Existing tool
  cancellation and unknown-execution recovery rules still apply.

## Compatibility

| Peer combination | Behavior |
| --- | --- |
| New Rust SDK + new daemon, Agent not opted in | Existing struct construction, wire JSON, and typed approval remain unchanged. |
| New Rust SDK + old daemon | Generic builder/use fails locally for missing capability; typed approval uses old RPC. |
| Old SDK + new daemon | Existing approval event/methods work; no generic event family is emitted for its Sessions. |
| Python/Java + new daemon | No new language API in Stage 4; old wire and approval behavior remain. |
| V1 or V2 persistent record | Store-open migrates through frozen V2 to V3 with interactions disabled; attach requesting true fails. |
| V3 persistent record | Runtime feature participates in attach equality; response/pending data is absent. |

No existing public struct field, method, event variant, baseline capability, or
typed approval JSON is removed or renamed.

## Test matrix

### Protocol

- Exact JSON and validation for request, pending value, cursor, Session/Run
  snapshots, Requested/Removed envelopes, fixed-window pages/gaps, producer and
  response RPCs, and source-compatible start wrappers.
- Every size/count boundary and open/recommended kind.
- Exact constants and off-by-one validation for replay page default 128/max
  256, subscriber output default 64/max 4,096, and Session journal hard bounds
  of 1,024 events/4 MiB.
- Existing AgentDefinition, StartThreadParams, RunSnapshot, RunEvent, and
  SessionEvent golden JSON remains unchanged.
- Compile fixtures continue using struct literals for all three existing public
  structs.
- Configuration V1 -> V2 -> V3 and V2 -> V3 migrations preserve every prior
  field, write once, and are idempotent on the next open; attach requires exact
  runtime-feature equality while still ignoring caller metadata.

### Core and daemon

- Generic request publishes before waiting and every recommended/custom kind
  passes without a kind switch.
- Session get and turn-filtered get are atomic; get/first-subscribe closes the
  race; fixed `through` pagination cannot drift.
- Offline replay, local lag recovery, oversized/count/byte eviction, retention
  gap, stream reset, and Run-retirement scrub.
- Same response sequential/concurrent/cross typed endpoint delivers once;
  conflict never mutates; invalid then corrected response remains usable.
- Owner, wrong Run, expired Run, disabled Session, inactive callback origin,
  duplicate request ID, and pending-count rejection.
- Deterministic response-vs-cancel races; timeout, terminal, Session close, EOF,
  and origin completion clear all waiters and routes.
- Original/replacement tool arguments remain independently validated.

### Store, SDK, and real runtime

- Restart while pending produces `RECOVERY_INTERRUPTED`, a new stream, no
  re-prompt, and no persisted request/response/fingerprint.
- Response followed by durable dispatch intent without outcome produces exactly
  one existing `UnknownExecution`.
- SDK capability/opt-in rejection sends zero business RPCs.
- Two Interaction watchers are independent; lagged/dropped watcher converges
  from snapshot/replay and never blocks Run result.
- Nested host-tool request on stdio/UDS makes progress without reader/writer
  deadlock and another Session remains usable.
- Embedded and managed runtimes have identical idempotency and cleanup.

## Acceptance criteria

1. A Rust host can atomically obtain pending interactions, watch a resumable
   session-scoped feed, filter by Run, and respond without transport code.
2. Clarification, form, auth, file/network permission, review, tool approval,
   and custom kinds share one open request/response shape.
3. Equivalent retries deliver once and succeed; conflicting retries return a
   stable error.
4. Invalid responses stay pending; cancel, timeout, callback finish, Session
   close, and EOF clear state and release waiters.
5. Typed approval source and wire APIs remain compatible and delegate to the
   generic transaction on an opted-in capable Session.
6. Existing public struct literals and frozen Run/Session events compile and
   serialize unchanged.
7. Responses and fingerprints never enter replay, logs, or Store; recovery
   never resumes an Interaction or repeats an effect.
8. All code and tests are Rust-only and add no CLI or UI.

## Deferred work

- Python and Java Interaction APIs.
- A CLI/TUI/desktop renderer or default policy.
- Persistent pending interactions or resuming a continuation after restart.
- Cached directory/domain/global permission grants.
- Credential vault and OAuth browser callback service.
- Any new variant in existing Run/Session event families.
- Interactions outside an active Run and remote multi-user arbitration.
