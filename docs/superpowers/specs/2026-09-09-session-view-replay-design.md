# Rust Session view and replay design

**Status:** accepted for implementation on 2026-09-09

**Scope:** Rust application SDK, protocol, and daemon substrate only
**Product boundary:** no CLI, TUI, desktop, or GUI product implementation

## Purpose

An application host must be able to render an agent Session, detach a view, add
another observer, and recover from a slow observer without owning the agent
loop. The current Run API cannot provide that contract: every Run has one event
consumer, its sequence restarts at each turn, and its snapshot only contains
completed canonical items.

Stage 2.1 adds an authoritative Session read model and a replayable Session
event feed. The daemon owns the truth and bounded replay journal. The Rust SDK
turns the one physical connection feed into independent local subscriptions.
This is the host-facing state layer on top of the Stage 1
`WhaleRuntime -> Agent -> WhaleThread -> RunHandle` lifecycle.

## Decisions

### Identity and ordering

`RunEvent.seq` remains scoped to one Run. Store revisions remain internal CAS
versions. Neither is a Session event cursor.

Every live Session attachment receives a fresh opaque `stream_id`. Its
`SessionCursor` contains `thread_id`, `stream_id`, and a checked, monotonically
increasing `seq`. A cursor is valid only for that exact live attachment.
Persistent recovery creates a new thread and stream; it begins from a new
authoritative snapshot and never replays old token deltas or tool effects.

Events are ordered by cursor, not wall-clock time. Delivery is ordered and may
contain duplicates across the replay/live merge. The SDK removes duplicates by
cursor. The protocol does not promise exactly-once delivery across a transport
failure.

### Authoritative projection

The daemon owns a short-lock `SessionViewRecord` independent of
`ThreadSession`. It contains:

- safe Session identity, agent name, initial metadata, and timestamps;
- canonical history committed so far;
- an optional active Run view, including partial item drafts;
- a lightweight summary of the most recently completed Run;
- the current Session cursor; and
- a bounded in-memory event journal.

`session.get` clones the projection and cursor under the same short lock. It
must remain responsive while the model call holds the `ThreadSession` mutex for
an entire turn.

An active Run view embeds the existing `RunSnapshot` without adding fields to
that public type. Partial message, reasoning, and tool-call state lives in
`InProgressItemProjection`. The projection applies `ItemStarted`, text,
reasoning, signature, and tool argument deltas. `ItemCompleted` removes its
draft and appends the canonical item once. Terminal reconciliation uses the
final Run snapshot and canonical turn history, clears drafts, clears the active
Run, and stores only a lightweight terminal summary.

Initial input may be observed through Core `ItemCompleted` events. The
projection de-duplicates by canonical item ID so that start-time and Core-time
reconciliation cannot append the same item twice.

### Journal and replay

The journal is bounded by both event count and serialized byte size. It tracks a
`replay_floor`: a cursor below that floor cannot be continued. Retained events
after the floor are always a contiguous suffix. A single oversized event may be
delivered live but need not be retained; committing it clears every entry at or
before its sequence and advances the floor to that sequence. A cursor exactly
at the floor can continue with later retained events.

Journal storage and Run payload retention have different bounds, but Run expiry
is also a hard disclosure boundary. Retention uses a two-phase conditional
retirement. It selects an `Arc`-identified candidate without exposing
`RunExpired`, releases the Run registry lock, removes every replay envelope for
that turn, and only then conditionally retires the same Run identity and exposes
`RunExpired`. To preserve a contiguous suffix, view eviction advances
`replay_floor` to the greatest removed sequence and removes all earlier
envelopes too. Replay may become unavailable just before Run expiry, but after
`turn.get` reports `RunExpired`, Session replay can never recover that Run's
snapshot or deltas. The projection keeps only a lightweight last-Run summary,
never every terminal `RunSnapshot`.

The daemon commits a projection change and journal envelope before attempting
the corresponding `session.event` notification. A failed or backpressured live
write therefore does not erase replayable state. Notification delivery never
holds the projection lock.

`session.subscribe` is a replay-page and synchronization-barrier RPC. It does
not allocate a daemon task or a daemon-side subscriber. The first request sends
`after` and no `through`; the daemon freezes `through` at the current high
watermark under the projection lock. Every later page sends that same
`through`. Pages contain only `after < seq <= through`, return `resume_after`,
and calculate `has_more` against the fixed window. A continuously active Run
therefore cannot make catch-up chase a moving tail. Live events above `through`
remain buffered in the SDK and are merged after the fixed replay window.

Gap behavior is explicit:

- a different stream yields `stream_reset`;
- a cursor older than retained history yields `retention`;
- a future sequence or different thread is invalid input;
- after a gap, callers fetch a fresh snapshot and start a new watch rather than
  applying a suffix to unknown state.

### Rust SDK subscriptions

The connection reader routes `session.event` into a per-Session event hub.
Every public subscription owns its own bounded queue and lag state. A slow or
dropped subscriber cannot block another subscriber, the protocol reader, or a
Run result.

`WhaleThread::watch()` installs its live route before calling `session.get`.
It returns the snapshot and an event stream that starts strictly after the
snapshot cursor. Events received during the request are buffered, sorted,
de-duplicated, and merged with replay if needed.

`WhaleThread::subscribe_from(cursor)` installs the live route before calling
`session.subscribe`. It merges the fixed replay window with concurrent live
events by cursor.

Each stream worker follows `Live -> CatchingUp(through) -> Live`. A broadcast
lag or sequence gap starts one catch-up from the last cursor already queued for
that stream. While replay pages are fetched, the worker does not copy live
payloads into its output queue; the shared live receiver remains bounded. After
reaching `through`, it drains live frames, discards duplicates, and starts
another fixed catch-up if the receiver itself lagged again. The bounded output
send is cancellation-aware. Cancelling one `recv()` future does not cancel or
duplicate catch-up because the worker owns the single replay transaction;
dropping the stream cancels the worker. A daemon gap ends only that stream with
typed `SessionViewError::ResyncRequired`.

Three cursors have distinct jobs. The Session worker's `last_enqueued` cursor is
the catch-up start because every earlier event is already ordered in its output
queue. The public stream's `last_received` cursor is what an application stores
when it drops and later creates a new subscription. A filtered Run subscription
also advances an internal `last_scanned` cursor across unrelated Session events,
while exposing the cursor of each matching `RunEventEnvelope`.

`RunHandle::subscribe_events()` uses the same Session feed and filters by
`turn_id`, enabling multiple observers and replay. It yields a
`RunEventEnvelope` containing both the Session cursor and `RunEvent`; the
underlying scan still advances across events for other Runs, so interleaving
cannot create a false gap. A caller can resume from the last exposed Session
cursor; replay may scan and discard unrelated events again. Existing
`RunHandle::events()` keeps its current single-consumer,
`AlreadySubscribed`/`EventLagged` behavior for source compatibility.

### Capability compatibility

This stage adds optional capabilities:

- `session_views.v1`
- `session_event_replay.v1`

They are daemon-advertised capabilities and are not added to the existing
`PROTOCOL_CAPABILITIES` SDK baseline. Old SDKs continue to use `turn.event`.
New view methods check the needed optional capability before sending a business
RPC and return a typed unsupported-capability error when talking to an older
daemon.

The daemon continues publishing the existing `turn.event` unchanged. Session
events use a new payload enum instead of extending `RunEventPayload`, whose
current exhaustive shape is preserved.

## Public protocol contract

The new `whale_protocol::session_views` module exposes these stable concepts:

```rust
pub struct SessionCursor {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

#[non_exhaustive]
pub struct SessionSummary {
    pub thread_id: String,
    pub agent_name: Option<String>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub revision: u64,
}

#[non_exhaustive]
pub struct InProgressItemProjection {
    pub item_id: String,
    pub item_type: String,
    pub phase: Option<MessagePhase>,
    pub text: String,
    pub call_id: Option<String>,
    pub raw_arguments: String,
    pub reasoning_signature: Option<String>,
}

#[non_exhaustive]
pub struct SessionRunView {
    pub snapshot: RunSnapshot,
    pub in_progress_items: Vec<InProgressItemProjection>,
    pub accepted_at_ms: u64,
    pub updated_at_ms: u64,
}

#[non_exhaustive]
pub struct SessionRunSummary {
    pub turn_id: String,
    pub status: RunStatus,
    pub usage: UsageMetrics,
    pub error: Option<RunFailure>,
    pub accepted_at_ms: u64,
    pub completed_at_ms: u64,
}

#[non_exhaustive]
pub struct SessionHistoryWindow {
    pub items: Vec<CanonicalItem>,
    pub start_index: u64,
    pub total_items: u64,
    pub capacity: u32,
}

#[non_exhaustive]
pub struct SessionSnapshot {
    pub summary: SessionSummary,
    pub history: SessionHistoryWindow,
    pub active_run: Option<SessionRunView>,
    pub last_run: Option<SessionRunSummary>,
    pub cursor: SessionCursor,
}

pub struct SessionEventEnvelope {
    pub thread_id: String,
    pub cursor: SessionCursor,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub payload: SessionEventPayload,
}

#[non_exhaustive]
pub enum SessionEventPayload {
    RunChanged { run: SessionRunView },
    RunEvent { event: RunEvent },
}

pub struct GetSessionParams {
    pub thread_id: String,
    pub history_limit: u32, // 1..=1024
}

pub struct SubscribeSessionParams {
    pub thread_id: String,
    pub after: SessionCursor,
    pub through: Option<SessionCursor>,
    pub limit: u32, // 1..=256
}

#[non_exhaustive]
pub enum ReplayGapReason {
    Retention,
    StreamReset,
}

pub struct ReplayGap {
    pub reason: ReplayGapReason,
    pub requested: SessionCursor,
    pub replay_floor: SessionCursor,
    pub current: SessionCursor,
    pub session_revision: u64,
}

pub struct SubscribeSessionResult {
    pub events: Vec<SessionEventEnvelope>,
    pub resume_after: SessionCursor,
    pub through: SessionCursor,
    pub has_more: bool,
    pub gap: Option<ReplayGap>,
}

#[non_exhaustive]
pub enum SessionProjectionError {
    InvalidState { message: String },
    StreamMismatch {
        expected_thread: String,
        expected_stream: String,
        actual_thread: String,
        actual_stream: String,
    },
    SequenceGap {
        expected: SessionCursor,
        actual: SessionCursor,
    },
}

impl SessionSnapshot {
    pub fn apply(
        &mut self,
        envelope: &SessionEventEnvelope,
    ) -> Result<bool, SessionProjectionError>;
}
```

The protocol methods and notification are:

```text
session.get
session.subscribe
session.event
```

`session.get` accepts a positive bounded `history_limit`; the returned history
window records its capacity, absolute start index, and total item count so the
public shape remains stable when Stage 2.2 adds history pagination.

`session.subscribe` accepts a `SessionCursor`, optional fixed `through`, and a
positive bounded page limit. Its result contains `events`, `resume_after`,
`through`, `has_more`, and `gap: Option<ReplayGap>`. `ReplayGap` includes the
reason, requested cursor, replay floor, current cursor, and current visible
Session revision. The floor is the authoritative boundary; a separate
oldest-event field is unnecessary.

`SessionSummary.revision` is the latest visible projection revision. In this
stage every journaled visible mutation increments both revision and Session
cursor sequence together. It remains a named field so future metadata CAS can
use a distinct counter without confusing it with Store recovery revision.

`SessionSnapshot::apply(&SessionEventEnvelope)` is the normative public reducer.
It requires the next contiguous cursor. Every envelope sets summary revision
and updated time from its cursor and timestamp. `RunChanged` replaces the active
Run. Stream events update the active Run snapshot/drafts and append completed
items to the bounded history window by item ID. `RunEvent::Finished` reconciles
committed items, creates `last_run` with `completed_at_ms` from the envelope,
and clears `active_run`. Protocol and daemon tests compare every snapshot field
after applying a suffix with `session.get` at the same cursor.

## Rust public API

The additive Rust surface is fixed as follows. Option fields are private.
Checked constructors enforce both non-zero and protocol maximum bounds;
accessors expose the accepted values. Defaults are output buffer 64 events,
replay page 128 events, and snapshot history 256 items.

```rust
pub struct SubscriptionOptions {
    buffer_capacity: NonZeroUsize,
    replay_page_size: NonZeroU32,
}

pub struct SessionWatchOptions {
    history_limit: NonZeroU32,
    subscription: SubscriptionOptions,
}

pub struct SessionWatch {
    pub snapshot: SessionSnapshot,
    pub events: SessionEventStream,
}

pub struct RunEventEnvelope {
    pub cursor: SessionCursor,
    pub event: RunEvent,
}

#[non_exhaustive]
pub enum SessionViewError {
    Sdk(SdkError),
    UnsupportedCapability { capability: &'static str },
    InvalidCursor { message: String },
    ResyncRequired { gap: ReplayGap },
    InvalidProjection { message: String },
}

impl WhaleThread {
    pub async fn snapshot(&self) -> Result<SessionSnapshot, SessionViewError>;
    pub async fn snapshot_with_history_limit(
        &self,
        history_limit: NonZeroU32,
    ) -> Result<SessionSnapshot, SessionViewError>;
    pub async fn watch(&self) -> Result<SessionWatch, SessionViewError>;
    pub async fn watch_with_options(
        &self,
        options: SessionWatchOptions,
    ) -> Result<SessionWatch, SessionViewError>;
    pub async fn subscribe_from(
        &self,
        cursor: SessionCursor,
        options: SubscriptionOptions,
    ) -> Result<SessionEventStream, SessionViewError>;
}

impl SessionEventStream {
    pub async fn recv(
        &mut self,
    ) -> Option<Result<SessionEventEnvelope, SessionViewError>>;
}

impl RunHandle {
    pub async fn subscribe_events(
        &self,
        after: Option<SessionCursor>,
        options: SubscriptionOptions,
    ) -> Result<RunEventSubscription, SessionViewError>;
}

impl RunEventSubscription {
    pub async fn recv(
        &mut self,
    ) -> Option<Result<RunEventEnvelope, SessionViewError>>;
}
```

When `after` is absent, a Run subscription begins after a newly fetched Session
snapshot cursor. Explicit Session close makes `recv()` return `None`.
Unexpected connection loss emits one `SessionViewError::Sdk` carrying the
transport/closed error and then ends. Existing `SdkError`, `RunEventStream`, and
all current method signatures remain unchanged.

## Publication points

The daemon updates the projection at these points:

1. Session publication stores agent name, metadata, timestamps, empty history,
   and a fresh stream identity.
2. Persistent attach initializes a fresh live projection from recovered
   canonical history and safe metadata. Recovery secrets, provider credentials,
   tool binding identifiers, and internal Store revisions never enter the view.
3. Run acceptance initializes the active Run projection. Its journal event is
   retained before model execution; live delivery occurs only after the start
   response acceptance gate opens.
4. Every streamed Run event updates the Run snapshot/drafts, commits the Session
   event, and only then attempts old and new notifications.
5. Cancel and approval resolution commit `RunChanged`, because those visible
   status transitions have no existing `RunEvent`.
6. Terminal store reconciliation completes first. The daemon then commits the
   final `RunEvent`, canonical history, cleared draft, and terminal summary
   before notification delivery.
7. Run retention selects conditional retirement candidates under the Run
   registry lock. Outside that lock, it first advances each affected Session
   replay floor and removes all envelopes through the last event belonging to
   that turn. It then re-locks the registry and exposes `RunExpired` only while
   removing the exact same `Arc` identity.

Checked sequence arithmetic fails closed. Owner checks for get/subscribe match
existing Session and Run isolation: a foreign and an unknown Session are
indistinguishable to the caller.

## Lifecycle limits in this stage

Replay means subscriber detach/resubscribe within one live `WhaleRuntime`.
Ordinary Session state is still destroyed on connection EOF. Persistent attach
creates a new live Session view and stream from durable canonical state; it does
not resurrect the old event stream.

Stage 2.1 deliberately supplies the viewer contract. Explicit Closing/Closed
replay, Session listing, independent history pagination, metadata CAS updates,
and long-lived view tombstones are deferred to Stage 2.2. In this stage explicit
Session close and connection loss terminate local SDK streams, and the daemon
removes the live projection with the Session. `session.get` includes a bounded
history window whose stable shape can be continued by the Stage 2.2 pager.

## Safety and resource invariants

- No view operation awaits while holding the projection lock.
- No view operation acquires the long-held `ThreadSession` lock.
- Journal mutation precedes notification delivery.
- Retained envelopes form a contiguous suffix after `replay_floor`.
- A frozen replay window has a finite high watermark under continuous writes.
- A local subscriber queue is bounded and isolated from all other subscribers.
- Cursor gaps are never silently skipped.
- Session event order spans multiple Runs even though each Run sequence restarts.
- Durable commit succeeds before a durable state change appears in the view.
- View serialization exposes no recovery secret, credential, provider config,
  system prompt, callback binding ID, or unknown-effect internals.
- Existing Run retention semantics remain unchanged.
- No product shell is added.

## Acceptance scenarios

1. A snapshot taken during a blocked model call returns immediately and contains
   partial text/reasoning/tool arguments.
2. A snapshot at cursor N plus events from N+1 reconstructs the same state as a
   later snapshot.
3. Two Runs with separate Run sequence ranges produce one strictly increasing
   Session cursor range.
4. Events produced with no active SDK subscriber can be replayed later.
5. Count, byte, oversized-event, and Run-retention eviction return precise
   structured gaps and never replay an expired Run payload.
6. Two Rust subscribers receive the same Run independently; starving one does
   not delay the other or `RunHandle::result()`.
7. An event arriving before a subscribe RPC response is emitted exactly once
   after replay/live merge.
8. Local lag triggers replay from the worker's `last_enqueued` cursor; a caller
   resumes a replaced stream from `last_received`. Server retention gap produces
   typed resync and a fresh watch converges.
9. Missing optional capabilities fail before a view business RPC is sent.
10. Existing Run JSON and legacy `RunHandle::events()` behavior stay unchanged.
