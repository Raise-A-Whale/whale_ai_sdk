# Rust Session management Stage 2.2 design

**Status:** proposed for implementation on 2026-09-09

**Depends on:** `docs/superpowers/specs/2026-09-09-session-view-replay-design.md`

**Scope:** Rust protocol, Store, daemon, and Rust application SDK substrate only

**Product boundary:** no CLI, TUI, desktop, GUI, or product-shell implementation

## Purpose

Stage 2.1 exposes a live-attachment snapshot and replay stream, but explicitly
removes that view when a Session closes. An application host still cannot list
the Sessions owned by its connection, page canonical history independently of a
snapshot, update display metadata safely, or finish rendering a Session through
`Closing` and `Closed`.

Stage 2.2 adds those management capabilities while preserving the entire Stage
2.1 contract. The daemon remains the authority. The Store remains a recovery
journal rather than an application catalog. A connection-local management
registry owns bounded live views and closed tombstones; the Rust SDK exposes
read-only view handles separately from the existing write-capable
`WhaleThread`.

## Non-goals

- No CLI, TUI, desktop, GUI, renderer, command parser, or application state
  store is added.
- No global enumeration of durable recovery records is added.
- No cross-connection Session observation or transfer is added.
- No resurrection of a previous attachment's event stream is added.
- No unbounded event or canonical-history archive is added.
- No exactly-once promise is made across process or transport failure.
- No Store revision, Store owner, lease epoch, recovery secret, provider
  credential, system prompt, or callback binding identifier enters a view.

## V1 compatibility is absolute

The following V1 wire methods and public Rust types remain byte-for-byte and
source compatible:

```text
session.get
session.subscribe
session.event
```

```rust
SessionCursor
SessionSummary
SessionSnapshot
SessionEventEnvelope
SessionEventPayload
SubscribeSessionResult
WhaleThread::{snapshot, snapshot_with_history_limit, watch,
              watch_with_options, subscribe_from}
RunHandle::subscribe_events
```

V1 continues to contain only `RunChanged` and `RunEvent`. Existing V1 replay
pages remain contiguous in their own `SessionCursor` sequence. Explicit close
continues to end a V1 SDK stream with `None`, and connection loss continues to
produce one transport error followed by `None`.

Stage 2.2 therefore does not append lifecycle or metadata variants to
`SessionEventPayload`, and it does not append required public fields to V1
snapshot structs. It defines a separate V2 projection, cursor, envelope, and
wire family. A daemon can maintain both projections for the same live Session:

- a Run-visible mutation is prepared and committed to V1 and V2 together;
- a metadata or lifecycle mutation advances only V2; and
- V1 and V2 have independent stream IDs and sequence numbers.

This separation is intentional. Advancing a shared sequence for a V2-only
event would make a later V1 replay page non-contiguous and would violate the
already shipped V1 reducer.

“Together” has a concrete transaction shape. Under the publication lane, a
`PreparedDualPublication` clones both current records, applies both reducers,
performs checked cursor/byte accounting, and computes both journal-retention
results without mutating either live record. The commit phase acquires the V1
then V2 short mutexes in that fixed order, verifies the prepared source
stream/sequence identities, and swaps both already validated records before
releasing either mutex. The swap contains no serialization, allocation, checked
arithmetic, reducer call, or other fallible operation. Notifications are queued
only after both swaps. If either preparation fails, neither side changes; if
the source identity changed, the owner connection fails closed and neither
candidate is installed.

All Stage 2.2 capabilities remain optional and stay out of
`PROTOCOL_CAPABILITIES`. An old client never invokes a V2 subscribe method and
therefore never receives a V2 notification. A new SDK checks each capability
before its business RPC and remains usable against an old daemon through the
unchanged V1 APIs.

## Identity, revision, and cursor domains

These domains must have different Rust types and must never be converted by
copying a numeric field:

| Domain | Public shape | Meaning | Lifetime |
| --- | --- | --- | --- |
| V1 Session cursor | `SessionCursor` | Stage 2.1 Run-visible projection position | One live attachment |
| V2 Session cursor | `SessionCursorV2` | All V2-visible Run, metadata, and lifecycle mutations | One live attachment |
| V2 view revision | `SessionSummaryV2.view_revision` | Exactly `SessionCursorV2.seq`; metadata CAS version | One live attachment |
| History anchor | `SessionHistoryAnchor` | Absolute canonical-item boundary in one V2 stream | One live attachment/tombstone |
| History page cursor | `SessionHistoryPageCursor` | Opaque fixed history window and next boundary | Cursor TTL |
| List page cursor | `SessionListCursor` | Opaque owner-catalog window and next ordinal | Cursor TTL |
| Store revision | `SessionRecord.revision` / `RecoverySnapshot.revision` | Durable journal CAS and authenticated recovery operations | One recovery record |
| Store epoch | recovery attachment epoch | Durable lease generation | One recovery record |

`SessionSummaryV2.view_revision` is a deliberately strong CAS version. Every
V2-visible event, including a Run event unrelated to metadata, changes it. A
caller that loses a race refreshes the V2 snapshot and decides whether to retry.
If a metadata-only version is ever needed, it must be a newly named field; the
Store revision is never reused.

All indexes and sequence increments use checked arithmetic and fail closed on
overflow.

## Owner namespace

The daemon derives `owner` exclusively from the current transport connection
ID. No list, get, history, subscribe, or metadata request accepts an owner
parameter. An unknown Session and a Session owned by another connection are
indistinguishable to the caller.

The owner namespace contains:

- published Open Sessions;
- Sessions in Closing;
- bounded Closed tombstones;
- a monotonic catalog ordinal for each published attachment; and
- an owner catalog generation used to invalidate pagination after removal.

Preparing Sessions are never visible. Connection EOF first performs existing
Run cancellation and durable detach cleanup, then removes the complete owner
namespace. Nothing in `session.list` enumerates unattached Store records.

## V2 protocol contract

### Capabilities and methods

```rust
pub const CAPABILITY_SESSION_CATALOG: &str = "session_catalog.v1";
pub const CAPABILITY_SESSION_HISTORY: &str = "session_history.v1";
pub const CAPABILITY_SESSION_METADATA_CAS: &str = "session_metadata_cas.v1";
pub const CAPABILITY_SESSION_LIFECYCLE_REPLAY: &str =
    "session_lifecycle_replay.v1";

pub const METHOD_SESSION_GET_V2: &str = "session.get.v2";
pub const METHOD_SESSION_SUBSCRIBE_V2: &str = "session.subscribe.v2";
pub const METHOD_SESSION_EVENT_V2: &str = "session.event.v2";
pub const METHOD_SESSION_LIST: &str = "session.list";
pub const METHOD_SESSION_HISTORY: &str = "session.history";
pub const METHOD_SESSION_METADATA_REPLACE: &str =
    "session.metadata.replace";
```

`session.get.v2`, `session.subscribe.v2`, and `session.event.v2` require
`session_lifecycle_replay.v1`. Each other method requires its correspondingly
named capability. Calling `session.subscribe.v2` is explicit proof that the
caller understands V2 notification payloads; V2 notifications are never sent
to a V1 subscription route.

### Snapshot and ordered events

```rust
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionCursorV2 {
    pub thread_id: String,
    pub stream_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSummaryV2 {
    pub thread_id: String,
    pub agent_name: Option<String>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub view_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionSnapshotV2 {
    pub summary: SessionSummaryV2,
    pub lifecycle: SessionLifecycleState,
    pub persistence: SessionPersistenceV2,
    pub history: SessionHistoryWindow,
    pub active_run: Option<SessionRunView>,
    pub last_run: Option<SessionRunSummary>,
    pub cursor: SessionCursorV2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionEventPayloadV2 {
    RunChanged { run: SessionRunView },
    RunEvent { event: RunEvent },
    MetadataChanged {
        metadata: serde_json::Map<String, serde_json::Value>,
    },
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

pub struct GetSessionV2Params {
    pub thread_id: String,
    pub history_limit: u32, // 1..=1024
}

pub struct SubscribeSessionV2Params {
    pub thread_id: String,
    pub after: SessionCursorV2,
    pub through: Option<SessionCursorV2>,
    pub limit: u32, // 1..=256
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplayGapReasonV2 {
    Retention,
    StreamReset,
}

pub struct ReplayGapV2 {
    pub reason: ReplayGapReasonV2,
    pub requested: SessionCursorV2,
    pub replay_floor: SessionCursorV2,
    pub current: SessionCursorV2,
    pub view_revision: u64,
}

pub struct SubscribeSessionV2Result {
    pub events: Vec<SessionEventEnvelopeV2>,
    pub resume_after: SessionCursorV2,
    pub through: SessionCursorV2,
    pub lifecycle: SessionLifecycleState,
    pub has_more: bool,
    pub gap: Option<ReplayGapV2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionProjectionErrorV2 {
    InvalidState { message: String },
    StreamMismatch {
        expected: SessionCursorV2,
        actual: SessionCursorV2,
    },
    SequenceGap {
        expected: SessionCursorV2,
        actual: SessionCursorV2,
    },
}
```

`SessionSnapshotV2::apply(&SessionEventEnvelopeV2)` is the normative V2
reducer. It requires the next contiguous cursor, applies Run variants with the
same rules as V1, replaces the complete metadata map for `MetadataChanged`, and
accepts only `Open -> Closing -> Closed` lifecycle transitions. Every applied
event sets `view_revision` to its cursor sequence and updates
`updated_at_ms`. No event is valid after `Closed`.

The V1 summary metadata remains the immutable attachment-start value. V2 is the
only projection that reflects metadata CAS during an attachment. A recovered
attachment starts both projections from the latest stored metadata, so V1
snapshot/replay equivalence remains intact without hiding a V2-only cursor
advance.

`SubscribeSessionV2Result` has the same fixed-`through` pagination shape as V1
but uses V2 cursors/envelopes and includes the lifecycle at `through`. The
lifecycle field lets a subscription from the final Closed cursor return an
empty page and terminate without waiting for another notification.

### Catalog

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionListCursor(String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionRunHeadline {
    pub turn_id: String,
    pub status: RunStatus,
    pub accepted_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionListEntry {
    pub summary: SessionSummaryV2,
    pub lifecycle: SessionLifecycleState,
    pub persistence: SessionPersistenceV2,
    pub cursor: SessionCursorV2,
    pub history_total_items: u64,
    pub active_run: Option<SessionRunHeadline>,
    pub last_run: Option<SessionRunSummary>,
}

pub struct ListSessionsParams {
    pub cursor: Option<SessionListCursor>,
    pub limit: u32,
}

pub struct ListSessionsResult {
    pub sessions: Vec<SessionListEntry>,
    pub next_cursor: Option<SessionListCursor>,
}
```

Publication assigns an owner-local, checked `catalog_ordinal`. A first page
freezes `through_ordinal`; later Session publications are excluded from that
pagination window. Entries are ordered by ordinal. Existing entries may show
their current metadata/lifecycle when their page is read, so the membership is
fixed but the result is not a multi-Session transactional snapshot. Each entry
carries its own V2 cursor and revision.

The opaque token binds owner, owner catalog generation, `after_ordinal`,
`through_ordinal`, expiry, and a daemon-secret authentication tag. It never
contains or aliases a Store revision. Closing keeps catalog membership. Any
tombstone removal increments the owner catalog generation, so a page cannot
silently skip an entry; a previously issued cursor instead returns a typed
`ListCursorExpired`.

### Opaque cursor authentication

List and history page cursors use HMAC-SHA256, not a plain hash or reversible
owner ID. `DaemonServer::new` obtains a fresh 32-byte key from the operating
system through `getrandom`; all clones of that server share the key. A restart
therefore invalidates prior cursors. Tests use an explicit fixed-key constructor
that is unavailable in non-test builds.

The token is `base64url_no_pad(payload) + "." + base64url_no_pad(tag)`. The
canonical payload is a versioned serde structure with token kind, fixed window,
next position, owner catalog generation where applicable, and expiry. The HMAC
input uses a domain prefix (`whale.session-list.v1` or
`whale.session-history.v1`), the raw current connection owner ID, and the exact
payload bytes. The owner ID is not present in the payload. Verification checks
a 4 KiB encoded-token limit before allocation and uses `hmac::Mac::verify_slice`
for constant-time tag comparison. Wrong kind, version, owner, key, tag, expiry,
or payload shape returns the corresponding typed invalid/expired cursor error.

The implementation adds `hmac = "0.12"`, `sha2 = "0.10"`,
`getrandom = "0.3"`, and `base64 = "0.22"` as workspace-managed daemon
dependencies. Cursor contents require integrity and owner binding, not
confidentiality.

### Canonical-history pages

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionHistoryAnchor {
    pub thread_id: String,
    pub stream_id: String,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionHistoryPageCursor(String);

pub struct GetSessionHistoryParams {
    pub thread_id: String,
    pub before: Option<SessionHistoryAnchor>,
    pub cursor: Option<SessionHistoryPageCursor>,
    pub limit: u32,
}

pub struct GetSessionHistoryResult {
    pub items: Vec<CanonicalItem>,
    pub start_index: u64,
    pub end_index: u64,
    pub through: SessionHistoryAnchor,
    pub current_end: SessionHistoryAnchor,
    pub next_cursor: Option<SessionHistoryPageCursor>,
}
```

`before` and `cursor` are mutually exclusive. With neither, the first request
freezes `through` at the current history length. Supplying `before` on the first
request freezes `through` at `min(before.index, current length)`; this lets an
application continue before `SessionHistoryWindow.start_index` without
re-fetching its tail. Later requests use only the opaque `next_cursor`.

Pages are selected backwards, while items inside a page are returned in
canonical oldest-to-newest order. Concurrent appends above `through` are
excluded. `current_end` reports the latest length without changing the fixed
window.

The page token binds owner, thread, V2 stream, fixed through, next exclusive
before, expiry, and an authentication tag. A persistent reattach has a new
thread and stream and rejects an old anchor or page cursor with
`HistoryStreamReset`. If bounded retention advances beyond the requested
boundary, the daemon returns `HistoryGap` with requested boundary, current
floor, and current end. It never silently omits an item. A single item larger
than the response byte cap returns `ResourceLimit` naming its absolute index.

The management registry owns a canonical-history archive separate from the
Store. It is updated only when canonical history commits. It serves both
ordinary and persistent attachments uniformly and moves with the view into a
Closed tombstone. Persistent Store contents do not become globally pageable.

### Metadata compare-and-set

```rust
pub struct ReplaceSessionMetadataParams {
    pub thread_id: String,
    pub expected_view_revision: u64,
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

pub struct ReplaceSessionMetadataResult {
    pub summary: SessionSummaryV2,
    pub cursor: SessionCursorV2,
    pub changed: bool,
}
```

The operation replaces the complete map; there is no merge-patch behavior.
Only Open Sessions accept it. The expected value is explicitly the V2 view
revision. A stale value returns `RevisionConflict` containing expected/current
V2 revisions and the current V2 cursor. With the correct revision, replacing a
map by an equal map returns `changed = false` and commits no event.

For a persistent Session, durable metadata replacement succeeds before
`MetadataChanged` becomes visible. A Store rejection commits no V2 event. A
process or transport failure after durable commit but before the response is an
unknown outcome; recovery exposes the stored metadata, and a live retry with
the old expected revision either observes the conflict or the connection has
already failed. The protocol does not claim an impossible atomic transaction
across a process crash.

## Publication lane

Every published Session owns one asynchronous `SessionPublicationLane` that is
separate from the short projection mutex and the long-held `ThreadSession`
mutex. All Run-visible publication, metadata CAS, and lifecycle publication use
this lane. Existing Core Run persistence remains a prerequisite of daemon
publication: its journal request and any `ThreadSession` guard complete before
the Run event waits for the lane. Metadata CAS performs its dedicated durable
command inside the lane because the expected V2 revision must remain fixed.
The order is:

```text
existing Run durable prerequisite, if any; release its guards
  -> publication lane
  -> short lifecycle/CAS validation
  -> prepare and fully validate candidate V1/V2 projection changes
  -> dedicated metadata SessionJournal command, if this is metadata CAS
  -> short, infallible projection swap and journal append
  -> release lane
  -> best-effort ordered notification I/O
```

No projection or lifecycle mutex is held across an await. No path takes the
`ThreadSession` mutex while holding or waiting for the lane. The Store journal
actor never acquires or awaits the publication lane. Consequently it can finish
an already queued Run command while metadata owns the lane, after which it can
process the metadata command; the Run event waits for the lane only after its
Store request has released every Store/Core guard.

Run-retention replay scrubbing also acquires the publication lane before
changing either V1 or V2 journal. Owner disconnect first marks the owner closed,
then waits for each accepted lane transaction before deleting its records. A
metadata transaction that entered before disconnect therefore either commits
both Store and V2 view before namespace removal, or fails before changing
either; disconnect cannot remove its record between durable success and the
prepared projection swap.

For metadata, candidate snapshots, cursor increments, serialized event sizes,
and the V2 reducer result are prepared before its durable command. For a Run,
the existing durable prerequisite has already completed, then both V1 and V2
candidate results are prepared before either projection is swapped. Once a
metadata durable command succeeds, its in-process projection swap cannot fail.
Notification failure does not roll back a journaled event.

Metadata request handlers own the accepted transaction in a daemon task, so
cancelling an individual RPC waiter cannot cancel the Store/view transaction.
CAS makes an uncertain response reconcilable. Existing close remains a
single-flight owned transaction; cancelling its first waiter does not cancel
the close.

## Lifecycle and tombstones

The public state machine is:

```text
                 explicit close
Preparing -> Open --------------> Closing -> Closed -> Evicted
    |          |                     |          |
    |          | transport EOF       |          | transport EOF
    +----------+---------------------+----------+--------------> removed owner namespace
```

`Preparing` remains internal and is never listed. Explicit close performs:

1. acquire the publication lane, re-check Open, switch the mutation gate to
   Closing, commit and publish the unique `LifecycleChanged(Closing)`, then
   release the lane;
2. reject new Runs, tool registration, context changes, metadata changes, and
   other business mutations;
3. cancel active Runs, join terminal delivery, and allow their final V1/V2 Run
   events to publish through the lane;
4. reacquire the lane and detach the persistent journal, if present;
5. commit and publish the unique `LifecycleChanged(Closed)`, release write-side
   Session/Run/callback resources, and retain the management record as a
   tombstone; and
6. complete every close waiter only after the Closed commit.

Holding the lane while joining a Run would deadlock the Run's terminal
publication, so the two lifecycle publications are separate lane turns. The
Closing mutation gate prevents unrelated new work between them.

If persistent detach is not confirmed, `Closed` is not published. The daemon
returns the existing typed Store failure/unknown outcome and fails the owner
connection rather than leaving a writable or falsely Closed attachment.

The final Closed envelope is replayable until tombstone eviction. A live V2
subscriber receives `Closing`, remaining terminal Run events, `Closed`, then
`None`. A late subscriber replays the same suffix. A subscriber starting at the
Closed cursor gets an empty fixed window whose returned lifecycle is Closed and
then ends. No event is valid after Closed.

V1 cleanup remains unchanged: its view/hub is removed during close, so existing
V1 streams simply end. V2 read handles and their replay route are independent
of the write-side Session fence.

### Ordinary and persistent lifecycle

| Boundary | Ordinary Session | Persistent Session |
| --- | --- | --- |
| Publication | Current owner only | Current owner after create/attach |
| Explicit close | In-memory Closed tombstone | Durable detach, then in-memory Closed tombstone |
| Connection EOF | Destroyed | Detached; owner namespace destroyed |
| Tombstone eviction | No remaining state | Recovery record remains, but old view disappears |
| Reappearance | Impossible | Authenticated attach creates new thread, V1/V2 streams, and revisions |
| Metadata authority | Live V2 projection | Stored configuration while detached; live V2 projection while attached |

A persistent recovery record may be reattached while an older Closed tombstone
still exists in the same owner namespace. They have different thread and stream
identities and may both appear in a list page. Recovery ID alone is not a view
identity.

## Persistent metadata schema and migration

`SessionRecord.schema_version` and the embedded configuration version are
separate. Stage 2.2 keeps the current record schema version and introduces this
configuration V2 shape:

```json
{
  "version": 2,
  "session": {
    "model": "...",
    "tools": [],
    "limits": null
  },
  "run_defaults": {},
  "metadata": {}
}
```

The `session` object is the current normalized `StartThreadParams` with
`session_id`, `metadata`, and host-only `binding_id` values removed. All other
normalization and durable tool-schema behavior remains unchanged. Metadata is
the only field exempted from attachment configuration equality.

Persistent create writes configuration V2 directly. `StoreRuntime::open`
idempotently migrates configuration V1 by moving `/session/metadata` to the
top-level `/metadata`, removing it from `/session`, and setting `version` to 2.
Migration preserves history, Runs, unknown executions, recovery identity,
epoch, and every metadata JSON value. It advances the internal Store revision
once through backend CAS. An unknown version or malformed non-object metadata
fails Store startup; migration never truncates or silently replaces data.

`StoreRuntime::attach` keeps its existing public signature. Internally it
normalizes the caller's configuration to V2 and compares only `session` and
`run_defaults`; caller-supplied metadata is ignored during attach. The stored
top-level metadata initializes the fresh V2 projection. This preserves source
compatibility while preventing stale attach parameters from overwriting a CAS
update.

`SessionJournal::replace_metadata` is a dedicated command that changes only the
top-level metadata and advances only the internal Store revision. It does not
reuse `replace_configuration`, does not accept a public Store revision, and is
valid while a Run is active because the publication lane orders it with Run
commits. Existing configuration V1 values that exceed the new write limit are
preserved and readable; every new metadata CAS replacement must satisfy the
Stage 2.2 resource limits.

## Rust application SDK

Stage 2.2 adds a cloneable, non-owning read capability:

```rust
pub struct SessionListOptions {
    cursor: Option<SessionListCursor>,
    limit: NonZeroU32,
}

pub struct SessionHistoryOptions {
    before: Option<SessionHistoryAnchor>,
    cursor: Option<SessionHistoryPageCursor>,
    limit: NonZeroU32,
}

pub struct SessionManagementWatchOptions {
    history_limit: NonZeroU32,
    subscription: SubscriptionOptions,
}

pub type SessionListPage = ListSessionsResult;
pub type SessionHistoryPage = GetSessionHistoryResult;

pub struct SessionWatchV2 {
    pub snapshot: SessionSnapshotV2,
    pub events: SessionEventStreamV2,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionManagementError {
    Sdk(SdkError),
    UnsupportedCapability { capability: &'static str },
    InvalidOptions { field: &'static str, message: String },
    SessionUnavailable,
    SessionNotOpen { lifecycle: SessionLifecycleState },
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
    TombstoneExpired { thread_id: String },
    ResourceLimit {
        resource: String,
        actual: u64,
        limit: u64,
        item_index: Option<u64>,
    },
    StorageFailure { outcome_unknown: bool },
    InvalidProjection { message: String },
}

pub struct SessionViewHandle { /* client + thread identity */ }

impl WhaleClient {
    pub async fn list_sessions(
        &self,
        options: SessionListOptions,
    ) -> Result<SessionListPage, SessionManagementError>;

    pub fn session_view(
        &self,
        thread_id: impl Into<String>,
    ) -> Result<SessionViewHandle, SessionManagementError>;
}

impl WhaleThread {
    pub fn session_view(&self) -> SessionViewHandle;

    pub async fn replace_metadata(
        &self,
        expected_view_revision: u64,
        metadata: serde_json::Map<String, serde_json::Value>,
    ) -> Result<ReplaceSessionMetadataResult, SessionManagementError>;
}

impl SessionViewHandle {
    pub fn thread_id(&self) -> &str;
    pub async fn snapshot(&self) -> Result<SessionSnapshotV2, SessionManagementError>;
    pub async fn history_page(
        &self,
        options: SessionHistoryOptions,
    ) -> Result<SessionHistoryPage, SessionManagementError>;
    pub async fn watch(
        &self,
        options: SessionManagementWatchOptions,
    ) -> Result<SessionWatchV2, SessionManagementError>;
    pub async fn subscribe_from(
        &self,
        cursor: SessionCursorV2,
        options: SubscriptionOptions,
    ) -> Result<SessionEventStreamV2, SessionManagementError>;
}

impl SessionEventStreamV2 {
    pub fn last_received(&self) -> Option<&SessionCursorV2>;
    pub async fn recv(
        &mut self,
    ) -> Option<Result<SessionEventEnvelopeV2, SessionManagementError>>;
}
```

The option APIs are fixed as follows:

```rust
impl SessionListOptions {
    pub fn new(limit: u32) -> Result<Self, SessionManagementError>;
    pub fn with_cursor(self, cursor: SessionListCursor) -> Self;
    pub fn limit(&self) -> u32;
    pub fn cursor(&self) -> Option<&SessionListCursor>;
}

impl Default for SessionListOptions { /* limit 64, no cursor */ }

impl SessionHistoryOptions {
    pub fn new(limit: u32) -> Result<Self, SessionManagementError>;
    pub fn before(
        self,
        anchor: SessionHistoryAnchor,
    ) -> Result<Self, SessionManagementError>;
    pub fn continue_from(
        self,
        cursor: SessionHistoryPageCursor,
    ) -> Result<Self, SessionManagementError>;
    pub fn limit(&self) -> u32;
    pub fn anchor(&self) -> Option<&SessionHistoryAnchor>;
    pub fn cursor(&self) -> Option<&SessionHistoryPageCursor>;
}

impl Default for SessionHistoryOptions { /* limit 128, current end */ }

impl SessionManagementWatchOptions {
    pub fn new(
        history_limit: u32,
        subscription: SubscriptionOptions,
    ) -> Result<Self, SessionManagementError>;
    pub fn history_limit(&self) -> u32;
    pub fn subscription(&self) -> &SubscriptionOptions;
}

impl Default for SessionManagementWatchOptions {
    /* history 256, existing SubscriptionOptions defaults */
}
```

`SessionViewHandle` cannot start/cancel a Run, register a tool, replace
metadata, or close the Session. It does not pin a server tombstone beyond its
retention limits. `WhaleThread` remains the write capability and its existing
methods retain the current Open-state fence. A view handle remains useful after
the write side reaches Closed.

`SessionListOptions`, `SessionHistoryOptions`, and
`SessionManagementWatchOptions` have private fields, checked constructors,
accessors, and defaults. V2 subscription workers reuse the Stage 2.1
live/replay merge rules: route-before-snapshot, mandatory fixed-window replay
barrier, per-subscriber bounded queues, independent `last_enqueued` and public
`last_received`, lag repair, cancellation-safe recv, and one disconnect error
then `None`.

The SDK keeps separate V1 and V2 hubs. Write-side release closes V1 as it does
today but does not close V2 before the Closed envelope is replayed/delivered.
The close RPC response triggers a V2 replay barrier in case its notification
was delayed. A V2 worker terminates cleanly after applying Closed.

## Typed errors

The protocol adds stable error data in a new `SessionManagementErrorData`
tagged enum. JSON-RPC messages are diagnostic only; SDK mapping uses code plus
typed `data.kind`, never string matching. The wire shape is:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionManagementErrorData {
    Unavailable,
    SessionNotOpen { lifecycle: SessionLifecycleState },
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
    TombstoneExpired { thread_id: String },
    ResourceLimit {
        resource: String,
        actual: u64,
        limit: u64,
        item_index: Option<u64>,
    },
    StorageFailure { outcome_unknown: bool },
}
```

The wire `Unavailable` value maps to the single SDK
`SessionManagementError::SessionUnavailable` variant. The SDK enum contains
that variant exactly once.

| SDK error | Required data | Meaning/recovery |
| --- | --- | --- |
| `UnsupportedCapability` | capability | SDK rejects before RPC |
| `InvalidOptions` | field/reason | Checked constructor or invalid wire input |
| `SessionUnavailable` | none | Unknown and foreign owner are identical |
| `SessionNotOpen` | current lifecycle | Reads remain allowed; writes do not |
| `RevisionConflict` | expected/current V2 revision and current cursor | Refresh V2 snapshot and retry deliberately |
| `ListCursorInvalid` | none | Malformed, forged, wrong-owner token |
| `ListCursorExpired` | none | TTL or catalog generation changed; restart list |
| `HistoryCursorInvalid` | none | Malformed, forged, or wrong-thread token |
| `HistoryStreamReset` | requested and current V2 stream | Fetch a fresh V2 snapshot |
| `HistoryGap` | requested, floor, current end | Repaint from retained history/snapshot |
| `TombstoneExpired` | thread ID | Previously owned Closed view was evicted |
| `ResourceLimit` | resource, actual, limit | Reduce page/write size; oversized item is explicit |
| `StorageFailure` | outcome known/unknown | No false view rollback; recover persistent state |
| `InvalidProjection` | validation reason | SDK rejects malformed daemon data |

Recommended JSON-RPC codes are `-32040` for Session management state,
`-32041` for revision conflict, `-32042` for cursor rejection, and `-32043`
for history gaps. Durable failures continue using `STORE_FAILED`; configured
Session limits continue using `SESSION_LIMIT_EXCEEDED`.

## Capacity and retention boundaries

The following values are part of the Stage 2.2 contract and are checked before
allocation:

| Resource | Default | Hard maximum |
| --- | ---: | ---: |
| List page | 64 entries | 256 entries and 1 MiB serialized response |
| History page | 128 items | 256 items and 1 MiB serialized response |
| V2 replay page | 128 events | 256 events |
| V2 subscriber output | 64 events | 4096 events |
| Metadata replacement | n/a | 64 KiB serialized JSON, 256 top-level keys, depth 16 |
| V2 event journal per Session | 1024 events / 4 MiB | same fixed bound |
| Management history archive per Session | 32 MiB | lower of 32 MiB and configured `max_history_bytes` |
| Closed tombstones per owner | n/a | 128 records, 64 MiB aggregate |
| Closed tombstone lifetime | 10 minutes | 10 minutes |
| List/history cursor lifetime | 5 minutes | 5 minutes |

Page count and byte limits both apply. A single over-limit entry/item returns a
typed error rather than disappearing. History archive eviction removes whole
canonical items, advances an absolute `history_floor`, and preserves item
order. V2 event retention remains an independent contiguous-suffix journal.

Tombstone pressure evicts the oldest Closed records first and never evicts Open
or Closing records. Every removal increments owner catalog generation. TTL is
checked on every management request and by a bounded periodic sweep; count and
byte limits remain hard even if the sweep is delayed. Checked byte accounting
includes snapshots, history items, and retained V2 envelopes.

## Safety invariants

- V1 JSON, cursor continuity, reducers, Rust signatures, and close behavior do
  not change.
- A V2 cursor is valid only for one exact live attachment or its retained
  tombstone.
- Owner identity always comes from the transport.
- Preparing is never externally visible.
- Every V2-visible mutation is serialized by the Session publication lane.
- Durable mutation completes before corresponding V2 visibility.
- No mutex guarding lifecycle, projection, catalog, or `ThreadSession` is held
  across an await.
- No notification I/O occurs under the publication lane or projection lock.
- Closing precedes all terminal cleanup events, and Closed follows them.
- Closed is the last V2 event and precedes a successful close response.
- A Store detach failure never produces a false Closed tombstone.
- Fixed list/history/replay windows finish under continuous Session activity.
- Catalog removal invalidates existing list cursors rather than silently
  changing membership.
- History retention produces a typed gap rather than silent truncation.
- Tombstones and all subscriber queues are bounded by count and bytes.
- Store records are never used as a cross-owner application catalog.

## Acceptance scenarios

1. Existing V1 protocol golden tests and Rust compile fixtures remain unchanged;
   an old client connected to a new daemon receives no V2 notification.
2. Two connections create Sessions on one daemon and each list only its own
   Open/Closing/Closed attachments.
3. A fixed list window excludes a concurrently created Session and returns a
   typed expired cursor if a tombstone is evicted between pages.
4. Snapshot history tail plus backward pages reconstructs retained canonical
   history exactly once while concurrent appends remain above fixed `through`.
5. A history cursor below the floor returns a structured gap; a cursor from an
   older persistent attachment returns stream reset.
6. Two concurrent metadata CAS requests with one expected V2 revision produce
   exactly one change. A Run publication can also win that revision race.
7. Persistent metadata survives close/recovery; caller metadata in attach
   neither overwrites it nor causes configuration mismatch.
8. A V2 live subscriber observes `Closing`, terminal Run events, `Closed`, then
   `None`; a late subscriber replays the same final suffix.
9. Cancelling the first close or metadata RPC waiter does not cancel the owned
   mutation. Later observation reports the one committed outcome.
10. Closed view/list/history remain readable until bounded eviction, while every
    write path returns the typed Closed state.
11. Transport EOF deletes the owner namespace and detaches persistent records;
    authenticated reattach starts fresh V1/V2 streams and cursors.
12. Missing optional capabilities fail locally before any corresponding
    business request is written.
