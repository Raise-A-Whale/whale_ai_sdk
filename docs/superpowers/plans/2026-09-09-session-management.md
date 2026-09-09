# Rust Session management Stage 2.2 implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add owner-scoped Session listing, canonical-history pagination,
metadata compare-and-set, and replayable Closing/Closed tombstones to the Rust
application SDK without changing Stage 2.1 behavior.

**Architecture:** Keep the existing V1 projection and wire family frozen. Add a
separate V2 management projection with its own cursor, bounded journal/history,
and one asynchronous publication lane per Session. The daemon owns owner-local
catalogs and tombstones; the Store persists metadata through a versioned
configuration schema; the Rust SDK separates cloneable read-only view handles
from the existing write-capable `WhaleThread`.

**Tech Stack:** Rust 2021, Tokio, serde/serde_json, DashMap, whale-protocol,
whale-store, whale-daemon, whale-sdk-rust, in-memory and SQLite Store fixtures.

**Spec:**
`docs/superpowers/specs/2026-09-09-session-management-design.md`

## Global constraints

- Rust protocol, Store, daemon, and Rust SDK only. Do not edit Python or Java.
- Do not create a CLI, TUI, desktop, GUI, renderer, or product shell.
- Preserve all V1 public Rust signatures and exact V1 JSON shapes.
- Keep every Stage 2.2 capability optional and out of
  `PROTOCOL_CAPABILITIES`.
- Never expose or reuse Store revision, Store owner, epoch, or recovery secret
  as a view/list/history cursor.
- No lifecycle, projection, catalog, or `ThreadSession` mutex may be held across
  an await. A Run releases its Core/Store guards before waiting for the
  publication lane; the Store actor never waits for that lane.
- Every production change follows a focused deterministic RED test, minimal
  GREEN implementation, and focused regression before the next task.
- Record exact RED/GREEN commands and counts in
  `.superpowers/sdd/2026-09-09-session-management/progress.md`.
- Do not commit, reset, clean, or push unless the user separately requests it.

---

## File structure and dependency graph

| File | Responsibility |
| --- | --- |
| `crates/whale-protocol/src/session_management.rs` | V2 wire types, distinct cursors, validation, reducer, errors, method/capability constants |
| `crates/whale-store/src/configuration.rs` | Parse, validate, and migrate persisted configuration V1/V2; isolate mutable metadata |
| `crates/whale-daemon/src/session_management.rs` | V2 record, publication lane, lifecycle journal, metadata transaction, tombstone state |
| `crates/whale-daemon/src/session_catalog.rs` | Owner-local fixed-window list and opaque cursor validation |
| `crates/whale-daemon/src/session_history.rs` | Bounded canonical archive and fixed-window backward pager |
| `crates/whale-sdk-rust/src/session_management.rs` | Read-only handle, V2 hubs/workers, list/history/CAS methods and typed errors |

```text
Task 1 protocol
   |
Task 2 Store schema/migration
   |
Task 3 publication lane + lifecycle + metadata + tombstone
   |
Task 4 catalog + history
   |
Task 5 Rust SDK + full compatibility verification
```

Each task ends at a meaningful reviewer gate. Do not start a dependent task
until its producer interfaces and focused tests are GREEN.

### Task 1: Freeze the additive V2 protocol contract

**Files:**

- Create: `crates/whale-protocol/src/session_management.rs`
- Modify: `crates/whale-protocol/src/lib.rs`
- Create: `crates/whale-protocol/tests/session_management_contract.rs`

**Interfaces:**

- Consumes: V1 `SessionHistoryWindow`, `SessionRunView`, `SessionRunSummary`,
  `RunEvent`, `RunStatus`, and `CanonicalItem` without changing them.
- Produces: the capability/method constants and every V2/list/history/CAS/error
  type fixed by the design. Tasks 3–5 must import these exact types.

- [ ] **Step 1: Add a V1 compatibility RED guard before introducing V2**

  Extend the new contract test with exact JSON fixtures copied from the existing
  Stage 2.1 golden values and compile-time calls to all existing public methods:

  ```rust
  #[test]
  fn v1_session_wire_remains_frozen() {
      let encoded = serde_json::to_value(v1_run_event_envelope()).unwrap();
      assert_eq!(encoded, v1_run_event_envelope_json());
      assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_VIEWS));
      assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_EVENT_REPLAY));
  }
  ```

  Run:

  ```bash
  cargo test -p whale-protocol --test session_management_contract \
    v1_session_wire_remains_frozen -- --nocapture
  ```

  Expected: PASS against the current implementation. Record this evidence-only
  compatibility baseline before writing V2 code.

- [ ] **Step 2: Write the failing V2 type and serde tests**

  Tests must reference the exact new names:

  ```rust
  use whale_protocol::session_management::{
      SessionCursorV2, SessionEventEnvelopeV2, SessionEventPayloadV2,
      SessionLifecycleState, SessionPersistenceV2, SessionSnapshotV2,
      SessionSummaryV2,
  };
  ```

  Cover exact JSON for Open snapshot, Run event, metadata replacement, Closing,
  Closed, ephemeral/persistent descriptors, and an empty Closed replay page.
  Assert V1 `SessionCursor` cannot be passed to V2 constructors at compile-time
  through the public API compile fixture.

  Run:

  ```bash
  cargo test -p whale-protocol --test session_management_contract \
    v2_snapshot_and_events_have_frozen_json -- --nocapture
  ```

  Expected: FAIL because `session_management` and the V2 types do not exist.

- [ ] **Step 3: Write failing reducer, fixed-window, and typed-error tests**

  Add deterministic cases for:

  - contiguous `SessionSnapshotV2::apply` convergence;
  - metadata whole-map replacement;
  - valid `Open -> Closing -> Closed` and rejection of every other lifecycle
    transition or event after Closed;
  - `view_revision == cursor.seq` after every event;
  - V2 replay fixed `through`, retention gap, stream reset, and empty Closed
    page termination;
  - `SessionListCursor` and `SessionHistoryPageCursor` remaining opaque and
    non-interchangeable;
  - history `before`/page-cursor mutual exclusion;
  - checked list/history/replay limits; and
  - exact `SessionManagementErrorData` JSON for all design error kinds.

  Run:

  ```bash
  cargo test -p whale-protocol --test session_management_contract -- --nocapture
  ```

  Expected: FAIL on missing constructors, validation, reducer, and error types.

- [ ] **Step 4: Implement the minimal protocol module**

  Add the constants verbatim from the spec and implement:

  ```rust
  impl SessionCursorV2 {
      pub fn validate(&self) -> Result<(), String>;
      pub fn checked_next(&self) -> Result<Self, String>;
  }

  impl SessionSnapshotV2 {
      pub fn validate(&self) -> Result<(), String>;
      pub fn apply(
          &mut self,
          envelope: &SessionEventEnvelopeV2,
      ) -> Result<bool, SessionProjectionErrorV2>;
  }
  ```

  Add checked constructors/validators for list, history, replay, metadata, and
  error data. Mark new extensible public enums and projection structs
  `#[non_exhaustive]`. Do not change `session_views.rs` except for imports proven
  necessary by the new module.

- [ ] **Step 5: Run focused GREEN and V1 regressions**

  ```bash
  cargo test -p whale-protocol --test session_management_contract -- --nocapture
  cargo test -p whale-protocol --test session_view_contract -- --nocapture
  cargo test -p whale-protocol
  cargo fmt --all -- --check
  ```

  Expected: all pass; existing V1 golden counts and JSON remain unchanged. Add
  exact counts to the progress ledger.

### Task 2: Migrate persisted metadata without changing recovery APIs

**Files:**

- Create: `crates/whale-store/src/configuration.rs`
- Modify: `crates/whale-store/src/lib.rs`
- Modify: `crates/whale-store/src/state.rs`
- Modify: `crates/whale-store/src/runtime.rs`
- Modify: `crates/whale-daemon/src/recovery.rs`
- Create: `crates/whale-store/tests/session_metadata.rs`
- Modify: `crates/whale-daemon/tests/recovery.rs`

**Interfaces:**

- Consumes: Task 1 metadata protocol values; current `StoreRuntime::{create,
  attach, open}` and `SessionJournal` public signatures.
- Produces:

  ```rust
  pub struct PersistedSessionConfigurationV2 { /* checked fields */ }

  impl PersistedSessionConfigurationV2 {
      pub fn parse_and_migrate(value: Value)
          -> Result<(Self, bool), StoreError>;
      pub fn attachment_identity(&self) -> (&Map<String, Value>, &Value);
      pub fn metadata(&self) -> &Map<String, Value>;
      pub fn into_value(self) -> Value;
  }

  impl SessionJournal {
      pub async fn replace_metadata(
          &self,
          metadata: Map<String, Value>,
      ) -> Result<Map<String, Value>, StoreError>;
  }
  ```

  `StoreRuntime::attach` retains its current public signature. Task 3 relies on
  `replace_metadata` being serialized by the existing journal actor.

- [ ] **Step 1: Write failing in-memory configuration migration tests**

  Build exact V1 records containing metadata, history, Runs, unknown
  executions, owner/epoch/revision, then assert:

  - migration moves `/session/metadata` to `/metadata` and sets configuration
    version 2;
  - no other JSON, record field, or canonical item changes;
  - Store revision advances exactly once;
  - opening the migrated Store again is idempotent;
  - absent V1 metadata becomes `{}`; and
  - unknown version or non-object metadata fails without rewriting the record.

  Run:

  ```bash
  cargo test -p whale-store --test session_metadata \
    opens_and_migrates_v1_configuration_exactly_once -- --nocapture
  ```

  Expected: FAIL because configuration V2 and migration do not exist.

- [ ] **Step 2: Write failing attach and journal metadata tests**

  Cover:

  - persistent create stores V2 directly;
  - attach compares normalized `session` plus `run_defaults`, ignores caller
    metadata, and retains stored metadata;
  - a real configuration mismatch still fails;
  - `SessionJournal::replace_metadata` changes only `/metadata` and advances
    internal Store revision;
  - replacement is allowed while a Run is active;
  - backend CAS/I/O failure poisons the journal and never reports false success;
    and
  - recovery inspect continues to expose its Store revision only through the
    existing recovery API.

  Run:

  ```bash
  cargo test -p whale-store --test session_metadata -- --nocapture
  ```

  Expected: FAIL on full-configuration equality and the missing journal command.

- [ ] **Step 3: Implement typed configuration parsing and migration**

  Preserve `/session/limits` and the current normalized tool schema. V2 is:

  ```json
  {"version":2,"session":{},"run_defaults":{},"metadata":{}}
  ```

  Modify `StoreRuntime::open` to perform migration inside the same checked CAS
  transaction as other startup record repair. Never truncate legacy metadata.
  `SessionRecord.schema_version` remains unchanged.

- [ ] **Step 4: Add the dedicated journal action**

  Add an `Action::ReplaceMetadata`/matching response. It must load the latest
  leased record, parse configuration V2, replace only the metadata object,
  advance Store revision through the actor's normal CAS, and return the stored
  map. Do not implement metadata CAS using Store revision.

  Update daemon recovery configuration creation to write V2. During attach,
  initialize the live Session metadata from `journal.record()` rather than the
  caller's `StartThreadParams.metadata`.

- [ ] **Step 5: Add SQLite parity and run GREEN regressions**

  Run the same migration/attach tests against `SQLiteStore`, then:

  ```bash
  cargo test -p whale-store --test session_metadata -- --nocapture
  cargo test -p whale-store
  cargo test -p whale-daemon --test recovery -- --nocapture
  cargo fmt --all -- --check
  ```

  Expected: all pass, including existing recovery revision/epoch and retention
  migration cases.

### Task 3: Add the publication lane, V2 lifecycle, metadata CAS, and tombstones

**Files:**

- Create: `crates/whale-daemon/src/session_management.rs`
- Modify: `crates/whale-daemon/src/server.rs`
- Modify: `crates/whale-daemon/src/session_views.rs`
- Modify: `crates/whale-daemon/src/session_lifecycle.rs`
- Modify: `crates/whale-daemon/src/recovery.rs`
- Modify: `crates/whale-daemon/src/retention.rs`
- Create: `crates/whale-daemon/tests/session_management.rs`
- Modify: `crates/whale-daemon/tests/session_close.rs`
- Modify: `crates/whale-daemon/tests/session_views.rs`

**Interfaces:**

- Consumes: all Task 1 V2 protocol types and Task 2
  `SessionJournal::replace_metadata`.
- Produces:

  ```rust
  struct SessionPublicationLane(tokio::sync::Mutex<()>);

  struct SessionManagementRegistry { /* owner-scoped V2 records */ }

  impl SessionManagementRegistry {
      async fn prepare_run_publication(...)
          -> Result<PreparedSessionPublication, SessionManagementFailure>;
      async fn replace_metadata(...)
          -> Result<ReplaceSessionMetadataResult, SessionManagementFailure>;
      fn get_v2(...) -> Result<SessionSnapshotV2, SessionManagementFailure>;
      fn subscribe_v2(...)
          -> Result<SubscribeSessionV2Result, SessionManagementFailure>;
      fn retain_closed(...) -> Result<(), SessionManagementFailure>;
      fn remove_owner(&self, owner: &str);
  }
  ```

  Task 4 adds list/history access to these records. Task 5 consumes only the V2
  wire methods, not daemon internals.

- [ ] **Step 1: Write the deterministic publication-lane RED tests**

  Use barriers rather than sleeps. Pause metadata after checking expected V2
  revision and before its Store reply; concurrently release a Run event whose
  durable prerequisite has completed. Assert only one lane occupant can commit,
  the final V2 cursor follows lane acquisition order, the Store actor can still
  reply, and no lifecycle/projection/Core mutex is held by the paused await.

  Add the opposite order and a cancelled metadata RPC waiter. The owned daemon
  transaction must complete once, and a retry with the stale expected revision
  must return the committed current revision/cursor.

  Inject a preparation failure independently for V1 and V2. Neither projection
  may advance. On success, pause after preparation and prove readers observe
  both old records or both new records, never a one-sided commit; notification
  counters remain zero until both swaps complete.

  Run:

  ```bash
  cargo test -p whale-daemon --test session_management \
    metadata_and_run_publication_share_one_lane -- --nocapture
  ```

  Expected: FAIL because the V2 registry/lane/methods do not exist.

- [ ] **Step 2: Write metadata CAS and durable failure RED tests**

  Cover ephemeral and real in-memory persistent Sessions:

  - two concurrent calls with the same expected V2 revision yield one change
    and one typed conflict;
  - equal replacement at the correct revision returns `changed=false` without
    an event;
  - Closing/Closed reject writes with typed state;
  - over-limit metadata is rejected before Store mutation;
  - Store failure commits no V2 cursor/event; and
  - recovery after a successful CAS loads stored metadata despite stale attach
    metadata.

  Run the focused test and record the exact missing behavior.

- [ ] **Step 3: Write lifecycle/tombstone ordering RED tests**

  A deterministic blocked Run must produce:

  ```text
  LifecycleChanged(Closing)
  Run terminal publication(s)
  LifecycleChanged(Closed)
  stream end
  ```

  Assert the close response is not delivered before Closed commit; cancelling
  the first close waiter does not cancel or duplicate the transaction; a late
  V2 subscription replays the final suffix; subscribing at the Closed cursor
  returns an empty terminal page; V2 get remains readable; every mutation is
  rejected; V1 streams retain their current close-to-`None` behavior.

  Add persistent detach success/failure cases. A failed/unknown detach must not
  publish Closed and must fail the owner connection.

- [ ] **Step 4: Implement the V2 registry and universal publication order**

  Install the management record atomically with Session publication. Give it a
  fresh V2 stream ID and lane. Mirror every Run-visible mutation into V1 and V2
  after its existing durable prerequisite by preparing both reducer results and
  serialized sizes before either projection swap. For metadata, prepare its V2
  candidate before the dedicated Store command, keep the lane across that
  command, then perform the already validated swap.

  Represent a Run update as `PreparedDualPublication`. Clone, reduce, serialize,
  and account for both candidates first. Acquire the V1 then V2 short mutexes in
  fixed order, verify source stream/sequence identities, and swap both records
  with no fallible operation between assignments. Queue both notifications only
  after releasing both locks. Run-retention replay scrub uses the same lane;
  disconnect marks the owner closed and waits for accepted lane work before
  removal.

  Route `session.get.v2`, `session.subscribe.v2`,
  `session.metadata.replace`, and `session.event.v2`. The notifier is ordered
  and bounded; publication commits precede notification I/O. Never send a V2
  notification on the V1 route.

  Rework close into two lane turns around Run join. Keep the management record
  after Closed; remove write-side Session, Run, callbacks, and V1 view. On EOF,
  join cleanup and delete the entire owner namespace.

- [ ] **Step 5: Run focused GREEN and compatibility regressions**

  ```bash
  cargo test -p whale-daemon --test session_management -- --nocapture
  cargo test -p whale-daemon --test session_close -- --nocapture
  cargo test -p whale-daemon --test session_views -- --nocapture
  cargo test -p whale-store --test session_metadata -- --nocapture
  cargo test -p whale-daemon
  cargo fmt --all -- --check
  ```

  Expected: all pass. Review lock order explicitly from each Store call site and
  record that no await occurs under projection/lifecycle/ThreadSession locks.

### Task 4: Add fixed-window owner catalog and canonical-history pager

**Files:**

- Create: `crates/whale-daemon/src/session_catalog.rs`
- Create: `crates/whale-daemon/src/session_history.rs`
- Modify: `Cargo.toml`
- Modify: `crates/whale-daemon/Cargo.toml`
- Modify: `crates/whale-daemon/src/session_management.rs`
- Modify: `crates/whale-daemon/src/server.rs`
- Modify: `crates/whale-daemon/src/retention.rs`
- Create: `crates/whale-daemon/tests/session_catalog.rs`
- Create: `crates/whale-daemon/tests/session_history.rs`

**Interfaces:**

- Consumes: Task 3 owner-scoped management records, tombstone transitions, and
  V2 cursors.
- Produces: `session.list` and `session.history` handlers, authenticated opaque
  cursor codecs, bounded `HistoryArchive`, tombstone pruning, and deterministic
  limit test seams. Task 5 relies on their exact Task 1 wire types/errors.

- [ ] **Step 1: Write owner-isolation and list-window RED tests**

  Use two real daemon connections. Assert:

  - each sees only its own ordinary and attached persistent Sessions;
  - Preparing is absent; Open, Closing, and Closed are present;
  - an unattached recovery record is absent;
  - page 1 freezes membership and a later Session is excluded from all remaining
    pages;
  - metadata/lifecycle may update an existing entry but its ordinal is stable;
  - a cursor is rejected across owners or after its five-minute TTL; and
  - tombstone removal increments catalog generation and makes page 2 return
    `ListCursorExpired` instead of silently skipping an entry.

  Run:

  ```bash
  cargo test -p whale-daemon --test session_catalog \
    list_window_is_owner_scoped_and_membership_is_fixed -- --nocapture
  ```

  Expected: FAIL because `session.list` is not routed.

- [ ] **Step 2: Write backward-history and retention RED tests**

  Build canonical items with distinct absolute indexes and byte sizes. Assert:

  - first page freezes through; concurrent append changes only `current_end`;
  - pages are selected backward and each page is returned oldest-to-newest;
  - a first `before` anchor at snapshot `start_index` avoids duplicating the
    snapshot tail;
  - page cursor cannot be reused for another owner/thread/stream;
  - persistent reattach returns `HistoryStreamReset` for the old cursor;
  - count and byte eviction advance floor by whole items and return
    `HistoryGap`;
  - one item over 1 MiB returns `ResourceLimit` with its index; and
  - Closed tombstone history remains readable until eviction.

  Run:

  ```bash
  cargo test -p whale-daemon --test session_history -- --nocapture
  ```

  Expected: FAIL because no management history archive/pager exists.

- [ ] **Step 3: Implement opaque cursor codecs and catalog paging**

  Add workspace-managed `hmac 0.12`, `sha2 0.10`, `getrandom 0.3`, and
  `base64 0.22` daemon dependencies. Generate one random 32-byte key per
  `DaemonServer` and expose a deterministic fixed key only under `cfg(test)`.
  Encode canonical versioned payload bytes with URL-safe unpadded base64. Sign
  with HMAC-SHA256 over a token-kind domain prefix, the raw connection owner,
  and the payload. Verify through `Mac::verify_slice` after enforcing a 4 KiB
  encoded-token limit.

  List tokens bind owner, owner generation, `after_ordinal`, fixed
  `through_ordinal`, and expiry. Never serialize the raw owner ID into results.
  Test a one-bit payload/tag change, wrong owner, wrong token kind, new daemon
  key, malformed base64, and paused-time expiry before catalog lookup.

  Allocate ordinals only when a prepared Session is successfully published.
  Keep membership through Closing/Closed. Remove only Closed records under
  tombstone pressure. Return compact Run headlines rather than active drafts.

- [ ] **Step 4: Implement the bounded history archive and pager**

  Mirror canonical commits into an archive owned by the V2 record. Initialize a
  persistent attachment from the newest suffix that fits the 32 MiB view cap.
  Track absolute floor/end indexes and serialized bytes with checked arithmetic.
  Tokens bind owner, thread, V2 stream, through, before, and expiry.

  Enforce page count plus 1 MiB serialized-response limit before forming the
  response. Do not acquire `ThreadSession` or query global Store records to serve
  a page.

- [ ] **Step 5: Implement tombstone pressure and run GREEN regressions**

  Enforce ten-minute TTL, 128 records, and 64 MiB aggregate bytes per owner;
  evict oldest Closed first. Check TTL lazily on every management request and
  from a bounded periodic sweep. Never evict Open/Closing. Every removal bumps
  owner generation and stops/removes the tombstone notifier and retained data.

  ```bash
  cargo test -p whale-daemon --test session_catalog -- --nocapture
  cargo test -p whale-daemon --test session_history -- --nocapture
  cargo test -p whale-daemon --test session_management -- --nocapture
  cargo test -p whale-daemon
  cargo fmt --all -- --check
  ```

  Expected: all pass with deterministic paused-time expiry/pressure tests.

### Task 5: Expose read-only V2 handles in the Rust SDK and verify the stage

**Files:**

- Create: `crates/whale-sdk-rust/src/session_management.rs`
- Modify: `crates/whale-sdk-rust/src/lib.rs`
- Modify: `crates/whale-sdk-rust/src/connection.rs`
- Modify: `crates/whale-sdk-rust/src/sessions.rs`
- Modify: `crates/whale-sdk-rust/src/session_views.rs`
- Create: `crates/whale-sdk-rust/tests/session_management.rs`
- Modify: `crates/whale-sdk-rust/tests/support/mod.rs`
- Modify: `docs/PROTOCOL_SPEC.md`
- Modify: `docs/RUST_APPLICATION_SDK.md`
- Modify: `docs/SDK_ARCHITECTURE_REVIEW.md`
- Modify: `docs/SESSION_VIEW_API.md`
- Update: `.superpowers/sdd/2026-09-09-session-management/progress.md`

**Interfaces:**

- Consumes: all four optional capabilities, Task 1 wire types/errors, and Task
  3/4 daemon endpoints.
- Produces: `SessionViewHandle`, `SessionManagementError`, checked list/history
  and watch options, `SessionWatchV2`, `SessionEventStreamV2`,
  `WhaleClient::{list_sessions,session_view}`, `WhaleThread::{session_view,
  replace_metadata}`. Existing V1 exports and methods remain untouched.

- [ ] **Step 1: Write public API and capability-guard RED tests**

  Compile and exercise this shape:

  ```rust
  let view: SessionViewHandle = thread.session_view();
  let page = runtime.client().list_sessions(SessionListOptions::default()).await?;
  let snapshot = view.snapshot().await?;
  let update = thread
      .replace_metadata(snapshot.summary.view_revision, replacement)
      .await?;
  ```

  For each missing optional capability, use a fake peer and assert the method
  returns `UnsupportedCapability` before its business request counter changes.
  Assert private option defaults: list 64, history 128, V2 output 64, replay
  128; invalid zero/over-limit values fail in constructors.

  Run:

  ```bash
  cargo test -p whale-sdk-rust --test session_management \
    public_options_and_capability_guards -- --nocapture
  ```

  Expected: FAIL because the module/public API does not exist.

- [ ] **Step 2: Write V2 stream and read/write separation RED tests**

  Use the deterministic fake peer to cover:

  - V2 hub installed before get/subscribe RPC;
  - snapshot/live race plus unconditional fixed replay barrier;
  - fixed-through multi-page replay, duplicate removal, broadcast lag repair,
    two independent subscribers, and public `last_received`;
  - cancelled `recv()` does not cancel the worker; Drop does;
  - unexpected disconnect yields exactly one error then `None`;
  - Closed delivered once then `None`, including an empty replay page starting
    at the Closed cursor;
  - V1 hub still ends immediately on existing close behavior; and
  - `SessionViewHandle` can read a Closed tombstone but has no write/close/Run
    methods, while `WhaleThread` writes reject after Closing.

- [ ] **Step 3: Write real-daemon list/history/CAS/lifecycle RED tests**

  Use two real in-process clients and Store-backed recovery. Verify owner
  isolation, stable list pages, snapshot-tail/history-page reconstruction,
  concurrent append, concurrent CAS, Run-vs-CAS ordering, live/late close
  replay, close response ordering, persistent metadata recovery, fresh stream
  reset, and EOF namespace cleanup.

  Use barriers/watch revisions rather than sleeps. For every failed/aborted
  operation wait for the concrete connection/session cleanup condition.

- [ ] **Step 4: Implement SDK routes, handles, workers, and typed mapping**

  Add a V2 hub map beside the existing V1 map. Route `session.event.v2` from the
  connection reader without awaiting user work. Reuse the Stage 2.1 catch-up
  state machine internally, parameterized by V2 envelope/replay types, while
  retaining the V1 public implementation and behavior.

  Split local checks into:

  ```rust
  ensure_session_writable(thread_id) // existing business mutation fence
  ensure_session_viewable(thread_id) // connection alive; Closing/Closed allowed
  ```

  Write-side release removes callbacks/Run/tool state and closes V1 hubs, but
  keeps the V2 route until Closed is delivered or the tombstone expires.
  `SessionViewHandle` stores only a client clone and thread ID; it never owns
  daemon cleanup.

  Current `ClientState::request_inner` discards `JSONRPCError.data` while
  constructing public `SdkError::Rpc`. Add this internal-only path:

  ```rust
  pub(crate) enum InternalRequestError {
      Sdk(SdkError),
      Remote(JSONRPCError),
  }

  async fn request_with_remote_error<P, R>(
      &self,
      method: &str,
      params: Option<P>,
      access: SessionRequestAccess,
  ) -> Result<R, InternalRequestError>;
  ```

  Keep public `SdkError` and every existing request signature unchanged. Make
  the old path delegate while preserving its current
  RunExpired/LimitExceeded/Rpc mapping; only Stage 2.2 uses the raw path. Map
  code plus `SessionManagementErrorData.kind` into `SessionManagementError`.
  Missing or malformed typed data becomes `InvalidProjection`; never inspect
  diagnostic strings. A fake-peer regression must prove legacy RPC retains its
  current public error shape while V2 retains and maps `data`.

- [ ] **Step 5: Run focused GREEN, full Rust verification, and documentation**

  ```bash
  cargo test -p whale-sdk-rust --test session_management -- --nocapture
  cargo test -p whale-sdk-rust --test session_views -- --nocapture
  cargo test -p whale-sdk-rust --test run_handle -- --nocapture
  cargo test -p whale-sdk-rust --lib --tests
  cargo test -p whale-protocol
  cargo test -p whale-store
  cargo test -p whale-daemon
  cargo test --workspace --quiet
  cargo test --doc -p whale-sdk-rust
  cargo check --workspace
  cargo fmt --all -- --check
  git diff --check -- \
    crates/whale-protocol crates/whale-store crates/whale-daemon \
    crates/whale-sdk-rust docs .superpowers/sdd/2026-09-09-session-management
  ```

  Expected: all pass. Document V1/V2 selection, read/write capability split,
  owner namespace, persistent fresh-stream behavior, cursor distinctions,
  close/tombstone lifetime, gaps, limits, and recovery. Do not add product-shell
  examples or claim Windows support beyond the crate's existing platform
  constraints.

## Final reviewer checklist

- [ ] V1 contract tests prove exact old JSON and behavior, not only successful
  deserialization.
- [ ] V2-only mutations never advance the V1 cursor.
- [ ] Run publication releases Core/Store guards, then prepares both V1/V2
  results and commits them together under the publication lane.
- [ ] Metadata keeps the lane across its Store command; the Store actor itself
  never takes the lane, so the lock graph has no cycle.
- [ ] `PreparedDualPublication` makes every fallible V1/V2 operation precede a
  fixed-order, infallible two-record swap; notifications follow both swaps.
- [ ] Metadata attach comparison excludes only metadata, and recovered stored
  metadata is authoritative.
- [ ] Store revision appears only in Store/recovery APIs.
- [ ] List/history cursor tokens are owner-bound, authenticated, expiring, and
  distinct Rust types; HMAC-SHA256 uses a per-daemon random key, domain
  separation, and constant-time tag verification.
- [ ] Existing SDK request methods and public `SdkError` remain unchanged; only
  an internal raw-remote-error path preserves JSON-RPC `data` for typed mapping.
- [ ] List membership and history through boundaries stay fixed under
  concurrent writes.
- [ ] Closing/Closed ordering, single-flight cancellation, and detach failure
  have deterministic tests.
- [ ] V2 Closed streams end after the final envelope; V1 streams retain existing
  close behavior.
- [ ] Tombstone count, bytes, TTL, catalog invalidation, and worker cleanup are
  all directly tested.
- [ ] Missing capabilities fail locally before a business RPC.
- [ ] No CLI, TUI, desktop, GUI, Python, or Java implementation entered the
  diff.
