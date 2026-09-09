# Rust generic Interaction implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development or superpowers:executing-plans.
> Execute one task at a time with RED -> GREEN evidence in the SDD ledger.

**Goal:** Give Rust application hosts one resumable, queryable Interaction
contract for clarification, forms, authentication, file/network permission,
review, typed tool approval, and custom requests.

**Architecture:** Optional `interactions.v1` adds an independent,
session-scoped pending snapshot, cursor, bounded Requested/Removed journal, and
fixed-window subscription. `turn.interactions.get` filters the authoritative
Session view by Run. A daemon-owned response transaction handles idempotency,
conflict, validation, cancellation, and cleanup. Existing typed approval stays
source- and wire-compatible and delegates to that transaction when enabled.

**Design:** `docs/superpowers/specs/2026-09-09-interaction-design.md`

## Global constraints

- This is Application SDK Stage 4. Change only Rust protocol, Core, daemon,
  Store integration, Rust SDK, tests, and documentation.
- Do not implement CLI, TUI, desktop, browser callback, or product policy.
- Leave `AgentDefinition`, `StartThreadParams`, and `RunSnapshot` unchanged,
  including downstream struct-literal construction.
- Do not add fields or variants to frozen `AgentStreamEvent`,
  `RunEventPayload`, `SessionEventPayload`, `RunSnapshot`, or Session V1/V2.
- Preserve `ApprovalRequested`, `RunApprovalParams`, `approval.resolve`,
  `turn.resolve_approval`, `RunSnapshot.pending_approvals`, and existing Rust
  `resolve_approval` signatures and JSON.
- Keep `interactions.v1` out of baseline `PROTOCOL_CAPABILITIES` until the
  complete optional feature ships.
- Never persist or publish a response, HMAC digest, credential, or
  waiter. Request payload/schema are replayable display data.
- Validate owner identity before revealing request existence. Never hold state
  locks over I/O, host code, or suspension.
- Use RED -> GREEN for every task and append exact commands/results to
  `.superpowers/sdd/2026-09-09-interaction/progress.md`.
- Do not commit, reset, clean, push, or discard unrelated shared-tree changes
  unless the root task explicitly changes that instruction.

## Compatibility gate

Before Task 1 and at completion, compile downstream fixtures that construct
`AgentDefinition`, `StartThreadParams`, and `RunSnapshot` with their existing
struct literals. Compare existing Agent, start-thread, Run event, and Session
event golden JSON byte-for-byte.

Opt-in lives in private `Agent` state via
`Agent::with_interactions_enabled()`. New start/create/attach wrapper params
flatten unchanged legacy params. Generic pending data lives only in
`InteractionSnapshot` and `TurnInteractionSnapshot`. Live/replay uses only the
independent `InteractionEventEnvelope` family.

Persistent configuration V2 is frozen by Stage 2.2. Interaction enablement
therefore uses configuration V3, whose only new member is
`runtime_features.interactions_enabled`. V1 migrates through the exact V2
metadata transform before V3; V2 adds the disabled feature; reopening V3 is a
no-op.

---

### Task 1: Freeze the independent protocol contract

**Files:**

- Create: `crates/whale-protocol/src/interactions.rs`
- Create: `crates/whale-protocol/tests/interaction_contract.rs`
- Modify: `crates/whale-protocol/src/lib.rs`

**Interfaces:** capability/method constants; request, pending, response,
cursor, Session snapshot, Run-filtered snapshot, event envelope, fixed-window
subscribe/gap/result, request/respond RPC types, and additive Session
start/create/attach wrappers.

- [ ] Write failing exact-JSON and validation tests for all size boundaries,
  recommended and custom kinds, schema shape, stable error codes, and
  `#[non_exhaustive]` projections.
- [ ] Freeze exact constants and off-by-one cases: replay page default 128 and
  max 256; subscriber output default 64 and max 4,096; each Session journal
  max 1,024 events and 4 MiB serialized bytes. Zero and max-plus-one option
  values fail validation.
- [ ] Add fixed-window tests: first page freezes `through`; later pages retain
  it; results satisfy `after < cursor <= through`; count/byte/oversized
  eviction advances a continuous replay floor; a serialized envelope over
  4 MiB advances the floor through its sequence and is not retained; new
  stream yields StreamReset.
- [ ] Add compile/golden tests proving the three public struct literals and all
  frozen Run/Session event JSON remain unchanged.
- [ ] Run RED:

```bash
cargo test -p whale-protocol --test interaction_contract -- --nocapture
```

Expected: the new protocol module/types do not compile.

- [ ] Implement `interactions.rs` and exports only. Ordinary RPC params deny
  unknown fields; flattened compatibility wrappers use a checked custom
  decoder because serde does not combine flatten with deny-unknown safely.
  Projections are non-exhaustive, constructors validate all limits, and
  external schema retrieval is disabled.
- [ ] Implement additive wrappers such as:

```rust
pub struct StartThreadWithInteractionsParams {
    #[serde(flatten)]
    pub session: StartThreadParams,
    pub interactions_enabled: bool,
}
```

Do not edit the wrapped public structs.

- [ ] Run GREEN:

```bash
cargo test -p whale-protocol --test interaction_contract -- --nocapture
cargo test -p whale-protocol
cargo fmt -p whale-protocol -- --check
```

Record confirmation that `PROTOCOL_CAPABILITIES` and legacy golden JSON did not
change.

---

### Task 2: Add the Core producer bridge and approval adapter

**Files:**

- Create: `crates/whale-core/src/interaction.rs`
- Create: `crates/whale-core/tests/interactions.rs`
- Modify: `crates/whale-core/src/lib.rs`
- Modify: `crates/whale-core/src/error.rs`
- Modify: `crates/whale-core/src/execution.rs`
- Modify: `crates/whale-core/src/approval.rs`
- Modify: `crates/whale-core/src/coordinator.rs`
- Modify: `crates/whale-core/src/engine.rs`

- [ ] Write controlled-bridge RED tests proving publication precedes
  suspension, custom kinds need no Core switch, ticket Drop removes only its
  identity, and Run cancellation wakes every exact-origin waiter.
- [ ] For typed approval, cover approve/reject/modified arguments, count tool
  invocation, and preserve validation both before suspension and after
  argument replacement.
- [ ] Run RED:

```bash
cargo test -p whale-core --test interactions -- --nocapture
```

- [ ] Implement transport-neutral `InteractionBridge`,
  cancellation-safe `InteractionTicket`, and
  `ToolContext::request_interaction` through additive/internal wiring.
- [ ] Keep direct-Core `ApprovalGate` behavior. When a bridge exists, map
  `whale.tool_approval` responses to the unchanged `ApprovalDecision`.
- [ ] Run GREEN:

```bash
cargo test -p whale-core --test interactions -- --nocapture
cargo test -p whale-core
cargo fmt -p whale-core -- --check
```

---

### Task 3: Implement daemon snapshot, journal, and subscription

**Files:**

- Create: `crates/whale-daemon/src/interactions.rs`
- Create: `crates/whale-daemon/tests/interaction_views.rs`
- Create: `crates/whale-store/tests/session_configuration_v3.rs`
- Modify: `crates/whale-daemon/src/lib.rs`
- Modify: `crates/whale-daemon/src/server.rs`
- Modify: `crates/whale-daemon/src/session_lifecycle.rs`
- Modify: `crates/whale-store/src/configuration.rs`

- [ ] Write registry RED tests for atomic get/barrier, fixed `through` during
  concurrent publication, ordered replay/live de-duplication, exact 1,024-event
  and 4-MiB journal eviction, oversized-event non-retention, stream reset,
  multiple watchers, Run-filtered get, and Run-retirement scrub before
  `RunExpired`.
- [ ] Write configuration RED tests proving V1 -> V2 -> V3 and V2 -> V3
  preserve `session`, `run_defaults`, and top-level `metadata`; each migration
  writes once; reopening V3 writes zero times; malformed/unknown versions do
  not rewrite. Assert persistent create writes V3 directly.
- [ ] Prove attach compares normalized `session`, `run_defaults`, and exact
  `runtime_features.interactions_enabled`, while retaining stored metadata.
  A legacy wrapper means false, and a migrated false record rejects an attach
  requesting true rather than silently upgrading it.
- [ ] Run RED:

```bash
cargo test -p whale-store --test session_configuration_v3 -- --nocapture
cargo test -p whale-daemon --test interaction_views -- --nocapture
```

- [ ] Add one `InteractionRegistry` per live Session. Under one short lock,
  assign a checked cursor, mutate pending state, retain/evict the envelope, and
  enqueue notification. Send transport frames outside the lock.
- [ ] Wire `session.interactions.get/subscribe`,
  `turn.interactions.get`, and `session.interaction_event`. A slow watcher must
  not block producers, the reader, or another watcher.
- [ ] Parse additive opt-in wrappers only with negotiated capability. Legacy
  params remain unchanged and default to disabled. Check only
  `interactions.v1`; this independent feed does not depend on
  `session_views.v1`.
- [ ] Extend the frozen configuration V2 parser with configuration V3. Reuse
  the exact V1 -> V2 migration first, then add the runtime-feature object in
  the same checked Store-open repair transaction. Do not persist pending
  Interaction data.
- [ ] Run GREEN:

```bash
cargo test -p whale-store --test session_configuration_v3 -- --nocapture
cargo test -p whale-daemon --test interaction_views -- --nocapture
cargo test -p whale-daemon
cargo test -p whale-store
cargo fmt -p whale-daemon -- --check
```

---

### Task 4: Implement one response transaction and typed facade

**Files:**

- Create: `crates/whale-daemon/tests/interaction_response.rs`
- Modify: `crates/whale-daemon/src/interactions.rs`
- Modify: `crates/whale-daemon/src/server.rs`
- Modify: `crates/whale-core/src/approval.rs`

- [ ] Write RED tests for sequential/concurrent equivalent retries, canonical
  object key order, conflicting retries, invalid-then-corrected response,
  vanished continuation, wrong owner/Run, expired Run, disabled Session,
  duplicate ID, inactive callback, count exhaustion, and deterministic
  response-versus-cancel arbitration.
- [ ] Prove cross-path idempotency among `turn.respond_interaction`,
  `turn.resolve_approval`, legacy `approval.resolve`, and Core resolution.
  Assert response/fingerprint never appears in a snapshot or event.
- [ ] Run RED:

```bash
cargo test -p whale-daemon --test interaction_response -- --nocapture
```

- [ ] Implement `Pending(request, origin, sender, compiled_schema)` and
  `Resolved(hmac_sha256_response_digest)`. Canonicalize JSON, compute
  HMAC-SHA256 with a random 256-bit process-local key using `hmac`/`sha2`,
  compare digests in constant time, and retain no response bytes. Do not use
  `sha256(key || json)`.
- [ ] On valid response: verify owner, validate without consuming, recheck the
  exact live identity, commit Removed and typed state, store the fingerprint,
  release once, then acknowledge.
- [ ] Mirror typed pending approval into the generic registry with identical
  IDs only for opted-in Sessions. Keep the legacy snapshot/event/methods exact
  and functional without opt-in.
- [ ] Run GREEN:

```bash
cargo test -p whale-daemon --test interaction_response -- --nocapture
cargo test -p whale-daemon
cargo test -p whale-core
```

---

### Task 5: Prove lifecycle, retention, and recovery boundaries

**Files:**

- Create: `crates/whale-daemon/tests/interaction_lifecycle.rs`
- Create: `crates/whale-store/tests/interaction_recovery.rs`
- Modify: `crates/whale-daemon/src/interactions.rs`
- Modify: `crates/whale-daemon/src/session_lifecycle.rs`
- Modify only if a defensive non-persistence assertion requires it:
  `crates/whale-store/src/state.rs`

- [ ] Write RED tests that Run cancel/timeout/terminal, callback completion,
  publication failure, Session close, and EOF remove exact entries, wake
  producer/nested-RPC waiters, close routes, and reject late response.
- [ ] Cancel the close caller and prove daemon-owned cleanup still completes.
  Retire one Run while another remains and prove identity-safe journal scrub.
- [ ] Restart Memory and SQLite Sessions while pending. Expect
  `RECOVERY_INTERRUPTED`, a fresh zero cursor, no re-prompt, and no persisted
  request/response/fingerprint/waiter. Preserve existing
  DispatchIntent/`UnknownExecution` behavior.
- [ ] Run RED:

```bash
cargo test -p whale-daemon --test interaction_lifecycle -- --nocapture
cargo test -p whale-store --test interaction_recovery -- --nocapture
```

- [ ] Implement ordered daemon-owned cleanup. Commit possible Removed events
  before terminal projection, wake waiters, close notifier, then delete the
  registry. EOF shares state cleanup without promising frame delivery.
- [ ] Do not add Interaction data to Store records.
- [ ] Run GREEN:

```bash
cargo test -p whale-daemon --test interaction_lifecycle -- --nocapture
cargo test -p whale-store --test interaction_recovery -- --nocapture
cargo test -p whale-store
```

---

### Task 6: Add the Rust SDK opt-in, watch, query, and response API

**Files:**

- Create: `crates/whale-sdk-rust/src/interactions.rs`
- Create: `crates/whale-sdk-rust/src/interaction_tests.rs`
- Modify: `crates/whale-sdk-rust/src/lib.rs` for public exports and the existing
  `ClientState`/notification-route registration
- Modify: `crates/whale-sdk-rust/src/agent.rs`
- Modify: `crates/whale-sdk-rust/src/sessions.rs`
- Modify: `crates/whale-sdk-rust/src/run.rs`
- Modify: `crates/whale-sdk-rust/src/contexts.rs`

Do not create a second client module; `ClientState` and notification routes
remain in `src/lib.rs`.

- [ ] Write counted-RPC RED tests: missing capability/private opt-in sends zero
  business RPCs; opt-in uses additive wrappers; legacy creation JSON remains
  exact; snapshot/barrier loses no event; live/replay merge is ordered and
  de-duplicated; lag replays and gaps resnapshot; watchers are independent;
  Run filtering does not mutate `RunSnapshot`; canceled futures leak no route;
  terminal/close ends streams; typed approval falls back on old daemons.
- [ ] Freeze watch-option defaults and checks: replay page 128/max 256 and
  subscriber output 64/max 4,096. Assert zero and max-plus-one reject locally
  with zero business RPCs and exact max values work.
- [ ] Run RED:

```bash
cargo test -p whale-sdk-rust interaction_tests -- --nocapture
```

- [ ] Keep opt-in only in private `Agent` state. Check
  `interactions.v1` before serializing the additive start/create/attach
  wrapper. Do not require `session_views.v1`.
- [ ] Implement `WhaleThread::interaction_snapshot`,
  `watch_interactions`, `subscribe_interactions_from`,
  `RunHandle::pending_interactions`/`respond_interaction`,
  `WhaleClient::respond_interaction`, and restricted
  `ToolContext::request_interaction`.
- [ ] Implement get -> fixed-window barrier -> live/replay merge. Give every
  watcher an independent bounded queue and recovery state; use weak route
  registration so Drop cannot retain the client or Session.
- [ ] Map stable RPC response codes through existing `SdkError::Rpc` to avoid
  extending an exhaustive public error enum.
- [ ] Run GREEN:

```bash
cargo test -p whale-sdk-rust interaction_tests -- --nocapture
cargo test -p whale-sdk-rust
cargo fmt -p whale-sdk-rust -- --check
```

---

### Task 7: Real-runtime proof and host documentation

**Files:**

- Create: `crates/whale-sdk-rust/tests/interaction_runtime.rs`
- Create: `docs/INTERACTION_API.md`
- Modify: `docs/PROTOCOL_SPEC.md`
- Modify: `docs/API_CONTRACTS.md`
- Modify: `README.md`
- Modify: `.superpowers/sdd/2026-09-09-interaction/progress.md`

- [ ] Write real-runtime tests for embedded/managed stdio and external UDS
  where supported: snapshot/replay/respond; a deliberately lost live frame;
  two isolated Sessions; concurrent equal responses; close/EOF release;
  nested host-tool progress alongside another Session; no response,
  credential, or fingerprint in captured wire/traces/Store.
- [ ] Document capability negotiation, private opt-in, snapshot/watch resync,
  custom kinds, typed approval compatibility, lifecycle/recovery, and safe
  payload rules. Add no UI or CLI workflow.
- [ ] Run final verification:

```bash
cargo test -p whale-protocol
cargo test -p whale-core
cargo test -p whale-daemon
cargo test -p whale-store
cargo test -p whale-sdk-rust
cargo check --workspace
cargo fmt --all -- --check
git diff --check
```

- [ ] Re-run downstream struct-literal and legacy golden fixtures explicitly.
  Confirm no baseline capability, persisted Interaction state, or
  Python/Java/CLI/UI change.
- [ ] Complete the ledger with exact results, changed files, public additions,
  compatibility/security evidence, platform coverage, and residual risks.

## Completion gate

1. Rust hosts can atomically get pending requests, resume a session-scoped
   feed, filter by Run, and respond without polling.
2. Fixed-window replay, replay floors, lag recovery, and stream reset converge.
3. Equivalent retries deliver once; conflict and invalid responses do not
   consume the request.
4. Cancel, timeout, origin completion, close, EOF, retention, and restart
   release waiters and clear state at their specified boundary.
5. Typed approval source signatures, JSON, and behavior remain compatible.
6. `AgentDefinition`, `StartThreadParams`, `RunSnapshot`, and frozen
   Run/Session events remain unchanged.
7. Responses/fingerprints never enter snapshot, replay, logs, or Store, and
   Interaction acceptance never replaces durable dispatch recovery.
8. All implementation and tests are Rust-only, with no CLI or UI behavior.
