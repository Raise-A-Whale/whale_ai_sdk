# Rust Session view and replay Stage 2.1 implementation plan

> Design: `docs/superpowers/specs/2026-09-09-session-view-replay-design.md`

**Goal:** Give Rust application hosts an authoritative, replayable,
multi-observer Session state API without implementing a CLI, TUI, desktop, or
GUI shell.

**Architecture:** `whale-daemon` maintains a Session projection and bounded
event journal independent of the execution lock. `whale-protocol` defines
cursor, snapshot, replay, gap, and notification contracts. `whale-sdk-rust`
routes one connection feed into isolated local subscriptions and merges replay
with concurrent live events.

**Constraints:** Preserve all existing Run and initialization wire shapes; keep
the new capabilities optional; Rust only; use RED tests before production code;
do not change existing Run retention behavior.

## Task 1: Freeze the protocol contract

**Files**

- Create: `crates/whale-protocol/src/session_views.rs`
- Modify: `crates/whale-protocol/src/lib.rs`
- Create: `crates/whale-protocol/tests/session_view_contract.rs`
- Modify: `docs/PROTOCOL_SPEC.md`

**RED**

- Add JSON golden tests for cursor, bounded-history snapshot, event envelope, replay page, and
  structured replay gaps.
- Assert `RunEvent` JSON remains byte-for-byte equal to its existing golden
  representation.
- Assert `session_views.v1` and `session_event_replay.v1` are absent from
  `PROTOCOL_CAPABILITIES`.
- Add validation cases for blank identity, zero/oversized page limits, cursor
  thread mismatch, future cursor, retention gap, and stream reset.

**GREEN**

- Add optional capability and method constants.
- Add the public types fixed in the design. Mark new extensible enums
  `#[non_exhaustive]`.
- Add pure parameter validation, a deterministic `SessionSnapshot::apply`
  reducer, and stable serde representations.
- Export the new module without changing current `runs` or baseline
  initialization types.

**Verify**

```bash
cargo test -p whale-protocol --test session_view_contract -- --nocapture
cargo test -p whale-protocol
cargo fmt --all -- --check
```

## Task 2: Add the daemon Session projection and journal

**Files**

- Create: `crates/whale-daemon/src/session_views.rs`
- Modify: `crates/whale-daemon/src/server.rs`
- Modify: `crates/whale-daemon/src/initialization.rs`
- Modify: `crates/whale-daemon/src/recovery.rs`
- Modify: `crates/whale-daemon/src/session_lifecycle.rs`
- Modify: `crates/whale-daemon/src/retention.rs`
- Create: `crates/whale-daemon/tests/session_views.rs`

**RED: projection unit tests**

- A fresh record has cursor zero and retains safe metadata/timestamps.
- Run events update the active snapshot and all partial draft variants.
- Completing an item removes its draft and appends its canonical item once.
- Terminal reconciliation clears the active Run and keeps a lightweight last
  summary plus canonical history.
- Two Runs share one strictly increasing Session sequence.
- A journal records events without subscribers.
- Count and serialized-byte limits advance the contiguous replay floor exactly;
  a single oversized event is live-only and makes older cursors unreplayable.
- Wrong stream returns `stream_reset`; a future cursor is invalid.
- A fixed `through` window finishes under continuous event publication.
- Applying a replay suffix with the protocol reducer equals the authoritative
  projection at the same cursor field-for-field.
- A bounded history snapshot returns the newest requested items with exact
  `start_index`, `total_items`, and `capacity`; applying completion/terminal
  events preserves those fields while evicting the oldest window entries.
- Retiring a Run drops its replay payload plus every earlier envelope needed to
  preserve a contiguous suffix.
- A phase-gated concurrent test proves replay is evicted before the same Run
  identity becomes observable as `RunExpired`.

**GREEN: registry**

- Implement `SessionViewRegistry` with a short mutex and configurable private
  `EventJournalLimits` test seam.
- Implement atomic create/get/begin-run/apply-run-event/run-changed/terminal,
  replay-page, and remove operations.
- Serialize outside no lock except the bounded size calculation needed during
  commit; perform no I/O under the lock.
- Preserve only safe display state.

**RED: daemon integration tests**

- `session.get` during a controlled blocked Run returns within a bounded test
  deadline and contains the current partial item.
- `session.subscribe` replays events produced without a subscriber.
- Replay across two Runs is Session ordered while Run sequences restart.
- A send failure still leaves the committed event replayable.
- foreign-owner get/subscribe behaves as not found.
- cancel and approval resolution advance the Session cursor.
- persistent attach creates a new stream from recovered history and exposes no
  recovery secret.
- close and EOF remove the view.

**GREEN: server integration**

- Add the registry to `DaemonServer` and advertise both optional capabilities
  dynamically from daemon initialization.
- Preserve normal create metadata and timestamps; initialize recovered views
  from safe recovered state.
- Route `session.get` and `session.subscribe` with owner validation.
- Commit run acceptance, stream projection changes, cancel/approval changes,
  and terminal reconciliation at the publication points in the design.
- Send `session.event` in addition to unchanged legacy notifications. A failed
  Session notification must not change Run terminal-delivery/retention logic.
- Remove views through existing close/disconnect cleanup.
- Implement two-phase conditional Run retirement: select exact `Arc` candidates,
  evict their Session replay payload outside the Run registry lock, then expose
  `RunExpired` only while removing the same identity.

**Verify**

```bash
cargo test -p whale-daemon --test session_views -- --nocapture
cargo test -p whale-daemon
cargo test -p whale-protocol
cargo fmt --all -- --check
```

## Task 3: Add Rust SDK watch and multi-subscriber fan-out

**Files**

- Create: `crates/whale-sdk-rust/src/session_views.rs`
- Modify: `crates/whale-sdk-rust/src/lib.rs`
- Modify: `crates/whale-sdk-rust/src/sessions.rs`
- Modify: `crates/whale-sdk-rust/src/run.rs`
- Create: `crates/whale-sdk-rust/tests/session_views.rs`

**RED: public behavior**

- `WhaleThread::snapshot()` returns the daemon projection.
- `WhaleThread::watch()` atomically yields a snapshot and events strictly after
  its cursor, including an event received before the RPC response.
- `subscribe_from()` replays offline events and merges concurrent live frames
  once and in order.
- Two subscriptions observe one Run independently.
- An unpolled or lagged subscriber does not affect a fast subscriber or Run
  result.
- Local lag follows one `Live -> CatchingUp(through) -> Live` worker state
  machine and resumes from the last event queued for that stream.
- Output backlog followed by hub lag replays each event exactly once; a filtered
  Run stream scans across another Run and resumes without a false gap.
- A daemon replay gap produces typed `ResyncRequired`; a new watch converges.
- `RunHandle::subscribe_events()` filters the Session feed by turn ID.
- close/disconnect terminates every stream and releases routes.
- an old daemon fails new methods locally for a missing optional capability,
  without sending the business RPC.
- legacy `RunHandle::events()` remains single-consumer with its current errors.

**GREEN: SDK internals**

- Add `SessionViewError`, marked `#[non_exhaustive]`, and re-export protocol view
  types.
- Store negotiated capability membership and add a local optional-capability
  guard.
- Create a per-Session hub before Session creation can deliver notifications.
- Route `session.event` without awaiting user work.
- Give each subscription an independent bounded queue, last-queued cursor,
  replay state, and cleanup guard.
- Implement watch/subscribe merge by cursor with deterministic de-duplication,
  a fixed replay high watermark, local-lag replay, typed resync, and
  cancellation-safe route cleanup.
- Yield `RunEventEnvelope` from filtered Run subscriptions so callers retain the
  exact Session cursor while the internal scanner crosses unrelated events.
- Use private option fields with checked upper bounds and documented defaults:
  output buffer 64, replay page 128, and snapshot history 256.
- Keep the current Run event path and result storage unchanged.

**Verify**

```bash
cargo test -p whale-sdk-rust --test session_views -- --nocapture
cargo test -p whale-sdk-rust --test run_handle -- --nocapture
cargo test -p whale-sdk-rust --lib --tests
cargo fmt --all -- --check
```

## Task 4: Document the Rust host contract and audit compatibility

**Files**

- Create: `docs/SESSION_VIEW_API.md`
- Modify: `docs/RUST_APPLICATION_SDK.md`
- Modify: `docs/SDK_ARCHITECTURE_REVIEW.md`
- Modify: `README.md`
- Update: `.superpowers/sdd/2026-09-09-session-view-replay/progress.md`

**Work**

- Document snapshot/watch/replay/resync and multi-view host usage.
- State the live-connection replay boundary and persistent fresh-stream rule.
- Explain why this is application substrate and contains no product shell.
- Record Stage 2.2 deferrals: close-event replay/tombstones, list/history
  pagination, metadata CAS, and durable event history.
- Review public Rust API names, serde compatibility, optional-capability
  negotiation, owner isolation, secret exposure, lock ordering, resource bounds,
  and Run retention interaction.

**Final verification**

```bash
cargo test --workspace --quiet
cargo test --doc -p whale-sdk-rust
cargo check --workspace
cargo fmt --all -- --check
cargo package -p whale-sdk-rust --allow-dirty --no-verify
git diff --check
rg -n "TODO|unimplemented!" crates/whale-protocol/src/session_views.rs \
  crates/whale-daemon/src/session_views.rs \
  crates/whale-sdk-rust/src/session_views.rs docs/SESSION_VIEW_API.md
```

The package command is a release audit. If internal workspace dependencies
still lack registry versions, record that known release-hardening blocker; do
not misreport it as a source-workspace test failure.

## Verification ledger

Record only commands actually run against the final source in
`.superpowers/sdd/2026-09-09-session-view-replay/progress.md`, including exact
pass/fail/ignore counts and independent review findings.
