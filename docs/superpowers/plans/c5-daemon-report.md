# C5 Daemon retention and session admission report

Date: 2026-09-08. Scope: `crates/whale-daemon/**`. No commit created.

## Implemented

- `server/retention.rs` (stored as `src/retention.rs`) owns run payload retention and one autonomous maintenance worker. Delayed delivery registration checks the exact RunRecord Arc, so a closed/removed invocation cannot make a new owner's same-ID run eligible. Registration precedes terminal-delivery waiter notification. Registry retirement atomically replaces a full `Arc<RunRecord>` with the original connection owner and accepted `(thread_id, turn_id)` identity. TTL begins only after successful final notification delivery. Preparing, running, approval, cancelling and failed/incomplete final delivery remain protected.
- Idle TTL and per-session terminal count use Tokio's monotonic clock. Ties use the run key for deterministic oldest-first ordering. The worker runs without new RPCs, uses weak references to the registry and every distinct configured Store, skips missed ticks, and is aborted when its owning retention state drops. Store sweeps are awaited serially; Store errors are logged without claiming cleanup succeeded. Store-owned accepted transactions retain their existing cancellation guarantees.
- `DaemonServer::with_retention_policy(self, RetentionPolicy) -> Result<Self, String>` validates JSON policy values and scheduler range with `Instant::checked_add`. Configure before sharing the server or starting maintenance; trying to fork a second policy over an already shared registry is rejected. Either builder order with `with_store_runtime` is supported. Outside a Tokio runtime, worker startup is deferred until `run` or `handle_message`.
- CLI `--retention-config PATH` loads and validates JSON before serving requests. Defaults remain disabled. Invalid fields and zero values fail startup. `session_limits.v1` is always advertised; `run_retention.v1` is advertised when the configured retention policy is enabled, independently of the optional Store recovery capability.
- Expired owned run `get`, `cancel`, approval resolution and duplicate start return `-32030 RunExpired`. Foreign connections cannot use expiry to bypass ownership checks. Session close and EOF remove retained payloads and expired IDs, while the existing closed-session fence prevents resurrection.
- Session creation applies typed limits before publication. Start checks history-plus-input and the accepted-turn quota before reserving a new ID. Rejections return `-32031`, leave history intact and allow a corrected request with the same unaccepted ID.
- A daemon accepted ordinal is restored during final cleanup, including cancellation or timeout before Core entry; it is not incremented before Core's own admission check. Failed durable `begin_run` does not consume a turn. Expired runs do not refund quota. Persistent attachment restores count from all stored run archives, and limits remain part of the normalized durable configuration.
- Core/Store limit errors map to `SESSION_LIMIT_EXCEEDED` terminal errors or `-32031` pre-acceptance errors. Actual model-request budget enforcement remains in Core before provider dispatch. Live retention does not modify durable run archives or session history.

## Red → green evidence

Observed failing behavior before its corresponding implementation:

1. A session with `max_accepted_turns=1` accepted a second new ID instead of returning `-32031`.
2. Zero history limits published a session instead of rejecting configuration.
3. An elapsed TTL still returned a full run snapshot rather than `RunExpired`.
4. An idle Store sweep left both detached records retained because its first scheduled instant was calculated only after the spawned task was first polled. Calculating the first tick before spawning fixed deterministic paused-clock behavior.
5. A cloned server accepted a second retention policy over the same registry, permitting competing maintenance workers. Startup-only configuration now rejects that fork.
6. A delayed old delivery marked a new owner's same-key active RunRecord eligible and retired it. The exact Arc identity check prevents this, with delivery registration ordered before waiter notification.
7. An unconfigured clone's incoming handshake replaced the shared Store maintenance reference with None; two configured Stores were left unswept. Deduplicated weak Store registration now preserves every live backend and serially sweeps each.

The final two supplemental regressions (pending-model protection and terminal model-request limit mapping) passed without further production changes.

## Final verification

After formatting owned Rust files:

```text
cargo test -p whale-daemon --all-targets
96 passed; 0 failed; 0 ignored
```

Breakdown: private retention unit tests 4; execution context 10; initialization 10; model providers 5; provider configuration 8; recovery 17; retention integration 16; run lifecycle 14; session close 10; transport cancellation 2. Binary and example test targets also compiled successfully with zero tests each.

The 20 new retention tests cover an actual idle wall clock and weak-reference deallocation, paused-clock boundary/count checks, no-follow-up-request Store cleanup, worker ownership/drop, in-flight held references, blocked/failed final delivery, pending model execution, cancellation before Core, quota continuity after live expiry and persistent reattachment, immutable archives, owner isolation, corrected rejected IDs, terminal error mapping, capabilities and real CLI validation. Existing close/EOF, deadline, durable write ordering and cancel arbitration suites remain green. Cross-language and full-workspace verification belongs to the parent task and is not claimed here.

## Deliberate limits

Retention bounds eligible run payloads, not every byte of process memory: exact accepted-ID tombstones remain until session close, and callers may hold returned snapshots or in-flight references. Use accepted-turn quotas and close sessions to bound those lifetimes. History budgets are admission checks, not trimming or rollback; completed oversized tool outcomes and uncertain side-effect evidence remain recorded. Store active/unknown records are protected by the Store policy and may leave requested budgets unmet. No new replay, plugin unloading, background summarization, global session TTL, or complete observability system was added.
