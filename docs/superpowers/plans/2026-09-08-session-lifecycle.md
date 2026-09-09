# Session close implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and TDD. Keep the shared worktree; do not commit or push.

**Goal:** Let an application release one Session's runtime and host resources without closing its client or disturbing another Session.

**Architecture:** Add an owner-scoped session.close RPC and a shared closing fence in each runtime/client. Cancel accepted execution, deliver its terminal event before acknowledging close, then release session/run/binding state. Close transactions must remain safe under concurrent registration, caller cancellation and disconnect.

**Tech Stack:** Rust/Tokio/serde, NDJSON JSON-RPC, Python synchronous SDK, Java 17.

**Spec:** [Agent application SDK design](../specs/2026-09-08-agent-application-sdk-design.md), sections 5.2–5.4 and phase C. The user has authorized continued implementation of this design through the active goal.

## Global constraints

- Closing one Session must leave the client and other Sessions usable.
- Session close releases live history, retained Run records/deduplication entries, successful retired tool bindings and context policy bindings for that Session.
- The runtime may keep lightweight closed-ID ownership tombstones until connection cleanup, preventing ID reuse from reviving stale Session handles. This is not an automatic TTL or constant-memory policy.
- A started run completes as cancelled when close wins before its terminal state. If completion already won, preserve that terminal state. Never publish two terminal events.
- Successful close must follow the terminal event on the wire. Already returned result values and buffered terminal events remain caller-owned copies; close does not promise to erase the caller's objects.
- New start/register operations are fenced during closing. Authoritative get/cancel/approval after close cannot succeed from stale server state. SDK snapshot/get_run must not return a cached live-looking success after close.
- Cancellation is cooperative for host code. The daemon and Python/Java clients do not wait for arbitrary synchronous business functions to return. Rust callback futures must yield normally so cancellation can drop them; blocking the executor can delay close. Late callbacks cannot regain bindings or publish accepted progress.
- Registration may wait for the current run's session lock, so close must not acquire a registration lock that is held across that RPC. Registration rechecks lifecycle before publication and rollback.
- An explicit rejected close can restore the local open state. An uncertain close outcome terminates the connection rather than allowing divergent state. Rust dispatched close continues in a background transaction if its waiter is cancelled.
- Protocol version negotiation, durable Store, automatic retention/TTL, ModelProvider/plugin lifecycle, native async SDK work and distribution remain required subsequent work. This plan does not redefine completion of the full goal.

## Shared protocol

Create whale_protocol::sessions with these public types:

```rust
pub const METHOD_SESSION_CLOSE: &str = "session.close";
pub struct CloseSessionParams { pub thread_id: String }
pub struct CloseSessionResult { pub thread_id: String, pub closed: bool }
```

CloseSessionParams::validate rejects an empty/whitespace-only ID. closed is true when this invocation closes an existing owned Session; a repeated/unknown close returns false. An existing Session or closed-ID tombstone owned by another connection must not be closed or transferred. Closed Session IDs cannot be reused during the owning connection's lifetime; create a fresh Session instead.

After acknowledgement, turn.get for old runs returns RunNotFound. SDKs may give an earlier explicit SessionClosed error for operations through a closed Session object. Existing result values remain readable; authoritative snapshot queries are no longer available.

## Task 1 — Protocol and integrated acceptance (root)

Files: crates/whale-protocol/src/sessions.rs and lib.rs; tests/session_contract.rs; scripts/verify_session_lifecycle.py; docs/SESSION_API.md and status references.

- [x] Write serialization/validation tests and run cargo test -p whale-protocol --test session_contract before implementing missing types.
- [x] Implement the shared wire types and re-export the sessions module.
- [x] Build the production daemon and extend public-client HTTP acceptance to exercise close during model/tool/context/approval work, prove no later model/tool work, and keep another Session working.
- [x] Verify late queries reject and state/binding cleanup is scoped. Add actual registration-versus-close regression with a registration waiting behind the old run.
- [x] Run three-language suites and affected existing stdio/HTTP acceptance; verify docs and record exact results.

The first wire assertion is:

```rust
assert_eq!(
    serde_json::to_value(CloseSessionParams { thread_id: "session-a".into() }).unwrap(),
    serde_json::json!({"thread_id":"session-a"})
);
```

## Task 2 — Daemon lifecycle and races (runtime worker)

Files: crates/whale-daemon/src/server.rs, src/session_lifecycle.rs, and tests/session_close.rs. Changes to core are limited to necessary execution cleanup.

- [x] Write failing literal-RPC tests: idle close, active cancellation, another Session remains usable, repeat close, wrong owner, get after close, and closed-ID reuse.
- [x] Add atomic closing state shared with create/start/register. Check it again after waits; avoid holding DashMap guards or global lifecycle locks across I/O.
- [x] Cancel all owned active work for the Session; pending starts must wait for physical acceptance delivery before publishing a terminal event. On disconnect, resolve undeliverable acceptance without emitting pre-acceptance events. Distinguish execution completion from terminal event publication and wait for the latter before close acknowledgement.
- [x] Remove histories, Run records, pending host/context requests and approvals; suppress late publication. Coordinate with disconnect so concurrent teardown cannot reopen state.
- [x] Test close during approval/host/context, close while registration waits for the session lock, close racing start/create, same-ID recreation, concurrent close and backpressured event output.
- [x] Run core/daemon tests and report actual cancellation/ordering behavior to SDK workers.

Example behavioral acceptance:

```text
start A -> wait for approval
register replacement A -> observe registration still pending
close A -> cancelled terminal precedes close acknowledgement
pending registration -> explicit rejection
turn.get A's old run -> RunNotFound
start B -> completed
```

## Task 3 — Rust SDK ownership (Rust worker)

Files: crates/whale-sdk-rust/src/lib.rs, agent.rs, run.rs, contexts.rs, a sessions module if needed, and tests/session_lifecycle.rs.

- [x] Write failing tests for close_session/WhaleThread.close and cleanup using public handles plus controlled transport where exact ordering must be injected.
- [x] Add a per-session closing fence shared by cloned handles. Install the transaction before sending close and complete it in a background task once dispatched.
- [x] After success remove the Session's run routes, name/version tool bindings, context policy and registration lock entries; cancel and drop its active callback futures by session identity.
- [x] Reject new start/register/snapshot/control on the closed Session, preserve caller-owned terminal results, and make get_run query unavailable records instead of reviving local routes.
- [x] Test dropped close waiter, explicit rejection/ambiguous outcome, callback resource release, late replies, concurrent registration and another usable Session. Run real-daemon tests after the runtime worker integrates the wire method.

## Task 4 — Python and Java lifecycle (client worker)

Files: sdks/python/src, tests/test_session_lifecycle.py and test_session_lifecycle_http.py; sdks/java/src/main and SessionLifecycleTest/SessionLifecycleHttpTest; language READMEs/examples.

- [x] Write failing tests for Python client.close_session/Thread.close and Java closeSession/AutoCloseable AgentThread.
- [x] Install the lifecycle fence without waiting for in-flight registration locks; coordinate cleanup with start/get/register rollback so late RPCs cannot republish state.
- [x] Track active host/context callbacks by Session, signal cancellation and release SDK-owned maps. Preserve independent Sessions and caller-owned terminal result values.
- [x] On explicit rejection restore open state; on uncertain mutation outcome close the connection. Repeated close returns false at client level; Java AutoCloseable close can discard the boolean.
- [x] Add real HTTP tests covering close in pending callback/approval work, independent Sessions, query rejection and registration races. Keep HTTP tests gated when no fixture environment is present.
- [x] Update examples to release Sessions and run complete client suites. Coordinate Maven/runner use with root so build directories are not concurrently cleaned.

## Completion evidence for C1

- [x] Independent implementation review checks race/owner/publication invariants rather than only method presence.
- [x] Three SDKs exercise actual runtime close and affected baseline flows remain green.
- [x] Public docs state exact result retention, close failure, ID reuse, cancellation and unsupported-peer behavior.
- [x] Full goal remains active while later C work is incomplete.

## Implementation decisions and regressions

- Daemon `SessionLifecycle` owns the short lifecycle lock, owner checks, closing/closed phases and shared close completion. Cleanup runs independently of the initiating RPC waiter. No lifecycle guard is held across transport or session-lock waits.
- A start is physically acknowledged before its terminal event can be published, including a concurrent close. Disconnect resolves acceptance that can no longer be delivered. Failed terminal delivery returns an internal RPC error after cleanup; SDKs must not reopen the Session in that uncertain state.
- Close bypasses registration locks held across daemon RPCs. Once closure is confirmed, Rust signals scoped request waiters and waits only for their local registration cleanup. Explicit close rejection preserves earlier acknowledged registrations.
- An RPC rejection with code `-32601` or `-32602` can restore local Open. Internal errors, unknown error codes, transport failures, timeouts and malformed ACKs close the entire client because the remote mutation outcome is uncertain. ACK validation checks both Session identity and a real boolean `closed` value.
- Rust close owns callback cancellation and completion. Two tests reproduced tool/context reply backpressure as a two-second close timeout before the final reply send was made cancellation-aware; both pass with the fix. The frame writer owns the complete frame, so dropping this wait does not truncate it.
- Python/Java legacy runs retain their outstanding final RPC until its response, timeout or disconnect. Session close removes their event routing while preserving already buffered terminal data. New requests and late binding publication cannot revive a closed Session.
- Independent review found and verified fixes for Java's legacy approval fence, event route cleanup after synchronous request failure, delayed create ACK republishing host/context bindings, and Python stale registration recreating a deleted lock.
- The first protocol test failed on missing Session wire types, then passed after implementation. The first public HTTP close test failed with `MethodNotFound` against the earlier daemon, then passed with production `session.close`.
- Full HTTP acceptance exposed a test that only allowed local SessionClosed for a pending registration. The daemon can instead deliver its precise `-32602 / SessionClosed` rejection before close ACK. Both language checks now allow exactly those two outcomes; a deterministic Python transport test verifies the earlier remote-rejection ordering.

- The final review reproduced Java legacy concurrent-start event loss: a busy second request replaced the first request's subscription. Legacy routes now have a per-session busy guard, including result-only consumers, and cleanup removes only the matching subscription. Three tests failed against the previous implementation; the final four-test concurrency suite passes.

## C1 verification ledger — 2026-09-08

Status at the C1 checkpoint: implemented and verified in the shared worktree. No commit or push. Subsequent [C2](2026-09-08-model-providers.md) adds ModelProvider/registry/capabilities. The full Agent SDK goal remains active. The counts below preserve the C1 checkpoint.

Current follow-up after [C3](2026-09-08-protocol-initialization.md): all three SDKs now share one automatic or explicit initialization per connection, negotiating protocol version 1 and all eight required capabilities before business requests or host callbacks. Malformed or incompatible peers fail the client closed, with owned child processes reaped before failure is returned; see [Initialization API](../../INITIALIZATION_API.md). Final C3 verification, including the accepted-cancellation race fix: Rust workspace 232 passed / 10 ignored; public acceptance 4 tests / 4 HTTP requests; 48 failure subprocess cases passed (8 modes × 2 tests × 3 languages). In these failure cases, early EOF may send zero initialization requests; other modes send exactly one, and all send zero business requests. Durable Store/recovery, automatic retention, plugin lifecycle, matching release installation, native async consumer work, observability and full-schema client generation remain pending; the full goal stays active. Historical checkpoint counts below are unchanged.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace` | 168 passed: protocol 19, adapters 28, core 29, daemon 41, Rust SDK 51; 4 HTTP tests ignored in this invocation |
| `cargo build -p whale-daemon --bins --examples` | Production daemon and stdio fixture built successfully |
| `PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -v` | 75 run: 64 passed, 11 gated HTTP tests skipped |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_python_stdio.py` | 7 passed through actual subprocess pipes |
| `WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" mvn -f sdks/java/pom.xml clean test` | 75 run: 57 unit tests and 3 actual stdio tests passed; 15 gated HTTP tests skipped |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_provider_http.py --all-sdks` | Python 7 + 6, Rust 3 and Java 4 + 6 passed; 110 actual HTTP requests verified |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_context_boundaries.py` | 3 passed with 18 subscenarios; 57 actual HTTP requests |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_session_lifecycle.py --all-sdks` | Public boundary tests 2, Python 5, Rust 5 and Java 5 passed; 28 actual HTTP requests |
| Formatting and documentation | All 53 changed/new Rust files pass rustfmt; `git diff --check` and local Markdown link validation pass |

The C1 HTTP count is 7 public-boundary requests, 9 Python, 3 Rust and 9 Java. The Rust invocation includes four non-ignored daemon tests already counted by `cargo test`; suite counts are not disjoint. Gated HTTP tests above are explicitly enabled by the integration runners rather than counted as passing in ordinary language test runs.

The public boundary resource test uses weak references to the original tool object, its replacement and the host context policy across twelve Session lifetimes on one client. It proves those SDK-owned references are released after close while the connection remains usable. It does not measure constant memory, prove automatic retention, or dispose application-owned references.

Next implementation boundary at the C1 checkpoint, now completed in [C2](2026-09-08-model-providers.md): introduce an owned, projected ModelRequest and ModelProvider execution interface, wrap the existing HTTP adapters, and inject a startup ProviderRegistry into the runtime. A non-HTTP provider must complete the same tool loop without Engine edits. Preserve existing wire defaults and explicitly validate unsupported input/option capabilities; exposing host-language model callbacks across RPC remains a separate extension.
