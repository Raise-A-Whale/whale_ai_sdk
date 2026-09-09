# Runtime Contract Implementation Plan

> For agentic workers: use subagent-driven-development for independent Rust runtime, Python SDK and Java SDK work; the controller owns protocol, Rust SDK and integration review.

**Goal:** Establish the reliable public run lifecycle needed by multiple Agent CLIs and applications, as the first implementation segment of the general pluggable SDK goal.

**Architecture:** Keep the Rust engine and local daemon. Add an asynchronous start API and a run registry; expose equivalent RunHandle semantics in Rust, Python and Java. Preserve legacy RPC compatibility while normalizing model-step events inside the engine.

**Tech Stack:** Existing Rust workspace/Tokio/Serde, Python standard library, Java 17/Jackson.

**Spec:** `docs/superpowers/specs/2026-09-08-agent-application-sdk-design.md`.

## Global Constraints

- Existing repository baseline is ba57cd4; work on feature/agent-sdk-runtime, retaining the existing design draft.
- Do not introduce application UI, external harness backends, network credentials or paid-provider calls in tests.
- Preserve existing APIs and support new run handles without exposing core maps or gates to application code.
- Tests must exercise production state transitions; mock only model responses or transport peers at external boundaries.
- Protocol reader threads must not block on consumers, callbacks or local tool execution.

## Shared Wire Contract

Protocol types live in `whale_protocol::runs`. All fields use snake_case. SDKs generate a UUID turn_id and install routing before starting.

```text
thread.start_turn params:
  {thread_id, turn_id, input_items, options?: RunTurnOptions,
   max_steps?: integer(default 10), timeout_ms?: positive integer}
response: {thread_id, turn_id}

turn.get / turn.cancel params: {thread_id, turn_id}
response: RunSnapshot

turn.resolve_approval params:
  {thread_id, turn_id, request_id,
   decision: approve|reject|modify_arguments, arguments?: JSON, feedback?: string}
response: {resolved, request_id}

turn.event notification params:
  {thread_id, turn_id, seq, type: stream, event: AgentStreamEvent}
or
  {thread_id, turn_id, seq, type: finished, snapshot: RunSnapshot}

RunSnapshot:
  {thread_id, turn_id, status: running|waiting_approval|cancelling|completed|failed|cancelled,
   items: CanonicalItem[], usage: UsageMetrics, pending_approvals: PendingApproval[],
   last_seq, result?: existing RunTurnResult, error?: {code,message}}
PendingApproval: {request_id, tool_call: CanonicalItem, reason?: string}
```

Only `finished` is terminal on the new notification channel. Engine terminal events are not forwarded as stream events on that channel. `RunSnapshot.result` uses legacy Completed/Failed/Interrupted status as appropriate; snapshot.status is authoritative for the new API. Client-local EventLagged/connection errors must not silently claim a server terminal state.

Host tool reverse requests add optional `thread_id`; new SDKs route scoped tools by (thread_id,name), falling back to legacy global tools only when no scoped binding exists. Existing peer messages without thread_id remain valid.

## Task 1: Protocol contract (controller)

Files: create `crates/whale-protocol/src/runs.rs` and `tests/run_contract.rs`; modify protocol lib exports and optional reverse-tool thread_id.

- [x] Add tests decoding stream and terminal fixtures and serializing modified-argument approval; verify missing module/types fail first.
- [x] Implement the exact shared structures and method constants, with optional fields defaulted and terminal-state helper.
- [x] Run `cargo test -p whale-protocol`; record output.

Example assertion:

```rust
let event: RunEvent = serde_json::from_value(json!({
  "thread_id":"s", "turn_id":"r", "seq":1, "type":"stream",
  "event":{"type":"text_delta","turn_id":"r","item_id":"i","delta":"hello"}
})).unwrap();
assert_eq!(event.turn_id, "r");
assert_eq!(event.seq, 1);
```

## Task 2: Engine and daemon run lifecycle (runtime worker)

Files owned: `crates/whale-core/**`, `crates/whale-daemon/**`.
Consumes: Task 1 types and constants. Produces: production implementations of all new wire methods and normalized legacy engine events.

- [x] Add failing tests for two provider steps producing one outer terminal event, parse errors returning failure, and full canonical input retention.
- [x] Normalize turn identity and provider terminals; preserve tool errors and cleanup approval waits on cancellation.
- [x] Add failing daemon tests for async acceptance, SessionBusy, cancellation while waiting approval, queryable final state, scoped tool callbacks, and connection cleanup.
- [x] Implement run registry, ordered sequenced event dispatch, result snapshots, scoped approval/cancel, and lifecycle management with bounded event delivery. Enforce positive limits and avoid hidden mutation of per-session sampling defaults.
- [x] Run focused core/daemon tests. Existing `thread.run_turn` still works; all newly accepted runs have one terminal notification.

Example expected sequence:

```text
start r1 -> accepted before model completion
start r2 on same session -> SessionBusy
approval event for r1 -> snapshot waiting_approval
cancel r1 -> terminal cancelled, pending approvals empty
start r3 on same session -> accepted
```

## Task 3: Python run handle (Python worker)

Files owned: `sdks/python/**`.
Consumes: Shared Wire Contract. Produces: `Thread.start_turn(...) -> RunHandle`, client `get_run(thread_id,turn_id)`, handle events/result/snapshot/cancel/resolve_approval.

- [x] Write failing unittest scenarios for immediate handle return, terminal result without consuming events, long streams, cancellation, approval and same-name scoped tools.
- [x] Implement per-turn routing and nonblocking dispatch. Use a bounded event queue with explicit EventLagged; terminal result is retained separately. Keep existing public APIs and add ergonomic exports.
- [x] Clean pending RPC on close; remove resolved subscriptions; support async host functions without serializing coroutine objects.
- [x] Run `PYTHONPATH=src python3 -m unittest discover -s tests -v` inside `sdks/python`.

Example intended consumption:

```python
run = thread.start_turn("analyze")
for event in run.events():
    render(event)
result = run.result(timeout=10)
```

## Task 4: Java run handle (Java worker)

Files owned: `sdks/java/**`.
Consumes: Shared Wire Contract. Produces: `AgentThread.startTurn(...) -> RunHandle`, client getRun, handle result/snapshot/cancel/approval and event subscription.

- [x] Write failing JUnit scenarios for immediate acceptance, result-only usage, callback-issued approval without reader deadlock, scoped tool routing, terminal failure and options serialization.
- [x] Implement per-run lifecycle and ordered callback dispatch off the protocol reader, bounded buffers with explicit lag failure, independent result future, snake_case options and new model types.
- [x] Fail pending requests on close/EOF and preserve legacy APIs.
- [x] Run `mvn test` inside `sdks/java`; report which test suites ran.

Example intended consumption:

```java
RunHandle run = thread.startTurn("investigate");
run.onEvent(event -> render(event));
run.result().thenAccept(result -> showResult(result));
```

## Task 5: Rust handle and cross-layer verification (controller)

Files owned: `crates/whale-sdk-rust/**`, protocol documentation and later integration fixtures.

- [x] Add failing integration tests for a run handle obtained before a blocked model finishes, result-only 1000-event runs, public approval and cancellation.
- [x] Implement RunHandle with nonblocking event routing and a separate terminal result channel. Scoped tool routing and EOF cleanup apply to all transports.
- [x] Replace legacy buffered Rust run_turn implementation with a wrapper that collects events from before acceptance on the protocol reader, preserving its return type. Its post-completion receiver necessarily retains the full turn.
- [x] Run workspace tests, Python and Java tests against the agreed wire contract; add a controlled real-stdio smoke test before claiming application integration works.
- [x] Have an independent agent review the combined lifecycle and fix important findings before marking this segment complete.

## Progress Ledger

- Baseline: cargo test --workspace --quiet passed 22 tests before production edits.
- Scope: this segment implements the run lifecycle; AgentDefinition, complete provider configuration, persistence, plugin/MCP packaging and clean-install examples remain part of the broader objective and are not declared complete here.


### Completed segment — 2026-09-08

- `cargo test --workspace`: 54 tests passed (adapters 6, protocol 11, core/daemon 24, Rust SDK 13); no failures.
- `PYTHONPATH=src python3 -m unittest discover -s tests -v` in `sdks/python`: 30 passed.
- `cargo build -p whale-daemon --example sdk_fixture` plus `PYTHONPATH=sdks/python/src python3 scripts/verify_python_stdio.py`: 7 passed through real subprocess stdio.
- `WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" mvn -f sdks/java/pom.xml clean test`: 23 passed, including 3 real stdio tests, zero skipped.
- Independent reviews: Java worker reviewed Rust client; runtime worker reviewed Python; real Python/Java stdio tests exposed shared transport defects and verified their fixes.
- Red-to-green defects included duplicate outer terminal/provider failure, shutdown blocked by an in-flight write, duplicate sequence cursor rollback, partial JSON frames after send cancellation, tool/event writer lock deadlock, terminal snapshot racing event routing removal, and cancelled snapshot omitting already committed items.
- Final architecture assessment: `docs/SDK_ARCHITECTURE_REVIEW.md`; additive public contract: `docs/RUN_API.md`.

### Explicit implementation boundaries

These are the boundaries at the A verification checkpoint above. Later B1/B2 and [C1 Session lifecycle](2026-09-08-session-lifecycle.md) add provider/context support and explicit Session close; current capabilities are recorded in the [architecture review](../../SDK_ARCHITECTURE_REVIEW.md).

- Model output was substituted at the model boundary in cross-language tests. Real daemon, engine, pipes, approvals and host tool dispatch were used; HTTP/SSE provider parsing was not exercised by these fixtures.
- Rust/Python prebuffer events from acceptance routing; Java starts token delivery when subscribed. All retain result completion independently; no token replay after disconnection is promised.
- Python/Java legacy methods retain the old wire endpoint, which now uses the same daemon run registry. Rust legacy uses the new endpoint with an unbounded compatibility collector because its old type returns events only after completion.
- Runs/session history remain in memory until connection cleanup; there is no explicit session-close RPC or durable Store. “Bounded” describes event queues, not total history retention.
- Modified tool arguments pass through but have no additional JSON Schema validation/audit model. Host function cancellation cannot promise external side effects stop.
- AgentDefinition, complete provider configuration/Responses parsing, ToolContext/ContextPolicy, persistent Store, protocol negotiation and release packaging remain open stages B/C of the broader SDK design.

### Formatting and document checks

- All 16 changed/new Rust files pass rustfmt; `git diff --check` passes.
- Full `cargo fmt --all -- --check` still reports baseline formatting differences only in eight unchanged files (adapter sources, core lib, daemon main, protocol events, and the old Rust integration test). They were not reformatted as part of this lifecycle change.
- Local links in the main README, architecture assessment, runtime API and design spec resolve.
- Changes remain uncommitted on `feature/agent-sdk-runtime`.
