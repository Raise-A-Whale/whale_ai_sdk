# Execution and model contexts implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Preserve the shared worktree; do not commit or push.

**Goal:** Let business Agents provide context-aware tools and replace model-context construction through all three SDKs, with validated execution and unchanged audit history.

**Architecture:** Retain the existing Rust runtime and reverse RPC. Add portable execution identity, cooperative cancellation and tool progress; introduce a ContextPolicy projection before each model step. Built-in full-history/recent-turn policies and a host callback share the same validation boundary.

**Tech Stack:** Rust/Tokio/serde, JSON Schema validation, Python synchronous host callbacks, Java 17 host callbacks, NDJSON JSON-RPC.

**Spec:** `docs/superpowers/specs/2026-09-08-agent-application-sdk-design.md`, sections 3–6. This implements B2; durable Store, plugin lifecycle, session close, handshake and packaging remain part of the full goal.

## Global constraints

- AgentDefinition remains portable data; host functions/resources are bound separately.
- Original history remains append-only. Context projection must preserve complete call/result groups and cannot cause tool execution or overwrite history.
- Cancellation is cooperative. No claim that external side effects stopped; no automatic tool retry.
- Progress and audit events use the existing ordered outer run stream. Late progress or callbacks cannot reopen terminal runs.
- Legacy tools and requests without context remain valid. Schema-invalid tool arguments must not invoke the host.
- Existing business scope is reused; no additional approval ceremony or commits are required for this authorized continuation.

## Shared wire contract (root)

New module `whale_protocol::contexts`:

```text
RunContextInfo {agent_name?: string, thread_id: string, turn_id: string, deadline_unix_ms?: u64}
ToolContextInfo {flatten RunContextInfo, call_id: string}
ContextPolicyConfig (tag type) = full_history | recent_turns {max_turns: usize} | host
ContextBuildRequest {context: RunContextInfo, step_index: usize, model: string, system_prompt: string|null, history: CanonicalItem[]}
ModelContext {system_prompt: string|null, items: CanonicalItem[]}
ToolCancelHostParams {call_id: string}
ToolReportProgressParams {call_id: string, message: string, progress?: f64}
ToolReportProgressResult {accepted: bool}
ContextCancelHostParams {request_id: string}
```

`ToolExecuteHostParams.context?: ToolContextInfo`. Existing top-level call_id remains the unique reverse RPC correlation key; context.call_id is the model's canonical call ID. New methods: daemon notification `tool.cancel_host`; client request `tool.report_progress`; daemon request `context.build_host`; daemon notification `context.cancel_host`. Progress is finite 0..1 when present and is accepted only for an active invocation owned by the connection.

Review uncovered an additional existing registration race: optimistic replacement by `(session, tool name)` can route an old running turn into the replacement function while daemon registration waits for the session lock. Complete the design's distinct callback-binding identity now: add optional `binding_id` to RegisterToolDefinition and ToolExecuteHostParams. SDKs generate a fresh opaque ID per registration and install `(session, binding_id)` before sending it; the daemon captures and echoes that ID. Requests carrying an ID resolve exactly that binding, with no name fallback. The name map remains for legacy peers only. Retired successful bindings remain alive until connection/session cleanup so old invocations can finish; failed unaccepted registrations remove their new ID. Per-name registration transactions still order updates and close a connection when remote mutation outcome is unknown. Add a real running-turn/replacement regression across the SDKs; do not equate a registration lock alone with invocation isolation.

`AgentDefinition.context_policy?: ContextPolicyConfig`; `StartThreadParams.context_policy?: ContextPolicyConfig` and `agent_name?: string`. Omitted policy means full history. Host policy requires a bound SDK callback before creating the session. Recent-turn count must be positive.

Add AgentStreamEvent `ToolProgress {turn_id, call_id, message, progress?}` and `ToolExecutionStarted {turn_id, call_id, original_arguments: Value, arguments: Value}`. The latter records the validated effective arguments before execution; the daemon retains these records in `RunSnapshot.tool_executions` as `ToolExecutionRecord {call_id, original_arguments, arguments}` so approval changes remain queryable. No secret/resource objects are serialized.

## Task 1 — Protocol and Rust SDK (root)

Files: protocol contexts/events/rpc/runs/agents and Rust SDK context module, agent, lib and tests.

- [x] Add failing serialization tests for optional compatibility, context identity, progress validation and policy configuration; run `cargo test -p whale-protocol --test execution_context_contract`.
- [x] Implement the shared types and add exhaustive event arms where required.
- [x] Provide Rust `ToolContext` with identity, `is_cancelled`, `cancelled().await`, deadline and `report_progress().await`. `HostTool.execute_with_context` defaults to old execute. Register the context synchronously before dispatch so immediate cancel is observed; clear it on completion/close.
- [x] Provide `HostContextPolicy.build(request, cancellation) -> ModelContext`; bind separately to Agent/session and dispatch outside the reader. Cancellation and disconnect notify the callback; late results are ignored by the runtime.
- [x] Validate public behavior with real runtime tests: original/effective argument audit, host progress, same-name isolation, cancellation and custom model context without history mutation.

## Task 2 — Runtime tools, schema and reverse bridges (runtime worker)

Files: core execution/coordinator/engine/session/error/lib/Cargo.toml; daemon server and extracted context bridge; relevant tests. ContextPolicy algorithm module belongs to the policy worker.

- [x] Write failing tests: wrong JSON type does not execute, modified invalid arguments do not execute, effective arguments recorded, context IDs match model IDs, cancellation reaches running host, late/foreign progress rejected.
- [x] Add `execution::CancellationToken` (new/cancel/is_cancelled/cancelled; cloneable) and core ToolContext. Extend ToolHandler through a compatible context-aware default method. Engine receives per-run identity/deadline/cancellation and passes per-call contexts.
- [x] Use an established JSON Schema validator, reject invalid schemas before publishing sessions or registrations, validate both initial and approved arguments. Do not fetch remote schema references. Persist effective argument execution records in snapshots.
- [x] Carry context in reverse tool requests. Pending-call guards send cancel notifications only for abandoned work; progress routes validate owner and active invocation. Full-frame transport remains cancellation-safe.
- [x] Integrate policy worker's ContextPolicy module into ThreadSession/Engine: execute projection before every model step, validate output before HTTP, leave session history unchanged. Implement context.build_host bridge with owner-scoped pending requests and cancellation cleanup.
- [x] Run core/daemon tests and share exact integration APIs with root and policy worker.

## Task 3 — ContextPolicy algorithms and validation (policy worker)

Files owned: new `crates/whale-core/src/context.rs`, `crates/whale-core/tests/context_policy.rs`; do not modify engine/session/lib while runtime worker owns them.

Interface:

```rust
#[async_trait]
pub trait ContextPolicy: Send + Sync {
    async fn build(&self, request: ContextBuildRequest, cancellation: CancellationToken)
        -> Result<ModelContext, String>;
}
pub struct FullHistoryContext;
pub struct RecentTurnsContext; // new(max_turns: usize) -> Result<Self, String>
pub fn validate_model_context(context: &ModelContext) -> Result<(), String>;
```

- [x] Red tests for retaining complete recent user turns, preservation of parallel call/result groups, invalid orphan/duplicate/unanswered calls, callback failure and original history immutability.
- [x] Implement full-history and recent-turn projection; a turn begins at UserMessage. Keep all items of the current turn including intermediate tool steps. Do not silently invent or drop missing tool results. Validate call/result associations and reject malformed projected histories.
- [x] Provide deterministic tests with actual canonical items and exact outputs. Coordinate integration with runtime worker; no provider calls or adapter rewrites.

## Task 4 — Python/Java consumers (client worker)

Files: sdks/python and sdks/java only.

- [x] Add failing public API tests for context-aware tools, context excluded from schema, identity/progress/cancellation, callback binding isolation, policy config serialization and immutable defaults.
- [x] Python `Tool.from_function(..., context_parameter="ctx")` injects ToolContext while excluding that parameter from JSON Schema. Old execute paths continue to work. Java `Tool.executeWithContext(context, arguments)` defaults to execute.
- [x] Both SDKs implement the wire contract and export host-context callback APIs; Agent definition stores only ContextPolicyConfig and binds the callable separately. Add session agent_name for consistent identity.
- [x] Install callback cancellation state before scheduling work; cancel on daemon notification and client disconnect. Report progress by RPC from the callback thread; do not block the protocol reader. Retain tool execution audit fields in snapshots.
- [x] Update examples to demonstrate business resources through context-aware tools and context projection; run complete client suites. Real stdio/HTTP acceptance is coordinated by root.

## Combined acceptance

- [x] Run the workspace and all client suites after integration.
- [x] Extend the production daemon/local HTTP fixture checks to prove all three clients deliver correct IDs, progress and modified arguments, reject invalid arguments before host effects, and send projected input while retaining full history.
- [x] Exercise cancellation while host tool/context callback is pending and ensure another session continues.
- [x] Independent review of implementation and actual test scope; update public API/docs and verification ledger. Leave the full goal active while C and remaining plugin contracts are unimplemented.

## Verification ledger — 2026-09-08

Status at the B2 checkpoint: implemented and verified in the shared worktree. No commit or push. The full Agent SDK goal remains active. Subsequent [C1](2026-09-08-session-lifecycle.md) adds explicit Session close and [C2](2026-09-08-model-providers.md) adds ModelProvider/registry/capabilities. The counts below preserve the B2 checkpoint rather than describing later suites.

Current follow-up after [C3](2026-09-08-protocol-initialization.md): all three SDKs now share one automatic or explicit initialization per connection, negotiating protocol version 1 and all eight required capabilities before business requests or host callbacks. Malformed or incompatible peers fail the client closed, with owned child processes reaped before failure is returned; see [Initialization API](../../INITIALIZATION_API.md). Final C3 verification, including the accepted-cancellation race fix: Rust workspace 232 passed / 10 ignored; public acceptance 4 tests / 4 HTTP requests; 48 failure subprocess cases passed (8 modes × 2 tests × 3 languages). In these failure cases, early EOF may send zero initialization requests; other modes send exactly one, and all send zero business requests. Durable Store/recovery, automatic retention, plugin lifecycle, matching release installation, native async consumer work, observability and full-schema client generation remain pending; the full goal stays active. Historical checkpoint counts below are unchanged.

| Check | Observed result |
| --- | --- |
| `cargo build -p whale-daemon --bins --examples` | Passed; production daemon and stdio fixture built |
| `cargo test --workspace` | 141 passed, 3 HTTP integration tests ignored in the ordinary invocation |
| Python unittest discovery | 59 run: 53 passed, 6 HTTP tests skipped without fixture environment |
| `verify_provider_http.py --all-sdks` | Python 7 baseline + 6 contexts, Rust 3, Java 4 provider + 6 contexts passed; 110 actual HTTP requests verified |
| `verify_context_boundaries.py` | 3 tests, 18 subcases passed; 57 actual HTTP requests |
| `verify_python_stdio.py` | 7 passed after correcting fixture call IDs across turns |
| Java `clean test` with stdio fixture | 56 run: 43 unit + 3 stdio passed, 10 HTTP tests skipped in this invocation; HTTP tests passed separately above |

The combined HTTP verifier examines the serialized model requests. All three clients prove projected system instructions/items reach HTTP while later host callbacks still receive original history. Context/progress/approval schema cases use the production daemon, real pipes and real TCP. Python and Java also test replacing a binding while an older run waits for approval; Rust's real daemon registration regression proves the same A-before-ack/B-after-ack behavior.

The separate context verifier covers all three supported model protocols: recent 1/2 complete user turns over three turns, failed/orphan/unanswered host projections before HTTP, healthy independent sessions, and summary projections whose completed tool records are never executed. Its deliberate full-history negative control failed all six pruning subcases, demonstrating the check can detect absent pruning.

### Failures found and resolved

- Protocol contract tests initially failed because execution/context types were absent; all three serialization/validation tests pass with optional legacy fields preserved.
- ToolContext success originally cancelled its cancellation signal. Separate completion and cancellation scopes now keep successful completion distinct while rejecting late progress.
- Callback cancellation is installed synchronously before spawning host work; immediate cancel, panic/error, disconnect, deadline and retained-context lifecycle cases now pass.
- Name-based optimistic replacement could dispatch an old running turn to a new callback. Versioned session bindings, exact-ID lookup, transaction ordering and explicit-rejection rollback fix the race. Unknown IDs do not fall back by name; ambiguous registration outcomes close the connection.
- Execution argument records are retained synchronously before dispatch, independent of event backpressure or cancellation. Invalid initial/replacement arguments never invoke the host.
- The old stdio fixture reused `fixture-call` across turns. Full-history validation correctly rejected the second run. The fixture now generates unique call IDs, and the long-stream test asserts uniqueness without weakening its completed/lag assertions. Python's full stdio suite and Java's affected stdio suite were rerun.

### Scope limits

These checks verify local runtime and protocol contracts, including the stated adapter subset. They do not verify paid model quality, all upstream API variants, cross-platform release installation or durable recovery. Original history and execution-argument audit are in memory; projected model requests are not yet durable audit records. Successful retired host bindings are retained until connection cleanup; explicit Session cleanup remains subsequent work.
