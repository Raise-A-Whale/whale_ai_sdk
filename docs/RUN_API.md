# Public run lifecycle

Agent construction and explicit provider/default configuration are described in
[Agent API](AGENT_API.md). The run API is additive to the legacy JSON-RPC endpoints. Types live in
[`whale_protocol::runs`](../crates/whale-protocol/src/runs.rs). A run is one turn's
execution; `RunHandle.id` and wire `turn_id` are the same identity.
All endpoints require [connection initialization](INITIALIZATION_API.md), which
the SDK performs automatically before its first ordinary RPC.

## Wire methods

| Method | Request | Response |
| --- | --- | --- |
| `thread.start_turn` | `thread_id`, client-generated `turn_id`, full canonical `input_items`, optional `options`, positive `max_steps` (default 10), optional positive `timeout_ms` | Accepted `thread_id`, `turn_id` |
| `turn.get` | `thread_id`, `turn_id` | `RunSnapshot` |
| `turn.cancel` | `thread_id`, `turn_id` | Current `RunSnapshot`; it may still be cancelling |
| `turn.resolve_approval` | `thread_id`, `turn_id`, `request_id`, `decision`, optional `arguments` / `feedback` | `resolved`, `request_id` |

Generation overrides use `model`, `temperature`, `max_tokens`,
`reasoning_effort`, `thinking_budget` and `prompt_caching`; they apply to the current run.
An omitted option inherits its Session default; false explicitly disables caching.
Unknown fields and unsupported provider options are rejected. Approval decisions are
`approve`, `reject`, or `modify_arguments`. The runtime validates both initial
and replacement arguments against the registered JSON Schema before invoking
the host. Invalid arguments become error tool results.

SDKs install per-turn routing before sending start. The daemon writes acceptance
before activating execution. A session permits one active turn; another start
returns `SessionBusy`. Reusing the same turn identity with matching parameters
returns the existing acceptance while the record is retained; conflicting parameters are rejected.
If configured retention retired its payload, the same ID returns `RunExpired` rather
than executing again. Live IDs are not rehydrated across restart.

Sessions, queries, cancellations, approvals and reverse tool replies belong to
the creating connection. Closing that SDK connection cancels its active runs
and releases its sessions. A front-end SSE connection belongs to the application;
closing it need not close the backend SDK connection.

## Events and snapshots

A `turn.event` notification has one of these parameter shapes:

```json
{"thread_id":"s","turn_id":"r","seq":1,"type":"stream","event":{"type":"text_delta","turn_id":"r","item_id":"i","delta":"Hello"}}
```

```text
{thread_id, turn_id, seq, type: "finished", snapshot: RunSnapshot}
```

Only `finished` is terminal. The server normalizes model step identifiers and
does not forward model `TurnCompleted` / `TurnFailed` as run stream terminals.
Sequences increase within a run; the terminal envelope's sequence equals
`snapshot.last_seq`.

A snapshot contains identity, status, committed items for this turn, usage,
pending approvals, `tool_executions` with original and effective arguments,
last sequence, optional final `result` and structured
`error {code,message}`. Running states are `running`, `waiting_approval` and
`cancelling`; terminal states are `completed`, `failed` and `cancelled`.
Terminal snapshots contain the legacy `RunTurnResult`: its cancellation status
is `interrupted`. Use snapshot status as the authoritative new status.
Cancellation acceptance and terminal-state commitment share one state lock. If a
cancel request is accepted as `cancelling` before terminal commitment, the final
status is `cancelled`, including when Core returns a cooperative cancellation
error. A cancel after terminal commitment returns the existing terminal result.

Results complete independently of event consumers. `EventLagged` is a local
subscription error; it does not turn a successful server run into a failed run.
Connection errors likewise do not invent a server terminal state. Cancellation
stops further model/tool dispatch, but cannot retract external effects of an
already executing host function.

The stream also carries `tool_progress` and `tool_execution_started`. ToolContext
provides cooperative cancellation and progress; completed invocations cannot
publish accepted progress. Execution records describe validated attempts, not
confirmation of external effects. See [Execution and model contexts](EXECUTION_CONTEXT_API.md).

## Language surfaces and buffering

| Surface | Start and result | Events |
| --- | --- | --- |
| Rust | `start_turn().await`, `result().await` | Single `events()` receiver; 256-event buffer starts before acceptance. Multi-observer hosts use `subscribe_events` / Session View. |

Late single consumers may get `EventLagged`. The SDK provides `snapshot`, `cancel`,
approval resolution and `get_run`. Retrieve pending approvals from snapshots if an
event was missed. No client promises token replay after disconnection. Ordinary sessions retain
run records only in memory. [Configured retention](RETENTION_API.md) can expire terminal
payloads after successful final delivery; [Session close](SESSION_API.md) releases
the remaining live records.
Explicit persistent sessions retain archived snapshots through [Recovery API](RECOVERY_API.md);
SQLite supports process-restart recovery with a fresh live session identity.
After close, old live queries fail; caller-owned terminal results and buffered
events remain readable. Inspecting archives does not revive an old RunHandle.

After live expiry, authoritative snapshot/get/cancel/approval and duplicate start
return `-32030 RunExpired`; already delivered results are unchanged. SDK finished
routes release strong ownership, but caller-held handles and buffered events
remain readable. Session admission rejection returns `-32031`; a budget exceeded
after acceptance produces `failed` with `SESSION_LIMIT_EXCEEDED`. Accepted cancelled
and expired IDs still count toward the session quota. See [Retention API](RETENTION_API.md).

Rust's legacy `run_turn` uses the new lifecycle and collects all events before returning its
old result-plus-receiver pair, so its memory use grows with turn length. Prefer
`start_turn` for long tasks.

New `tool.execute_host` requests include optional `thread_id`, `binding_id` and
execution `context`; modern clients route by session and exact callback version.
Replacing a tool preserves the binding used by an older running turn. An unknown
provided binding ID fails without falling back to the current name binding.
Rust requires a unique scoped binding for modern routing. Use the modern daemon
for session isolation.

## Validation

The Rust SDK and daemon stream transports send complete frames through a
connection-owned writer task. Cancelling a send while retaining the connection does not cancel
half a physical frame. Permanently closing a connection interrupts blocked I/O.

See [Rust handle integration](../crates/whale-sdk-rust/tests/run_handle.rs) and the
default `cargo test --workspace` suite. Those lifecycle fixtures substitute model
output. Optional gated tests that require a production daemon and local HTTP/SSE
provider fixture can be enabled when those fixtures are available; see the Agent API
for its supported protocol subset and verification boundary.
