# Tool execution and model contexts

The Rust SDK supports context-aware host tools and per-step model-context policies. Agent definitions remain portable data. Host callbacks, database clients and other application resources are bound separately and remain in the application process.

## ToolContext

A tool receives canonical execution identity (Agent name, session ID, run ID and model tool-call ID), the run deadline, a cooperative cancellation signal and a progress reporting method. An older daemon may omit this identity; Rust exposes `info() -> Option<&ToolContextInfo>` for that compatibility case.

```rust,ignore
#[async_trait]
impl HostTool for Lookup {
    async fn execute_with_context(
        &self,
        context: ToolContext,
        args: Value,
    ) -> Result<CanonicalToolOutput, String> {
        if context.is_cancelled() {
            return Err("Lookup cancelled".into());
        }
        context
            .report_progress("Reading records", Some(0.5))
            .await
            .map_err(|e| e.to_string())?;
        Ok(CanonicalToolOutput::structured(json!({
            "query": args["query"],
            "run_id": context.info().and_then(|i| i.turn_id.as_deref()),
        })))
    }
}
```

The context parameter is injected by the SDK and is excluded from the model-visible JSON Schema. Existing tools without context parameters continue to execute normally. Resources can be captured in the host function or service object; they are not serialized into ToolContext or model input.

| Surface | Context-aware entry | Cancellation | Progress |
| --- | --- | --- | --- |
| Rust | `HostTool::execute_with_context(ToolContext, Value)`; defaults to `execute` | `is_cancelled()`, `cancelled().await` | `report_progress(message, Option<f64>).await` |

Progress is optional, finite and between 0 and 1. The returned boolean says whether the daemon accepted the report for that active invocation; it does not mean every event subscriber consumed it. The daemon publishes a `tool_progress` event with the canonical call ID. Foreign, finished or cancelled invocations cannot publish further accepted progress.

Cancel notifications and local deadline expiry signal the host callback. Closing the client signals its active callbacks and releases SDK-owned resources. Cancellation does not undo effects or forcibly stop arbitrary synchronous host code. Application tools should check the signal or pass it to the underlying business operation. Successful tool completion closes its progress scope without reporting a cancellation.

## Parameter validation and execution records

The runtime validates tool parameter schemas before publishing a session or registration batch. It uses the `jsonschema` validator with external schema retrieval disabled: local references work, while remote or filesystem schema dependencies fail instead of being loaded.

Before execution it validates model arguments against the registered schema. When approval replaces arguments, it validates the replacements again. Invalid arguments produce an error tool result and do not call the host function. This lets the model handle the error in its next step.

Each validated execution attempt adds a `tool_execution_started` event and a record in `RunSnapshot.tool_executions`:

```json
{
  "call_id": "model-call-id",
  "original_arguments": {"query": "original"},
  "arguments": {"query": "approved replacement"}
}
```

These records are retained independently of event consumption, including cancellation under backpressure. They record the arguments selected for the attempt, not proof that an external side effect completed. Canonical history retains the original call and its eventual result or explicit unknown-result error. C4 [persistent sessions](RECOVERY_API.md) commit execution intent before dispatch and each outcome independently; SQLite preserves these across daemon restarts. Ordinary sessions still use in-memory history. Recovery marks dispatched calls without saved outcomes as unknown and does not execute them again.

## ContextPolicy

`AgentDefinition.context_policy` selects a policy:

```json
{"type": "full_history"}
{"type": "recent_turns", "max_turns": 2}
{"type": "host"}
```

Omitting it selects full history. `recent_turns` retains complete recent user turns, including intermediate model messages and complete tool batches. It does not mean the last N individual messages or model steps. `max_turns` must be positive.

A host policy is a separately bound callback invoked before each model step. It receives `ContextBuildRequest` with execution identity, a zero-based step index, selected model, instructions and a copy of the committed history. It returns `ModelContext {system_prompt, items}`. A null system prompt explicitly selects no instructions.

Rust binds `Arc<dyn HostContextPolicy>` with `agent.with_context_policy(policy)`, which selects the host policy. Its `build(request, CancellationSignal)` is async. A host policy without a callback binding fails before session creation.

The engine validates every returned projection before invoking its ModelProvider, including HTTP-backed and registered native implementations. Tool-call IDs must be unique within the projection, each call must have exactly one result, and a pending call batch cannot be interrupted by another conversation message. Results within a batch may be returned in any order. A host policy can replace old completed turns with summaries, but it cannot leave orphan results or unanswered calls. Projection items are never appended to the authoritative session history and are never executed as new tool requests.

Built-in policies validate the full history before pruning so an invalid old turn is not silently hidden. Host policies own their projection; the engine validates the returned model context. Provider-specific restrictions still apply after generic context validation.

## Binding identity and reverse RPC

The wire contract distinguishes three IDs:

| Field | Meaning |
| --- | --- |
| `ToolExecuteHostParams.binding_id` | Exact version of the host callback, scoped by session; captured at registration |
| `ToolExecuteHostParams.call_id` | Unique correlation key for this reverse RPC invocation |
| `ToolExecuteHostParams.context.call_id` | Canonical tool-call ID produced by the model |

A new registration gets a new binding ID. An older running turn continues to call its original binding while the daemon waits to register a replacement. Requests with a binding ID resolve that version exactly; unknown IDs are rejected without falling back to the current name binding. Older peers without IDs retain name-based compatibility routing.

Successful retired bindings remain until [Session close](SESSION_API.md) or connection cleanup so dispatched invocations can use their captured version. Session close releases the SDK-owned current/retired bindings and context policy; automatic retention controls remain subsequent work. Registration transactions serialize conflicting updates and roll back explicit daemon rejections. A timeout or transport error leaves the remote mutation outcome unknown and closes the connection to prevent further execution with inconsistent bindings. Rust registration continues in its background transaction once dispatched even if the caller stops awaiting it.

New reverse methods are `context.build_host`, `context.cancel_host` and `tool.cancel_host`; host progress uses the `tool.report_progress` request. All cancellation and progress routes are invocation-scoped. The reader installs callback state before scheduling application code, so an immediately following cancellation is not lost.

## Verification

```sh
cargo build -p whale-daemon --bins --examples
cargo test --workspace
```

Default workspace tests cover host projection while later callbacks still receive original history, initial and modified schema failures without host effects, progress, cancellation and independent sessions, plus recent complete turns across supported model protocols, failed host policies before any model request, and summary projections. Fixtures are local and deterministic; they do not prove full upstream API coverage or release installation on another platform.
