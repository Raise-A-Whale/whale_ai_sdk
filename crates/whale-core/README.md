# whale-core

`whale-core` implements Whale's model/tool execution engine. It owns the agent turn loop, provider abstraction, tool scheduling, approval coordination, context projection, interaction validation, cancellation, and the in-process `ThreadSession` state used by the daemon.

The crate builds on `whale-protocol`, `whale-adapters`, and `whale-store`. It does not expose the application runtime, own IPC connections, implement the JSON-RPC server, or provide a CLI or UI. `whale-daemon` places a transport and session-ownership boundary around these primitives; `whale-sdk-rust` is the application-facing owner.

## Public entry points

- `model::ModelProvider` and `model::ProviderFactory` define native model-provider extensions.
- `provider::ProviderRegistry` freezes startup registrations and resolves configured providers.
- `AgentEngine` and `ThreadSession` drive turns and session state.
- `ToolRegistry`, `ToolHandler`, `ToolExecutionCoordinator`, and `ApprovalGate` coordinate tool calls and approvals.
- `ContextPolicy`, `FullHistoryContext`, `RecentTurnsContext`, `CancellationToken`, and `ToolContext` support execution policy and cancellation.

## Minimal library use

```rust
use whale_core::provider::ProviderRegistry;

let registry = ProviderRegistry::new();
let _shared_at_daemon_start = std::sync::Arc::new(registry);
```

Most applications should enter through `whale-sdk-rust`; direct Core use is intended for daemon and extension authors who also own the required provider, store, and lifecycle wiring.

The repository is still being prepared for package release. This README does not claim a crates.io publication or a Linux/Windows verification matrix.
