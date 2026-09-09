# whale-protocol

`whale-protocol` defines the shared, serializable contract used across Whale's Rust packages. It contains canonical conversation items and stream events, JSON-RPC request and response schemas, initialization negotiation, agent and provider configuration, session and run lifecycle types, recovery, retention, interactions, and session-view cursors.

This is a data-contract library. It does not open transports, call model providers, execute tools, own sessions, or provide a CLI or UI. Higher layers depend on these types so the wire shape has one source of truth.

## Public entry points

- Root exports include `CanonicalItem`, `CanonicalContent`, `CanonicalToolOutput`, `AgentStreamEvent`, `UsageMetrics`, and the base JSON-RPC types.
- Domain schemas live in public modules such as `agents`, `initialization`, `models`, `recovery`, `retention`, `runs`, `session_management`, `session_views`, and `sessions`.
- Validation methods on request and configuration types enforce contract invariants before a caller sends data.

## Minimal library use

```rust
use whale_protocol::{CanonicalItem, MessagePhase};

let user = CanonicalItem::user_text("Inspect this workspace");
let answer = CanonicalItem::assistant_text("Done", MessagePhase::FinalAnswer);
let history = vec![user, answer];

assert_eq!(history.len(), 2);
```

The crate contains no application shell. A CLI, TUI, desktop application, or service should consume it through an owning runtime layer such as `whale-sdk-rust`, or use it directly when implementing a compatible transport boundary.

The repository is still being prepared for package release. This README does not claim a crates.io publication or a Linux/Windows verification matrix.
