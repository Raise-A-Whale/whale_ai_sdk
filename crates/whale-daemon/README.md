# whale-daemon

`whale-daemon` is Whale's JSON-RPC server and connection/session ownership boundary. It routes initialization, agent, session, run, recovery, retention, interaction, context, model-provider, and reverse host-tool messages to `whale-core`, while publishing authoritative session views and preserving legacy run delivery.

The daemon depends on `whale-protocol`, `whale-core`, `whale-store`, and `whale-adapters`. It does not provide an end-user Agent CLI, TUI, desktop interface, workspace product, or model sandbox. Host callbacks execute in the embedding SDK/application process and must be treated as application code.

## Public entry points

- `DaemonServer` constructs and runs a Whale protocol server.
- `DaemonServer::with_provider_registry` installs a frozen native-provider registry.
- Store-backed construction enables persistent sessions, recovery, and retention.
- `HostToolBridge` handles reverse host-tool calls.
- `AnyTransportWriter`, `OutgoingTransport`, `StdioWriter`, and `UnixStreamWriter` expose the supported transport boundary.

## Minimal library use

```rust
use whale_daemon::DaemonServer;

let server = DaemonServer::default_server();
let _sessions = server.sessions();
```

`DaemonServer::run` requires an application-owned line reader and outgoing transport. `whale-sdk-rust` supplies those pieces for embedded, managed-process, and external Unix-domain-socket sources.

The current daemon transport implementation and its release verification are Unix-targeted, with local validation on macOS. A non-Unix/Windows compilation and verification matrix has not been delivered. The package includes daemon infrastructure, not a product CLI or UI.

The repository is still being prepared for package release; this README does not claim a crates.io publication.
