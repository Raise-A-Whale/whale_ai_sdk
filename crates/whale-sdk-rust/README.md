# whale-sdk-rust

`whale-sdk-rust` is the Rust application foundation for products such as Agent CLIs, desktop applications, services, and other host shells. The host owns its interface, workspace behavior, authentication, and product policy. The SDK owns the Whale connection, readiness negotiation, Agent composition, Session and Run handles, host callbacks, ToolPack lifecycles, recovery, retention, and shutdown reporting.

All runtime sources use the same Whale JSON-RPC and `whale-daemon` execution path:

- **Embedded** runs a supplied or default `DaemonServer` behind in-memory transport.
- **ManagedProcess** spawns an executable supplied by the caller. That executable must implement the matching Whale Daemon protocol and accept the SDK-injected `--listen stdio` argument. A Codex, Claude Code, OpenCode, or pi CLI is not a compatible managed executable.
- **ExternalUds** attaches to a caller-owned Whale Daemon over a Unix-domain socket.

External UDS is a trusted-local-peer transport. The initialization handshake
checks protocol and capability compatibility; it does not authenticate the
daemon or inspect peer credentials. The host must create the socket in a
directory it controls, restrict directory/socket permissions, and choose the
daemon process it trusts.

`WhaleRuntime::open` returns only after source creation and protocol initialization succeed. `WhaleRuntime` is the unique connection owner and is not cloneable; cloned `WhaleClient`, `Agent`, `WhaleThread`, and `RunHandle` values are capabilities and do not keep that owner alive.

## Public entry points

- `WhaleRuntime`, `RuntimeOptions`, and `RuntimeSource` select and own the runtime source.
- `AgentDefinition`, `Agent`, and `HostTool` compose reusable Agent behavior.
- `ToolPack` binds fresh, session-scoped host resources with a frozen manifest.
- `WhaleThread` and `RunHandle` expose Session and Run lifecycle operations.
- Session views, recovery, retention limits, provider inspection, and host context policies are available as typed APIs.

Custom `ProviderRegistry` and `StoreRuntime` values are installed on a
`DaemonServer` before that server is passed to `RuntimeSource::Embedded`.
Managed-process and external-UDS runtimes use the provider and Store
configuration of their daemon process; `RuntimeOptions` does not inject those
objects across a process or socket boundary.

## Minimal library use

```rust,no_run
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;

    let definition = AgentDefinition::new("application-agent", "YOUR_MODEL");
    let agent = runtime.agent(definition, Vec::new())?;
    let session = agent.create_session().await?;

    let run = session.start_turn("Explain the current task.").await?;
    let _result = run.result().await?;

    let _closed = session.close().await?;
    let _shutdown = runtime.shutdown().await?;
    Ok(())
}
```

Actual turns require a configured provider and credentials. Built-in provider
configuration currently supports an environment-variable credential reference
or explicit no-auth, resolved in the daemon environment. Desktop keychains,
OAuth, secret brokers, and multi-tenant asynchronous credential resolution are
not built in; an application can supply a custom `ModelProvider` while a
general credential-resolver contract remains future work.

Applications using ToolPacks should explicitly await every Session's `close()` before runtime shutdown so normal callback quiescence and reverse close complete. Runtime shutdown and `Drop` provide a connection-wide emergency fence for unclosed sessions; they are not equivalent to awaited ToolPack close.

The current package has no lightweight attached-client feature split. Depending
on `whale-sdk-rust` also builds the embedded daemon/Core/Store graph even when an
application only selects managed process or external UDS at runtime.

The current SDK and daemon transport code are Unix-targeted and have been
validated locally on macOS. Linux CI is future work; Windows is unsupported.
This crate is a library foundation and does not implement a CLI, TUI, desktop
product, or provider-specific product shell. The `0.1.x` source-compatibility
policy is documented in the repository's
[`docs/API_STABILITY.md`](https://github.com/Raise-A-Whale/whale_ai_sdk/blob/main/docs/API_STABILITY.md).

The repository is still being prepared for package release; this README does not claim a crates.io publication.
