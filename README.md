# Whale AI SDK

[English](README.md) | [简体中文](README.zh-CN.md)

[![Rust CI](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/ci.yml)
[![Rust Security Audit](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/security.yml/badge.svg)](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/security.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**A Rust runtime foundation for building reliable, stateful AI agents.**

Whale gives Rust applications a stateful Agent runtime—model and tool execution,
Sessions, Runs, approvals, and lifecycle ownership—with optional durable-session
recovery when persistent storage is explicitly configured. Build the CLI, TUI,
desktop app, service, workspace model, and business policy that fit your product;
Whale runs the agent underneath.

> **Early-stage project.** The public API is currently **0.1.x** and may evolve.
> See the [API stability policy](docs/API_STABILITY.md) before depending on it in
> production.

## Why Whale?

Agent products need more than a model request loop. They need a runtime that can
stream output, route tool calls, pause for approvals, recover conversations, and
close resources predictably. Whale supplies those runtime concerns through one
typed Rust SDK and a single canonical daemon/Core execution path.

| Whale provides | Your application provides |
| --- | --- |
| Agent loop, canonical conversation IR, model adapters, tool execution, Sessions and Runs | CLI, TUI, GUI, HTTP API, workspace UX, authentication, and product policy |
| Ready-before-return connection lifecycle and bounded shutdown | Packaging, deployment, and the application process model |
| Streaming, event replay, snapshots, approvals, recovery, retention, and budgets | Business tools, permissions, workflows, and domain data |

## Highlights

- **One runtime, three connection modes.** Run embedded in-process, own a managed
  Whale-compatible daemon, or attach to an external Unix-domain socket.
- **Model-agnostic execution path.** Built-in adapters cover OpenAI Chat
  Completions, OpenAI Responses, and Anthropic Messages; custom ModelProvider
  implementations use the same tool loop.
- **Typed, stateful Agent primitives.** Reusable AgentDefinition, independent
  Sessions, asynchronous Runs, streamed events, cancellation, deadlines, and
  deterministic terminal results.
- **Host-owned tools with safe boundaries.** Bind Rust HostTools, validate JSON
  Schema before execution, report progress, coordinate cancellation, and require
  approval where needed.
- **Recoverable Session state.** Session views provide snapshots and bounded event
  replay; optional SQLite-backed storage supports explicitly created persistent
  Session archives and recovery across restarts.
- **Lifecycle-aware resources.** ToolPacks create fresh session-scoped resources,
  roll back failed setup, and close in a defined order.

## Architecture

~~~text
Your Rust product (CLI / TUI / GUI / service)
                  │
                  ▼
          whale-sdk-rust
                  │  typed JSON-RPC, ownership, lifecycle
                  ▼
            whale-daemon
                  │
                  ▼
 whale-core ── whale-adapters ── model providers
      │
      ├── whale-protocol  (canonical IR and wire contracts)
      └── whale-store     (optional durable Session storage)
~~~

All runtime sources use the same protocol and execution path. This lets an
application begin with the embedded runtime and move to a separately managed
daemon without rewriting its Agent integration.

## Quick start

Whale is not yet published to crates.io. Until release, depend on the Rust SDK
directly from this repository. This reference tracks the default branch; pin a
tag or revision for reproducible production builds when one is available:

~~~toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
whale-sdk-rust = { git = "https://github.com/Raise-A-Whale/whale_ai_sdk", package = "whale-sdk-rust" }
~~~

Set a provider credential, then create an embedded runtime, an Agent, a Session,
and a Run:

~~~sh
export OPENAI_API_KEY="..."
~~~

~~~rust
use whale_sdk_rust::{
    AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig, RuntimeOptions, WhaleRuntime,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;

    let mut definition = AgentDefinition::new("assistant", "YOUR_MODEL");
    definition.system_prompt = Some("You are a concise, helpful assistant.".into());
    definition.provider_config = Some(ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: None,
        auth: Some(ProviderAuth::Env {
            variable: "OPENAI_API_KEY".into(),
        }),
    });

    let agent = runtime.agent(definition, Vec::new())?;
    let session = agent.create_session().await?;
    let run = session.start_turn("Hello, Whale.").await?;
    let result = run.result().await?;
    println!("{:?}", result);

    session.close().await?;
    runtime.shutdown().await?;
    Ok(())
}
~~~

For provider configuration, tools, events, and production lifecycle handling,
continue with the [Rust application SDK guide](docs/RUST_APPLICATION_SDK.md).

## What you can build

Whale is designed to sit underneath products such as:

- an opinionated coding or research CLI;
- a desktop or TUI assistant with streamed output and approvals;
- an internal service that turns business workflows into tool-using Agents;
- a long-lived workspace application that recovers explicitly persisted
  conversations and retained Run history after restart.

It deliberately does **not** ship a ready-made CLI, TUI, desktop interface,
workspace implementation, plugin marketplace, or generic provider credential
manager. Those are product choices owned by the host application.

## Runtime capabilities

### Agents, tools, and model context

- AgentDefinition captures reusable, portable Agent configuration; Sessions have
  independent history and bindings.
- HostTool implementations run in the Rust host and receive execution identity,
  deadlines, cooperative cancellation, and progress reporting through ToolContext.
- Tool arguments are validated before execution and again after approval-time
  edits. Concurrent calls preserve model-visible order; exclusive tools form a
  per-registry barrier.
- ContextPolicy supports full history, recent complete turns, and custom model
  projections without mutating the canonical Session history.

### Sessions and Runs

- WhaleRuntime::open returns only after source creation and protocol initialization
  have succeeded.
- RunHandle exposes asynchronous acceptance, event subscriptions, results,
  cancellation, deadlines, and one outer terminal state.
- Session views provide authoritative snapshots, bounded fixed-window replay, and
  independent observers that do not block one another.
- The optional Interaction API supports clarification, forms, permissions, and
  review/approval flows. Pending state is observable and replayable within a live
  Session attachment, but is not persisted across recovery.

### Storage and lifecycle

- Optional SessionStore implementations include process-local MemoryStore and
  durable SQLite storage. Create persistent Sessions explicitly to retain archives
  and recover them across restarts.
- Retention policies and SessionLimits bound retained Run records, accepted turns,
  and model request size without silently discarding completed results.
- ToolPack factories bind new resources per live Session and define setup,
  rollback, normal close, and emergency-fence behavior.

## Model providers

| Protocol | Default endpoint | Default credential variable |
| --- | --- | --- |
| OpenAI Chat Completions | https://api.openai.com/v1/chat/completions | OPENAI_API_KEY |
| OpenAI Responses | https://api.openai.com/v1/responses | OPENAI_API_KEY |
| Anthropic Messages | https://api.anthropic.com/v1/messages | ANTHROPIC_API_KEY |

ProviderConfig also supports a custom base URL and explicit no-auth local
services. For non-HTTP providers or custom credentials, register a ModelProvider
at daemon startup. Read the [Agent API](docs/AGENT_API.md) and
[model provider API](docs/MODEL_PROVIDER_API.md) for the supported protocol
surface and extension contract.

## Runtime modes and platform support

| Mode | Use it when | Ownership |
| --- | --- | --- |
| Embedded | Your Rust application should contain the runtime. | The application owns the in-memory connection and local daemon loop. |
| ManagedProcess | Your application starts a compatible daemon binary. | The application owns its direct child process and reaps it on shutdown. |
| ExternalUds | A trusted local daemon already exists. | The application owns only its Unix-socket connection. |

The current transport implementation targets Unix. CI runs on Ubuntu, and local
validation has been performed on macOS; Windows is not supported. An external UDS
peer is trusted-local: protocol initialization checks compatibility, not peer
identity. Hosts must control the socket path and its permissions.

## Repository layout

~~~text
crates/
  whale-protocol/   Canonical IR, JSON-RPC, and event contracts
  whale-adapters/   Model request serialization and SSE parsing
  whale-core/       Agent loop, Sessions, tool scheduling, and approvals
  whale-store/      Journal, in-memory/SQLite storage, and recovery
  whale-daemon/     Transport, Run management, and connection ownership
  whale-sdk-rust/   Public Rust application SDK
fixtures/           Protocol and out-of-repository consumer fixtures
scripts/            Packaging and consumer verification helpers
docs/               API contracts and integration guides
~~~

## Documentation

| Start here | Go deeper |
| --- | --- |
| [Rust application SDK](docs/RUST_APPLICATION_SDK.md) | [Architecture](docs/ARCHITECTURE.md) |
| [Agent composition and providers](docs/AGENT_API.md) | [Protocol specification](docs/PROTOCOL_SPEC.md) |
| [Runs and streamed events](docs/RUN_API.md) | [Execution and model contexts](docs/EXECUTION_CONTEXT_API.md) |
| [Host interactions and approvals](docs/INTERACTION_API.md) | [Session ToolPacks](docs/TOOL_PACK_API.md) |
| [Session views and replay](docs/SESSION_VIEW_API.md) | [Recovery](docs/RECOVERY_API.md) and [retention](docs/RETENTION_API.md) |
| [Initialization contract](docs/INITIALIZATION_API.md) | [Public API contracts](docs/API_CONTRACTS.md) |

## Develop and verify

### Requirements

- Rust **1.85** or newer (the workspace MSRV is 1.85)
- A Unix environment; CI runs on Ubuntu and local validation has been performed on macOS

~~~sh
cargo test --workspace --all-features
cargo run -q -p whale-protocol --example initialization_contract -- --check
cargo build -p whale-daemon --bins --examples
./scripts/verify_rust_packages.sh
~~~

The default test suite covers protocol contracts, the Core loop, storage, daemon
behavior, and SDK integrations. A small number of real-process or external HTTP
fixture tests are marked with the Rust `ignore` attribute because they require
local infrastructure.

## Contributing

Issues and pull requests are welcome. Before submitting a change, please run the
verification commands above and keep changes focused. For an overview of public
compatibility expectations, see the [API stability policy](docs/API_STABILITY.md).

## License

Whale AI SDK is licensed under the [Apache License 2.0](LICENSE).
