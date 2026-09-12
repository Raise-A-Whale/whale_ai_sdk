# Registered model implementations

Model providers and connection initialization are implemented and covered by the workspace protocol, Core, daemon and SDK tests. This page documents the public provider extension contract.

`ModelProvider` executes one model step. It receives the ContextPolicy projection,
tool definitions and sampling options, and produces model events. Engine retains
ownership of conversation history, tool execution, approvals, run state and the
outer terminal event. A provider can use a local inference library, a remote SDK
or another transport without implementing an Agent loop.

## Registration and selection

Rust applications implement `whale_core::model::ModelProvider` and register it in
`whale_core::provider::ProviderRegistry` at startup. Register a shared instance
through `register_provider`, or a `ProviderFactory` through `register_factory`.
Factories synchronously construct and validate an instance for the requested
model; they must not make model requests during construction or inspection.
Duplicate reference names are rejected. A Session retains its chosen instance;
this is startup registration, not a hot plugin replacement API. There is no plugin
unload or registry shutdown lifecycle.

Inject the registry with `DaemonServer::with_provider_registry`. A direct Rust
consumer can use `ThreadSession::with_id_prompt_and_provider`. An executable
example, including tools and projected input evidence, is
[`model_provider_fixture.rs`](../crates/whale-daemon/examples/model_provider_fixture.rs).
It uses the public trait and registry without the legacy Engine stream override.

All applications can select that registered implementation. The first ordinary RPC
automatically awaits the shared mandatory `protocol.initialize` handshake; callers
can also invoke `initialize` explicitly. Current SDKs negotiate protocol version 1
and require all eight implemented daemon features, including `model_providers.v1`.
This connection descriptor does not inspect or enumerate registered models. See
[connection initialization](INITIALIZATION_API.md).

```text
connect → initialize and validate ACK → provider.inspect → create Session → start Run
```

For example:

```rust
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};
use whale_protocol::models::InspectProviderParams;

// A custom daemon binary already registered `your-provider` at startup.
let runtime = WhaleRuntime::open(RuntimeOptions::managed("path/to/your-custom-daemon")).await?;
let info = runtime.client().inspect_provider(InspectProviderParams {
    model: "your-model".into(),
    provider_ref: Some("your-provider".into()),
})
.await?;
let mut definition = AgentDefinition::new("business-agent", "your-model");
definition.provider_ref = Some("your-provider".into());
let agent = runtime.agent(definition, Vec::new())?;
let session = agent.create_session().await?;
let result = session.start_turn("Analyze these records").await?.result().await?;
```

References must be nonempty without surrounding whitespace. A reference conflicts
with either `provider` or `provider_config`. Missing references and unsupported
model names fail; there is no fallback to a default HTTP endpoint. Existing
explicit `ProviderConfig` and legacy `provider` callers retain their selection
paths through the built-in HTTP wrapper.

This stage supports Rust embedding and custom-daemon registration. Direct
cross-process host model callback injection would need an additional reverse
streaming RPC contract.

## Capability inspection

After successful connection initialization, a raw protocol consumer can send:

```json
{"jsonrpc":"2.0","id":1,"method":"provider.inspect","params":{"model":"your-model","provider_ref":"your-provider"}}
```

The result echoes `model` and optional `provider_ref`, and includes typed
`capabilities`. Inspection also accepts existing `provider` / `provider_config`
selection. It does not invoke `stream` or make a model HTTP request.

SDKs using a reference perform inspection before every Session creation, validate
the descriptor and verify the echoed reference and effective default model. An old
daemon without compatible initialization fails before inspection or creation; there
is no automatic legacy fallback. An unsupported inspection method also stops
creation, even if a peer advertised the capability. Model overrides in Session
defaults are considered before inspection; per-run overrides are validated again
by the runtime.

| Capability | Meaning |
| --- | --- |
| `scope` | `protocol` describes an implemented wire subset; `model` is a model-specific declaration |
| `user_content`, `assistant_content`, `tool_result_content` | Supported text/image/audio forms at each input position |
| `tool_calls` | Tool definitions and tool-call/result history are supported |
| `reasoning_text`, `reasoning_signatures`, `encrypted_reasoning` | Canonical reasoning forms the provider can accept |
| `options` | Supported optional temperature, max_tokens, reasoning_effort, thinking_budget and prompt_caching settings |

Built-in HTTP providers return protocol-scoped descriptors. They do not probe the
remote model or guarantee account-specific availability. Structured tool results
are encoded as text in the built-in adapters. A descriptor is an allowed subset;
request validation can additionally reject invalid payloads or option combinations.

The daemon validates initial model/options/tools before publishing a Session.
Direct Rust Session constructors store their configuration without this creation
check; Engine validates every actual projected request before calling the model. This
prevents unsupported audio, assistant images or tool-result content from being
silently discarded. ContextPolicy may remove unsupported history when building a
valid projection; the provider only receives that projection.

C2 also applies these input capability checks to completed model output items.
Consequently, a provider cannot currently declare that it produces reasoning or
assistant images while refusing those forms as later input, even if a ContextPolicy
would remove them before the next request. Incremental events are not checked
against these capability flags at emission time. This is a current restriction,
not a separate output capability contract; applications must not interpret every
observed delta as an accepted, committed output item.

Session defaults and per-run options support `thinking_budget` and `prompt_caching`
alongside the existing sampling fields. An omitted option inherits the Session
default; `prompt_caching: false` explicitly disables a true default for that run.
Later runs recover the defaults. Unknown option names are rejected. The supported
fields still depend on the selected provider's capabilities and request validation.

## Model request and event ownership

`ModelRequest` owns `RunContextInfo`, `step_index`, a unique `step_id`, the complete
`ModelContext`, tool definition snapshots and `SamplingOptions`. It contains no
ThreadSession, ToolRegistry or host function objects. Providers must use its
projected system prompt and items, not independently reload the original history.

`ModelEvent` provides item starts, text/reasoning/tool argument deltas, reasoning
signatures, completed model items and `StepFinished { usage }`. A step requires
exactly one explicit finish. Empty/truncated streams, repeated completion and
events after completion fail. Providers cannot emit approvals, run completion,
user messages or tool results. Engine checks the model output before tool dispatch
and owns matching tool results.

Each invocation gets a distinct cancellation token. Cancelling a run, closing its
Session or dropping its execution interrupts both stream creation and stream
consumption, signalling the token before releasing the pending provider future or
stream. Sharing a provider across Sessions does not share their cancellation.
Async implementations must yield and avoid blocking executor threads. Cancellation
does not forcibly stop detached threads or reverse external side effects.
The token also stops invocation-owned work when a successfully consumed stream is
released before tool execution. Use the RunHandle terminal state to distinguish
an interrupted run from successful model-step cleanup.

## Compatibility and verification

`ProtocolAdapter` remains the HTTP request/SSE conversion interface.
`HttpModelProvider` owns HTTP transport, so Engine executes the common provider
interface. Old adapter-based Session constructors wrap HTTP. The Rust adapter
accessor becomes optional because a native provider has no protocol adapter;
consumers should prefer `provider()` and explicitly handle absent adapters.

Legacy Engine `with_client` affects adapter-based HTTP Sessions only; explicitly
injected providers keep their own clients. Legacy stream callbacks receive a
projection-only Session view and must emit real completion. EOF is not upgraded to
success for old fixtures.

```sh
cargo test --workspace
cargo build -p whale-daemon --bins --examples
```

Default workspace tests capture actual native ModelRequests from a separately
registered provider, check host results and projections, and use local traps to
detect accidental native-provider HTTP dispatch. They also use the production HTTP
path to check unsupported inputs before any request and valid image preservation at
the HTTP boundary. Optional gated tests that require a production daemon binary and
a local HTTP model fixture are marked `#[ignore]`.

Connection initialization is implemented; its version/features and failure-cleanup
contract are described in [INITIALIZATION_API.md](INITIALIZATION_API.md). It does
not supply model authentication, dynamic registration or plugin unloading. Full
plugin lifecycle, automatic retention, complete protocol
schema/client generation, observability and matching
release installation remain separate required work in the full Agent SDK goal.

C4 adds optional [durable sessions and recovery](RECOVERY_API.md), including each
validated ModelRequest committed before provider dispatch. Its presence in the
archive does not prove the provider received it or completed a response. Recovery
does not resume model execution; the application rebinds its provider selection
before starting a new turn.
