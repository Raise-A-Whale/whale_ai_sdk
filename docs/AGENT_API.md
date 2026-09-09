# Reusable Agents and provider configuration

Applications compose a business Agent with the public Rust SDK.
An AgentDefinition contains portable data; host functions and business service
objects are bound separately. The Agent captures a configuration copy and tool
metadata, then creates sessions with independent histories.

## Portable definition

```json
{
  "name": "measurement-analyst",
  "model": "your-model",
  "system_prompt": "Use lookup to analyze business measurements.",
  "provider_config": {
    "api": "openai_responses",
    "base_url": "https://api.openai.com/v1",
    "auth": {"type": "env", "variable": "OPENAI_API_KEY"}
  },
  "tool_names": ["lookup"],
  "default_options": {"temperature": 0.2, "max_tokens": 512},
  "context_policy": {"type": "full_history"},
  "max_steps": 4,
  "timeout_ms": 30000
}
```

The binding names must exactly match `tool_names`: missing, duplicate and extra
bindings fail at Agent construction. Tool metadata is copied; executing a static
tool still invokes the original host function/service. Session history is independent,
but a static host object remains shared by intention. Rust applications that need a
fresh resource for each live Session can use a ToolPack. ToolContext supplies invocation
identity, cancellation and progress; it does not itself create a fresh business
resource per session. Context-aware tools and separately bound model-context callbacks
are described in [Execution and model contexts](EXECUTION_CONTEXT_API.md).

Close each Session when its conversation is finished to release its live history,
Run records and SDK-owned binding references. Rust Sessions provide async `close`.
The reusable Agent and business resources still referenced by the application
remain owned by the application. See [Session resource lifecycle](SESSION_API.md).

## Rust Session ToolPack

`WhaleClient::agent_with_tool_packs` and `WhaleRuntime::agent_with_tool_packs` are
additive Rust-only constructors. Each `ToolPack` exposes stable metadata when the
Agent is built and creates a new `BoundToolPack` for every ephemeral create,
persistent create, and recovery attach. The Agent shares only the factory and frozen
manifest; bound resources belong to one live Session.

Session creation stays behind a `Preparing` gate until all packs bind, their handlers
match the frozen manifest, and the daemon acknowledges the Session. Failure rolls
back returned packs in reverse declaration order. Explicit Session close first stops
and awaits callbacks, removes exact binding routes, and then awaits pack close in
reverse order exactly once. Connection loss and Runtime Drop can only invoke the
synchronous emergency fence, so applications that require awaited cleanup close every
Session before shutting down the Runtime.

Names in a pack manifest are reserved from `WhaleThread::register_tool`; unrelated
dynamic registration retains its existing behavior. ToolPack supplies `HostTool`
handlers only. `ModelProvider`, `HostContextPolicy`, Store, credentials, and a future
full-Agent backend keep separate owners and contracts. Persistent bind context exposes
the safe recovery ID, never the recovery secret, configuration, Store revision or
history. The complete contract and compile-checked example are in
[Rust Session ToolPack API](TOOL_PACK_API.md).

An Agent's defaults apply to its sessions. Model/temperature/token overrides
apply only to an individual run; later runs recover the defaults. Step limits
and deadlines are enforced by the daemon. A deadline failure terminates the
logical run, not necessarily an already executing external host operation.

## Provider configuration

| `api` | Default base URL | Appended path | Default credential variable |
| --- | --- | --- | --- |
| `openai_chat_completions` | `https://api.openai.com/v1` | `/chat/completions` | `OPENAI_API_KEY` |
| `openai_responses` | `https://api.openai.com/v1` | `/responses` | `OPENAI_API_KEY` |
| `anthropic_messages` | `https://api.anthropic.com/v1` | `/messages` | `ANTHROPIC_API_KEY` |

`base_url` includes any version prefix. It must be an absolute HTTP(S) URL without
embedded credentials, query or fragment. API-compatible private services can use
the same protocol with their own endpoint and model name.

- `auth: {type: env, variable: ...}` resolves a credential in the daemon process.
- Omitting `auth` on explicit configuration uses the conventional variable and
  rejects missing or empty credentials before publishing a session.
- `auth: {type: none}` explicitly selects unauthenticated use; no authorization
  or API-key header is sent. This is useful for a local model service.
- Secret values are absent from AgentDefinition and canonical history. SDKs that
  spawn the daemon inherit the host environment; an existing daemon uses its own.

Legacy `provider: openai|anthropic` still works. Unknown names are rejected.
Supplying both legacy provider and explicit config requires matching protocol
families. Legacy calls without explicit configuration retain model-name inference
and the older optional environment credential behavior.

`session.start_thread` adds optional `provider_config`, `options`, `agent_name`
and `context_policy` fields. Context policy defaults to full history.
The built-in HTTP path selects the three protocols above. A separate optional
`provider_ref` selects a startup-registered ModelProvider; it conflicts with
`provider` and `provider_config`. The SDK inspects its capabilities and effective
model before creation so an older daemon cannot silently ignore the reference.
See [Model provider API](MODEL_PROVIDER_API.md) for registration, typed inspection
and the difference between a protocol subset and model-specific capabilities.

## Rust

Rust exports `AgentDefinition`, `ProviderConfig`, `ProviderApi`, `ProviderAuth`
and `Agent` from `whale_sdk_rust`. Construct a definition with
`AgentDefinition::new(name, model)`, set its configuration fields, and bind
`Vec<Arc<dyn HostTool>>` through `runtime.agent(definition, tools)` or
`client.agent(definition, tools)`.
For per-Session resources, call `agent_with_tool_packs(definition, static_tools,
pack_factories)` and declare pack tool names in the same `tool_names` set.
`agent.create_session().await` returns a `WhaleThread` supporting the same
[RunHandle contract](RUN_API.md). `start_turn_with_options` applies per-run
model overrides while retaining the definition's limits.

## Model protocol coverage

Responses now has an independent serializer and streaming parser. It supports
text, image inputs, function calls and matching outputs, reasoning summaries and
encrypted reasoning continuation, usage, phase and explicit failure states.
The completed event must contain the complete output and completed status;
truncated or malformed streams fail the run instead of returning a blank success.

Responses audio, refusal items, provider-native tools, raw reasoning content and
Anthropic-only thinking signatures currently fail explicitly because they are
not supported by this adapter path. Annotations/logprobs are not retained in
Canonical IR. This is a supported subset, not a claim of complete upstream API
coverage. Model-specific capabilities and cross-provider history migration need
additional policies.

The current Chat path supports user text/images and assistant text. Capability
validation rejects Audio and non-text assistant content before dispatch. Other
positions and reasoning forms are checked against the selected implementation's
descriptor. Thinking budget and prompt caching are available as Session defaults
and per-run options where supported; explicit false disables caching for that run.

## Verification

```sh
cargo test --workspace
cargo build -p whale-daemon --bins --examples
```

Default workspace tests cover Agent composition, provider selection, capability
checks, and host-tool loops with in-process fixtures. Optional gated tests that
require a production daemon binary and a local HTTP/SSE model fixture are marked
`#[ignore]` and can be enabled explicitly when those fixtures are available.

Fixture payloads represent the upstream fields consumed by the adapters. This
proves the local integration contract; it does not test paid model quality,
provider-specific account access, all upstream response variants, or release
installation on other platforms.
