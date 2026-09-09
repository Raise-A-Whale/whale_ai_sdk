# whale-adapters

`whale-adapters` translates Whale's canonical protocol records into supported model-provider wire formats and parses provider Server-Sent Events back into canonical `AgentStreamEvent` values. The current built-in adapters cover Anthropic Messages and OpenAI Chat Completions or Responses.

The crate depends on `whale-protocol` for canonical data. It does not run an agent loop, own a provider connection, select application credentials, persist sessions, or provide a CLI or UI. HTTP dispatch and retry policy belong to a provider implementation such as the one in `whale-core`; applications remain responsible for how credentials are supplied.

## Public entry points

- `ProtocolAdapter` defines request serialization, provider identity and endpoint selection, capability reporting, and SSE parsing.
- `AnthropicAdapter` and `OpenAIAdapter` implement the supported provider wire subsets.
- `OpenAIWireApi` selects Chat Completions or Responses.
- `SamplingOptions`, `ToolDefinition`, `AdapterError`, and `BoxedEventStream` support custom adapter use.

## Minimal library use

```rust
use whale_adapters::{OpenAIAdapter, ProtocolAdapter, SamplingOptions};

let adapter = OpenAIAdapter::new("application-supplied-key");
let options = SamplingOptions::new("application-selected-model");

assert_eq!(adapter.provider_name(), "openai");
assert_eq!(options.model, "application-selected-model");
```

Constructing an adapter does not make a network request. Provider APIs evolve independently, so callers should use the declared capabilities and treat unsupported canonical content as a validation error.

The repository is still being prepared for package release. This README does not claim a crates.io publication or a Linux/Windows verification matrix.
