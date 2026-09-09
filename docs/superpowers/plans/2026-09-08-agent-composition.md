# Agent composition and provider integration plan

Status: segment B1 implemented and verified in the working tree. This completes Agent/Provider composition only; the overall SDK objective remains active, including B2 extension contexts/policies and C persistence, lifecycle, compatibility and distribution.

Previous goal turn: progress (production lifecycle changes, 114 passing tests across suites, and architecture evidence). Current worktree remains authoritative; do not reset or commit other workers' changes.

## End state of this segment

Applications can define a reusable business Agent, select a provider protocol and endpoint through the public SDK, bind host tools, create independent sessions and run them. The production HTTP/SSE path (including OpenAI Responses) is verified against a local deterministic server. This segment moves toward the full SDK objective; ToolContext, context policy, durable stores and release packaging remain required follow-ups.

## Shared contract owned by controller

Add `whale_protocol::agents`:

```text
ProviderApi = openai_chat_completions | openai_responses | anthropic_messages
ProviderAuth = {type: env, variable: string} | {type: none}
ProviderConfig = {api: ProviderApi, base_url?: string, auth?: ProviderAuth}
AgentDefinition = {
 name, model, system_prompt?: string, provider_config?: ProviderConfig,
 tool_names: string[] (default []), default_options: RunTurnOptions (default {}),
 max_steps: positive integer (default 10), timeout_ms?: positive integer
}
```

`session.start_thread` gains optional `provider_config` and optional `options` defaults. Legacy `provider` remains valid; unknown explicit legacy provider is rejected. If both provider and provider_config are supplied, require they name the same protocol family. Explicit config validates URL and resolves credentials before publishing a session. `auth` omitted on explicit config means the conventional provider API key environment variable and fails if missing; `{type:none}` explicitly permits an unauthenticated local service. Credentials are references, not secret values serialized into definitions/history.

AgentDefinition is data; host tool objects remain outside it. SDK `client.agent(definition, tools)` validates exact required tool-name bindings (no missing/duplicate names), captures an immutable configuration copy, and returns an Agent that creates independent sessions. Instructions/model/provider/tool defaults and run limits must reach production runtime requests. Individual run overrides must not mutate definition/session defaults. Existing create-thread APIs remain usable.

## Parallel tasks

1. Controller: protocol types and serialization fixtures, Rust Agent composition, public documentation, shared real HTTP fixture and cross-language verification.
2. Runtime worker: validated provider selection/factory in daemon, per-session option defaults and invalid provider/URL/credential tests; adapt Rust struct literals in core/daemon tests only. Own core/daemon except fixture shared with controller.
3. Adapter worker: complete Responses request/stream handling with official API evidence; tests for text, tools, usage, failure/incomplete/EOF and fragmented SSE. Own whale-adapters. Empty auth must omit auth headers for explicitly unauthenticated clients.
4. Client worker: Python and Java AgentDefinition/ProviderConfig/Agent public composition and default merging, tests/docs. Own sdks/python and sdks/java.

## Verification and acceptance

- Write failing boundary/behavior tests first; tests must exercise real decisions, not duplicate implementation.
- Two different Agent definitions, same tool name and separate session histories work without core changes.
- Public SDK configuration chooses both endpoint and wire API; local HTTP fixtures inspect request model/instructions/tool schema/defaults/overrides and respond with SSE through production adapter parsing.
- Responses multi-step host tool loop emits a single outer terminal and retains final text/usage. Error or truncated provider streams cannot be reported as success.
- Missing binding, invalid config and credentials fail clearly before creating a partially usable Agent/session.
- Run relevant Rust/Python/Java tests, actual daemon stdio+HTTP integration, and independent review before declaring this segment complete.

## Verification ledger — 2026-09-08

| Check | Observed result |
| --- | --- |
| `cargo test --workspace` | 92 passed; 1 controlled HTTP test ignored in the ordinary suite |
| `PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -v` | 35 passed |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_python_stdio.py` | 7 passed through real subprocess pipes and runtime; only model output is substituted |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_provider_http.py --all-sdks` | Python 7, Rust 1 explicitly enabled, Java 4 passed; 66 real HTTP requests inspected against a local model fixture |
| `WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" mvn -f sdks/java/pom.xml clean test` | 30 passed (27 unit + 3 real stdio); 4 HTTP tests skipped here and passed separately above |

The HTTP fixture uses the production daemon and adapters. It validates exact tool-call/result association and business payloads, default generation options, per-run overrides and restoration, parallel Chat tool batches, provider errors, truncated streams, and same-session reuse after cancellation. It exercises the Python CLI and Java application-controller example through public SDK APIs. It does not validate paid provider service behavior, all upstream API variants, model quality, restart recovery or clean release installation.

Independent review and integration tests found and fixed Chat parallel-call grouping, Chat/Anthropic missing terminal handling, unpaired history after cancellation/failure, Rust cleanup when session creation is dropped, immutable tool metadata capture, and Java snapshot/event completion races. Regression cases failed before the corresponding fixes.

The final Python EOF test initially failed under load because its one-second request timeout included interpreter startup. A controlled 1.2-second startup delay reproduced the failure while the process remained alive and no EOF had occurred. The test now waits for an explicit ready response before requesting exit, still requiring ConnectionError and an empty pending map; the same delayed startup and parallel reruns pass. Production EOF handling was unchanged.

The architecture review and design status distinguish B1 from the remaining ToolContext, schema validation, ContextPolicy, SessionStore, session close, provider/plugin lifecycle, handshake and distribution work. No commit or push was performed.
