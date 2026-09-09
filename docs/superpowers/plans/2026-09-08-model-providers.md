# Model Provider execution boundary

> Execute through subagent-driven-development and TDD in the existing shared worktree. The active user goal authorizes continued implementation. Do not commit or push.

**Goal:** Applications select registered model implementations through a stable model execution contract; adding a non-HTTP implementation does not require editing Engine.

**Architecture:** ModelProvider consumes an owned projection and returns model-step events. HttpModelProvider owns HTTP and wraps existing ProtocolAdapters. A startup registry resolves custom factory references; daemon and all SDKs expose typed capability inspection and protect reference selection against older peers.

**Spec:** [Agent application SDK design](../specs/2026-09-08-agent-application-sdk-design.md), model extension and capability requirements; [current architecture assessment](../../SDK_ARCHITECTURE_REVIEW.md). This is C2 within the full goal. Store, retention, plugins, handshake, native async client work, observability and release verification remain required later work.

## Frozen cross-layer contract

Protocol owns a new `whale_protocol::models` module:

```rust
pub const METHOD_PROVIDER_INSPECT: &str = "provider.inspect";
pub enum ModelCapabilityScope { Protocol, Model }
pub enum ModelContentKind { Text, Image, Audio }
pub enum ModelOption { Temperature, MaxTokens, ReasoningEffort, ThinkingBudget, PromptCaching }
pub struct ModelCapabilities {
    pub scope: ModelCapabilityScope,
    pub user_content: Vec<ModelContentKind>,
    pub assistant_content: Vec<ModelContentKind>,
    pub tool_result_content: Vec<ModelContentKind>,
    pub tool_calls: bool,
    pub reasoning_text: bool,
    pub reasoning_signatures: bool,
    pub encrypted_reasoning: bool,
    pub options: Vec<ModelOption>,
}
pub struct InspectProviderParams {
    pub model: String,
    pub provider_ref: Option<String>,
    pub provider: Option<String>,
    pub provider_config: Option<ProviderConfig>,
}
pub struct InspectProviderResult {
    pub model: String,
    pub provider_ref: Option<String>,
    pub capabilities: ModelCapabilities,
}
```

Enums serialize as snake_case. Optional fields are omitted when absent. `ModelCapabilities::text_only()` creates a model-scoped descriptor permitting text in the three positions, no tools/reasoning/optional sampling settings. Structured tool results are represented as text by current adapters. A protocol-scoped descriptor describes the implemented wire subset and does not claim that every remote model supports it.

`AgentDefinition` and `StartThreadParams` gain optional `provider_ref`. A reference must be nonempty with no surrounding whitespace, and conflicts with either `provider` or `provider_config`. Unknown references never fall back to HTTP. `validate_provider_selection(provider_ref, provider, provider_config)` in protocol models centralizes these portable checks; `InspectProviderParams::validate()` also requires a nonempty model.

`RunTurnOptions` additionally exposes `thinking_budget: Option<u32>` and `prompt_caching: Option<bool>` so every declared optional sampling capability is reachable through all SDKs. Absent values inherit defaults; `prompt_caching: false` explicitly disables a true Session default for that run. Budget must be positive. Unknown option fields and mistyped values are rejected instead of silently ignored. Python keyword additions and Java getter/setter/copy/merge paths preserve existing calls; daemon applies and restores both options alongside existing sampling fields.

Every SDK creation using a reference first calls inspect and checks the echoed model/reference. Unsupported method, invalid descriptor or mismatched identity aborts before `session.start_thread`. This is necessary because older daemons ignore unknown optional start fields. Existing configuration-only callers keep their old path. General connection handshake remains separate required work.

Core public types live in `whale_core::model`:

```rust
pub struct ModelRequest {
    pub context: RunContextInfo,
    pub step_index: usize,
    pub step_id: String,
    pub model_context: ModelContext,
    pub tools: Vec<ToolDefinition>,
    pub options: SamplingOptions,
}
#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError>;
    async fn stream(&self, request: ModelRequest, cancellation: CancellationToken)
        -> Result<ModelEventStream, ModelError>;
}
pub trait ProviderFactory: Send + Sync {
    fn create(&self, model: &str) -> Result<Arc<dyn ModelProvider>, ModelError>;
}
```

`ModelEventStream` yields `Result<ModelEvent, ModelError>`. Events contain ItemStarted, TextDelta, ReasoningDelta, ReasoningSignature, ToolCallDelta, ItemCompleted and StepFinished with usage; item fields mirror the corresponding AgentStreamEvent but omit outer run IDs. Errors cover invalid request, unsupported capabilities, transport and malformed/failed model streams. Providers cannot emit approvals or outer run completion. ItemCompleted must be model-originated content, not a synthetic user message or tool result.

`ProviderRegistry::new()`, `register_factory(&mut self, id, Arc<dyn ProviderFactory>)`, `register_provider(&mut self, id, Arc<dyn ModelProvider>)`, and `resolve(&self, id: &str, model: &str)` are public in `whale_core::provider`. Registration rejects invalid or duplicate IDs. Resolution returns a provider or ModelError, and inspection never calls stream. Factories are synchronous construction/validation only; do not perform model requests. A Session retains its selected instance.

`ThreadSession::with_id_prompt_and_provider(id, prompt, provider, tools, options)` and `provider()` expose native providers. Old adapter constructors wrap HttpModelProvider. The old mandatory adapter accessor cannot describe native providers: replace it with an optional accessor and document Rust source migration, avoiding fake adapters or panics. Preserve legacy with_client/with_stream_provider behavior where representable; old stream callbacks receive a projection-only Session view, and must emit a genuine model terminal. Update fixtures that previously equated EOF with success.

## Runtime invariants

- Every step builds ContextPolicy, validates tool-call/result pairing, creates the owned ModelRequest, validates declared input/options/tool capabilities, and invokes its selected provider. Custom implementations cannot read original Session history through this interface.
- Validate model/default options/initial tools before Session publication; validate dynamic projections and per-run overrides again before every stream call. Unsupported content must fail before HTTP or custom stream invocation; no silent audio/image dropping.
- Exactly one StepFinished is required. EOF before it, duplicate completion, post-completion content/errors, provider failure or invalid model-originated items fail the run. Preserve the single outer terminal and existing unknown tool-result cleanup. Never auto-complete a truncated custom stream for compatibility.
- Each model invocation receives its own cancellation token. Cancel/drop/close must signal it while waiting for stream creation or consuming a stream; cleanup cannot cancel a different Session sharing the provider. Async implementations must yield normally.
- The HTTP wrapper owns request serialization, POST, response status and byte-stream conversion. Engine executes the common provider interface; a legacy custom HTTP client may select a wrapper but cannot introduce a second execution loop.
- Existing supported HTTP requests, tool results, approval, usage, cancellation, deadlines, scoped bindings and C1 resource release continue through the same public APIs.

## Task 1 — Protocol and acceptance (root)

Files: protocol `models.rs`, `agents.rs`, `rpc.rs`, `lib.rs`, tests `model_contract.rs`; new `scripts/verify_model_providers.py`; API/architecture documentation.

- [x] Add failing tests for `provider_ref` round trip, conflict/blank rejection, inspect identity and typed capabilities; observe missing-module/type failures, then implement shared types.
- [x] Build a public-client verifier using a custom daemon example that registers a non-HTTP provider. Python/Rust/Java consumers must inspect and select it, execute two model steps and a host tool, and preserve projected versus original history.
- [x] Verify unsupported input/options cause zero provider invocation, unknown refs fail, old peers cannot silently select HTTP, and malformed inspection identity prevents session creation.
- [x] Run full language suites, existing production HTTP/SSE verifiers and C1 close acceptance. Record concrete counts and incomplete full-goal requirements.

Wire acceptance example:

```json
{"jsonrpc":"2.0","id":1,"method":"provider.inspect","params":{"model":"local-test","provider_ref":"local"}}
```

The result must echo `model: local-test`, `provider_ref: local`, with the typed descriptor; creating that Agent must not invoke any HTTP adapter.

## Task 2 — Core and HTTP providers (runtime worker)

Files: `whale-core/src/model.rs`, `http_provider.rs`, `provider.rs`, `session.rs`, `engine.rs`, `error.rs`, exports; adapter capability validation and core/adapter tests.

- [x] Test an independent trait implementation receiving a host projection and producing one tool call, then the matching result plus final message. Assert recorded ModelRequests differ from original history and have stable outer/unique step identity.
- [x] Test pending stream creation, pending first event, mid-stream cancellation, shared-provider isolation, empty/truncated/duplicate/post-terminal streams and forbidden model item types before implementation.
- [x] Implement provider types, registry and HTTP wrapper. Test duplicate registration and unknown model/ref errors; preserve adapter compatibility constructors.
- [x] Move Engine dispatch onto ModelProvider with per-call cancellation/drop ownership and strict steps. Migrate owned fixtures to explicit model terminals and projection views.
- [x] Validate per-position content, tools, reasoning forms and sampling options. Test Chat/Anthropic audio, assistant image and unsupported tool-output content with zero HTTP dispatch, while existing valid text/image cases remain supported.
- [x] Run core/adapter suites, report exact public signatures and send compatibility changes to daemon/Rust worker.

## Task 3 — Daemon and Rust SDK (daemon/Rust worker)

Files: daemon server and model fixture example/tests; Rust SDK Agent/config/inspect methods and model-provider integration tests.

- [x] Add literal-RPC tests for inspect with no model execution, startup registry reference selection, unknown/conflicting ref rejection before Session publication and model option/tool capability rejection.
- [x] Add `DaemonServer::with_provider_registry` while preserving existing constructor. Default configs use the HTTP wrapper; references use the registry. Inspection and creation share selection and validation.
- [x] Add Rust typed `inspect_provider`, reference preflight with strict identity, configuration plumbing and old-peer regression. Failed preflight must not leak host/context bindings.
- [x] Build a custom daemon example registering a non-HTTP provider only through public APIs. It must support a deterministic two-step tool flow, captured projection evidence and cancellable waits for integration acceptance; it must not use Engine stream override.
- [x] Add real-daemon tests for cancellation/session close with native provider and run Rust SDK/daemon suites. Coordinate deprecated fixture terminals with core worker.

## Task 4 — Python and Java SDKs (language worker)

Files: language AgentDefinition/create/inspect models, tests, README and examples.

- [x] Add optional reference while preserving existing Python calls and Java builder/constructor APIs. Reject reference conflicts and malformed names before transport dispatch.
- [x] Implement typed inspect API and mandatory preflight when selecting a reference. Unit-test old peer MethodNotFound and wrong reference/model: neither sends a start request or publishes bindings.
- [x] Add gated actual-stdio tests using the new custom daemon fixture for inspect, non-HTTP tool loop, host projection and Session close/peer isolation. Root owns the fixture runner and exact call-count check.
- [x] Run full suites and report results. Coordinate Maven with root; no concurrent clean/test jobs.

## Completion boundary

- [x] Independent review checks execution, cancellation, unsupported-content and old-peer failure boundaries.
- [x] Demonstrate a separately registered non-HTTP implementation without Engine changes and all three language consumers selecting it.
- [x] Document that this stage supports Rust embedding/custom-daemon registration and language selection, not direct Python/Java model callback injection.
- [x] Keep the full Agent SDK goal active until its remaining requirements are implemented and verified.


## C2 verification ledger — 2026-09-08

Status at the C2 checkpoint: implemented and verified in the shared worktree; no commit or push. The full Agent SDK goal remains active.

Current follow-up after [C3](2026-09-08-protocol-initialization.md): all three SDKs now share one automatic or explicit initialization per connection, negotiating protocol version 1 and all eight required capabilities before business requests or host callbacks. Malformed or incompatible peers fail the client closed, with owned child processes reaped before failure is returned; see [Initialization API](../../INITIALIZATION_API.md). Final C3 verification, including the accepted-cancellation race fix: Rust workspace 232 passed / 10 ignored; public acceptance 4 tests / 4 HTTP requests; 48 failure subprocess cases passed (8 modes × 2 tests × 3 languages). In these failure cases, early EOF may send zero initialization requests; other modes send exactly one, and all send zero business requests. Durable Store/recovery, automatic retention, plugin lifecycle, matching release installation, native async consumer work, observability and full-schema client generation remain pending; the full goal stays active. Historical checkpoint counts below are unchanged.

| Check on final integrated sources | Observed result |
| --- | --- |
| `cargo test --workspace --quiet` | 204 passed: protocol 25, adapters 33, core 43, daemon 47, Rust SDK 56; 8 gated tests ignored |
| `cargo build -p whale-daemon --bins --examples` | Passed |
| `PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests` | 87 run: 72 passed, 15 gated tests skipped |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_python_stdio.py` | 7 actual stdio tests passed |
| `WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" mvn -f sdks/java/pom.xml clean test` | 87 run: 65 unit and 3 actual stdio tests passed, 19 gated tests skipped |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_provider_http.py --all-sdks` | Python 7 + 6, Rust 3, Java 4 + 6 passed; 110 actual HTTP requests |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_context_boundaries.py` | 3 tests / 18 subscenarios passed; 57 actual HTTP requests |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_session_lifecycle.py --all-sdks` | Public boundary tests 2, Python 5, Rust 5 and Java 5 passed; 28 actual HTTP requests |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_model_providers.py --all-sdks` | Public HTTP 3 + native 3, Python 4, Rust 5 and Java 4 passed; 35 native model calls, zero native HTTP dispatch, and 6 supported HTTP requests |
| Changed-file Rust formatting and documentation checks | 66 changed/new Rust files pass rustfmt; local Markdown links in 17 files resolve; `git diff --check` passes |

The new native verifier counts 8 root invocations (6 malformed streams and 2 pending-cancellation cases), plus 9 invocations from each language. Each language proves normal and peer flows, host projection with tool-result history, and pending cancellation. JSONL evidence checks effective model, outer identity, unique step ID, projected instructions and matching tool results. A local HTTP proxy trap records any accidental native-provider network path; it remained empty. The Rust invocation also contains one in-process sampling-options test already included in the workspace suite. Counts across suites are not disjoint, and gated tests are explicitly enabled by the integration runners.

The six HTTP requests are three valid image-preservation cases, one per protocol, and three Anthropic requests proving thinking budget / prompt caching override and restoration. Unsupported user audio, assistant images and tool-result audio are rejected before dispatch. Anthropic temperature 1 is preserved alongside thinking; explicit prompt caching false removes cache markers for only that run.

### Failure evidence and review

- Protocol tests first failed on missing model types and reference fields. They passed after shared wire types, strict descriptors and reference conflict validation were added.
- Strict completion checks reject empty/truncated streams, duplicate or post-terminal completion, unfinished tool/reasoning items, forbidden model-originated user/tool-result items and repeated call identities before any tool dispatch. The duplicate-call negative case originally produced two host effects; its regression now records zero effects and zero tool executions.
- Stream creation returning an error originally left a retained cancellation token unset. A failing regression demonstrated that outcome; creation now transfers cancellation responsibility only after a successful stream handoff. Drop probes also verify cancellation before inner future/stream destruction and stream release before tool execution.
- Compatibility review found that legacy custom HTTP clients could be ignored by wrapper selection; a real HTTP request now verifies the custom User-Agent, while explicitly injected providers retain their own clients.
- Final HTTP regression exposed reused Responses fixture item IDs across turns. A two-turn probe failed the second turn with `Duplicate started model item ID`. The local fixture now derives response/item IDs from its unique request call ID; all existing HTTP suites pass without weakening runtime duplicate checks.
- Python and Java sampling-option regressions observed missing public fields before implementation. All public creation/start/legacy paths now preserve positive thinking budgets and explicit false, reject unsupported field types, and leave later defaults intact.
- Independent read-only reviews covered core/adapters, daemon/Rust preflight, and Python/Java SDK integration. They confirmed reference preflight rejects old peers and wrong identity before Session creation or binding publication. Final documentation review clarified the remaining output-capability coupling and direct Rust constructor validation timing.

### Compatibility and remaining boundaries

- Native implementations register in Rust embedding or a custom daemon at startup. Python and Java can select those references; there is no cross-process host model callback protocol or runtime plugin unload.
- `ThreadSession::adapter()` is optional for native providers. Legacy stream callbacks receive only the model projection and must emit explicit completion. These are documented Rust source/fixture migration requirements.
- Capabilities describe accepted input and options; C2 also applies them to completed model output items. This restricts providers whose generated content differs from their accepted history forms. Incremental deltas are not independently capability-gated; see [ModelProvider API](../../MODEL_PROVIDER_API.md).
- Daemon creation validates configuration before publication; direct Rust constructors defer validation until Engine execution. Shared providers remain held by the startup registry after individual Session close. Cancellation is cooperative and does not stop detached external work.
- These local deterministic tests do not establish full upstream API coverage, performance, production resilience or clean installation on another platform.

Next implementation boundary at the C2 checkpoint, now completed in [C3](2026-09-08-protocol-initialization.md): connection initialization, explicit protocol version and feature compatibility, and failure before ordinary RPC when SDK and daemon do not agree. Build this on the existing connection ownership and public-client contracts; preserve the accepted full-goal scope.
