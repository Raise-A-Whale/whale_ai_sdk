# Connection initialization and protocol compatibility

Status: C3 implemented and verified, within the accepted [Agent SDK design](../specs/2026-09-08-agent-application-sdk-design.md). The active goal authorizes implementation in the current shared worktree. No commit, push, release or additional approval gate. The previous C2 turn made progress: completed final verification, documented the model extension contract and clarified remaining SDK boundaries.

## Design and global constraints

Each connection negotiates once with `protocol.initialize`. SDK construction retains its existing synchronous/asynchronous signatures. A public `initialize` operation permits explicit startup validation; every ordinary outgoing SDK request first awaits the same automatically initiated handshake. This avoids duplicating eager constructors and lazy fallback paths, and keeps Rust `in_process` compatible. Handshake validation is mandatory, with no silent legacy mode.

Use a protocol version independent from package version. Version 1 freezes the current A/B1/B2/C1/C2 wire semantics; package versions are diagnostic metadata. Supported versions are an explicit nonempty unique list of positive u32 integers; the daemon chooses the highest common version. Unknown additional server capabilities are permitted; missing required capabilities are fatal to an SDK connection. Capability names are nonempty strings without surrounding whitespace, with no duplicates. Unknown optional handshake fields may be ignored for additive bootstrap evolution; required known fields remain strictly typed.

The startup capability list describes daemon features, not model content support, authentication, permissions, installed model names or client callback implementations. All current SDKs require the eight implemented features below because their public surface exposes all of them. A custom protocol consumer may request fewer. An unknown required feature fails initialization; it does not cause the daemon to claim support. This stage adds no new business execution functionality.

```rust
pub const METHOD_INITIALIZE: &str = "protocol.initialize";
pub const PROTOCOL_VERSION: u32 = 1;
pub const SUPPORTED_PROTOCOL_VERSIONS: &[u32] = &[1];
pub const PROTOCOL_CAPABILITIES: &[&str] = &[
    "runs.v1", "scoped_tools.v1", "tool_context.v1", "context_policy.v1",
    "session_close.v1", "provider_config.v1", "model_providers.v1", "sampling_options.v1",
];
pub const PROTOCOL_NOT_INITIALIZED: i64 = -32010;
pub const PROTOCOL_INCOMPATIBLE: i64 = -32011;
pub const PROTOCOL_ALREADY_INITIALIZED: i64 = -32012;

pub struct PeerInfo { pub name: String, pub version: String }
pub struct InitializeParams {
    pub client: PeerInfo,
    pub protocol_versions: Vec<u32>,
    pub required_capabilities: Vec<String>,
}
pub struct InitializeResult {
    pub server: PeerInfo,
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}
// All types derive Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema.
// PeerInfo::validate and InitializeParams::validate return Result<(), String>.
// InitializeParams::sdk(name, version) requires all PROTOCOL_CAPABILITIES.
// InitializeResult::negotiate(params, server) returns Result<Self, String>.
// InitializeResult::validate_for(&self, params) returns Result<(), String>.
```

Example request and result:

```json
{"jsonrpc":"2.0","id":1,"method":"protocol.initialize","params":{"client":{"name":"whale-python","version":"0.1.0"},"protocol_versions":[1],"required_capabilities":["runs.v1"]}}
```

```json
{"jsonrpc":"2.0","id":1,"result":{"server":{"name":"whale-daemon","version":"0.1.0"},"protocol_version":1,"capabilities":["runs.v1","scoped_tools.v1","tool_context.v1","context_policy.v1","session_close.v1","provider_config.v1","model_providers.v1","sampling_options.v1"]}}
```

Connection state is shared by cloned writers and isolated from other writers: New → Initializing → Ready or Failed/Closed. Before initialization, ordinary RPC returns -32010 without publishing sessions, runs or callback bindings. Invalid parameters return -32602; incompatible versions/features return -32011 and leave an otherwise live raw connection New so it can make a corrected attempt. Duplicate concurrent or completed initialization returns -32012. A successfully accepted initialization becomes Ready only after its full response is delivered. Requests racing the pending acknowledgement wait for that outcome, then dispatch or fail; they never execute before acknowledgement. Dropping the initializer or failing its write must resolve waiters and prevent subsequent work. Existing EOF ownership cleanup applies.

SDK initialization is shared by concurrent callers and cached only after strict response validation. MethodNotFound, timeout, EOF, malformed result, wrong/unoffered version or missing required capability permanently fails the client, clears pending work and closes its transport/owned process. Retrying an ordinary API on that client must not send another initialization or business request. Cancellation of a single Rust initializer waiter must not cause a second handshake: use a connection-owned completion, preserving explicit close cancellation. Handshake has a finite 10-second request deadline; callers can abandon their own wait without changing shared protocol state.

Review clarification: all waiters, including callers arriving during failure cleanup, observe initialization failure only after owned-process cleanup completes. EOF before the first call must still enter this cleanup path. Incoming reverse tool/context requests before local readiness are rejected with -32010 without invoking host code or blocking the reader on the pending handshake; pre-ready business notifications are ignored. Responses remain routable so the initialization can complete normally.

## Task 1 — Shared contract and public acceptance (root)

- [x] Add `whale-protocol/src/initialization.rs` and export it after observing failing integration tests for missing types. Tests cover typed wire values, version negotiation, malformed/empty/duplicate versions and capabilities, missing required capability, invalid peer identity, additional server capabilities and response version validation.
- [x] Generate bootstrap JSON schema and one shared valid/invalid response fixture from Rust protocol types. Python/Java/Rust tests consume the same fixture so numeric and boolean coercion cannot diverge. This is a bootstrap contract artifact; full protocol model generation remains required later work.
- [x] Add `scripts/verify_protocol_initialization.py` using literal stdio against production daemon and actual public SDKs. Prove pre-init creation fails; valid init unlocks a real two-step model/tool turn; incompatible init creates no session/model calls; simultaneous connections remain independent; duplicate init fails without poisoning the initialized connection.
- [x] Add a deterministic old/incompatible/malformed stdio peer for all three SDKs. Capture its received methods and prove at most one handshake and no business request; verify owned process exit. EOF already observed before initialization can produce zero requests. Exercise one healthy SDK model/tool run per language through existing actual HTTP/native acceptance.
- [x] Run integrated suites and existing provider/context/session/model verifiers after workers freeze; record exact results and remaining full-goal requirements.

## Task 2 — Daemon connection state (runtime worker)

Own only `whale-daemon/**` including tests/examples. Root owns protocol/scripts; other workers own clients.

- [x] Add failing literal-RPC tests for pre-init gating, incompatible versions/features, malformed bootstrap values, duplicate initialization, writer-clone shared state, separate connection isolation, blocked acknowledgement ordering and cancellation/write failure. Make assertions about absent session/model/host work, not only error messages.
- [x] Implement connection-scoped state in a small initialization module; connect request dispatch and acknowledgement to this state. Preserve existing start-ack/terminal/close ordering, connection EOF cleanup and frame writer behavior.
- [x] Migrate existing daemon test setup and example direct protocol clients through an explicit successful init helper; do not bypass production checks or make every mock transport silently ready. Record state tests separately from migrated baseline suites.
- [x] Run daemon tests and report final source signatures, counts and integration caveats. Keep core/adapters unchanged unless a demonstrated C3 dependency requires coordination.

## Task 3 — Rust SDK (upstream worker)

Own only `whale-sdk-rust/**`, including tests and internal fixtures.

- [x] Add typed public async `initialize() -> Result<InitializeResult, SdkError>` and `SdkError::ProtocolCompatibility(String)`. Retain existing constructors; ordinary RPC shares a connection-owned initializer before dispatch. Export the protocol bootstrap types.
- [x] Observe red tests for old peer, wrong/malformed version, missing required capabilities and concurrent first operations. Validate every field without accepting zero/boolean/floating versions. Unknown additional capabilities remain acceptable. Test aborting the first initializer waiter and explicit close during initialization.
- [x] On incompatibility close the connection/owned process and clear pending state; a failed client must not send later business requests. Migrate existing internal tests with explicit compatible handshake responses, preserving their original assertions and ordering after readiness.
- [x] Add gated actual stdio tests for the root failure peer and successful production daemon. Run owned suites and provide red/green evidence. Coordinate daemon fixture builds with its owner.

## Task 4 — Python and Java SDKs (language worker)

Own only `sdks/python/**` and `sdks/java/**`.

- [x] Add typed `PeerInfo` / `InitializeResult` and explicit `client.initialize()` APIs; expose a distinct protocol compatibility error. Constructors retain current signatures. Central ordinary request paths must share one initialization with strict response parsing; Java must reject JSON boolean/string/floating coercions explicitly, Python must reject bool as int.
- [x] Test concurrent first callers, old peers, wrong/missing/duplicate capabilities, malformed identity/version, additional optional capabilities, timeout/EOF, close during initialization and post-failure calls. Assert zero start/inspect/tool dispatch after failed handshake and closure of the owned transport.
- [x] Consume root's shared protocol fixture and add gated real stdio failure tests. Migrate existing test peers by replying to initialization explicitly; do not alter public MockTransport defaults to auto-approve a handshake.
- [x] Update language READMEs with automatic vs explicit readiness and prototype wire migration. Run full suites; no concurrent Maven jobs. Report exact outcomes.

## Completion boundary

- [x] Independently review acknowledgement ordering and close/cancellation races, all public constructor paths, strict cross-language parsing and failed process cleanup.
- [x] Update protocol and architecture docs to mark only C3 initialization complete; preserve historical C1/C2 ledgers.
- [x] Keep durable Store/recovery, retention, full plugin lifecycle, native async consumer work, observability, complete schema/client generation and clean release installation as outstanding full-goal work.

## Review and verification checkpoints

- Shared contract: six Rust tests passed after the missing-module red. The generated fixture has 24 response cases (four valid and twenty invalid), consumed by all three language clients. The artifact generator's `--check` passed.
- Daemon: ten new initialization tests plus 47 existing tests passed. The initial red demonstrated that uninitialized creation actually succeeded before the gate was implemented. Held acknowledgement, cancellation, failed writes and EOF prove ordering and absent business state.
- Independent review found pre-ready reverse callback execution, failure being observed before cleanup by concurrent Python/Java callers, and early EOF bypassing owned-child cleanup. All three findings were fixed and independently re-reviewed before the final failure-peer matrix. Java forced process termination additionally required waiting for the asynchronous kill to be reaped; a controlled Process regression first failed and then passed.
- Rust fixes were independently re-reviewed: central nonblocking incoming gate and early-EOF cleanup transaction address the findings. The pre-cancellation-fix Rust workspace checkpoint was 230 passed, zero failed, ten gated tests ignored; final verification below includes the two additional cancellation regressions. Production daemon binaries/examples rebuilt successfully.
- Public bootstrap acceptance passed four tests: actual stdio gating/corrected retry/duplicates, two connections to one UDS daemon, and explicit/automatic public Python initialization followed by model/tool loops. Four actual HTTP requests were observed. The full three-language failure-peer matrix also passed: eight modes, two tests per language, 48 subprocesses, zero business requests and every PID exited. The early-EOF tests permit zero initialization requests; other modes require exactly one. Timeout cases use the real 10-second deadline.
- Final Java regression with the existing real stdio fixture exposed a cancellation race: an accepted cancel returned `cancelling`, then the terminal result became `failed/RUN_FAILED` with an internal cancellation message. A public Python reproduction observed the same failure at loop index 100; a focused Java repeat observed it on attempt 20. A deterministic test pauses inside one execution poll until a real cancel RPC has acknowledged `cancelling`; it first reproduced the failure, then passed after the fix. Cancellation acceptance and terminal commitment now use the same snapshot lock, and daemon cancel intent is published before the Core cancellation token is triggered. Session close and EOF use the same path. A second regression preserves genuine errors and post-terminal cancellation behavior. The final daemon suite has 59 passing tests, with no Core or initialization changes for this fix.


## C3 verification ledger — 2026-09-08

C3 is complete in the shared working tree. No commit, push or release has been made. The full Agent SDK goal remains active; this stage establishes connection compatibility and preserves existing execution behavior.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --quiet` on final sources | 232 passed: protocol 31, adapters 33, core 43, daemon 59, Rust SDK 66; zero failures and 10 gated tests ignored |
| `cargo build -p whale-daemon --bins --examples` | Passed; all following production-daemon checks used rebuilt binaries |
| `cargo run -q -p whale-protocol --example initialization_contract -- --check` | Both generated files match; 24 shared response cases: 4 valid and 20 invalid |
| Python full unittest discovery | 99 run: 82 passed, 17 gated tests skipped |
| `scripts/verify_python_stdio.py` | 7 actual stdio tests passed |
| Java `clean test` with `WHALE_JAVA_STDIO_FIXTURE` | 101 run: 77 unit and 3 actual stdio tests passed, zero failures/errors, 21 gated tests skipped |
| `scripts/verify_protocol_initialization.py --all-sdks` | Public bootstrap 4 tests / 4 actual HTTP requests; 48 failed-peer subprocess cases passed |
| `scripts/verify_provider_http.py --all-sdks` | Python 7 + 6, Rust 3 and Java 4 + 6 passed; 110 actual HTTP requests |
| `scripts/verify_context_boundaries.py` | 3 tests / 18 subscenarios passed; 57 actual HTTP requests |
| `scripts/verify_session_lifecycle.py --all-sdks` | Public boundary 2, Python 5, Rust 5 and Java 5 passed; 28 actual HTTP requests |
| `scripts/verify_model_providers.py --all-sdks` | Public boundary 6, Python 4, Rust 5 and Java 4 passed; 35 native calls, zero native HTTP, and 6 supported HTTP requests |
| Targeted actual Python cancellation reproduction after rebuilding | 500 immediate start/cancel iterations on one Session all returned `interrupted` |
| Repository consistency checks | Local Markdown links resolve in 23 files; rustfmt passes for all 76 changed/new Rust files; `git diff --check` passes |

The failed-peer matrix covers `old_peer`, `wrong_version`, `missing_capability`, `boolean_version`, `old_peer_ignore_term`, `eof`, `early_eof` and `no_reply`, with explicit and automatic initialization for every language. The client tests verify owned-process exit before explicit client cleanup; the runner additionally audits all 48 PIDs and peer request logs. No business request was received. Early EOF may precede the first initialization request; all other modes receive exactly one initialization. The timeout cases use the actual 10-second handshake deadline, and the SIGTERM-ignoring mode exercises forced process cleanup.

The full failed-peer matrix preceded the final daemon-only cancellation arbitration fix; the SDK initialization and failure-peer code remained unchanged. Public bootstrap acceptance was rerun on the rebuilt daemon after that fix and again passed 4 tests / 4 HTTP requests. All other checks in the table were rerun on the final sources or rebuilt binaries. Counts across suites overlap, and environment-gated tests are enabled explicitly by the appropriate verifier; these numbers do not establish production reliability or platform compatibility.

### Review and migration

- Independent daemon/client reviews covered acknowledgement ordering, strict parsing, pre-ready callback rejection, concurrent failure cleanup and early EOF. All reported findings were fixed and re-reviewed; the individual reports preserve red/green evidence: [daemon](c3-daemon-report.md), [Rust](c3-rust-report.md), [Python and Java](c3-languages-report.md).
- Accepted cancellation now wins terminal arbitration under the same snapshot lock. Core token state is not used as cancellation intent because normal Core cleanup can also set that token. Genuine execution failures and deadline outcomes remain intact when no cancellation has been accepted; a terminal snapshot is unchanged by later cancellation.
- Final independent source review of the cancellation fix found no new lock-order or arbitration issue: close/EOF release the run registry lock before awaiting cancellation, the snapshot-locked helper does not await or acquire Session/decision locks, and unaccepted start cleanup preserves its existing connection-closed outcome.
- This changes the prototype wire contract: old clients need initialization for the new daemon, and new clients reject old daemons without initialization. There is no silent legacy fallback. Constructors retain their signatures, with explicit or automatic readiness on public operations.
- The eight feature capabilities describe protocol functionality. Model capability inspection remains separate, and neither mechanism supplies application authentication or tool authorization.
- The shared schema and fixtures cover bootstrap only. They do not constitute complete protocol model/client generation or matching SDK/Daemon installation.

### Remaining full-goal work

Durable Store/recovery, automatic retention, complete plugin lifecycle, native async consumers, observability, full protocol schema/client generation, and matching clean release installation remain incomplete. The next implementation boundary is Store and recovery, using the [research notes](store-recovery-notes.md) as input to a concrete design. Record actual validated model projections and individual tool dispatch/results durably; restore interrupted/unknown outcomes without automatically repeating external tool effects. External Codex/OpenCode/pi execution belongs to the separately scoped AgentBackend design.
