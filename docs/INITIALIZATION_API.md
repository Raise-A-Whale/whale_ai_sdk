# Connection initialization

Status: C3 is implemented and verified in the current working tree. The [plan and verification ledger](superpowers/plans/2026-09-08-protocol-initialization.md) record the contract, test results and remaining full-goal work. No commit or release has been made; this page describes the new working-tree protocol.

Every SDK connection checks the daemon's protocol version and required features
before sending its first ordinary RPC. Constructors keep their existing signatures;
constructing a client or composing an AgentDefinition alone does not prove readiness.
Call `initialize` explicitly when an application needs a startup health check:

```rust,no_run
# async fn f() -> Result<(), Box<dyn std::error::Error>> {
# let client = whale_sdk_rust::WhaleClient::in_process(std::sync::Arc::new(
#     whale_daemon::DaemonServer::default_server(),
# ));
let info = client.initialize().await?;
println!("{} {:?}", info.protocol_version, info.server.version);
# Ok(())
# }
```

Rust uses `client.initialize().await?`. Ordinary
Session creation, provider inspection and other RPC entry points automatically
await the same handshake. Concurrent first callers share one initialization.
Successful repeated calls return the validated connection result without another
wire request. Local Agent definitions and tool functions can be prepared before
initialization because they do not execute a daemon request.

## Wire contract

```json
{"jsonrpc":"2.0","id":1,"method":"protocol.initialize","params":{"client":{"name":"my-agent-app","version":"0.1.0"},"protocol_versions":[1],"required_capabilities":["runs.v1"]}}
```

The daemon selects the highest mutually supported protocol version and returns:

```json
{"jsonrpc":"2.0","id":1,"result":{"server":{"name":"whale-daemon","version":"0.1.0"},"protocol_version":1,"capabilities":["runs.v1","scoped_tools.v1","tool_context.v1","context_policy.v1","session_close.v1","provider_config.v1","model_providers.v1","sampling_options.v1"]}}
```

Protocol version is independent from package version. Version 1 defines the
current wire shapes and behavior; server package version is diagnostic metadata.
Names and package versions must be nonempty strings without surrounding whitespace.
Version lists must contain unique positive u32 integers. Booleans, strings and
floating numbers are invalid versions. Capability lists contain unique nonempty
strings without surrounding whitespace.

All current language SDKs require all eight baseline features. A custom protocol
consumer can request fewer. Unknown required features cause initialization failure;
unknown additional server capabilities and additional bootstrap object fields can
be ignored. A selected version must have been offered by the client.

| Capability | Implemented daemon contract |
| --- | --- |
| `runs.v1` | Run acceptance, events, result/snapshot, cancellation, deadline and approval |
| `scoped_tools.v1` | Session-owned, versioned host tool bindings and preserved tool errors |
| `tool_context.v1` | Execution identity, cooperative cancellation, deadline and progress |
| `context_policy.v1` | Per-step context projection, recent turns and host context callbacks |
| `session_close.v1` | Explicit Session release with terminal-before-close acknowledgement |
| `provider_config.v1` | Explicit model protocol, endpoint and credential reference selection |
| `model_providers.v1` | Registered ModelProvider selection and typed `provider.inspect` |
| `sampling_options.v1` | Declared six sampling option fields, validation and per-run restoration |
| `session_recovery.v1` (optional) | Explicit persistent sessions, authenticated inspect/attach/acknowledge/forget; advertised only after configured StoreRuntime startup recovery succeeds |

Recovery APIs explicitly check `session_recovery.v1` before dispatch. Ordinary clients still require only the baseline eight capabilities, and ordinary sessions remain in memory even when a store is configured. MemoryStore and SQLiteStore can both expose the recovery API; the capability alone does not assert disk durability. Backend selection and retained-key usage are described in [Recovery API](RECOVERY_API.md).

These capabilities describe daemon protocol functionality. Model modalities,
reasoning and option support still come from [provider inspection](MODEL_PROVIDER_API.md).
Initialization does not list installed models, perform model HTTP requests,
authenticate an application or grant tools permission to run.

## Ordering and failure

The initialization acknowledgement precedes business execution. Requests arriving
while that acknowledgement is pending wait for its outcome. A failed write or
cancelled initializer resolves waiting work as failure and cannot make the
connection ready later. Writer clones share readiness; another stdio/UDS connection
must negotiate independently.

| Error | Meaning for a raw protocol consumer |
| --- | --- |
| `-32010` | Ordinary request received before initialization |
| `-32011` | No mutually supported version or a required feature is missing |
| `-32012` | Initialization is already in progress or has already occurred |
| `-32602` | Invalid initialization parameter shape or value |

A raw connection can correct invalid/incompatible initialization parameters and
try again before a successful handshake. SDK policy is stricter: MethodNotFound,
incompatibility, invalid response, EOF or the 10-second handshake deadline
permanently fails that client. It closes its transport and any daemon process it
owns, clears pending work, and rejects later business APIs without retrying the
handshake. Use a new client after correcting the daemon/version configuration.
The 10-second limit bounds the handshake request; waiting for process cleanup can
extend the time before the caller receives the failure.
Closing a client while initialization is pending wakes its waiters. Abandoning one
Rust initialization waiter does not restart or cancel the shared handshake.
Reverse tool/context requests received before readiness are rejected without
executing host functions; the reader continues processing the initialization
response. Failure is published to all initialization callers after owned-process
cleanup, including when EOF arrives before the first ordinary API call.

## Compatibility migration and verification

This is an intentional prototype wire transition. An old client sending ordinary
requests to the new daemon receives ProtocolNotInitialized. A new client connected
to an old daemon stops on an unsupported initialization method. There is no
automatic legacy fallback that can silently ignore new options. Replace SDK and
daemon together when adopting this working-tree version; matching release
installation is still future work.

The Rust-owned [response fixture](../fixtures/protocol/initialization-v1.json) has
valid and invalid cases. The generated
[JSON schemas](protocol/initialization-v1.schema.json) describe structural shapes;
negotiation and semantic set/name rules are also enforced by validators and the
shared fixture. These bootstrap artifacts do not yet generate the entire SDK.
`scripts/protocol_peer_fixture.py` provides deterministic incompatible stdio peers
for failure-path experiments.

```sh
cargo run -q -p whale-protocol --example initialization_contract -- --check
cargo test --workspace
cargo build -p whale-daemon --bins --examples
```

Public verification uses real stdio, independent connections to one UDS daemon,
and optional incompatible subprocess peers whose method log
must contain at most one initialization and zero business requests before process
exit. When EOF is observed before the first call, no initialization needs to be
sent. The eight failure modes cover old peers, wrong or malformed versions,
missing features, EOF before/after dispatch, no reply, and a peer ignoring SIGTERM.
Existing provider, context, Session and native-model integration suites continue
to validate the same execution paths after initialization.
