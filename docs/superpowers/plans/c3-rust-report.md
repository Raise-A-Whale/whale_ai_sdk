# C3 Rust SDK implementation report

Scope: `crates/whale-sdk-rust/**`. No daemon/core/protocol source changes or commits were made for this task. Source is frozen for root's combined acceptance.

## Public behavior

- `WhaleClient::initialize().await -> Result<InitializeResult, SdkError>` is public; `InitializeParams`, `InitializeResult`, and `PeerInfo` are re-exported. Compatibility failures use `SdkError::ProtocolCompatibility(String)`.
- Existing constructor signatures are unchanged. `in_process`, `spawn_daemon`, and `connect_uds` construct a client; explicit initialization or the first ordinary RPC starts the same lazy handshake.
- A connection-owned `OnceLock`/watch completion starts one background transaction. Dropping any waiting future does not cancel/restart it. Ordinary requests await strict typed validation before dispatch. Agent/session tool and policy bindings are not published before readiness.
- `InitializeParams::sdk("whale-rust", package_version)` requests protocol version 1 and all eight required features. Shared protocol validation rejects malformed identity/version/capability fields and missing features, while accepting unknown additional capabilities/optional fields.
- Initialization has a 10-second deadline. Rejection, malformed result, EOF, or timeout closes the client transport and kills/reaps an owned subprocess before publishing failure. Later APIs return an error without sending another handshake or business request. Explicit close interrupts an in-flight handshake.
- Existing private mock peers now explicitly exchange and consume the initialization request/reply before their original protocol assertions. There is no production compatibility bypass. Internal callback progress uses the already-initialized connection guard.

## TDD and verification evidence

The initial four regression tests were run before implementing `initialize` and `ProtocolCompatibility`; compilation failed with seven missing-method/variant errors. After implementation all four passed. Additional tests cover virtual-time timeout, real transport EOF, and the shared 24-case fixture (`fixtures/protocol/initialization-v1.json`) through actual client ingress.

`cargo test -p whale-sdk-rust` completed with **66 passed, 0 failed, 10 ignored**, including 46 library tests and the existing in-process, UDS/stdio, tool/context cancellation, registration, session close, provider, and run-handle regressions. The ignored cases require externally supplied HTTP/native/initialization peers.

Actual stdio verification ran `cargo test -p whale-sdk-rust --test protocol_initialization -- --ignored --test-threads=1` for each of these modes: `old_peer`, `wrong_version`, `missing_capability`, `boolean_version`, `eof`, and `no_reply`. Both explicit initialization and automatic `create_thread` initialization passed in every mode: **12 passed, 0 failed**. The two no-reply tests exercised real deadlines and took 20.02 seconds together.

Each stdio test checks PID liveness **before** calling explicit `client.close()`, then asserts that subsequent initialization and creation calls fail. Independent inspection of the JSONL peer log found 12 processes, exactly 12 `protocol.initialize` requests, and zero business requests. Local evidence log: `/var/folders/p1/x37pjq1554q3j4581dxv45nh0000gn/T/whale-c3-rust-o2f2qm92/peer.jsonl` (temporary test artifact, not a shipped fixture).

Gated tests are in `crates/whale-sdk-rust/tests/protocol_initialization.rs`:

- `explicit_initialization_failure_reaps_owned_peer`
- `automatic_initialization_failure_reaps_owned_peer`

They consume root's `WHALE_PROTOCOL_PEER`, `WHALE_PROTOCOL_PEER_MODE`, `WHALE_PROTOCOL_PEER_LOG`, and `WHALE_PROTOCOL_PEER_LANGUAGE` environment contract. The existing native-provider two-step tool test now also calls `initialize()` twice and compares its cached descriptor; its stream budget is unchanged.

## Acceptance boundary

The complete Rust SDK suite and actual failing-peer modes were executed here. Root is rebuilding the production binaries/examples and owns the final combined healthy HTTP/native acceptance; this report does not substitute mock tests for that end-to-end verification. The handshake validates protocol features, not model support or authorization. No legacy wire fallback, reconnect/recovery, durable storage, or full schema-generated client surface was added.


## Independent review follow-up

Three additional tests were first observed failing and then passed:

- Before readiness, a reverse tool request executed the installed probe and returned a successful tool result. The central ingress now rejects reverse requests with `-32010` before invoking tool/context dispatch. The test covers both New and Initializing, checks zero callback effects/registrations, proves the same reader continues receiving the initialization ACK, and then executes both legitimate callbacks after readiness.
- Before readiness, malformed `turn.event` payloads reached business routing and disconnected the connection. Business notifications are now ignored until Ready. Response processing always remains available; ingress never awaits initialization.
- A process that closed stdout before the first API call but continued sleeping survived the old early-closed initializer return. A deterministic test waits for reader EOF, calls initialize, and checks the owned child's exited status before explicit close. Removing that early return sends even this failure through the original connection-owned cleanup transaction, with no wire dispatch after closure and no change to cleanup-before-completion ordering.

The full SDK suite was rerun after these changes: **66 passed, 0 failed, 10 ignored**. Source is frozen again. The actual stdio acceptance also supports `early_eof` (zero or one initialization request is valid depending on reader ordering) and `old_peer_ignore_term`; all other failure modes still require exactly one initialization request and zero business requests.

Final actual stdio rerun: all eight modes (`old_peer`, `wrong_version`, `missing_capability`, `boolean_version`, `eof`, `early_eof`, `old_peer_ignore_term`, `no_reply`) passed both explicit and automatic tests: **16 passed, 0 failed**. The no-reply pair took 20.03 seconds. Final JSONL audit recorded 16 processes and 15 initialization requests (one early-EOF process closed before dispatch), with zero business requests; every test checked process exit before explicit close. Evidence log: `/var/folders/p1/x37pjq1554q3j4581dxv45nh0000gn/T/whale-c3-rust-final-fvl8trt1/peer.jsonl`.
