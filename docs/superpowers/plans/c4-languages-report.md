# C4 Python / Java implementation handoff

Status: SDK implementation, ordinary tests and the production three-language SQLite/restart verifier have passed in the shared worktree; final production results are recorded below. No commit or push. Sources follow the frozen [SessionStore plan](2026-09-08-session-store.md) and `whale-protocol/src/recovery.rs`. This report does not claim the full Agent SDK goal is complete.

## Public surface

| Operation | Python | Java |
| --- | --- | --- |
| Allocate caller-owned credentials before sending | `RecoveryKey.new()` | `RecoveryKey.newKey()` |
| Persistent creation | `agent.create_persistent_session(key)` | `agent.createPersistentSession(key)` |
| Inspect stored state | `client.inspect_recovery(key)` | `client.inspectRecovery(key)` |
| Bind and attach at the inspected revision | `agent.recover_session(key)` | `agent.recoverSession(key)` |
| Read the attached key | `session.recovery_key` | `session.getRecoveryKey()` |
| Acknowledge exact unknown effects | `session.acknowledge_unknown(revision, execution_ids)` | `session.acknowledgeUnknown(revision, executionIds)` |
| Forget detached durable state | `client.forget_session(key, revision)` | `client.forgetSession(key, revision)` |

Existing constructors and ordinary Agent session creation keep their signatures and semantics. Python supplies daemon Store arguments through public `SubprocessStdioTransport(..., extra_args=["--session-store", path])`; Java uses its existing daemon/extraArgs constructor.

New public typed values are `RecoveryKey`, `RecoverySnapshot`, `RecoveredRun`, `ModelInputRecord` and `UnknownExecution`. Python uses dataclasses; Java uses records with snake_case JSON fields and normal camelCase accessors. Java recovery revisions, epochs and history revisions use `BigInteger` for the full unsigned 64-bit wire range; acknowledge/forget also have positive-long conveniences. Versioned configuration, serialized model requests and effective options remain JSON values as specified by the shared contract.

Archived `RecoveredRun.snapshot` preserves original identities and is never registered as a new live RunHandle. New attachments receive a new UUID thread ID and new tool binding UUIDs. Agent max steps, timeout, sampling defaults, provider configuration/reference, context configuration and tool declarations travel with persistent creation/attachment. A null Python model override correctly preflights the base model.

## Transaction and secret handling

- Recovery APIs first check optional negotiated `session_recovery.v1`; baseline initialization still requires only the original eight capabilities. Missing recovery support dispatches no persistent request.
- Client-generated UUIDs allow callbacks to be staged before the create/attach RPC. Staged tool/context reverse calls are rejected without execution. Bindings activate only after a validated matching thread/key/epoch response; these operations never resume a model/tool automatically.
- Explicit prepublication refusals `-32601`, `-32602`, `-32020` and `-32021` roll back only staged bindings. All other create/attach failures close the connection, including malformed identity, invalid epoch, IO/timeout/interruption and `STORE_FAILED`. A concurrent close prevents later publication.
- Acknowledge/forget validate revisions and exact IDs; uncertain write outcomes or malformed mutation responses close the connection. The application retains the original key and uses a new client to inspect the durable outcome.
- Recovery keys validate UUID IDs and 64 hexadecimal secret characters. Factories use secure random 32-byte secrets. Python repr/str and Java toString omit the secret. Explicit JSON serialization retains it for application-owned secret storage.
- Raw frame debug logs were removed from both SDK clients/transports, including malformed JSON diagnostics. They retain frame/error category diagnostics without printing recovery request/response bodies.

## Red-to-green evidence and ordinary verification

Initial Python recovery tests failed importing the absent RecoveryKey/API; initial Java tests failed compiling missing recovery types. Subsequent Python regressions demonstrated: zero revision wrongly dispatched, STORE_FAILED acknowledge/forget did not close, debug logs contained the generated recovery secret, and null model override preflighted `None`. Each now passes with the corresponding behavior fixed.

Protocol-peer tests cover fresh binding staging, no callback before attachment activation, ACK identity mismatch, close preventing republishing, explicit refusal versus uncertain IO/store outcomes, preservation of run defaults, typed archived records, inspect/attach/acknowledge/forget flow, no accidental old live handles, optional capability gating and unsigned revisions. Java IO failure verifies all staged bindings disappear while the caller's key remains available. MockTransport did not gain any automatic handshake or persistence behavior.

Commands from repository root:

```sh
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -v
mvn -q -f sdks/java/pom.xml clean test
```

Results on the handoff sources:

- Python: **112 total, 93 passed, 19 environment-gated skips**, no failures/errors. Includes 11 recovery unit tests; the two new production recovery tests are gated.
- Java: **113 total, 87 passed, 26 environment-gated skips**, no failures/errors. Includes 10 recovery unit tests; the two new production recovery tests are gated. No legacy RealStdio environment was enabled in this invocation; enabling it executes its three otherwise skipped tests.
- `git diff --check` passed. Both SDK README local Markdown links resolve.

## Real production test handoff

Selectors: Python `test_recovery_stdio.py` / `RecoveryStdioTests` (2 tests); Java `RecoveryStdioTest` (2 tests). Both require:

```sh
WHALE_RECOVERY_DAEMON=/absolute/path/to/whale-daemon
WHALE_RECOVERY_BASE_URL=http://127.0.0.1:PORT/v1
```

Each test uses an isolated temporary SQLite file and starts the production binary with `--session-store PATH`. The restart test completes a real HTTP/tool run, closes/reaps the owned daemon, opens a new process on the same DB, checks committed history and two completed model input records, reattaches matching host bindings without an effect, then runs a new turn. It explicitly closes the attachment and forgets detached state. The second test rejects a wrong key and changed model configuration, then successfully reattaches the correct configuration with zero model/tool executions.

The HTTP fixture models are `python-recovery-restart` and `java-recovery-restart`: **4 HTTP requests per language** expected across two tool/model turns. `python-recovery-rejected`, `python-recovery-changed`, `java-recovery-rejected` and `java-recovery-changed` are negative setup/configuration cases and should cause **zero HTTP requests**.

```sh
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -p test_recovery_stdio.py -v
mvn -q -f sdks/java/pom.xml -Dtest=RecoveryStdioTest test
```

The root owns actual execution against the completed Store/Core/Daemon and abrupt-crash/external-counter acceptance. The tests above were compiled/discovered but skipped in ordinary verification; no real C4 persistence pass is claimed here yet. Maven is released and SDK sources are frozen pending integration findings.

## Consumer examples and remaining scope

Python `examples/durable_agent.py` persists a newly allocated key with restrictive permissions before its first persistent creation attempt and recovers on subsequent invocations. Unknown effects require application review; the example does not automatically acknowledge them. Java `examples/DurableOperationsController` calls the application's credential-storage callback before dispatch and exposes inspect/recover/reviewed-acknowledgement/forget without a web framework dependency. Neither example logs recovery credentials.

Retention policy, complete plugin lifecycle, matching release installation, native async consumers, observability and full-schema client generation remain outside this C4 SDK implementation. Full-goal completion also depends on the root's Store/runtime/crash acceptance.

## Production acceptance follow-up

The first production pass subsequently completed through `scripts/verify_recovery.py --all-sdks`: six raw-RPC boundary tests passed (9 actual HTTP requests), Python `RecoveryStdioTests` passed 2/2 (4 HTTP), and Java `RecoveryStdioTest` passed 2/2 (4 HTTP). The observed total was **17 HTTP requests**, with zero model dispatch for rejected configurations/keys. The Rust consumer selector is pending root implementation; this invocation covered the two SDKs owned by this report, not an unimplemented Rust test.

The raw verifier covers explicit close/restart and fresh attachment identity; actual stored model input IDs/context/options/usage; credential resolution at HTTP while raw credentials and recovery secrets are absent from stored records; an fsynced external effect followed by SIGKILL before its result; exact acknowledgement before a new turn; two parallel calls with one durable known result and only the pending call becoming unknown; wrong key/active/stale/configuration rejection; competing-process database lock and release on death; and a durable terminal snapshot surviving kill before the application's event-consumption path is invoked. Read-only SQLite inspection synchronizes the precise commit-before-kill windows without changing records. Provider HTTP counts and external-counter contents detect accidental automatic replay.

The verifier's first draft used the incorrect start method `turn.start`, producing MethodNotFound. It was corrected to the existing shared `thread.start_turn`; subsequent rejection assertions require the exact expected error classes (`-32602` or `-32021`) so a missing method cannot pass as a valid rejection. No production changes were required by these runs. The final strengthened raw suite passed again after adding exact error codes, database-lock diagnostics, and credential/model-input checks. Final three-language combined verification remains the root's responsibility once its Rust consumer is ready.


## Final three-language production verification

After the daemon worker confirmed its legacy cancellation guard and all production sources frozen, the production binary was rebuilt and the complete verifier rerun:

```sh
cargo build -p whale-daemon --bin whale-daemon
python3 scripts/verify_recovery.py --all-sdks
```

Both commands passed. Final results: **6 raw-RPC boundary tests + 2 Python + 2 Rust + 2 Java tests**, all passed. Actual HTTP count: **21 = 9 raw boundary + 4 Python + 4 Rust + 4 Java**. Wrong-key/configuration/rejected paths sent zero model HTTP requests. The default `--all-sdks` path now includes `cargo test -p whale-sdk-rust --test recovery_stdio -- --include-ignored` and verifies the Rust model counts exactly; `--rust-test` remains an explicit selector override.

No core/daemon/SDK production fix was needed during verifier integration. SDK sources and verifier are frozen; Maven is released with no running test sessions. These deterministic persistence and crash tests do not establish disk-corruption resilience, cross-platform installation, automatic retention or completion of the broader Agent SDK goal.
