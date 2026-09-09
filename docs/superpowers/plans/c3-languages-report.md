# C3 Python / Java language implementation report

Scope: only `sdks/python/**`, `sdks/java/**`, and this report. No commit or push.

## Public behavior

- Python: `WhaleClient.initialize() -> InitializeResult`; exports immutable `PeerInfo`, `InitializeResult`, and `ProtocolCompatibilityError` (a distinct `ConnectionError` subtype).
- Java: `WhaleClient.initialize() -> InitializeResult`; public records in `com.whale.ai.models` and `ProtocolCompatibilityException` in `com.whale.ai`.
- Constructors preserve their previous signatures and do not start a second eager initialization path. Every ordinary central RPC joins the connection-owned initialization. The handshake sends the SDK identity, protocol version list `[1]`, and all eight required capabilities, with a fixed 10-second request deadline.
- Parsing rejects boolean, floating-point, string, zero, negative, oversized, missing, null, and unoffered versions; invalid peer identities; non-array, duplicate, malformed or missing required capabilities. Extra fields and valid extra capabilities are accepted. Both SDKs consume the same 24-case Rust-generated fixture, including 4 valid cases.
- A failed initialization closes its owned transport/process before publishing the shared failure. Concurrent and later initializers join that completion, including while cleanup is blocked. A first initialization after an already-observed EOF also performs owned-resource cleanup and returns a compatibility error. Successful cached initialization does not reopen an explicitly closed client.
- Reader-side readiness checks are nonblocking. Pre-ready reverse requests receive `-32010` without registering or executing tool/context callbacks; pre-ready business notifications are ignored. Bootstrap replies continue through the response path. Rejection writes run outside the reader callback.
- Public `MockTransport` defaults were not changed. Existing business unit peers explicitly negotiate through test-only helpers, then install their original business handlers. The Python literal EOF subprocess test now explicitly replies to initialization before its original readiness/EOF scenario.

## Red-to-green evidence

1. Initial Python tests failed with missing `initialize`, typed result and compatibility error APIs; Java failed compilation for the corresponding missing symbols. The new initialization suites now pass.
2. Shared JSON fixture tests validate all 24 response cases through actual client initialization, not only a standalone parser. A separate Java Unicode whitespace test first failed because `String.strip()` did not reject NBSP; validation now handles those whitespace boundaries.
3. Independent review reproduced pre-ready host execution and event delivery. Python returned a tool result instead of `-32010`; Java observed code `0`, a host invocation, and a delivered notification. Tests now prove no host side effects for both New and Initializing states, with the reader still able to accept the subsequent handshake reply.
4. Close-barrier tests first observed a second initializer/ordinary request fail before cleanup finished. They now keep every waiter pending until the explicit barrier is released; no arbitrary sleep establishes correctness.
5. `ProcessCleanupTest` first observed `ProcessStdioTransport.close()` return while a controlled, legally asynchronous `destroyForcibly()` operation still reported the process alive. Close now serializes cleanup, waits for forced termination to be reaped, and restores an interrupted caller's interrupt status afterward.
6. Actual `early_eof` tests use a close-handler latch to wait until EOF has been processed before calling initialize. They verify zero outgoing requests and a dead PID before explicit client close. Actual `old_peer_ignore_term` verifies Java's force-kill path against a subprocess ignoring SIGTERM.

## Verification

Python full suite (from `sdks/python`):

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Result: **99 tests, 82 passed, 17 environment-gated skips**, no failures/errors.

Java clean full suite (without any HTTP/native/stdio fixture variables):

```sh
mvn -q -f sdks/java/pom.xml clean test
```

Result: **101 tests, 77 passed, 24 environment-gated skips**, no failures/errors. With only `WHALE_JAVA_STDIO_FIXTURE` enabled, the expected classification is 80 passed / 21 skipped; the root will verify that environment separately.

Dedicated real stdio failure tests were run separately for **old_peer**, **early_eof**, and **old_peer_ignore_term**, with **2/2 passing per language per mode**. These check process/PID exit before the test's explicit `client.close()` / try-with-resources cleanup. Old-peer modes log exactly one initialization and no business RPC. The explicitly synchronized early-EOF mode logs zero requests.

Invocation pattern (run from the repository root; use separate log paths per language/run):

```sh
WHALE_PROTOCOL_PEER="$PWD/scripts/protocol_peer_fixture.py" \
WHALE_PROTOCOL_PEER_MODE=early_eof \
WHALE_PROTOCOL_PEER_LOG=/tmp/whale-python-c3-early.jsonl \
WHALE_PROTOCOL_PEER_LANGUAGE=python PYTHONPATH=sdks/python/src \
python3 -m unittest discover -s sdks/python/tests -p test_initialization_stdio.py -v

WHALE_PROTOCOL_PEER="$PWD/scripts/protocol_peer_fixture.py" \
WHALE_PROTOCOL_PEER_MODE=early_eof \
WHALE_PROTOCOL_PEER_LOG=/tmp/whale-java-c3-early.jsonl \
WHALE_PROTOCOL_PEER_LANGUAGE=java \
mvn -q -f sdks/java/pom.xml -Dtest=InitializationStdioTest test
```

## Root integration handoff

- Python selector: `test_initialization_stdio.py`, class `InitializationStdioTests`, two tests.
- Java selector: `InitializationStdioTest`, two tests.
- Supported root modes: `old_peer`, `wrong_version`, `missing_capability`, `boolean_version`, `eof`, `no_reply`, `old_peer_ignore_term`, `early_eof`.
- `no_reply` uses the real 10-second handshake deadline; the root owns the final eight-mode/all-SDK matrix. It was not shortened in the gated tests. Python's fast unit timeout test patches only the timeout constant in that test; Java's unit timeout test waits for the actual deadline.
- Environment distinction: ordinary Java tests skip three `RealStdioTest` cases when `WHALE_JAVA_STDIO_FIXTURE` is absent. Enabling that variable moves those three cases from skipped to executed; it does not add tests. Other environment-gated suites have their own variables.
- README sections describe explicit versus automatic readiness, strict failure/cleanup behavior, connection feature capabilities versus model capabilities, and migration from prototype peers with no handshake.
- Package release installation, native Python async consumers, broader schema/code generation, storage/recovery/retention, plugin lifecycle and observability remain outside this C3 scope.

## Subsequent Java stdio cancellation investigation

The root's full suite with `WHALE_JAVA_STDIO_FIXTURE` enabled exposed an intermittent `RealStdioTest` cancellation failure. The default-environment counts above remain historical results; the enabled-fixture outcome requires the daemon fix and a new full verification. Only the two cancellation assertions were enhanced to print the complete result and a fresh `turn.get` snapshot on failure; expected statuses and timeouts are unchanged. No Java production source was changed.

```sh
WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" \
mvn -q -f sdks/java/pom.xml -Dtest=RealStdioTest test

WHALE_JAVA_STDIO_FIXTURE="$PWD/target/debug/examples/sdk_fixture" \
mvn -q -f sdks/java/pom.xml '-Dtest=RealStdioTest#realStdioApprovalCancellationAndReuse' test
```

The first command passed all three tests. Repeating the second command against the same binary passed 19 times, then failed on attempt 20 at the first pending-run cancellation. Both the returned result and fresh daemon snapshot had `status=failed`; the snapshot reported `error.code=RUN_FAILED`, `error.message="Internal error: Run cancelled"`, `last_seq=3`, empty pending approvals/tool executions and zero usage. The root independently reproduced the related message `Internal error: Run cancelled during model context construction` through Python and the same fixture. Java's RunHandle forwards the daemon result without changing its terminal status. These observations attribute the failure to runtime cancellation arbitration, not Java initialization or enum conversion. Maven ownership has returned to the root for verification after the runtime fix and fixture rebuild.
