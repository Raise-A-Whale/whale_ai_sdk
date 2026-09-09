# C5 production retention verifier

Status: raw production and all-three-SDK integration passed against the production daemon. No production source changes.

## Files and commands

New standalone runner: `scripts/verify_retention.py`. It imports the existing `provider_fixture.ProviderFixture` and reviewed `verify_recovery` pipe/helper implementation without modifying either file. The new peer only adds `--retention-config` alongside `--session-store` for an owned production daemon.

```sh
cargo build -p whale-daemon
python3 scripts/verify_retention.py
python3 scripts/verify_retention.py --all-sdks
```

`--daemon PATH` overrides the production binary. No credentials or external network are required. The verifier SIGKILLs only a daemon child it owns to create a deliberately uncertain external effect. Files live in per-test temporary directories.

## Observed raw result

`cargo build -p whale-daemon` succeeded. `python3 scripts/verify_retention.py` passed **6 tests / 12 real HTTP requests** in 4.911 seconds.

1. An ordinary two-turn quota session completes a real tool loop, then sends no RPC for 600 ms with TTL 100 ms / sweep 20 ms. Get/cancel/approval and duplicate start of the expired ID return `-32030`; the fsynced external effect counter remains unchanged. A fresh second ID executes, and a third admission returns `-32031` with no extra effect.
2. A terminal-count bound of one retires the older ID after two completed loops; duplicate start cannot replay the older effect and the latest result remains queryable.
3. Initial history and actual ModelRequest byte limits stop before provider/tool dispatch: zero HTTP and zero host calls. The initial oversized history fails admission with `-32031`; the actual request limit produces terminal `SESSION_LIMIT_EXCEEDED`.
4. A completed large UTF-8 tool output remains in real SQLite and inspect history despite exceeding the next-dispatch budget. The run terminates with `SESSION_LIMIT_EXCEEDED`, only one model request occurred, and the next turn is rejected without repeating the known effect.
5. Live run expiry does not remove the durable archive or its two actual ModelInputs. The attached record survives idle maintenance. After explicit close and a further RPC-free idle interval, read-only SQLite shows a retired identity tombstone with cleared history/runs, while inspect/recreate reject the same recovery key.
6. SIGKILL after a fsynced tool effect but before its response creates an unknown execution. After restart, a 150 ms detached TTL plus one-session/one-byte retention pressure retires an eligible neighboring record but preserves the unknown record, history and acknowledgement requirement. Reattach/blocked turn/close produce no automatic model/tool replay; subsequent idle maintenance still protects the unresolved record.

SQLite observation is read-only and does not trigger a daemon request. Every idle interval asserts the client's RPC counter stayed constant. Exact per-model HTTP counts reject missing/skipped positive paths and accidental dispatch on rejected paths.

## Three-language integration contract

`--all-sdks` always runs Python, Rust and Java sequentially, with no silent Rust exclusion:

- Python `test_retention_stdio.py` / `RetentionStdioTests` (2 tests).
- Rust `cargo test -p whale-sdk-rust --test retention_stdio -- --include-ignored` (2 expected positive consumers).
- Java `mvn -f sdks/java/pom.xml -Dtest=RetentionStdioTest test` (2 tests).

It supplies `WHALE_RETENTION_DAEMON` and `WHALE_RETENTION_BASE_URL` and verifies each language emitted exactly four requests for `{language}-retention-live` and four for `{language}-retention-durable`. Observed combined result from `python3 scripts/verify_retention.py --all-sdks`: **12 tests passed / 36 HTTP requests**. Raw 6 passed (3.857 s), Python 2 passed (1.123 s), Rust 2 passed / 0 ignored (0.56 s), Java 2 passed / 0 skipped (1.273 s). Each language's exact 8-request model counts passed. The command exited 0; Maven reported BUILD SUCCESS and is released after this run.

## Scope limits

This runner adds production evidence for the selected lifecycle and protection boundaries. Store-level migration/write-failure/attach-vs-GC race tests and failed final-delivery/backpressure protection remain owned by the respective Store/Daemon suites and root acceptance. It does not claim fixed RAM/disk usage, secret erasure, completed installation distribution, native async consumers or completion of the full SDK goal.

## Final regression after all query-ownership fixes

After the Daemon maintenance races and the final Rust/Python/Java concurrent-query ownership corrections, rebuilt the production binary and executed these commands sequentially against the settled source:

| Command | Observed tests | HTTP requests | Log |
| --- | ---: | ---: | --- |
| `python3 scripts/verify_retention.py --all-sdks` | 12 passed | 36 | `/tmp/whale-c5-retention-final3.log` |
| `python3 scripts/verify_recovery.py --all-sdks` | 15 passed | 23 | `/tmp/whale-c5-recovery-final3.log` |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_session_lifecycle.py --all-sdks` | 17 passed | 28 | `/tmp/whale-c5-session-final3.log` |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_provider_http.py --all-sdks` | 26 passed | 110 | `/tmp/whale-c5-provider-final3.log` |

Every command exited 0; every explicitly enabled SDK suite had zero failures, errors or skips. Build log: `/tmp/whale-c5-build-final3.log`. The four combination runners were repeated after the last query-ordering source change; Maven ran sequentially and is released.
