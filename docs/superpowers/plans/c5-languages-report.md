# C5 Python / Java implementation report

Status: SDK source frozen for root production integration. No commit or release.

## Public APIs and wire

- Python exports frozen `SessionLimits(max_accepted_turns=None, max_history_bytes=None, max_model_request_bytes=None)`, `RunExpired(RpcError)` (`-32030`) and `LimitExceeded(RpcError)` (`-32031`). `AgentDefinition.limits` is appended with a default of `None`; `WhaleClient.create_thread(..., limits=None)` preserves existing call sites.
- Java exports `SessionLimits(BigInteger, BigInteger, BigInteger)` and a builder accepting positive `long` / `BigInteger`; its record accessors preserve the complete u64 range. `AgentDefinition.Builder.limits(...)` and an additional longest `createThread` overload preserve the older overloads. `RunExpiredException` and `LimitExceededException` extend `RpcException` with the same codes.
- All three provided fields must be positive u64 integers. JSON booleans, floats, strings, overflow and unknown fields are rejected. Absent limits preserve default serialization. Ordinary create, persistent create and recovery attach forward `session.limits`. A configured limits object requires `session_limits.v1` before business publication; the existing eight baseline initialization requirements remain unchanged.

## Ownership and authoritative queries

- Python retains its existing `WeakValueDictionary` handle cache and identity-conditional strong-route removal after finished result/event handling. `RunHandle.snapshot()` and cached `client.get_run()` now perform `turn.get` instead of returning a stale local terminal snapshot.
- Java now has separate strong active routes and weak cached handles. A `ReferenceQueue` removes collected identity entries on incoming/client lookup/finish activity; this is opportunistic identity cleanup, not a dedicated maintenance thread. Completed payloads themselves are immediately collectable when the application drops its handles/futures/subscriptions.
- Java records the finished envelope, schedules both completion futures on its worker executor, publishes/closes event subscriptions, then removes the exact strong route. The scheduled tasks retain the handle until completion. This preserves the rule that a reader must never execute user CompletableFuture continuations. `close()` uses orderly executor shutdown, and dispatch after shutdown uses an asynchronous fallback. A deterministic test queues both completion tasks behind a barrier, closes the client after strong-route removal, then releases the barrier and obtains both completed futures.
- Held results and event buffers remain readable after remote expiry. Cached get/snapshot, cancel and approval remain remote operations; typed expiry does not overwrite a delivered result. Existing terminal-query-before-finished tests preserve live subscriptions, and late finished notifications cannot recreate routes.

## Test evidence

Red: new Python retention tests initially failed importing the absent `SessionLimits`; Java retention compilation initially failed on missing SessionLimits/error/cache APIs. Implemented the APIs and routing changes, then migrated the old cached-get mock peer to answer an actual `turn.get` without weakening its route-preservation assertions.

Green commands (no gated process environment configured):

```sh
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -v
mvn -q -f sdks/java/pom.xml clean test
```

- Python: **121 run, 100 passed, 21 skipped**, no failures/errors.
- Java: **122 run, 94 passed, 28 skipped**, no failures/errors. Three preexisting real-stdio tests are included in the skipped count because `WHALE_JAVA_STDIO_FIXTURE` was not set.
- C5 unit additions: Python 7 tests; Java 7 tests. They cover typed expired queries with retained results/events, dropped-handle collection, dead weak identity cleanup, late events, strict numbers/default omission, capability refusal, ordinary/persistent/attach forwarding and Java queued-result close safety. Existing run lifecycle tests continue to cover query-before-finished and nonblocking continuations.

## Actual-process consumers ready for root runner

Both suites require `WHALE_RETENTION_DAEMON` and `WHALE_RETENTION_BASE_URL`, use real production stdio with `--retention-config` and `--session-store`, and skip explicitly when either environment variable is absent.

- Python file `sdks/python/tests/test_retention_stdio.py`, class `RetentionStdioTests`:
  - `test_idle_expiry_preserves_result_and_quota_counts_expired_ids`
  - `test_live_expiry_keeps_durable_archive_and_limits_across_attach`
- Java class `com.whale.ai.RetentionStdioTest`:
  - `idleExpiryPreservesResultAndQuotaCountsExpiredIds`
  - `liveExpiryKeepsDurableArchiveAndLimitsAcrossAttach`

Each language expects exactly 8 real HTTP requests: `{language}-retention-live` 4 and `{language}-retention-durable` 4. Each model performs two tool-loop turns, then admission rejects a third turn with no tool effect. Tests use a 100 ms terminal TTL / 20 ms sweep and a 500 ms RPC-free interval, verify authoritative expiry plus held result/event survival, and check a new turn ID still succeeds. The durable case additionally inspects actual model input archives after live expiry and checks accepted-turn limits across close/reattach with a fresh session ID. At this worker's handoff, the gated cases had been compiled and discovered but not yet run; the root verification addendum below records their later execution against the final C5 daemon.

## Root verification addendum

The worker counts above describe the first bounded language implementation pass. Subsequent shared query-ownership review added concurrency regressions in both clients. The settled Python suite is **129 run, 108 passed, 21 skipped**; the settled Java clean suite is **131 run, 103 passed, 28 skipped**. Root then enabled the gated retention consumers against the final production daemon: Python and Java each passed 2 tests / 8 HTTP requests, as part of the **12 tests / 36 HTTP requests** all-language retention combination. See the [Python query report](c5-python-query-report.md), [Java query report](c5-java-query-report.md) and [production verifier report](c5-retention-verifier-report.md).

## Limits and remaining work

Retention is configured and disabled by default. Caller-owned payloads and lightweight identity fences remain; no constant-memory promise. Admission byte budgets are not token estimates and do not discard completed external outcomes. README examples describe configuration and optional process consumers. Full schema generation, native asynchronous Python consumers, matched release installation and other full-goal work remain outside this task.
