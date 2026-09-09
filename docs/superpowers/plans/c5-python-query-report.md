# C5 Python concurrent query ownership correction

Status: Python source frozen after targeted and complete suite verification. No Java, Maven, Rust, Daemon or integration-runner changes.

## Root cause

`WhaleClient.get_run` checked the weak handle cache separately from installation and assigned the first caller exclusive cleanup responsibility. If caller A created a provisional route and caller B concurrently queried that shared handle, A's RPC rejection/timeout removed the route and failed the shared result future. B could then receive a valid running snapshot yet return a poisoned, unrouted handle. The first caller's terminal restore also closed events despite another query or a real notification having already observed live work.

## Fix

- `client.py:719`: cache lookup, provisional-route installation and query-waiter acquisition are atomic under the existing client lock. Every call still sends its own authoritative `turn.get` and validates response identity.
- Per-handle query-only/waiter/observed-live state lives on the handle, guarded by the client lock. `finally` releases only that caller's ownership. An RPC error never fails the shared run result; a provisional route is removed only when its last waiter leaves and no live work was observed.
- `client.py:147`: notification routing records live observation before dispatching the event to the handle. A running query response also records this observation. Terminal query responses therefore cannot close live event streams before the actual finished notification.
- Only a query-only handle that never observed live work receives snapshot-only event closure. Existing started handles always retain their event route until finished.
- A failed query's exception traceback can keep the weak-cached provisional handle alive. A later retry reacquires a provisional strong route for that same handle, so its successful running response continues to receive actual events. Completed handles are not re-routed.
- `run.py:149`: the old RPC-owning `_restore` helper is replaced by a narrowly scoped snapshot-only event-close helper; query failure cleanup no longer calls `_fail` or unconditionally deletes the weak cache.

## Red → green evidence

New `sdks/python/tests/test_run_query_concurrency.py` uses two real Python query threads and a controllable transport peer. RPC dispatch queues synchronize exact response order; no scheduler sleeps are used. Configured short RPC deadlines intentionally exercise timeout cleanup.

Initial targeted run: **6 tests, 4 failures + 1 error + 1 passed**. Failures proved route loss, shared-result timeout poisoning and early event closure. After fixing those, a further weak-cache retry test failed with a missing route, then passed after provisional route reacquisition. Cross-language review then added the opposite response order: A returns terminal while B is pending and later reports running. That case failed because A closed the route immediately; terminal-only closure now waits for the final query lease.

Final command:

```sh
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -p test_run_query_concurrency.py -v
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -v
```

- Targeted: **8 passed**, 0 failed/errors (0.215 s).
- Complete Python: **129 run, 108 passed, 21 skipped**, 0 failed/errors (1.040 s).

Logs: `/tmp/whale-c5-python-query-red.log`, `/tmp/whale-c5-python-query-retry-red.log`, `/tmp/whale-c5-python-query-green.log`, `/tmp/whale-c5-python-query-full.log`.

Covered boundaries: A error/B running → finished; A timeout/B running → finished; lone timeout releases strong route without fabricating a result; both terminal/running response orders; real stream arrives before terminal query; weak-cached failed-query retry; terminal-only lookup closes its iterator without retaining a strong route. Existing full-suite tests continue to cover cached authoritative expiry, session close, event lag and get timeout on an already started run.

The 21 skipped tests are environment-gated production suites. Root subsequently ran the retention, recovery, session-lifecycle and Provider/Context all-language combinations against the settled source; every explicitly enabled consumer passed. Those observed totals and logs are recorded in the [production verifier report](c5-retention-verifier-report.md).
