# C5 Java concurrent query ownership correction

Status: source verified by targeted and complete Maven suites. No Rust, Python, Daemon or integration-runner changes are attributed to this repair.

## Root cause and correction

`WhaleClient.getRun` previously separated lookup from registration and let the first waiter remove and fail the shared handle on any RPC error. Concurrent caller B could receive a valid running snapshot after caller A failed or was interrupted, but its route/result had already been removed or poisoned. A terminal query could also synthesize an archive terminal event while another query had not yet reported running.

The client now acquires a per-handle query lease atomically under the lifecycle lock. `RunHandle` counts waiters and records real-event/running observations plus a terminal query candidate. Query failure is local to its RPC. The final waiter either removes an unobserved failed placeholder, synthesizes the saved terminal for an archive-only lookup, or leaves an observed live route until the real `finished` event. Result completion remains dispatched through the client executor, so reader and lifecycle locks do not run user `CompletableFuture` continuations.

Cancellation propagation was fixed at the request boundary. `requestAsync` owns a distinct outer future and cancels only that invocation's `requestRaw` future. Cancellation while the shared initialization is pending neither cancels initialization nor dispatches the abandoned business RPC later. Once a raw request exists, cancellation removes it from pending maps through the existing completion hook.

## Evidence

`QueryRouteTest` controls two Java threads and exact response order. Its first run exposed five assertion failures, two poisoned-result errors and one already-correct cleanup case. The final nine query cases cover A-error/B-running, A-interrupt/B-running, both terminal/running response orders, terminal plus failed peer, a real stream preceding the query reply, failed and interrupted single-placeholder cleanup, and cancellation during shared initialization.

Targeted `QueryRouteTest` + `RunHandleTest` + `RetentionTest`: **31 passed** after the final shared-initialization case (`/tmp/whale-c5-java-targeted-final3.log`). Fresh `mvn -f sdks/java/pom.xml clean test`: **131 tests, 103 passed, 28 skipped, 0 failures/errors**. Environment-gated production cases are enabled by the root integration runner rather than the ordinary clean suite. Final clean log: `/tmp/whale-c5-java-final2.log`.

Matched binary/process validation is recorded in the C5 ledger and [production verifier report](c5-retention-verifier-report.md) after the final Rust and Python query corrections.
