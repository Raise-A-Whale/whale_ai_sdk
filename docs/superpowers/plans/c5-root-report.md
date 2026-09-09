# C5 protocol, Core and Rust SDK report

The working tree adds disabled-by-default retention policy types, optional session admission limits and typed errors. No release, commit or push was performed.

## Protocol and Core

`whale_protocol::retention` defines the frozen policy/limit structs, strict positive-u64 validation, optional capability names and error codes. Absent Agent limits preserve the old serialized shape. Counting writers measure UTF-8 JSON bytes without allocating a second serialized payload; history is measured as one joined array. Protocol tests observed missing fields/types before implementation and now pass 3 cases covering default omission, malformed numbers/fields and full-u64 bounds. Equivalent SDK numeric cases are tested in each language; C5 does not add another generated fixture format.

Core stores optional limits and accepted-turn count, validates input before mutation, then checks current history before context construction and tool dispatch, and the complete ModelRequest before journal/provider dispatch. Known oversized outcomes remain durable and stop subsequent dispatch instead of being discarded. Five targeted cases were red before the new API/checks, then passed: initial Unicode input, restored count, large projected prompt, large model tool-call output, and a large completed tool outcome preserved through failed finalization. Daemon restores the exact accepted ordinal on paths cancelled before Core; direct embeddings must own equivalent journal admission/finalization.

## Rust SDK ownership

`SessionLimits` is re-exported. Agent creation, persistent creation and recovery require `session_limits.v1` before business RPC/bindings. Limits are forwarded in the existing definition; RPC `-32030` and `-32031` become `SdkError::RunExpired` and `SdkError::LimitExceeded`.

Pending runs have strong event routes; completed handles are cached only weakly. Reader processing settles result/closes the event sender before removing the exact route. RunState Drop clears its own weak entry, and session/client close clear indexes. Held results and buffered events remain usable. Remote get/snapshot/control calls never treat cached completion as proof of server retention. Newly queried terminal states end their otherwise empty subscription; a previously live subscription waits for actual finished.

Independent review found a pre-existing query cancellation/concurrency gap made relevant by retention: an abandoned new query left a strong route, while the first failed query could delete a second query's pending route. Cross-language comparison then exposed two more orderings: a real event can precede the query reply, and one query can report terminal while another still-pending query later reports running. Deterministic regressions failed before each correction. `QueryRouteLease` now acquires under the registry entry lock, counts all tentative query waiters and defers snapshot-only closure until the last waiter. The last unsuccessful waiter removes only its own state; any real-event or running observation transfers route lifetime to `finished`. A failed weak-cached provisional handle reacquires its strong route on retry. Both acquisition and release use registry-entry then query-state lock ordering, and one query error never fails a shared handle. All **12** targeted Rust ownership/capability tests pass.

## Production consumer and observed evidence

`tests/retention_stdio.rs` drives the production daemon through public APIs with local HTTP and real SQLite. Both cases run two complete model/tool loops, idle with no RPC until TTL has elapsed, confirm typed expiry and preserved results/buffers, reject a third run with no effects, and preserve the durable archive/count through attachment. Each case makes exactly four HTTP requests.

- Fresh `cargo test --workspace`: **347 passed, 0 failed, 14 ignored** after all query-ordering corrections. Per crate: protocol 37, adapters 33, Core 56, Daemon 96, Rust SDK 86, Store 39. Ignored fixture consumers are enabled by their dedicated runners.
- Final production retention runner after these fixes: **12 tests / 36 HTTP**, including Rust 2 / 8 HTTP with no ignored tests.
- Final recovery runner: **15 tests / 23 HTTP**; final session lifecycle runner: **17 tests / 28 HTTP**. Existing provider HTTP regression also passed **26 tests / 110 HTTP**; later changes only affect route ownership and maintenance.
- Python complete suite: **129 run, 108 passed, 21 skipped**; Java clean suite: **131 run, 103 passed, 28 skipped**. Each language also has deterministic concurrent-query tests for error/cancellation, both terminal/running reply orders, real-stream-before-reply and weak-cache or initialization-cancellation ownership.
- Final hygiene: rustfmt checked all **103** changed/new Rust files; `git diff --check` passed; **233** local links across **39** Markdown files resolved.

Python/Java query ownership corrections are recorded separately, with final language and production-process counts in the C5 ledger. The broad goal remains active: plugin lifecycle, native async consumers, complete protocol/client generation, observability and matched installation/distribution are not complete. Byte policies are admission budgets, not fixed memory, disk or token guarantees; ID tombstones and caller-held payload remain.
