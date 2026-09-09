# Retention and session limits implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for independent tasks and evidence-backed reviews. Existing goal authorization covers implementation; do not commit, push or release.

**Goal:** Implement autonomous protected payload retention plus explicit session admission limits for a long-running, reusable Agent SDK.

**Architecture:** Daemon owns live run expiration and autonomous maintenance; Store owns CAS-safe durable payload retirement. Core checks admission budgets, while SDKs release completed routing ownership and preserve caller-owned results.

**Tech Stack:** Existing Rust/Tokio/Serde/SQLite, Python and Java clients; no new runtime service.

**Spec:** [Retention design](../specs/2026-09-08-retention-design.md). Read it before implementation; its exact names, wire types and protection rules are the shared contract.

## Global constraints

- Disabled-by-default retention and optional per-Agent SessionLimits preserve the default wire and history contract.
- Active work, failed terminal delivery and unacknowledged unknown executions are never automatically deleted.
- Expired accepted IDs cannot execute again. Store tombstones and closed-session fences remain effective.
- No byte-to-token equivalence or constant RAM/disk claim; preserve completed results before refusing later dispatch.
- No concurrent Maven jobs. Language worker owns Maven until explicitly released. All workers own only their paths below.
- Core/Store callers cannot bypass configured admission checks. No async storage await while holding synchronous lifecycle/registry locks.

## Task 1 — Protocol, Core and Rust SDK (root)

Files: create `crates/whale-protocol/src/retention.rs`, `tests/retention_contract.rs`; modify protocol AgentDefinition/StartThreadParams exports; modify Core session/engine/error; add `crates/whale-core/tests/session_limits.rs`; update Rust SDK Agent/client/run and add retention tests/production consumer.

Interfaces: exact protocol structs/constants in spec. Core `ThreadSession::set_limits(Option<SessionLimits>)`, `limits() -> Option<&SessionLimits>`, `check_run_admission(&[CanonicalItem]) -> Result<(), CoreError>`, `restore_accepted_turns(u64)`, and `accepted_turns() -> u64`. `CoreError::LimitExceeded(String)` maps to SESSION_LIMIT_EXCEEDED. Set limits validates; use `set_limits(...)?` returning `Result<(), CoreError>`.

- [x] Write protocol tests for disabled default, positive numeric bounds, bool/float rejection, unknown fields and omission-preserving Agent/Session serialization; run `cargo test -p whale-protocol --test retention_contract` and observe missing-feature failure.
- [x] Implement frozen types, validation, fields and matching numeric/omission cases in the protocol and language tests. Update owned Rust literal initializers; notify workers when protocol compiles.
- [x] Write Core tests proving rejected initial history invokes zero context/model/tools and makes no history change; oversized actual model projection includes tools/system/options in budget; completed tool outcome survives budget failure and no next model request occurs. Observe failures, implement entry/precontext/prerequest/pretool checks and direct accepted-count tracking, then run Core suites.
- [x] Implement Rust capability check and Agent limits forwarding. Write reader ownership/expired query tests first; release strong routes only after finished, maintain weak handle cache cleanup, keep delivered results and existing event ordering.
- [x] Add Rust actual process consumer for idle expiration, preserved result, quota rejection and durable archive independence.

## Task 2 — Store retirement (upstream_comparison)

Files: `crates/whale-store/**` only; add Store report at `docs/superpowers/plans/c5-store-report.md`.

Consumes: protocol StoreRetentionPolicy/SessionLimits. Produces: `StoreRuntime::sweep_retention(&self, &StoreRetentionPolicy, now_ms: u64) -> Result<RetentionReport>`; public report fields as in spec. Implement metadata/migration, protected backend retirement and begin-run quotas; coordinate any trait extension names with root/runtime worker before consumer integration.

- [x] Write deterministic memory tests for TTL edge, active/unknown protection, quota/bytes, retired identity rejection, attach-vs-GC race and canceled sweep waiter. Run and record red.
- [x] Implement v2 metadata and v1 migration grace, safe trait defaults/optimized metadata page scanning, owned sweep serialization and checked backend retirement. Keep ordinary CAS append-only constraints.
- [x] Add actual SQLite old-schema reopen, UTF-8 payload release, competing attach/write-failure and cancellation tests; prove only payload disappears and unknown/active records survive pressure.
- [x] Run feature-isolated and SQLite suites, review backward behavior and write exact result/API report. Do not edit Daemon/Core/SDK paths.

## Task 3 — Daemon registry and maintenance (runtime_implementation)

Files: `crates/whale-daemon/**` only; add `c5-daemon-report.md` under plans.

Consumes: protocol RetentionPolicy/SessionLimits/errors, Core methods and Store sweep. Produces: `with_retention_policy(self, RetentionPolicy) -> Result<Self, String>`, CLI `--retention-config PATH`, optional capabilities and effective autonomous maintenance.

- [x] Write failing TTL/delivery barrier/count/duplicate-start tests using controlled Tokio time and a real idle-clock test; verify that current full records remain retained and old ID behavior differs from required expiration.
- [x] Extract focused run registry/retention state and preserve existing lifecycle lock order. Retire only successful final-delivery records, keep lightweight accepted-ID tombstones and ensure get/cancel/approval/start reject expired identities after ownership checks.
- [x] Wire one autonomous, stoppable worker without server ownership cycle; have it call Store sweep and report failures without false successful cleanup. Add CLI parsing/validation and stop/drop tests.
- [x] Enforce accepted-ID/history admission before run publication/journal.begin_run; attach restores archived accepted count and limits; Core performs actual execution checks. Persist limits in normalized configuration and reject mismatched attach.
- [x] Test quota across expired/cancelled/archived runs, exact zero dispatch, close/EOF races, persistent archive independence and old default behavior. Run Daemon full suites and report.

## Task 4 — Python/Java ownership and limits (python_implementation)

Files: `sdks/python/**`, `sdks/java/**` only, plus `c5-languages-report.md` under plans. Own Maven until handoff.

Consumes: SessionLimits wire, session_limits.v1, RUN_EXPIRED=-32030, SESSION_LIMIT_EXCEEDED=-32031. No turn.expired notification.

- [x] Add failing tests for cached snapshot/get receiving authoritative expiry, held result/buffer survival, dropped-handle collection, terminal query before finished and late event non-resurrection.
- [x] Implement exact strong-route release after reader finished, Java weak reference queue cleanup, Python weak-cache semantics, typed expiry errors, and retained result independence.
- [x] Add portable SessionLimits validation/Agent config with capability gating before ordinary/persistent creation; forward limits on recovery and preserve default serialized output.
- [x] Add real-process gated consumers using `WHALE_RETENTION_DAEMON` and `WHALE_RETENTION_BASE_URL`; configure daemon with `--retention-config` and local JSON. Complete a real tool loop, idle until expired, prove old result readable/new authoritative query expired/new ID succeeds/quota rejects and SQLite archive remains.
- [x] Run targeted then complete language suites, hand over Maven and record results/API details.

## Task 5 — Production acceptance and documentation (root, delegate after workers finish)

Files: `scripts/verify_retention.py`, docs API/architecture/README/ledger.

- [x] Use production Daemon with controlled HTTP, real clocks and SQLite. After terminal delivery, send no new request until TTL passes; then verify expiry and preserved client results. Test duplicate raw start with same ID against an external counter.
- [x] Verify a detached SQLite record actually retires on idle maintenance, active/unknown records remain under quota pressure, v1 migration survives reopening and archived results outlive live registry TTL.
- [x] Check SessionLimits at actual model/tool boundary and three-language consumers. Run C4 recovery and existing nonpersistent regressions after new source settles.
- [x] Independently review production concurrency/ownership and documentation claims, fix concrete findings with regressions, run formatting/local-link checks, and record only observed results.
- [x] Complete C5 ledger when evidence covers every spec requirement. Keep automatic compaction, tokenization-specific projection budgets, plugin lifecycle, native async clients, observability, full generation and matched distribution accurately scoped; do not mark full goal complete while its required work remains.

## Final C5 acceptance ledger

Observed on the settled 2026-09-08 working tree. Suite totals overlap; they are listed by boundary and are not added into one score.

| Boundary | Final observation | Evidence |
| --- | --- | --- |
| Rust workspace | **347 passed, 0 failed, 14 ignored**; protocol 37, adapters 33, Core 56, Daemon 96, Rust SDK 86, Store 39 | `/tmp/whale-c5-workspace-final3.log` |
| Store feature isolation | SQLite/default **39 passed**; no-default-features **26 passed** | [Store report](c5-store-report.md) |
| Daemon retention/lifecycle | **96 passed, 0 failed** | [Daemon report](c5-daemon-report.md) |
| Rust route ownership/limits | **12 passed**, including both terminal/running query orders, failure/cancellation, real-stream-first and retry | `/tmp/whale-c5-rust-query-final.log`; [root report](c5-root-report.md) |
| Python SDK | **129 run, 108 passed, 21 skipped**; dedicated query suite **8 passed** | `/tmp/whale-c5-python-final2.log`; [query report](c5-python-query-report.md) |
| Java SDK | **131 run, 103 passed, 28 skipped**; final query/handle/retention classes **31 passed** | `/tmp/whale-c5-java-final2.log`; [query report](c5-java-query-report.md) |
| Retention production combination | Raw boundary 6 + Python 2 + Rust 2 + Java 2 = **12 passed / 36 HTTP** | `/tmp/whale-c5-retention-final3.log`; [verifier report](c5-retention-verifier-report.md) |
| Recovery production regression | Raw boundary 9 + Python 2 + Rust 2 + Java 2 = **15 passed / 23 HTTP** | `/tmp/whale-c5-recovery-final3.log` |
| Session lifecycle production regression | Public boundary 2 + Python 5 + Rust 5 + Java 5 = **17 passed / 28 HTTP** | `/tmp/whale-c5-session-final3.log` |
| Provider/Context production regression | Python 13 + Rust 3 + Java 10 = **26 passed / 110 HTTP** | `/tmp/whale-c5-provider-final3.log` |
| Repository hygiene | rustfmt **103 files**; diff whitespace clean; **233** local links across **39** Markdown files resolved | `/tmp/whale-c5-rust-files.txt`; final root checks |

The review found two Daemon maintenance/identity races and cross-language query ownership orderings. Each concrete failure received a deterministic regression. The independent reviewer covered the first Daemon and Rust query fixes; the last terminal-first/running-later correction and all final suite executions are root evidence rather than relabeled independent evidence. See the [independent review](c5-independent-review.md).

C5 is complete within its frozen scope. Retention stays disabled by default; byte limits remain admission budgets rather than token, RAM or disk guarantees; active and unknown work remains protected; accepted-ID and recovery tombstones can accumulate. Automatic compaction, plugin lifecycle, native async clients, full protocol/client generation, observability and matched installation/distribution remain part of the still-active broader SDK goal.
