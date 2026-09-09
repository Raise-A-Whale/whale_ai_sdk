# Durable SessionStore and recovery

Status: C4 implemented and verified in the current working tree on 2026-09-08. All C4 tasks and acceptance requirements below are complete. The full Agent SDK goal remains active: retention, plugin lifecycle, native async consumers, observability, complete protocol generation and matched release/clean installation are still required. No commit, push or release has been made.

## Design choices and boundaries

Use a pluggable transactional SessionStore, with MemoryStore and optional SQLiteStore. A small independent `whale-store` crate depends on protocol types, not the model runtime; Core uses its journal and Daemon enables SQLite. This new dependency boundary keeps SQLite optional for embedded consumers and lets storage implementations be reused independently. SQLite uses WAL with synchronous FULL and an exclusive process-held lock; startup refuses a competing writer. Files are local storage, not a distributed shared-database service. Sources checked: [SQLite durability](https://www.sqlite.org/pragma.html#pragma_synchronous), [WAL](https://www.sqlite.org/wal.html), [rusqlite](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Connection.html).

Saving only a terminal snapshot loses completed parallel tool outcomes on a crash. Saving event notifications is also insufficient because enqueuing a tool event does not await durable storage. Therefore Core directly awaits a run journal: validated model requests before provider dispatch, model items before publication, tool dispatch intent before host execution, each individual result immediately after completion, and authoritative history/result before terminal publication. Write failure stops the run and further dispatch; it is not a tool error the model can ignore.

Recovery restores history and results, never automatically resumes a model call, repeats a tool, or revives old approvals. A dispatched call without a durable result becomes an explicit unknown execution. A model call never dispatched is distinguishable from a tool whose effect is unknown. Stable result IDs and idempotent settlement avoid generating duplicate error results across restarts. New turns are blocked until the application acknowledges the exact unknown execution IDs at the current revision; acknowledgment does not mean the tool succeeded or stopped.

Durable recovery ID is separate from the current live thread ID. Every attachment uses a fresh thread ID and binding IDs. Original archived RunSnapshots retain the original thread/turn identities and remain queryable via recovery inspection; new RunHandles belong to the new live thread. This preserves existing closed-handle semantics without adding an epoch field to every legacy RPC. Store writes carry an internal epoch/owner lease, so old actors cannot overwrite a newer attachment.

Durability is explicit: existing `create_session` stays unchanged. Applications allocate a `RecoveryKey` before calling `create_persistent_session(key)` and retain it even if a creation response is lost. The key includes a random recovery ID and an unguessable secret; only its SHA-256 digest is stored. Duplicate creation cannot execute new work; after an uncertain create outcome the application can inspect/attach using the retained key. SDK/Daemon diagnostic output must not include these secrets or raw recovery messages.

`close` cancels and releases the live attachment, retaining durable data. `forget` is a separate explicit operation allowed only while detached, with revision validation; it leaves an authenticated tombstone to prevent resurrection. TTL/size retention remains part of the full goal and will operate on these durable boundaries rather than delete active or unresolved records.

## Frozen public protocol

New methods (ordinary RPC, still requiring C3 initialization):

- `session.create_persistent`: `{key, session: StartThreadParams, run_defaults}` -> `{thread: StartThreadResult, key, epoch}`.
- `session.recovery.inspect`: `{key}` -> `RecoverySnapshot`.
- `session.recovery.attach`: `{key, expected_revision, session: StartThreadParams, run_defaults}` -> the same persistent Session result, with fresh live thread ID.
- `session.recovery.acknowledge`: `{key, expected_revision, execution_ids}` -> `RecoverySnapshot`; only the attached owning connection can acknowledge.
- `session.recovery.forget`: `{key, expected_revision}` -> `{recovery_id, forgotten}`; active records are rejected.

`RecoveryKey { recovery_id: String, secret: String }` has redacted Debug output. `SessionRunDefaults { max_steps: usize, timeout_ms: Option<u64> }` preserves Agent defaults which previously lived only in SDK handles. `RecoverySnapshot` contains recovery ID, revision, epoch, attached flag, sanitized configuration, full committed history, archived run snapshots/model input records and unknown executions. It never exposes the digest, connection owner or callable bindings.

Configuration is a versioned JSON object containing sanitized `StartThreadParams` and `run_defaults`. Strip live session and host binding IDs; normalize tool ordering; preserve provider/config/reference, model, prompt, options, Agent name, tool schema/approval/parallel declarations and ContextPolicy kind/settings. Persist environment variable names, never resolved credential values or HTTP headers. Attach must match the saved configuration exactly after sanitization and validate newly supplied tools/context/provider before claiming a lease. Missing/extra/stale definitions fail without partial publication.

Advertise optional `session_recovery.v1` only when a Store is configured and startup recovery succeeds. Existing SDKs still require only the eight C3 baseline capabilities. Recovery APIs explicitly check this optional capability; an old peer cannot silently ignore their persistence requirements. Existing wire structs and constructor signatures remain unchanged.

SDK surfaces: `RecoveryKey.new()`, `Agent.create_persistent_session(key)`, `Agent.recover_session(key)`, `client.inspect_recovery(key)`, `session.acknowledge_unknown(revision, execution_ids)`, `client.forget_session(key, revision)`, and `session.recovery_key`. Java/Rust use their normal naming conventions. Inspect exposes archived RunSnapshots and actual model input records; it does not pretend old live handles are attached. Recovery allocates and stages callback bindings before dispatch, validates response identity and rolls back rejected transactions. A dispatched request with unknown outcome fails the connection closed; Rust owns the transaction independently of an abandoned waiter.

## Store and journal contract

The `whale-store` crate owns serialized SessionRecord/StoredRun/ModelInputRecord/StoredCall types, a CAS backend trait, MemoryStore, optional SQLiteStore, and a StoreRuntime facade. Record configuration and model request payloads are versioned JSON values; their writers are the typed protocol/ModelRequest serializers. Backend state validates revisions, owner/epoch leases, run identities and operations.

`SessionStore: Send + Sync` provides `durable() -> bool`, async `create(SessionRecord)`, `load(&str) -> Option<SessionRecord>`, `compare_exchange(id, expected_revision, replacement)` and `list()`. Creation rejects an existing recovery ID; CAS rejects mismatched revision and malformed replacement. SQLite executes blocking transactions outside async executor threads. MemoryStore follows the same CAS contract but reports no cross-restart durability.

`StoreRuntime::open(Arc<dyn SessionStore>)` performs startup recovery before being published. It exposes async `create(key, configuration, owner, thread_id) -> SessionJournal`, `inspect(key) -> RecoverySnapshot`, `attach(key, expected_revision, configuration, owner, fresh_thread_id) -> SessionJournal`, and `forget(key, expected_revision) -> bool`. A daemon creates this facade once per exclusively owned store. Active records cannot be attached or forgotten. Startup recovers each interrupted run, detaches prior leases and never calls any provider/tool.

`SessionJournal` is cloneable and tied to a recovery ID, owner, live thread ID and epoch. It serializes accepted commands through an owned FIFO worker; dropping an individual waiter does not cancel an accepted commit. Reads and settlement join the same queue. An I/O/CAS failure poisons the live journal, preventing later work from assuming an uncertain commit did not happen. The backing record remains inspectable from storage and is recovered after reopening.

Public journal methods to freeze before consumers compile:

```text
recovery_id() -> &str; epoch() -> u64
record().await -> SessionRecord
begin_run(params: StartTurnParams, snapshot: RunSnapshot, effective_options: Value).await -> bool
commit(turn_id: &str, mutation: RunMutation).await -> CommitReceipt
finalize(turn_id: &str, candidate: RunSnapshot).await -> FinalizedRun { snapshot, history }
replace_configuration(configuration: Value).await -> ()
acknowledge(expected_revision: u64, execution_ids: Vec<String>).await -> RecoverySnapshot
detach().await -> ()
```

`begin_run` atomically saves run parameters, history start, effective options and input items before acceptance; a matching duplicate returns false, a conflicting duplicate fails. No active run or unresolved unknown may precede a new run. `RunMutation` variants:

```text
ModelInput { step_id: String, step_index: usize, request: Value }
ModelItem { step_id: String, item: CanonicalItem }
ModelStepFinished { step_id: String, usage: UsageMetrics }
DispatchIntent { call_id: String, execution: ToolExecutionRecord }
ToolOutcome { call_id: String, result: CanonicalItem }
ToolBatch { call_ids: Vec<String> }
```

ModelInput records the actual current history revision. ModelItem persists complete canonical items and allocates stable result/execution IDs for tool calls. A DispatchIntent requires a completed model step and cannot be accepted twice, even with identical parameters; it is not a retry instruction. ToolOutcome may also represent a rejected/not-dispatched tool and returns the canonical result with its stable ID in `CommitReceipt.result_item`. Each outcome is saved inside its own tool future; ToolBatch commits those results to history in original call order. A later call's Store failure must not remain hidden behind an earlier pending call; collection uses unordered completion with final ordering or an independent fatal-error path.

Finalize and startup recovery reconcile committed outcomes into history first, then complete remaining calls: known result preserved; intent without result -> stable unknown error plus unresolved execution; no intent -> not-dispatched error. They rebuild result items and execution audit from durable data. Pending approvals are invalidated. Startup changes nonterminal runs to `failed/RECOVERY_INTERRUPTED`, increments the terminal sequence once, and reuses that result on later reopen. Terminal write failure cannot emit Completed; the affected live attachment must reject further work until it is closed and recovered.

## Implementation ownership and tasks

- [x] Root: protocol types/tests, Cargo workspace wiring, Core ModelRequest serialization and awaited journal hooks; preserve existing nonpersistent execution APIs. Add Core failure-injection and parallel-result persistence tests.
- [x] Store worker: new `whale-store/**` only, backend/journal/recovery state machine, real SQLite reopen/process-lock tests and deterministic write/late-commit/unknown-result tests. Coordinate final public signatures before daemon integration.
- [x] Daemon worker: `whale-daemon/**` only, persistence/recovery module, startup Store CLI, optional capability, creation/acceptance/finalization ordering, fresh attachment IDs, durable registration, detach/forget, secret-safe diagnostics. No asynchronous Store call while holding a synchronous lifecycle lock; create/attach use explicit reservations and cleanup.
- [x] Language worker: `sdks/python/**`, `sdks/java/**` only, typed recovery APIs, staged binding transactions, preserved Agent defaults, failed-response cleanup and consumers/tests. No concurrent Maven jobs.
- [x] Root: Rust SDK recovery APIs and binding guards; production-process verifier with a persistent external side-effect counter, restart/crash points and all three language consumers.

## Acceptance requirements

- [x] Old nonpersistent suites stay green; configured SQLite alone does not change ordinary Session close semantics.
- [x] Persistent creation and run acceptance are durable before success ACK; failures and uncertain outcomes have an inspectable key and no duplicate dispatch.
- [x] Actual validated model projection, effective options and tool definitions survive restart; raw credentials and old bindings do not.
- [x] Parallel one-completed/one-pending tool crash preserves the completed result. A counter increment followed by process death before outcome persistence yields unknown and recovery causes zero new effects.
- [x] Unknown acknowledgment is explicit, revision-scoped and exact; wrong key, stale revision, missing bindings/provider, active attachment, old epoch and late callbacks cause zero dispatch.
- [x] Reattach uses fresh live identity; old handles cannot target the new attachment. New turns run normally after proper binding and unknown acknowledgment.
- [x] Terminal-before-notification crash preserves queryable result without rerun; interrupted approvals/context/model calls recover as nonrunning records.
- [x] Disk failure prevents later dispatch and successful terminal claims. Competing process Store opens fail while the writer lives and succeed after its death.
- [x] Explicit close retains durable state; forget is distinct, detached-only and prevents record revival. Cross-language actual subprocess tests use real SQLite and production runtime paths.
- [x] Documentation clearly separates completed C4 capabilities from remaining retention/plugin/async/observability/generation/distribution work. Keep the full goal active until every accepted requirement is implemented and verified.


## Final verification ledger (2026-09-08)

The production daemon was rebuilt after the last source changes. Counts are test-run observations, not a performance score or released support matrix; the integration suites overlap some workspace coverage.

| Command / boundary | Final result |
| --- | --- |
| `cargo build -p whale-daemon --bins --examples` | Successful production and fixture build |
| `cargo test --workspace --all-targets` | **291 passed, 12 ignored**: protocol 34, adapters 33, Core 51, Daemon 76, Rust SDK 74, Store 23 |
| Python unittest discovery, `PYTHONPATH=sdks/python/src` | **112 run: 93 passed, 19 skipped** |
| `mvn -f sdks/java/pom.xml clean test` | **113 run: 87 passed, 26 skipped**, no failures/errors |
| `python3 scripts/verify_recovery.py --all-sdks` | **15 passed**: raw RPC/process 9, Python 2, Rust 2, Java 2; **23 actual HTTP requests** (11 + 4 + 4 + 4) |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_provider_http.py --all-sdks` | Passed, **110 actual HTTP/SSE requests** across existing provider/context consumers |
| `PYTHONPATH=sdks/python/src python3 scripts/verify_session_lifecycle.py --all-sdks` | Passed, **28 actual HTTP requests**, existing session cleanup consumers retained |
| Changed/untracked Rust source `rustfmt --check` | **93 files passed** |
| Markdown local links / `git diff --check` | **25 Markdown files**, no broken local links; no whitespace errors |

Store's feature-isolated suite additionally passed 15 tests with `--no-default-features`; the SQLite-enabled 23-test suite includes two subprocess helpers. See [Store report](c4-store-report.md) and [language/verifier report](c4-languages-report.md). Two Rust recovery consumers are ignored in ordinary workspace discovery and explicitly enabled by the recovery verifier; other ignored tests require their documented fixtures.

### Evidence by requirement

- Protocol tests first failed because recovery types did not exist; the final typed contract validates keys, UUID live identities, revision/acknowledgment bounds and redacted Debug output.
- Core's five journal-boundary tests first failed: provider dispatch preceded a saved request, tool dispatch lacked durable intent, and parallel results were lost or later errors hidden. They now verify commit-before-dispatch, immediate individual result persistence, ordered batch history and prompt failure when a later parallel commit fails behind a pending tool. ModelRequest serialization has a separate actual-projection/options/identity test.
- Daemon's 17 new recovery tests cover create/begin/finalize commit barriers, Store failure, owned preparation/registration and close/EOF races, configuration normalization, first CAS uncertainty, authentication, exact unknown acknowledgment, new live identity and archived snapshots. A persistent legacy run waiter disappearing during begin now also triggers connection cleanup.
- Rust's eight new recovery unit tests cover optional capabilities, inactive staged callbacks, exact identity/defaults, definite rejection vs uncertain outcome, abandoned creation cleanup, full unsigned revision transmission and abandoned acknowledgment/forget mutation ownership. If closing an unclaimed attachment is rejected, the SDK now closes the connection so the lease has an EOF cleanup path.
- A separate review found Core usage accumulation could panic before Store's overflow check. Two regressions first reproduced the panic, then covered all five counters in both ordinary and persistent two-step runs. Checked accumulation fails atomically, retains prior progress, prevents a second tool effect, emits a failed terminal and permits durable finalization without inventing unknown results.
- Actual SQLite/process tests fsync an external side-effect counter, then SIGKILL the daemon before a host result is returned. On restart the result is unknown and model/tool counts do not increase. Parallel tests independently observe a known outcome in SQLite before SIGKILL; it survives while only the pending call becomes unknown. Repeated startup keeps stable IDs and archives.
- The actual process verifier also kills the daemon during approval, host context and held model HTTP waits. Archived runs become `failed/RECOVERY_INTERRUPTED`, old approvals disappear and no undispatched tool becomes unknown; attach itself creates no callbacks or model requests.
- Actual terminal persistence is observed in SQLite before killing the daemon without consuming the finished notification; inspect returns that saved terminal, not a rerun. Wrong secrets, active attachments, stale revisions and missing/changed tool/context/provider configuration are rejected without additional dispatch.
- SQLite tests verify competing processes cannot own one database; process death releases the lock. Injected SQLite write errors poison the journal, reopening recovers actual storage, and canceled command waiters cannot cancel already accepted FIFO operations.
- Credentials are resolved into a real local HTTP Authorization header, while raw credentials and recovery secrets are absent from the SQLite record and recovery snapshot. Frame logging is tested separately in Daemon, Python and Java. Configuration retains credential reference names and excludes connection/binding IDs.

ModelInput is a durable record of the validated projection **before** dispatch. Its presence alone is not proof the provider received the request. Unknown tool acknowledgment is a recorded application decision to continue, not a success claim, cancellation guarantee or automatic compensation. MemoryStore provides the same transaction/recovery API within one runtime but no process-restart durability. The API capability describes support, not a backend durability promise.

## Remaining full-goal work

C4 supplies durable boundaries for automatic retention and plugin ownership; it does not implement those features. Continue with explicit retention policies for terminal runs, detached sessions and history growth, then owned plugin/resource registration and teardown, native async Python/Java consumers, structured observability, complete generated protocol clients and matched SDK/daemon distribution verified in clean installations. Full external Codex/OpenCode/pi runtimes remain a separate optional AgentBackend contract, not a ModelProvider substitution. Keep the overarching goal active.
