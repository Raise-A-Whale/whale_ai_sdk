# C4 Store implementation report

Scope: only the new `crates/whale-store/**` crate and this report. No Core, daemon, protocol, SDK or workspace source edits; no commits. The Store API is ready for integration. This component report does not declare the full C4 daemon/SDK acceptance complete.

## Public interfaces

The crate depends on protocol DTOs, serde/JSON, Tokio, async-trait, UUID and SHA-256. Feature `sqlite` enables rusqlite 0.40.2 with bundled SQLite and fs2. Default builds do not require SQLite.

- `SessionStore: Send + Sync`: `durable() -> bool`; async `create(SessionRecord)`, `load(&str) -> Result<Option<SessionRecord>>`, `compare_exchange(&str, expected_revision, SessionRecord)`, `list() -> Result<Vec<SessionRecord>>`.
- `MemoryStore::new() -> Self`; `SQLiteStore::open(path) -> Result<Self>`.
- `StoreRuntime::open(Arc<dyn SessionStore>) -> Result<Self>`; `durable() -> bool`; async `create(RecoveryKey, Value, owner: String, thread_id: String) -> Result<SessionJournal>`, `inspect(&RecoveryKey) -> Result<RecoverySnapshot>`, `attach(&RecoveryKey, expected_revision, Value, owner: String, fresh_thread_id: String) -> Result<SessionJournal>`, `forget(&RecoveryKey, expected_revision) -> Result<bool>`.
- Cloneable `SessionJournal`: synchronous `recovery_id()` / `epoch()`; async `record`, `begin_run`, `commit`, `finalize`, `replace_configuration`, `acknowledge`, `detach`, exactly following the frozen plan. All asynchronous operations return `Result<_, StoreError>`.
- `CommitReceipt { revision, result_item }`; `FinalizedRun { snapshot, history }`. Protocol `ModelInputRecord` / `UnknownExecution` are reused. `SessionRecord`/`StoredRun`/`StoredCall` remain serialized Store-owned records.

Store errors distinguish precondition failures (`Invalid`, `NotFound`, `Unauthorized`, `Conflict`, `Active`, `UnknownExecutions`, `StaleLease`, `Forgotten`) from uncertain storage failures (`Io`, `Poisoned`). A journal backend CAS failure is immediately classified as Io/Poisoned, including a backend Conflict; this differs from rejecting a stale application revision before mutation. The daemon must not report that poisoned journal result as a safe retryable rejection.

## Implemented boundaries

Revisions begin at 1. CAS validates schema, identities, append-only history, immutable committed run/results, and lease transitions. Recovery IDs cannot be recreated after forget. Only SHA-256 secret digests are stored; record Debug omits secrets and contents, and recovery snapshots omit digests/owners. Configurations must be sanitized by their typed writers; the backend does not serialize adapter or host callback objects.

Every journal command, including reads and settlement, joins one owned FIFO worker. An abandoned waiter cannot cancel an accepted commit. Backend I/O/CAS failure poisons that journal; later work cannot assume a commit failed to happen. Direct authenticated inspection remains available. Poisoned detach deliberately fails: the live attachment must be released and the Store reopened for startup recovery before a new attachment can be claimed.

Model input payloads capture the current history revision. Model calls reserve stable result/execution IDs. Dispatch is legal only after a completed model step and can never be accepted twice. Outcomes are durable independently of batch completion; `ToolBatch` publishes results in original call order. Finalization and startup prefer known outcomes, then create stable not-dispatched or unknown results. Unknown effects require exact revision-scoped acknowledgment before another run. Recovery does not call providers/tools or revive approvals. Archived terminal snapshots remain unchanged on reopen and late outcomes are rejected.

SQLite uses WAL and synchronous FULL. A canonical-path sidecar lock is held through the lifetime of the connection and its accepted blocking jobs. Lock files are not unlinked; competing processes fail, and killing the holder releases the lock. Hard-linked database aliases are explicitly rejected because independent WAL names are unsafe. SQLite transaction work uses `spawn_blocking`; opening/configuring the database is synchronous.

## Red/green evidence and verification

Observed red stages:

1. The initial test failed to compile for missing MemoryStore/StoreRuntime, after the root wired the workspace member.
2. CAS accepted erased committed history, and ToolOutcome was accepted before model-step completion. Both regressions passed after stronger state validation.
3. Locking the SQLite data file itself caused `database is locked` on macOS. Five real SQLite tests failed, then passed after canonical-path sidecar locking.
4. Summing oversized model usage panicked and killed the journal worker. Overflow is now rejected before the next completed-step record is committed; the prior state can still be finalized.
5. A backend CAS Conflict escaped as a safe application rejection. The first such journal failure is now Poisoned, and later work is blocked. The daemon worker independently reproduced its incorrect -32021 response before this fix.

`cargo test -p whale-store --features sqlite`: **23 passed, 0 failed**, comprising 15 state-machine/journal tests and 8 SQLite test entries (two are helper entry points used by actual child processes). Coverage includes canceled waiters/FIFO reads, lost commit acknowledgment after a successful CAS, injected write failure, immutable terminal results, late commands, stable unknown IDs, exact acknowledgment, wrong keys, configuration/lease fencing, and authenticated tombstones.

SQLite acceptance uses actual subprocesses: one holds the process lock and is killed; another persists two dispatch intents plus one completed result, records two effects in an external counter, and is killed before terminal settlement. Startup preserves the known result, records exactly one unknown, and a second reopen preserves identical snapshots/IDs. The external counter remains 2. This proves Store recovery itself does not repeat effects; the full Core/daemon/SDK business execution check remains root-owned. A real SQLite trigger also forces transaction write failure, rather than relying only on mock errors.

Default-feature and formatting verification are recorded after the final run below. Runtime-wide HTTP/native/crash acceptance belongs in the root C4 ledger.

## Remaining boundaries

TTL/size retention, event replay, distributed writers, database encryption and exactly-once external effects are not implemented here. The CAS backend stores whole session records; this is a correctness boundary, not a claim of high-volume storage performance. Creation/attachment RPC transactions must be owned by the daemon so a dropped RPC waiter cannot orphan a newly acquired lease. Full C4 completion still requires the integrated Core, daemon and three SDK acceptance.

Final default verification: `cargo test -p whale-store --no-default-features --quiet` completed with **15 passed, 0 failed**; `cargo fmt -p whale-store -- --check` exited 0. Store production sources are frozen for independent integration review.
