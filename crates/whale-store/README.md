# whale-store

`whale-store` provides transactional state for persistent Whale sessions. This library crate depends on `whale-protocol`, not Core, adapters, daemon, or SDK callback objects. It does not execute models or tools and does not provide a CLI or UI.

## Public entry points

- `MemoryStore::new()` provides the same CAS semantics without cross-process durability.
- Enable feature `sqlite` for `SQLiteStore::open(path)`. SQLite uses WAL, `synchronous=FULL`, and a process-held sidecar lock. The path's parent must exist. Symlinks are canonicalized; hard-linked database files are rejected because separate WAL names are unsafe. Do not remove the `.whale-lock` sidecar while a process may hold it.
- `StoreRuntime::open(Arc<dyn SessionStore>)` validates records, reconciles interrupted runs, and detaches old leases before returning. Create one facade per exclusively owned backend.
- `create` and `attach` return a cloneable `SessionJournal`. Journal writes, reads, finalization, acknowledgment, and detach share an owned FIFO actor. Abandoning a waiter does not cancel an accepted command.

## Minimal library use

```rust,no_run
use std::sync::Arc;
use whale_store::{MemoryStore, SessionStore, StoreRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(backend).await?;
    assert!(!runtime.durable());
    Ok(())
}
```

All asynchronous operations return `Result<_, StoreError>`. Backends implement `create`, `load`, `compare_exchange`, and `list`; revisions begin at 1. Compare-exchange validates append-only history, immutable committed results, record schema, and lease transitions. Configuration and actual model requests are JSON objects supplied by typed, sanitized writers; do not pass resolved credentials or HTTP request headers.

Model items reserve stable call result and execution identities. Dispatch requires a completed model step and can be accepted only once. Each tool outcome is durable independently of other tools; `ToolBatch` publishes results in original call order. Finalization/startup reconcile known outcomes before adding not-dispatched or unknown-execution results. Recovery never invokes providers or tools, resumes approvals, or treats an unknown effect as failure of the external operation. New runs remain blocked until the exact unresolved execution IDs are acknowledged at the current revision.

`close` integration should finalize and then detach the journal. Detach retains durable data; `forget` is a separate, authenticated, detached-only operation retaining a tombstone. A fresh attachment must use a new live thread identity and a matching sanitized configuration.

An I/O or CAS failure poisons that journal. Further writes, reads through that journal, and detach fail; direct authenticated inspection remains available. Closing the live session after such an error does not prove its persistent lease was released. Reopen the Store runtime to recover uncertain commits before attaching again. Caller-owned create/attach transactions must also survive an abandoned RPC waiter; the journal cannot repair an unobserved application-level creation by silently transferring its lease.

This implementation does not provide session-event replay, distributed writers, database encryption, exactly-once external side effects, or a fixed memory/disk cap. Configurable TTL and payload-budget retention is described below. SQLite transactions run through `spawn_blocking`; opening/configuring the database is synchronous. Journal workers remain alive until all handles are dropped and all accepted commands have finished.

Verification:

```sh
cargo test -p whale-store --no-default-features
cargo test -p whale-store --features sqlite
```

The SQLite tests include actual child-process lock contention/death, crash recovery of parallel call outcomes, and an external counter that recovery must not change. Full daemon/Core/SDK execution acceptance is performed by the workspace integration verifier.

## Retention and admission limits

`StoreRuntime::sweep_retention(&StoreRetentionPolicy, now_ms)` owns and serializes
accepted maintenance across runtime clones, even when its waiter is dropped. A
runtime owner (the daemon's maintenance worker, or an embedding application) must
schedule it; Store does not create an independent periodic task. Policies default
to disabled. The sweep uses detached TTL first and then oldest-detached order to
reduce record/serialized-byte overages. Active attachments and unresolved unknown
executions remain protected; the report exposes budgets that could not be met.

Schema v2 records persist creation, update, detachment and retirement metadata.
Opening v1 data establishes one persisted migration-time grace period, including
migration of existing forgotten tombstones. Startup recovery starts a new detached
age for previously attached records. Inspection never extends the age.

Retirement releases configuration/history/run/model-input payloads and obsolete
attachment IDs, while keeping an authenticated identity tombstone. It never replays
work or permits a retired recovery ID to be created again. Reported payload bytes
are UTF-8 bytes of the complete serialized non-tombstone record, including metadata;
SQLite file size, free pages, WAL and retained tombstones are separate. No fixed
memory/disk bound, immediate file shrink or forensic erasure is promised.

Existing custom `SessionStore` implementations inherit safe metadata pagination
and revision-CAS retirement defaults. Memory and SQLite optimize metadata scans;
SQLite extracts metadata without materializing every full record in Rust and
rechecks eligibility in its retirement transaction. Startup recovery still scans
full records as required by the existing recovery implementation.

When `configuration.session.limits` is present, `begin_run` enforces accepted-turn
and combined-history admission before mutation. `ModelInput` checks current history
and the full serialized request, while `DispatchIntent` checks current history.
`StoreError::LimitExceeded` is a definite admission rejection, not journal poison.
Completed model items, tool outcomes and terminal settlement remain recordable when
large output exceeds a limit. No automatic archive/history trimming occurs.

The repository is still being prepared for package release. SQLite behavior and the
current release checks have been validated locally on macOS; this README does not
claim a crates.io publication or a Linux/Windows verification matrix.
