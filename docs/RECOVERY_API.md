# Persistent sessions and recovery

C4 is implemented in the current working tree. It adds an explicit SessionStore and recovery API; ordinary `create_session` calls still create in-memory sessions. These changes have not been committed or released. The [implementation plan](superpowers/plans/2026-09-08-session-store.md) records the contract and verification ledger.

Persistence preserves committed history, actual model inputs, tool outcomes and run snapshots. Recovery attaches that data to a new live session. It never resumes an interrupted model request, invokes a tool again or restores an old approval automatically.

## Enable a store

Build and start the production daemon with a SQLite path:

```sh
cargo build -p whale-daemon
./target/debug/whale-daemon --session-store ./sessions.sqlite
# --store is an alias for --session-store.
```

The default transport is stdio. For a shared local daemon, add `--listen uds:///tmp/whale.sock`. Store startup recovery finishes before the daemon begins accepting connections.

SQLiteStore uses WAL and synchronous FULL with an exclusive process-held sidecar lock. Only one runtime may own that local database at a time; a second writer fails at startup. Reopening after the owner exits or dies recovers interrupted records before advertising readiness. This is local storage, not a distributed lease or a shared network database service. Preserve the database and SQLite WAL files; the recovery key alone contains no history.

The existing client constructors can pass daemon arguments:

```rust
use whale_sdk_rust::WhaleClient;

let client = WhaleClient::spawn_daemon_with_args(
    "target/debug/whale-daemon",
    &["--session-store", "sessions.sqlite"],
).await?;
```

All recovery methods require the optional `session_recovery.v1` capability after normal [connection initialization](INITIALIZATION_API.md). The daemon advertises it only when a StoreRuntime is installed after successful startup recovery. The eight baseline SDK requirements remain unchanged. A missing recovery capability causes the recovery operation to fail before dispatch; it does not turn persistent creation into ordinary creation.

## Save the key before creating the session

Allocate and securely save a RecoveryKey **before** sending persistent creation. A creation acknowledgement can be lost after the record was committed. The preallocated key lets the application inspect that uncertain outcome after reconnecting; creating another session or retrying a tool would not establish what happened.

The key contains a UUID recovery ID and a generated secret encoded as 64 hexadecimal characters. Possession authorizes recovery access. The store saves only the secret digest; inspect does not expose the digest, transport owner or callable bindings. Debug representations and daemon frame diagnostics redact or omit secrets. Application serialization still contains the secret: do not log it or put it in a public artifact. There is no key-reset service in this prototype.

The following snippets assume an `agent` assembled with your definition and exact tool/context bindings, as described in [Agent API](AGENT_API.md). `save_key` is an application-provided durable secret-storage function, **not an SDK method**. Use the same Agent configuration and matching bindings when recovering. A Rust ToolPack keeps the same frozen manifest but binds fresh live resources; it never restores a prior bound object.

### Rust

```rust
use whale_sdk_rust::RecoveryKey;

let key = RecoveryKey::new();
save_key(&key)?; // Application storage; RecoveryKey supports serde.
let session = agent.create_persistent_session(&key).await?;
let result = session.start_turn("Analyze these records").await?.result().await?;
session.close().await?;

let saved = client.inspect_recovery(&key).await?;
let restored = agent.recover_session(&key).await?;
assert_ne!(restored.id(), session.id());
assert_eq!(restored.recovery_key(), Some(&key));
restored.close().await?;
```

Rust owns dispatched recovery mutation tasks independently of the caller's wait. Abandoning a successful creation/attachment waiter closes the unclaimed live session and retains its durable record. It does not roll back a committed store write.

If the Rust Agent was constructed with `agent_with_tool_packs`, both
`create_persistent_session` and `recover_session` bind every pack once for the new live
attachment. The bind kind exposes only `PersistentCreate { recovery_id }` or
`PersistentAttach { recovery_id }`: it never exposes `RecoveryKey.secret`, revision,
epoch, stored history, Provider configuration, or credentials. Each attachment gets a
new Session ID, bound object and binding ID. Manifest metadata remains the Agent's
frozen copy. See [Rust Session ToolPack API](TOOL_PACK_API.md).

Recovery snapshots expose `revision`, `runs`, `history`, `configuration` and
`unknown_executions`. Revision and epoch values use unsigned 64-bit integers on the
wire. To reload a key, reconstruct `RecoveryKey` from the application-saved recovery ID
and secret.

After a daemon restart, create a new client using the same SQLite path, reconstruct the Agent with the same configuration and fresh callable bindings, load the saved key, and call `recover_session` / `recoverSession`. The SDK first inspects the revision and then requests attachment. A concurrent attachment or changed revision is rejected; the application must inspect again rather than assume it acquired ownership.

## Identity, configuration and archives

The durable recovery ID is independent of the live thread ID. Every creation/attachment request supplies a fresh client-generated UUID, which the daemon must honor. Every attachment uses fresh host binding IDs. Closed IDs and archived run identities cannot be revived: old `turn.get` calls do not target the new session.

`inspect_recovery` returns the complete committed history and archived runs. Each archived entry has its original `RunSnapshot`, effective sampling options and `model_inputs`. A model input record includes the actual validated ModelRequest, step ID/index, history revision, completion flag and recorded usage. The request includes the ContextPolicy projection, sampling configuration, tool definitions and invocation identity; it is not an HTTP header dump. Inspect is a read operation, not a subscription to historical token events.

Attachment compares a versioned, sanitized configuration: provider selection/reference, model, prompt, sampling defaults, Agent name, run limits, SessionLimits admission budgets, tool schemas/approval/parallel declarations and ContextPolicy settings. Live session and binding IDs are removed and tool order is normalized. Credential environment variable names are retained; resolved credential values and HTTP headers are not stored. The required provider and callbacks must be available again. Missing, extra or changed definitions are rejected before a live session is published.

Tool registration on a persistent session updates the stored configuration before acknowledgement. If you replace tool metadata or add tools, reconstruct the matching Agent configuration for the next recovery. Callable code and dependency objects are not serialized, and matching metadata does not prove that newly supplied business code has identical behavior. In Rust, names from a frozen ToolPack manifest cannot be replaced through dynamic `register_tool`. Other dynamically registered names retain the existing rule above and must be reconstructed by the caller on the next Agent attachment; they are not adopted by a ToolPack automatically.

## Unknown execution outcomes

A process can die after a tool changes an external system but before its result reaches durable storage. Recovery preserves any completed parallel outcomes individually. A call with committed dispatch intent but no committed result becomes an unknown execution; a call without dispatch intent receives a distinct not-dispatched result. Stable result identities prevent repeated recovery from appending duplicate settlement results.

Interrupted nonterminal runs become `failed` with `RECOVERY_INTERRUPTED` during startup recovery. Pending approvals are invalidated. New turns are blocked while any execution outcome remains unacknowledged. Attachment itself remains possible so the application can inspect and resolve this condition.

After attaching, obtain a **fresh** snapshot and review the unresolved records against the application's external systems. Then acknowledge exactly all unresolved execution IDs at that snapshot revision. Here `session` is the current attached session; `application_approves_continuing` is an application-provided review/approval function, not an SDK method:

```rust
// Application review happens here. Acknowledgement permits future turns;
// it does not claim that an effect succeeded, failed, stopped or was undone.
// `application_approves_continuing` is application logic, not an SDK method.
if !unresolved.is_empty() && application_approves_continuing(&unresolved) {
    session
        .acknowledge_unknown(
            snapshot.revision,
            unresolved.iter().map(|item| item.execution_id.clone()).collect(),
        )
        .await?;
}
```

Only the current attached owning connection may acknowledge. Wrong keys, stale revisions, duplicate/missing/extra IDs and a different live owner are rejected. Re-inspect after a revision conflict; do not silently substitute a new revision for a review already made.

Acknowledgement records an application decision to continue with uncertainty. It neither changes the original tool outcome into success nor replays the tool. Explicit application compensation or reconciliation belongs to the application.

## Close, forget and failures

[Session close](SESSION_API.md) cancels and joins live work, releases bindings and detaches persistent sessions. For Rust ToolPacks, successful explicit close additionally waits for callback quiescence and reverse-order pack close exactly once. Connection EOF or Runtime/client teardown cannot await that async close; it uses the synchronous emergency fence. Applications that require normal resource-close results must await each Session close before connection or Runtime shutdown. Stored history, model inputs and results remain inspectable until explicit forget or configured detached-record retirement. The daemon waits for committed terminal state and terminal delivery before acknowledging successful live close. Other sessions on the connection remain usable unless an uncertain transport/mutation failure requires closing the whole client.

For permanent logical removal, first close the live session, inspect its current revision, then explicitly forget:

```rust
let saved = client.inspect_recovery(&key).await?;
client.forget_session(&key, saved.revision).await?;
```

Forget rejects an active attachment and keeps an authenticated tombstone so the recovery ID cannot be recreated or attached again. It is not a promise of forensic erasure from SQLite pages, WAL files or backups.

| RPC error | Meaning |
| --- | --- |
| `-32020 RECOVERY_UNAVAILABLE` | This daemon has no configured store |
| `-32021 RECOVERY_REJECTED` | Definite rejection, such as wrong key, stale revision, active attachment, configuration mismatch or unresolved outcomes |
| `-32022 STORE_FAILED` | Storage failure or uncertain mutation outcome; do not assume the write was rolled back |
| `-32601` / `-32602` | Unsupported method / invalid request |

A definite rejection removes staged local bindings while leaving the existing client usable. An uncertain result, malformed acknowledgement or transport failure closes the client and releases its bindings. A journal I/O or backend CAS failure stops further dispatch and poisons that live attachment; terminal persistence failure cannot report Completed. Closing releases live resources but cannot repair a poisoned journal. Correct the storage problem, close/restart the owning daemon (or reopen its StoreRuntime), and inspect/recover using the retained key. A lost acknowledgement is not permission to repeat a side effect.

## Rust embedding and independent extensions

SQLite is optional in `whale-store`; the daemon enables its `sqlite` feature. An embedded application can install an already-open StoreRuntime:

```rust
use std::sync::Arc;
use whale_daemon::DaemonServer;
use whale_sdk_rust::WhaleClient;
use whale_store::{SQLiteStore, StoreRuntime};

let backend = Arc::new(SQLiteStore::open("sessions.sqlite")?);
let store = Arc::new(StoreRuntime::open(backend).await?);
let server = Arc::new(DaemonServer::default_server().with_store_runtime(store));
let client = WhaleClient::in_process(server);
```

Use `MemoryStore::new()` instead of SQLiteStore for an in-memory store with the same recovery/CAS API during one runtime lifetime. It supports detach/reattach but does **not** provide process-restart durability. Advertising `session_recovery.v1` describes the API, not a guarantee that its selected backend writes to disk.

`whale-store` depends on protocol types independently of Core. Its `SessionStore` trait exposes transactional `create`, `load`, `compare_exchange` and `list`, plus `durable()`. C5 adds `metadata_page` and `retire_detached` with safe defaults for existing backends; built-in stores optimize metadata scans. Custom implementations must preserve the revision/epoch/owner invariants and durability semantics; wrapping arbitrary persistence calls is insufficient. StoreRuntime performs startup recovery; SessionJournal serializes accepted mutations through an owned worker so dropping a waiter cannot cancel an accepted commit.

Core can also be embedded without Daemon. `ThreadSession::set_journal` supplies its optional journal; Core awaits actual model inputs before provider dispatch, complete model items before publication, dispatch intent before tool execution, each outcome before proceeding, and ordered tool batches before the next model request. The embedding owner must perform the surrounding durable create/begin/finalize/detach lifecycle, use the journal's finalized history and snapshot, and preserve publication ordering. Setting a journal alone is not a complete durable session service. See [Store runtime](../crates/whale-store/src/runtime.rs), [Core session](../crates/whale-core/src/session.rs) and [daemon recovery integration](../crates/whale-daemon/src/recovery.rs).

## Verification and remaining scope

```sh
cargo test --workspace
cargo build -p whale-daemon --bins --examples
```

Default workspace tests cover recovery contracts, store state machines, and SDK
inspect/attach paths. Optional gated tests that require a production daemon,
actual SQLite files, subprocess termination/restart and a local HTTP model fixture
are marked `#[ignore]`. Those scenarios include an externally counted tool effect
without a saved outcome, partial parallel completion, competing database processes
and a terminal result committed before the client consumes its notification.
Recovery must create no new model/tool effects. Detailed observations belong to the
implementation ledger rather than an implied released compatibility promise.

C5 adds disabled-by-default [retention and session admission budgets](RETENTION_API.md). Live Run expiry does not remove these archives. Automatic detached retirement uses persisted detachment age, rechecks active/unknown protections at commit and leaves authenticated tombstones. Inspect does not renew TTL; v1 records receive once-persisted migration grace when upgraded to v2. Count/byte budgets include protected records and may remain unmet. The byte total measures logical serialized payload, not SQLite file size.

SessionLimits are part of matching attachment configuration, and accepted-turn quota includes all archived runs across attachments. Independent Store owners must schedule `sweep_retention` themselves; installing StoreRuntime alone does not start maintenance. These policies do not make MemoryStore durable, discard uncertain effects or implement automatic replay.

Automatic history compaction, tokenizer-specific budgets, general plugin discovery/install/dependency/hot-reload lifecycle, observability, full protocol schema/client generation and SDK/daemon release distribution remain unfinished. Rust Session ToolPack bind/rollback/close is implemented, but it is a focused host-resource contract rather than that broader plugin system.
