# Retention and session admission limits

C5 adds optional automatic payload retention and per-session admission budgets in the working tree. Both are disabled by default. Ordinary sessions remain in memory; a Store is still required for [persistent sessions](RECOVERY_API.md). These changes are not a released compatibility guarantee. The [C5 plan](superpowers/plans/2026-09-08-retention.md) records implementation and validation status.

## Configure the runtime

Save this example as `retention.json`; choose values appropriate to your application:

```json
{
  "sweep_interval_ms": 1000,
  "runs": {
    "terminal_ttl_ms": 300000,
    "max_terminal_runs_per_session": 20
  },
  "store": {
    "detached_ttl_ms": 604800000,
    "max_retained_sessions": 1000,
    "max_retained_payload_bytes": 1073741824
  }
}
```

```sh
./target/debug/whale-daemon --retention-config retention.json \
  --session-store sessions.sqlite
```

The Store flag is optional when only live run retention is needed. Store policy fields have no backing records to retire without a configured Store. The same startup arguments work with SDK subprocess constructors described in [Recovery API](RECOVERY_API.md).

All provided values must be positive unsigned 64-bit integers. Unknown fields, zero, booleans and fractional values are rejected. Omitted limits are disabled; `sweep_interval_ms` defaults to 1000. An empty policy does not enable maintenance. Invalid CLI configuration fails before serving requests.

Rust embedding uses protocol types and the daemon builder:

```rust
use std::sync::Arc;
use whale_daemon::DaemonServer;
use whale_protocol::retention::{RetentionPolicy, RunRetentionPolicy};
use whale_sdk_rust::WhaleClient;

let policy = RetentionPolicy {
    runs: RunRetentionPolicy {
        terminal_ttl_ms: Some(300_000),
        max_terminal_runs_per_session: Some(20),
    },
    ..Default::default()
};
let server = DaemonServer::default_server().with_retention_policy(policy)?;
let client = WhaleClient::in_process(Arc::new(server));
```

Configure before cloning/sharing the server or starting its maintenance worker; otherwise the builder returns an error. `with_store_runtime` may precede or follow this builder. If server clones select distinct StoreRuntime instances, maintenance tracks each live Store through deduplicated weak references; a clone without a Store cannot disable their sweeps. Maintenance runs periodically without new requests, does not overlap itself, and stops when its owning runtime state is released. Outside Tokio, startup is deferred until the server runs or handles a message. Extremely large intervals that exceed the scheduler range are rejected.

## Live run expiry and held results

A run becomes eligible only after it is terminal **and its final notification has been successfully written**. TTL uses elapsed monotonic time from that delivery, not model completion or snapshot commitment. Expiry happens on a subsequent sweep, not necessarily at the exact TTL boundary. Count retention keeps the most recently delivered eligible runs in each session. Preparing, active, cancelling, waiting-approval and incomplete/failed-delivery records remain protected.

Expiry removes the full daemon RunRecord but retains an owner-scoped accepted-ID tombstone. A later `turn.get`, `turn.cancel`, approval resolution or `thread.start_turn` reusing that ID returns `-32030 RunExpired`; the old ID never becomes permission to execute again. A request that already acquired the record may finish. Close/EOF clears these run tombstones, while the existing closed-session fence prevents old handles from reviving.

SDK `result()` values and already buffered events held by the caller remain readable. Snapshot/get/control APIs still query the daemon: a cached result does not turn an expired remote query into success, and an expiry error does not overwrite a delivered result. Finished routing releases SDK strong ownership; weak caches preserve live handle lookup without keeping dropped completed payloads alive. This does not free results, subscriptions or buffers that application code still holds.

| Surface | Remote expiry | Pre-acceptance limit rejection |
| --- | --- | --- |
| Rust | `SdkError::RunExpired` | `SdkError::LimitExceeded` |

These map RPC errors, not every failed result. If a limit is reached after acceptance, the run finishes as `failed` with snapshot error code `SESSION_LIMIT_EXCEEDED`; inspect the result/snapshot normally.

Live expiry does not delete canonical session history or persistent run archives. An archived run remains accessible through recovery inspect until its entire detached record is forgotten or retired; it cannot be rehydrated as an old live RunHandle.

## SessionLimits in each language

Limits belong to the Agent definition and are forwarded to ordinary creation, persistent creation and attachment. The SDK requires `session_limits.v1` before dispatching configured limits; an older daemon cannot silently ignore them. New daemons always advertise that capability, and advertise optional `run_retention.v1` when retention is enabled. The original eight baseline requirements remain unchanged.

The following snippets assume an existing `client`; configure a provider and credentials as described in [Agent API](AGENT_API.md):

```rust
use whale_sdk_rust::{AgentDefinition, SessionLimits};

let mut definition = AgentDefinition::new("analysis", "YOUR_MODEL");
definition.limits = Some(SessionLimits {
    max_accepted_turns: Some(100),
    max_history_bytes: Some(4_000_000),
    max_model_request_bytes: Some(1_000_000),
});
let agent = client.agent(definition, Vec::new())?;
let session = agent.create_session().await?;
```

Optional fields omitted from a definition retain existing unbounded behavior. For persistent recovery, limits are part of the configuration that must match the stored definition; reattachment does not reset them.

| Limit | Checked boundary |
| --- | --- |
| `max_accepted_turns` | Before reserving a new run ID. Includes cancelled-before-Core runs, expired runs and durable archives across attachments. Rejected unaccepted requests do not consume quota. |
| `max_history_bytes` | UTF-8 JSON bytes of canonical history plus proposed input at admission; Core rechecks current history before context construction and before another tool batch. |
| `max_model_request_bytes` | Complete validated serialized ModelRequest, including prompt, projection, tool definitions, options and identity, before journaling the request or invoking the provider. |

A pre-acceptance rejection leaves history and the proposed new ID untouched. Byte budgets are admission checks, **not token counts or hard RAM/disk/billing caps**. ContextPolicy may run before the actual projected request can be measured. Existing output `max_tokens` remains a separate model generation option.

A model response or already executing tool batch can itself exceed the history budget. Known results and uncertain-effect settlement are still recorded; later model/tool dispatch is refused. Data is not silently trimmed or rolled back. Start a new session when the retained history cannot admit another turn; automatic compaction is not implemented.

## Detached Store retention

Automatic retirement requires a detached record, terminal runs and no unacknowledged unknown execution outcomes. Active and unknown records remain protected even under count/byte pressure. TTL starts at detachment; inspect does not renew it. Startup recovery of a previously attached record starts a new detached age. A clock rollback does not expire a timestamp still in the future.

Store sweeps apply TTL, then retire oldest eligible records to reduce count and logical payload-byte overages. Totals include protected records, so a target may remain unmet. The backend rechecks revision and protection conditions at the retirement transaction; a concurrent attach and retirement cannot both win.

Retirement clears history, runs, model inputs, configuration payload and obsolete attachment IDs, leaving an authenticated tombstone. The key cannot recreate or attach that recovery ID. Explicit revision-checked forget retains its separate semantics. Neither operation promises forensic erasure, immediate SQLite file shrinking or deletion of backups.

Schema v2 stores creation/update/detachment and retirement metadata. Existing v1 records receive a migration-time grace period persisted once; reopening does not repeatedly reset their TTL. SQLite pages/WAL and tombstones are not the logical serialized payload-byte total. MemoryStore supports the same policy within its runtime lifetime but adds no process durability.

Independent Store owners call `StoreRuntime::sweep_retention(&policy, now_ms).await?`. Accepted sweeps complete under Store ownership even if the waiter is dropped. `RetentionReport` reports examined/retired records, active/unknown protections, remaining counts/bytes and unmet budgets. Daemon schedules these calls; a standalone StoreRuntime does not install a timer. Custom SessionStore backends have default `metadata_page` and `retire_detached` implementations and must preserve atomic revision/protection checks.

Direct Core embeddings use `ThreadSession::set_limits` and Core's admission checks. Durable embedding owners must also restore accepted counts and perform the journal begin/finalize/detach lifecycle; attaching a journal alone does not supply daemon admission or maintenance ownership.

## Scope and verification

This bounds eligible payloads, not every allocation: accepted-ID and recovery tombstones, application-held handles, active sessions and protected unknown records can remain. Session quotas and explicit close complement retention. Automatic history compaction, tokenizer-specific budgets, plugin unload, native async client surfaces, full protocol generation, observability and matched SDK/daemon distribution remain separate work.

See the [daemon report](superpowers/plans/c5-daemon-report.md), [Store report](superpowers/plans/c5-store-report.md), [production verifier report](superpowers/plans/c5-retention-verifier-report.md) and [C5 validation ledger](superpowers/plans/2026-09-08-retention.md) for the observed unit, concurrency and three-language production checks. These results cover the documented policy boundaries; they do not imply every scheduler, backend or distribution environment has been tested.
