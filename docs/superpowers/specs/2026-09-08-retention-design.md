# Retention and session admission limits

Status: C5 design within the existing, active Agent SDK implementation goal. The previous turn completed C4 with production SQLite/process evidence. This is the next accepted long-running-runtime requirement; no commit, push, release or additional approval is part of this work.

## Purpose and decisions

Applications need to release completed run payloads and old detached durable sessions without losing active work or causing old IDs to execute again. Use explicit, disabled-by-default runtime policies. Keep the current history and recovery audit semantics: do not silently trim canonical history, tool results or model input archives to fit a budget. Enforce session admission limits before new work instead.

Three approaches were examined. Request-triggered eviction cannot clean an idle server. Removing run IDs together with payloads permits repeated side effects. Trimming stored history under ordinary CAS breaks current history offsets/call ledgers and leaves copies inside archived ModelRequests. The selected design uses autonomous maintenance, protected record retirement with retained identity tombstones, and separate admission limits. Future explicit compaction can define a new audit contract; C5 does not pretend a byte limit is an exact model token count.

## Frozen protocol types

All config structs live in `whale_protocol::retention`, derive Debug/Clone/PartialEq/Eq/Serialize/Deserialize, deny unknown fields and use snake_case wire fields. Optional fields default to None and omit None when serialized. All provided numeric limits are positive; all absent means disabled. Durations are u64 milliseconds, counts/bytes are u64. `sweep_interval_ms` defaults to 1000 and must be positive.

```rust
pub struct RunRetentionPolicy {
    pub terminal_ttl_ms: Option<u64>,
    pub max_terminal_runs_per_session: Option<u64>,
}
pub struct StoreRetentionPolicy {
    pub detached_ttl_ms: Option<u64>,
    pub max_retained_sessions: Option<u64>,
    pub max_retained_payload_bytes: Option<u64>,
}
pub struct RetentionPolicy {
    pub sweep_interval_ms: u64,
    pub runs: RunRetentionPolicy,
    pub store: StoreRetentionPolicy,
}
pub struct SessionLimits {
    pub max_accepted_turns: Option<u64>,
    pub max_history_bytes: Option<u64>,
    pub max_model_request_bytes: Option<u64>,
}
```

Each type has `validate() -> Result<(), String>` and `is_enabled() -> bool`; Defaults disable optional limits. RetentionPolicy defaults its nested structs. Add `limits: Option<SessionLimits>` to AgentDefinition and StartThreadParams with serde default/omit-None, preserving the old default wire. Public Rust struct literals in owned source need the new field.

Constants: `CAPABILITY_SESSION_LIMITS = "session_limits.v1"`, `CAPABILITY_RUN_RETENTION = "run_retention.v1"`, `RUN_EXPIRED = -32030`, `SESSION_LIMIT_EXCEEDED = -32031`. New Daemons always advertise supported session limits; advertise run retention when configured. Existing eight baseline requirements remain unchanged. New SDKs require session_limits.v1 before dispatching an Agent configured with limits, including persistent create/attach. No extra expiry notification is required. Query/cancel/approval/duplicate-start for retired run IDs returns RunExpired after ownership validation. SDKs map it to a typed error. Configured limits must never be silently ignored by an old daemon.

## Runtime payload retention

DaemonServer gains `with_retention_policy(self, RetentionPolicy) -> Result<Self, String>` and CLI `--retention-config PATH` reads this JSON at startup. The same policy applies in stdio, UDS and embedded clients. No policy means prior retention behavior.

An eligible live run must be terminal and its final notification successfully delivered. TTL starts from successful terminal delivery, not model finish, store commit or terminal snapshot visibility. Pending preparation, active/cancelling/approval work, incomplete/failed delivery and close failure evidence are protected. Expiration releases the full RunRecord but retains a lightweight accepted-ID tombstone under the same registry lock used by duplicate start. This prevents a reused ID becoming new work. Earlier RPCs holding an Arc may complete; later control RPCs return RunExpired. Session close/EOF release its run tombstones while the existing closed-session identity fence prevents resurrection.

One maintenance worker runs periodically without new RPCs, never overlaps itself, and must not keep its owner alive through an Arc cycle. It must stop when the runtime owner disappears; accepted Store work completes rather than becoming an uncertain dropped transaction. Tests can call an explicit sweep with a supplied Instant, but production must actually schedule sweeps. Run maintenance and Store retirement do not hold synchronous lifecycle or registry locks across I/O awaits.

TTL and count bounds limit full completed payloads. Exact accepted-ID tombstones and caller-held results remain; `max_accepted_turns` provides a finite session admission quota for applications that require one. No fixed process RAM claim is made while unbounded sessions, handles, frames or outputs are allowed.

## Durable detached-session retirement

StoreRuntime exposes `sweep_retention(&self, policy: &StoreRetentionPolicy, now_ms: u64) -> Result<RetentionReport>`. An accepted sweep is owned and serialized even if its caller stops waiting. RetentionReport (in whale-store) includes examined, retired, protected_active, protected_unknown, remaining_sessions, remaining_payload_bytes and unmet budget amounts. Daemon maintenance calls this API; custom embedded Store owners may call it directly under their lifecycle.

Only non-tombstone records with no owner, all runs terminal and no unacknowledged unknown executions can be retired automatically. Candidate scan is insufficient: the final backend transaction/CAS rechecks revision and protection predicates. Attach and retirement can have only one winner. Revision conflicts skip that candidate, not poison another journal. Disk failures remain visible and cannot be reported as successful cleanup.

TTL is measured from detachment and inspect does not renew it. Startup recovery of a previously attached record begins its detached age at recovery. Process-wall-time rollback must not turn a future timestamp into an expired age. Schema v2 stores created/updated/detached timestamps and retirement reason/time. Existing v1 records get migration-time grace, saved once so reopening does not continually reset TTL. Migration must preserve old data and be tested with actual SQLite v1 JSON.

Retirement first handles TTL, then retires oldest eligible records by (detached_since_ms, recovery_id) to reduce count/logical payload-byte overages. Budget totals count all non-tombstone payloads, including protected active/unknown records; reports expose unresolved overages if protected data exceeds the target. Payload size means UTF-8 serialized bytes, not characters or SQLite file size. Metadata scanning should be paginated; SQLite should avoid retaining all full records merely to collect candidates. Trait additions need safe defaults for existing custom SessionStore implementations, with optimized Memory/SQLite implementations where useful.

A retired record uses the existing authenticated tombstone semantics and releases history, runs, model inputs, configuration payload and obsolete attachment-ID lists. Its recovery ID cannot be recreated or attached. Tombstones remain; released SQLite pages/WAL do not promise forensic erasure or immediate file shrinking. Automatic retention never forgets unresolved effects even under count/byte pressure. Explicit user-directed forget retains its existing separate semantics.

## Session admission and model request limits

ThreadSession owns optional SessionLimits and a count of accepted turns for direct Core consumers. Expose `set_limits`, `limits`, `check_run_admission(&[CanonicalItem]) -> Result<(), CoreError>`, and an accepted-count restoration method for durable attach. Core returns a distinct LimitExceeded error, with a stable SESSION_LIMIT_EXCEEDED terminal code. Daemon maps preacceptance failures to -32031.

Before accepting a new run, compare the existing canonical history plus proposed input against max_history_bytes and count accepted IDs against max_accepted_turns. Rejection must not append history, occupy a new run ID or invoke provider/tool/context callbacks. Daemon's accepted-ID quota includes cancelled-before-execution and expired IDs; durable quota includes archived runs across attachments. Store begin_run enforces configured count/history limits as well, since direct journal consumers can bypass Daemon. Configuration is read from normalized `configuration.session.limits` when present; custom configurations without it have no limit.

Core checks current history before constructing context and again before a new tool batch. It measures the entire validated serialized ModelRequest (system prompt, projection, tools, options, identity) before journaling it or invoking the provider. Limits are checked before any awaited dispatch. Existing output `max_tokens` remains a model generation option; byte limits are not presented as token estimates.

A model output or already executing parallel tool batch may itself exceed the history threshold. Complete known results and unknown settlement must remain recordable; do not discard a result or poison a journal solely to meet a configured soft history target. Stop further model/tool dispatch once the threshold is exceeded and finalize normally as failed. The next turn is refused until the application starts a new session or uses a future explicit compaction contract. Request/history limits are admission budgets, not hard caps on arbitrary incoming frame allocations, a single provider response or external billing.

## Client ownership and queries

Reader handling of finished must first settle result and publish/close the event stream, then remove the exact strong route. Java/Rust need weak handle caches with dead-entry cleanup to preserve repeated lookup identity where currently promised; Python's existing weak cache is retained. The client must not strongly retain completed payloads after the caller drops all handles. Already held result futures, handles and event buffers remain readable after remote expiration.

Snapshot/get/cancel/approval remain authoritative remote operations even when the local handle has a terminal result. A cached completed result is not proof the daemon still retains control state. Expired RPC errors must not overwrite previously delivered results. A snapshot reaching terminal before finished must not close an existing live event route early. A newly queried archived terminal with no live route must finish its event receiver without waiting for an event that will never arrive. Late/duplicate terminal notifications cannot recreate a route.

## Acceptance

1. Default config preserves old behavior; numeric/unknown-field fixtures match all SDKs and old peers reject configured session limits before bindings/RPC.
2. Autonomous TTL and count cleanup retires payloads with no new requests; finished backpressure/error, active and unknown work remain protected.
3. Expired lookup and repeated start return RunExpired; an external side-effect counter proves no replay. In-flight lookup/close races are deterministic.
4. Actual SQLite v1 migration/reopen preserves timestamps/data; detached TTL/count/UTF-8 byte pressure clear payloads while tombstones prevent resurrection. Attach-vs-GC races and write failure are exercised.
5. Accepted-turn, history and actual ModelRequest limits reject before effects; oversized already-completed tool output remains durable, blocks later dispatch and survives recovery.
6. Three languages release dropped completed payloads, preserve held results/buffers, expose authoritative expiration and send Agent limits through ordinary/persistent/reattach APIs.
7. Production-process consumers validate idle expiry and quotas with real HTTP and SQLite; existing nonpersistent and C4 suites remain green.
8. Document configured admission budgets, unknown protection, retained identity overhead and unfinished full-goal work accurately. Keep full goal active beyond C5.
