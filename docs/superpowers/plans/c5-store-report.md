# C5 Store implementation report

Scope: `crates/whale-store/**` only, plus this report. No commits or publication.

## Implemented interfaces

- `StoreRuntime::sweep_retention(&StoreRetentionPolicy, now_ms: u64) -> Result<RetentionReport>` owns the accepted operation and serializes sweeps across clones. Daemon owns periodic scheduling; Store exposes no extra timer.
- `RetentionReport` public `u64` fields: `examined`, `retired`, `protected_active`, `protected_unknown`, `remaining_sessions`, `remaining_payload_bytes`, `unmet_sessions`, `unmet_payload_bytes`. Protection counts can overlap. Remaining totals are refreshed after candidate processing; concurrent live activity may change them afterward.
- `SessionStore::metadata_page(after_id: Option<&str>, limit: usize) -> Result<Vec<RetentionMetadata>>`: ascending exclusive-cursor paging, default implemented through old `list`; Memory avoids full-payload clones and SQLite extracts JSON metadata without materializing all records in Rust.
- `SessionStore::retire_detached(id: &str, expected_revision: u64, now_ms: u64, reason: &str) -> Result<bool>`: safe default checks then revision CAS; SQLite checks and replaces in one transaction. Active/unknown/revision conflicts never retire the candidate. Sweep skips definitive conflicts, propagates I/O failure.
- `StoreError::LimitExceeded(String)`: pre-mutation, retry-safe rejection; daemon maps to `SESSION_LIMIT_EXCEEDED=-32031`.

## State and safety

Schema v2 adds optional-for-v1-deserialization `created_at_ms`, `updated_at_ms`, `detached_since_ms`, `retired_at_ms` and `retirement_reason`. New records are v2. Opening old data performs one CAS migration with current-time grace and preserves payload; previously attached records acquire a new detached age only when recovered. Existing forgotten v1 records support one exact metadata-only migration. Ordinary history/run/call CAS invariants remain append-only and terminal results immutable; v2 creation timestamps cannot be rewritten or updated timestamps moved backward.

Automatic retirement requires no owner, all runs terminal and no unacknowledged unknown. TTL arithmetic uses checked subtraction. TTL candidates precede quota candidates in oldest-detached/ID order. Budgets include protected non-tombstone records, exposing overages rather than deleting them. Retirement clears payload and obsolete attachment IDs, retaining authenticated recovery identity/revision/epoch and retirement metadata. Tombstone IDs cannot be recreated or attached.

`begin_run` parses normalized `configuration.session.limits`, checks archived accepted count and exact concatenated canonical-history UTF-8 bytes before inserting the run/input. Matching duplicate run acceptance remains idempotent. `ModelInput` additionally checks full serialized request and history; `DispatchIntent` checks history. Counting reuses protocol streaming serialization helpers. Model output, per-call outcomes, batches and finalization remain recordable above a history budget. Admission failure does not poison the journal.

## Evidence

Initial red: `cargo test -p whale-store --test retention` failed on missing retention protocol/API, timestamp metadata and `LimitExceeded`. Subsequent targeted tests cover TTL boundaries/clock rollback, migration stability, UTF-8 bytes, active and unknown protection, exact ID tombstones, counts across 132 records and 128-row pages, attachment winning a gated scan race, aborted sweep waiter ownership/serialization, direct-journal count/history/request/dispatch admission and preserved oversized outcomes.

Actual SQLite tests write the old table and v1 JSON directly, migrate both payload and forgotten tombstones, close/reopen and verify unchanged grace/revision; clear stored JSON payload while keeping the ID; prove UTF-8 byte accounting; reject stale/current-active retirement; protect active/unknown via optimized metadata; inject an aborting UPDATE trigger and verify I/O failure leaves original revision/payload; cancel a waiter after real SQLite COMMIT and verify the owned sweep settles without resurrecting data.

Final verification:

- `cargo test -p whale-store --features sqlite --quiet`: **39 passed, 0 failed** (11 retention memory/admission, 5 actual SQLite retention, 8 existing SQLite/process entries, 15 existing journal tests).
- `cargo test -p whale-store --no-default-features`: **26 passed, 0 failed**, SQLite tests correctly excluded.
- `rustfmt --edition 2021 --check crates/whale-store/src/*.rs crates/whale-store/tests/retention*.rs`: exit 0.

 Existing C4 real process-lock/death/external-counter tests remain in the suite; C5's periodic production daemon and SDK process acceptance are root/daemon work, not claimed by these backend tests.

## Limits retained deliberately

Tombstone count can grow; SQLite pages/WAL need not shrink or erase old physical bytes. Payload accounting means the entire serialized non-tombstone record, including metadata, not filesystem size. Optimized maintenance scans retain only non-tombstone metadata; existing startup recovery still loads full records. Custom backend default pagination may use its full `list`, and may override it for scale. Retention is admission/maintenance policy, not a hard bound on frames, one output or concurrent active sessions. No automatic archive/history trim, replay, TTL helper without runtime scheduling, or fixed-resource guarantee is introduced.
