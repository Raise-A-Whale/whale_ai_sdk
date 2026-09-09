# C3 daemon handoff

Implemented mandatory connection initialization in `crates/whale-daemon/src/initialization.rs`, connected through `server.rs` and `session_lifecycle.rs`. No changes to core, adapters, protocol, SDKs or scripts in this task; no commit.

- Connection clones share state; independent writer identities negotiate separately. Invalid parameters (`-32602`) and incompatible versions/features (`-32011`) leave New. Duplicate concurrent/completed initialization returns `-32012`; ordinary New requests return `-32010` before provider inspection or session publication.
- Successful negotiation reserves Initializing atomically. Only successful full response delivery marks Ready. Pending business requests wait for that outcome. An acknowledgement guard marks Failed on cancellation; failed writes and EOF close the connection state and wake waiters. Failed/Closed business requests return internal error `ConnectionClosed`.
- EOF cannot be undone by a late successful acknowledgement. Existing start-acceptance, run-terminal, session-close ordering and cancellation-safe frame writing are preserved.

Public signatures remain unchanged: `DaemonServer::handle_message`, `dispatch_request`, `run`, and `disconnect_connection`. Initialization must pass through `handle_message` (or the real `run` reader) because returning a response from `dispatch_request` cannot establish delivery. Direct business dispatch now requires readiness too. Existing test setups use explicit real initialization via `tests/common/mod.rs`; no transport silently becomes ready.

Red evidence: `cargo test -p whale-daemon --test initialization ordinary_requests_require_initialization_without_publishing_sessions -- --exact` failed before production changes: the uninitialized start returned success (`error.code` was null, expected `-32010`). The completed new suite covers malformed numeric/identity/capability fields, corrected retry, duplicate initialization, independent connections, cloned writers, held ACK ordering, canceled initializer, failed ACK with existing waiters, disconnect during ACK, and zero provider/host work before readiness.

Final verification: `cargo test -p whale-daemon --all-targets` passed **57 tests**, zero failures/ignored: initialization **10**, execution_context **10**, model_providers **5**, provider_config **8**, run_lifecycle **12**, session_close **10**, transport_cancellation **2**. Both daemon examples also compiled. `cargo fmt -p whale-daemon` completed.

Boundary: the SDK owns the agreed 10-second initialization deadline. A custom raw peer's blocked writer remains pending until its send is canceled, fails, or its connection reaches EOF; this task adds no daemon timer. A small per-connection state marker remains for the daemon lifetime to prevent stale writer resurrection, alongside existing closed-connection ownership markers. This is not durable storage, retention/TTL, authentication or a plugin lifecycle.

## Final acceptance follow-up: cancellation arbitration

A real Java stdio run and a separate Python reproduction exposed an existing run race: the outer `select!` could poll its cancel branch as pending, then Core could observe cancellation and return an error during execution's poll. That candidate error was previously committed as `Failed` even after a `turn.cancel` response had acknowledged `Cancelling`.

A deterministic new regression pauses a test-only ContextPolicy inside that poll, completes the real cancel RPC, then releases the policy with its cancellation error. Before the fix it failed with `RUN_FAILED` / `status: failed`; after the fix it reports `CANCELLED` / `cancelled` / legacy result `interrupted`. A second test preserves a genuine uncancelled Core error and verifies that late cancel returns the unchanged failed terminal.

`RunRecord::request_cancel_locked` and `request_cancel` now share the snapshot-lock linearization with terminal commit. Cancel RPC, Session close and EOF skip already-terminal snapshots, record `Cancelling`, publish daemon cancel intent, then signal Core. `execute_run` rechecks that intent while holding the same snapshot lock before committing its terminal. The Core cancellation token is not used to classify outcomes because Core also signals it during successful cleanup. Pending acceptance Drop remains synchronous and publishes daemon intent first.

Final follow-up `cargo test -p whale-daemon --all-targets`: **59 passed, 0 failed, 0 ignored**, with run_lifecycle increasing from 12 to **14**; all other suite counts remain unchanged. This fix touched only daemon `server.rs`, `session_lifecycle.rs`, and `tests/run_lifecycle.rs`; initialization and SDK production modules are unchanged. Root owns rebuilt actual stdio/HTTP and full-workspace revalidation.
