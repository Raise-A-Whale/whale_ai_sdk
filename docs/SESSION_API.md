# Session resource lifecycle

Status: C1 is implemented and verified in the working tree. The exact tests and remaining full-goal scope are recorded in the [implementation plan](superpowers/plans/2026-09-08-session-lifecycle.md). These changes have not been committed or released.

Closing a Session releases that conversation's runtime state and SDK-owned binding references. The client connection and other Sessions remain usable. This supports applications that keep one client alive for many independently scoped conversations.

## Public API

| Surface | Session operation | Client operation |
| --- | --- | --- |
| Rust | `session.close().await -> Result<bool, SdkError>` | `client.close_session(thread_id).await` |

The boolean is true for the invocation that closes an existing owned Session, and false for a repeated close or an unknown Session. Concurrent closes join the same closing operation; only the initiating invocation reports true. A Session or retained closed-ID marker owned by another connection cannot be closed by this client.

With an existing Agent definition and tool list (see [Agent API](AGENT_API.md)):

```rust,no_run
# async fn f(agent: whale_sdk_rust::Agent) -> Result<(), Box<dyn std::error::Error>> {
let session = agent.create_session().await?;
let run = session.start_turn("Analyze these records").await?;
let _result = run.result().await?;
session.close().await?;
// The result is a caller-owned value; the Session's live records are released.

let another = agent.create_session().await?;
let another_run = another.start_turn("A separate conversation").await?;
let _another_result = another_run.result().await?;
another.close().await?;
# Ok(())
# }
```

Closing a reusable Agent's Session does not dispose the Agent itself or business objects still referenced by it. Applications own database clients and other injected resources; removing a binding releases the SDK's reference, not every reference in the application.

## Wire contract

```json
{"jsonrpc":"2.0","id":20,"method":"session.close","params":{"thread_id":"session-a"}}
```

```json
{"jsonrpc":"2.0","id":20,"result":{"thread_id":"session-a","closed":true}}
```

`thread_id` must be a nonempty string. The daemon records closing before waiting for execution cleanup, which fences new runs and tool registrations. A registration already waiting for a Session lock rechecks the lifecycle when the lock becomes available; it cannot publish a replacement after close.

The runtime cancels active logical work, clears pending approvals and reverse callbacks, and emits the run's one terminal event before acknowledging successful close. If the run had already completed, its existing terminal state is preserved. Daemon cleanup does not wait for non-cooperative synchronous host code to return. Rust close waits for its async callback futures to be dropped, so callbacks must yield and must not block Tokio executor threads with synchronous work. Offload blocking work appropriately; closing does not forcibly stop that work or undo external side effects.

After success, the daemon removes the Session's live history, retained Run records and live deduplication keys. It can retain a lightweight closed-ID ownership marker until connection cleanup, so recreating a Session with that ID cannot revive old handles. Create a fresh Session to start again. For an explicitly persistent Session, close detaches the live attachment and retains its committed history and archived runs in the configured Store, subject to any configured detached-record retention policy. Recovery creates a fresh live ID and bindings; old handles remain closed. See [persistent sessions and recovery](RECOVERY_API.md).

## Handles, callbacks and failure

An existing terminal result and already buffered events remain readable through the original handle. Snapshot/get_run/control operations after close fail with SessionClosed or RunNotFound; they do not return a cached snapshot as if it were an active runtime record. If terminal delivery fails, close is not reported as successful.

The SDK removes that Session's run routes, current/retired tool bindings, context policy and registration locks. Active tool/context callbacks receive cancellation; Rust also drops its async callback futures. Late callback replies do not restore a run or binding, and finished or cancelled invocations cannot report accepted progress. Unrelated Session callbacks remain active.

An explicit pre-mutation rejection, such as unsupported `session.close` on an older daemon or invalid parameters, leaves the Session open locally. Previously dispatched operations keep their acknowledged changes while close is pending, so rejecting close does not roll back an accepted tool registration. A timeout, transport failure, internal RPC error or malformed acknowledgement makes the remote mutation outcome uncertain, so the SDK closes the connection. In that failure case all client-owned work is affected; callers must not keep using potentially inconsistent bindings.

Rust continues the close transaction in a background task once dispatched, even if its caller stops awaiting it. Stopping an await does not silently abandon remote resource cleanup.

## Verification boundary

```sh
cargo test --workspace
cargo build -p whale-daemon --bins --examples
```

Default workspace tests cover owner isolation, repeated/concurrent close and
lifecycle races, plus SDK cleanup, callback reply backpressure and close failure.
Optional gated tests that hold actual model HTTP responses (close interrupting a
pending model request, terminal delivery before acknowledgement, peer Session on
the same connection still running) are marked `#[ignore]` and can be enabled when
a production daemon and HTTP fixture are available.

This API provides explicit resource release. C4 adds optional SessionStore, detached durable records and cross-restart recovery through SQLite; it does not make ordinary sessions persistent. `forget_session` is a separate, authenticated, revision-checked operation on detached records and leaves a tombstone. A store failure during close returns `STORE_FAILED`; live cleanup cannot repair a poisoned journal, so the owning runtime must be reopened before recovery. C5 adds optional automatic Run TTL/count and detached Store retention, plus SessionLimits admission budgets. These are disabled by default and do not replace close: exact accepted-ID markers and application-owned results can remain after payload expiry. Byte limits do not count tokens or trim history. See [Retention API](RETENTION_API.md) for configuration and Rust examples, and [Recovery API](RECOVERY_API.md) for durable lifecycle semantics.
