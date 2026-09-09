# Whale Rust Application SDK Design

Date: 2026-09-09

## Goal

Make Whale a reusable Rust foundation from which a host application can build a CLI, TUI, desktop UI, or background service around an AI Agent without changing Whale internals. The SDK supplies runtime lifecycle, Agent composition, Session and Run state, resumable events, interactions, and resource ownership. It does not implement a CLI, TUI, desktop shell, project browser, or product-specific workflow.

This phase is Rust-only. It changes and verifies `whale-sdk-rust` and the Rust runtime pieces that it directly owns. Python and Java SDK APIs, compatibility shims, fixtures, and release gates are outside this phase.

## Product boundary

The host application owns presentation, navigation, workspace selection, authentication UI, product policy, and packaging choices. Whale owns the model/tool loop, authoritative Session and Run state, provider invocation, host-tool dispatch, approval/interaction state, persistence, and connection lifecycle.

Codex, OpenCode, pi, and DeepSeek Harness are architecture references. This phase does not wrap those complete products as Agent backends. A Whale Agent is assembled from the existing portable `AgentDefinition` plus narrow extension points. A complete external Agent runtime would require a separate `AgentBackend` contract only when a concrete consumer needs it.

## Chosen architecture

Keep one daemon-centered execution contract so embedded and sidecar consumers observe the same behavior:

```text
Rust CLI / TUI / Desktop host
  -> WhaleRuntime
       -> AgentDefinition + bound ToolPacks + ContextPolicy
            -> SessionHandle / SessionSnapshot
                 -> RunHandle / RunSnapshot / Interaction
       -> whale-daemon transport boundary
            -> whale-core
                 -> ModelProvider / ToolRegistry / SessionStore
```

`WhaleRuntime` is the application entry point. Its default mode runs `DaemonServer::default_server()` in the current Tokio runtime, which gives a Rust application a no-binary setup path. Explicit sources also support an owned sidecar binary and an attached Unix socket. `open` completes protocol initialization before returning. `shutdown` is explicit, bounded, cancellation-safe, and single-flight; dropping the Runtime remains emergency cleanup.

Internally, connection capability and execution ownership are separate before the public Runtime is added. Cloneable `WhaleClient` values contain request/event state, a writer, and a cloneable stop handle. A non-cloneable `ConnectionOwner` owns the SDK reader task plus either the embedded connection/server-loop task or the managed child. Runtime construction returns both pieces, stores the capability in `WhaleClient`, and stores the owner in `WhaleRuntime`. Dropping a non-final client clone cannot stop the Runtime, and retaining a client clone cannot keep the owned process alive after the Runtime closes. Legacy `WhaleClient` constructors retain a compatibility owner internally, but their public signatures and behavior remain source-compatible.

The owner runs one shared shutdown transaction. Explicit `WhaleClient::close`, peer EOF, `WhaleRuntime::shutdown`, startup rollback, and emergency Drop all converge on that transaction. Concurrent shutdown callers observe the same immutable outcome, and abandoning an `open` or `shutdown` waiter does not cancel cleanup that has already started.

Do not add a broad `RuntimePlugin` trait. Existing model, context, and store traits retain their focused responsibilities. Add a session-scoped `ToolPack` contract because application tools commonly own workspace, database, MCP, or shell resources that must be created and released with a Session.

## Public object model

```text
WhaleRuntime
  -> WhaleClient (advanced transport access)
  -> Agent
       -> WhaleThread / future SessionHandle
            -> SessionSnapshot
            -> RunHandle
                 -> RunSnapshot
                 -> RunEvent subscription
                 -> Interaction response
```

The existing `WhaleClient`, `Agent`, `WhaleThread`, and `RunHandle` remain source-compatible. New application APIs wrap or extend them instead than fork the execution path.

## Runtime lifecycle

The Rust API will expose:

```rust
pub enum RuntimeSource {
    Embedded { server: DaemonServer },
    ManagedProcess {
        executable: PathBuf,
        args: Vec<OsString>,
        environment: Vec<(OsString, OsString)>,
    },
    ExternalUds { path: PathBuf },
}

pub struct RuntimeOptions {
    pub source: RuntimeSource,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

pub struct RuntimeInfo {
    pub mode: RuntimeMode,
    pub peer: InitializeResult,
    pub process_id: Option<u32>,
}

pub struct RuntimeShutdown {
    pub mode: RuntimeMode,
    pub forced: bool,
}

pub struct WhaleRuntime { /* non-cloneable lifecycle coordinator */ }
```

`RuntimeOptions::embedded()` is the default. `WhaleRuntime::open` validates positive deadlines, creates the selected transport, and awaits `protocol.initialize`; it closes partially created resources before returning an error. `RuntimeInfo` is immutable evidence of the initialized peer and negotiated capabilities.

Managed process arguments are passed as `OsString` array elements and never through a shell. External UDS mode owns only its local connection. Embedded mode owns its SDK reader and the `DaemonServer::run` connection/server loop it starts. Managed mode additionally owns and reaps its direct child process. `RuntimeMode` fully describes those semantics, so there is no separate `RuntimeOwnership` enum that could represent an invalid combination. All modes use the same `WhaleClient` request and event state machines.

Managed environment overrides are applied only to the child. Debug output exposes environment variable names but never values; `RuntimeInfo` contains only the mode, initialized peer, and the managed child PID when one exists. `WhaleRuntime::open` is cancellation-safe: immediately after a child, socket, or embedded task is created, a construction guard owns its `ConnectionOwner`. Returning an error performs bounded cleanup, while abandoning the `open` future triggers emergency cleanup without waiting in `Drop`.

`startup_timeout` is one deadline covering source creation and `protocol.initialize`; it does not wrap and abandon the existing hard-coded initialization worker. `shutdown_timeout` is one total deadline. The owner reserves part of that deadline for forced cleanup, so a peer that ignores graceful EOF cannot consume the entire budget before kill/abort and reap are attempted.

The Stage 1 shutdown promise is deliberately narrow and testable:

- every mode stops and joins or aborts the SDK reader task;
- every mode closes its local connection writer;
- embedded mode joins or aborts the one `DaemonServer::run` connection/server loop that it started;
- managed mode waits for the direct child, then kills and reaps it when graceful EOF is insufficient;
- external UDS mode never terminates or claims ownership of the remote daemon.

Stage 1 does not claim that every detached request, Run, retention, store, or other internal daemon task has been joined. A later daemon-wide graceful-shutdown contract would require explicit daemon task ownership and is separate from this application Runtime API. If any resource in the Stage 1 promise misses the total deadline, `shutdown` returns a structured phase-specific timeout and leaves emergency cleanup armed rather than waiting without a bound.

## Application state contract

Delivery is split into two source-compatible increments. Stage 2.1 ships the
viewer contract: bounded authoritative get, partial in-progress projection,
fixed-window replay, multiple Rust subscribers, and typed resync. Stage 2.2 adds
Session list, independent history pagination, metadata CAS, Closing/Closed
replay, and view tombstones. The complete Stage 2 acceptance boundary includes
both increments.

A desktop view must be rebuildable without relying on events that happened while the view was detached. Add authoritative application projections:

```text
SessionSummary: identity, agent name, lifecycle, metadata, timestamps, revision
SessionSnapshot: summary, canonical history, active/retained Run summaries
HistoryPage: canonical items plus stable cursor
SessionEventEnvelope: session sequence, timestamp, payload
```

Required operations are `session.list`, `session.get`, `session.history`, `session.update_metadata`, and `session.subscribe`. Listing is scoped to the current runtime/application namespace; it must not reveal recovery bearer secrets. Metadata is display and filtering data, not executable configuration.

Subscriptions accept a typed cursor and freeze a replay high watermark for all
pages. The daemon retains a bounded event journal independently from individual
SDK subscribers. If the requested cursor is too old, it returns
`ResyncRequired` with the current Session revision; the application obtains a
new snapshot and subscribes from that revision. A slow or detached UI cannot
block Agent completion. Run expiry also removes that Run's replay payload so
the Session journal cannot bypass the Run retention contract.

Run snapshots remain authoritative for outer-turn completion. In-progress item projections must include committed text/tool-argument deltas needed to repaint the current view after a subscriber is replaced. Rust supports multiple independent bounded subscribers for rendering, logging, and telemetry.

Persistent recovery remains explicit. A `PersistedSessionRef` combines the caller-owned recovery credential with safe Agent identity and display metadata for application storage. Attaching a persistent Session returns a new live Session identity and never replays uncertain external effects.

## Interaction contract

The current tool approval API remains available. Add a generic suspended interaction:

```text
InteractionRequested { request_id, kind, title, payload, response_schema? }
PendingInteraction
InteractionResponse { request_id, response }
turn.respond_interaction
```

`kind` is an open string and `payload` is JSON so a host can render approval, clarification, form, authentication, file/network permission, or review interactions. The daemon owns pending state and idempotency: repeating the same response succeeds, while a different response for the same request conflicts. Tool approval is exposed as a typed compatibility wrapper over its existing semantics until it can migrate safely.

## ToolPack lifecycle

A ToolPack provides tools, not context policy or UI behavior:

```rust
#[async_trait]
pub trait ToolPack: Send + Sync {
    fn manifest(&self) -> ToolPackManifest;
    async fn bind(&self, context: SessionBindContext)
        -> Result<Box<dyn BoundToolPack>, ToolPackError>;
}

#[async_trait]
pub trait BoundToolPack: Send + Sync {
    fn tools(&self) -> Vec<Arc<dyn HostTool>>;
    async fn close(&self) -> Result<(), ToolPackError>;
}
```

Binding occurs once for every new or recovered live Session. Bind failures and rejected Session creation unwind completed packs in reverse order. Session close first rejects new calls, then cancels or joins pack invocations, and finally closes bound packs in reverse order. `close` is idempotent. Rust `Drop` is emergency cleanup and never substitutes for awaited close.

## Compatibility and capability gates

New RPC families are optional capabilities rather than additions to the protocol-v1 global baseline:

- `session_views.v1`
- `session_event_replay.v1`
- `interactions.v1`
- `tool_packs.v1` is an SDK composition capability and does not require a wire feature by itself.

`WhaleRuntime` lifecycle is local SDK behavior over existing initialization and connection EOF semantics, so it does not advertise or require a wire capability.

Existing Rust APIs continue to require the current baseline and remain source-compatible. A new Rust API checks its own optional capability locally before sending the corresponding request. Python and Java behavior is not an acceptance gate for this phase.

## Delivery stages

1. **Rust application runtime:** connection capability/owner separation, cancellation-safe source opening, then public `WhaleRuntime` with ready-before-return behavior, immutable diagnostics, explicit ownership, bounded single-flight shutdown, and a public non-UI embedding example/test.
2. **Session view and replay:** Stage 2.1 delivers bounded get, authoritative
   Session snapshots, cursored events, multiple Rust subscribers, and resync;
   Stage 2.2 completes list/history pagination/metadata/lifecycle views.
3. **Agent resource composition:** session-scoped ToolPack bind/rollback/close, including persistent attachment.
4. **Generic interactions:** protocol state, Rust APIs, idempotent replies, cancellation and close behavior.
5. **Release hardening:** complete Rust schema generation/drift checks, clean crate installation, supported-target artifacts where sidecar mode is used, structured runtime diagnostics, and documentation.

Each stage must ship working, testable behavior on its own. No stage creates a CLI, TUI, or desktop application.

## Acceptance scenarios

1. A Rust host opens the default embedded runtime, defines two Agents with different tools, creates independent Sessions, streams results, approves/cancels work, and shuts down without directly constructing `DaemonServer` or transport objects.
2. A view subscribes to a running Session, detaches, resubscribes with a cursor, and reconstructs exactly the committed UI state without delaying `RunHandle::result`.
3. A desktop-style host lists Sessions, reads paginated history, updates a title/workspace metadata field, and restores a persistent Session through a stored `PersistedSessionRef`.
4. Two subscribers consume one Run independently; lagging one subscriber does not fail the other or the Run.
5. A ToolPack with session-owned resources is bound once, survives multiple turns, and is closed exactly once after all callbacks quiesce. A later pack bind failure rolls back earlier packs.
6. A generic interaction survives snapshot/query, accepts one idempotent response, rejects a conflicting response, and is cleared by cancellation or Session close.
7. All new behavior has deterministic Rust protocol, daemon, SDK, and real-transport tests that run under ordinary `cargo test` without an opt-in environment variable or a prebuilt binary. Existing workspace tests remain green.

## Deferred work

- Implementing any CLI, TUI, desktop window, project browser, or product-specific tool suite.
- Python and Java exposure of the new application APIs.
- A global plugin installer, marketplace, project auto-loading, or implicit code execution.
- Wrapping Codex, OpenCode, pi, or other complete Agent products as backends.
- Claiming Windows support before the daemon and Rust transports compile and pass lifecycle tests there.
