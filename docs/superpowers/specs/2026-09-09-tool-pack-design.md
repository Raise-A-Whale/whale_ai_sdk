# Rust session-scoped ToolPack design

**Status:** proposed for implementation on 2026-09-09

**Scope:** `whale-sdk-rust` application composition and lifecycle only

**Product boundary:** no CLI, TUI, desktop, GUI, project browser, or product tool suite

## Purpose

Application tools often own a workspace checkout, database connection, MCP client,
shell worker, or other resource whose lifetime must match one live Session. The
current `Agent` captures `Arc<dyn HostTool>` values and shares them across every
Session created from that Agent. This works for stateless callbacks, but it has no
formal place to allocate one resource set per Session, roll back a partially bound
set, wait for in-flight callbacks, or close resources in dependency order.

This stage adds a Rust-only, session-scoped `ToolPack` contract. A reusable
`ToolPack` is a factory. Each fresh or recovered live Session binds a new
`BoundToolPack`; that bound value is owned by the SDK until the Session closes.
Normal close first quiesces callbacks and then closes packs in reverse bind order.
An internal owner guard supplies synchronous emergency cleanup when awaited cleanup
cannot run.

ToolPack is a library foundation for future application shells. It does not load
plugins, discover projects, execute implicit commands, or implement a user
interface.

This document refines the Stage 3 sketch in
`2026-09-09-rust-app-sdk-design.md`; where signatures differ, this focused
design is authoritative for ToolPack implementation.

## Current substrate and gap

The design extends existing behavior instead of creating a second execution path:

- `HostTool` in `crates/whale-sdk-rust/src/lib.rs` separates tool metadata from
  `execute_with_context`. It is `Send + Sync`, and callback execution already
  receives per-call cancellation and deadline information through `ToolContext`.
- `AgentDefinition` in `whale-protocol::agents` is portable data. Its
  `tool_names` are validated as nonempty and unique; callable objects remain in
  the Rust host.
- `WhaleClient::agent` snapshots current tool metadata into `BoundTool` while
  retaining the live handler. `SessionBindingsGuard` installs per-Session name
  and immutable binding-id routes before Session creation and removes exact
  identities if creation fails.
- `ClientState` routes reverse tool calls by `(thread_id, binding_id)` and tracks
  every active callback in `tool_callbacks`. `SessionLifecycle` changes an open
  Session to `Closing`/`Releasing`, stops new callbacks, cancels existing callback
  futures, and awaits their `finished` fences.
- `WhaleThread::register_tool` serializes changes per `(thread_id, tool_name)`.
  Immutable binding IDs keep an older handler alive for an invocation already in
  flight, while the name route can advance to a new handler.
- Persistent recovery serializes tool definitions but strips live `binding_id`
  values. Attach creates a fresh live Session identity and requires the caller to
  supply matching callable bindings again.
- Provider selection is separate. `AgentDefinition` carries `provider_ref` or
  `ProviderConfig`; daemon startup owns `ProviderRegistry` and `ModelProvider`
  instances. `HostContextPolicy` is another focused, Session-scoped callback and
  remains separate from tools.

The missing owner is observable in two places. Agent-level host objects are shared
across Sessions, and `SessionBindingsGuard` can only remove callback maps; it has no
async close hook. Connection teardown similarly cancels callbacks and clears maps
without a resource-specific emergency fence. ToolPack fills only this gap.

## Goals

1. Bind each pack exactly once for every live Session creation attempt that reaches
   the binding phase, including persistent create and recovery attach.
2. Give every successful live Session its own bound pack instances. Agent clones
   share factories and frozen manifests, never bound resources.
3. Snapshot stable tool metadata before Session creation and preserve the current
   daemon protocol and persistent configuration comparison.
4. Roll back successfully bound packs in reverse order when a later bind,
   validation step, Session request, or response check fails.
5. Reject new Session callbacks before cleanup, cancel and await in-flight tool and
   context callbacks, release SDK handler references, and then close packs in
   reverse order.
6. Invoke normal `close` at most once per bound pack. Concurrent Session close
   callers continue sharing the existing single-flight transaction.
7. Provide nonblocking emergency cleanup for task cancellation, panic, connection
   loss, Runtime Drop, or executor teardown.
8. Preserve all existing Rust public method signatures and all protocol wire
   shapes. Existing Agents without packs behave exactly as before.

## Non-goals

- No protocol method, capability, or daemon change. `tool_packs.v1` is a local SDK
  composition feature, not a wire capability.
- No Python or Java parity requirement.
- No plugin installer, dependency resolver, hot reload, filesystem discovery,
  marketplace, permission UI, or product-defined tool collection.
- No Provider, Store, Agent backend, or context-policy registration through a
  ToolPack.
- No automatic Session close when one clone of `WhaleThread` is dropped. Explicit
  `WhaleThread::close`, client/runtime shutdown, and connection loss keep their
  existing ownership meaning.
- No serialization of Rust resources, bound pack values, recovery secrets, or
  callback identities.

## Alternatives considered

### Chosen: reusable factory plus SDK-owned bound value

The manifest is available before allocation, while `bind` returns the one value the
Session owns and later closes. This is the smallest contract that supports stable
wire metadata, per-Session resources, reverse rollback, and async cleanup without
turning the Runtime into a general plugin container.

### Rejected: clone or recreate individual HostTools

A closure that returns `Vec<Arc<dyn HostTool>>` per Session would solve handler
identity but has no owner for shared pack resources, ordered rollback, or one close
operation. Adding lifecycle methods directly to `HostTool` would also change the
existing public trait and close the same resource once per tool rather than once per
pack.

### Rejected: create the Session and call `register_tool` afterward

Post-ACK registration creates a readiness window in which the daemon Session exists
without its declared tools. It also requires extra durable mutations during
recovery, makes partial registration visible remotely, and cannot roll back Session
creation atomically. Initial pack tools therefore continue using the existing
`StartThreadParams.tools` field.

### Rejected: broad RuntimePlugin or daemon-side pack protocol

Provider, context, Store, tool, and future AgentBackend lifetimes are different.
One plugin trait would obscure those owners and require a new wire/versioning model.
The current daemon already has the tool definitions and reverse RPC needed for this
stage, so a `tool_packs.v1` protocol capability would add no behavior.

## Chosen public contract

The new API lives in `crates/whale-sdk-rust/src/tool_packs.rs` and is re-exported
from the crate root.

```rust
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolPackTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub supports_parallel: bool,
    pub require_approval: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolPackManifest {
    pub id: String,
    pub tools: Vec<ToolPackTool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionBindKind {
    Ephemeral,
    PersistentCreate { recovery_id: String },
    PersistentAttach { recovery_id: String },
}

#[derive(Clone)]
pub struct SessionBindContext {
    // Private fields; SDK construction prevents contradictory identity/kind pairs.
    session_id: String,
    agent_name: String,
    kind: SessionBindKind,
    session_cancelled: CancellationSignal,
}

impl SessionBindContext {
    pub fn session_id(&self) -> &str;
    pub fn agent_name(&self) -> &str;
    pub fn kind(&self) -> &SessionBindKind;
    pub fn session_cancelled(&self) -> CancellationSignal;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ToolPackError {
    message: String,
}

impl ToolPackError {
    pub fn new(message: impl Into<String>) -> Self;
    pub fn message(&self) -> &str;
}

#[async_trait]
pub trait ToolPack: Send + Sync {
    fn manifest(&self) -> ToolPackManifest;

    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError>;
}

#[async_trait]
pub trait BoundToolPack: Send + Sync {
    fn tools(&self) -> Vec<Arc<dyn HostTool>>;

    async fn close(&mut self) -> Result<(), ToolPackError>;

    /// A synchronous, nonblocking stop fence used only when awaited close cannot
    /// finish. The default relies on ordinary Rust Drop/RAII.
    fn emergency_close(&mut self) {}
}
```

`BoundToolPack::close` takes `&mut self` because the SDK keeps the bound value in
one owner guard while awaiting it. If that future is abandoned, the same guard can
still call `emergency_close` before dropping the value. The bound value is not
shared with callers even though its tool handlers are `Arc` values.

The additive Agent constructors are:

```rust
impl WhaleClient {
    pub fn agent_with_tool_packs(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
        packs: Vec<Arc<dyn ToolPack>>,
    ) -> Result<Agent, SdkError>;
}

impl WhaleRuntime {
    pub fn agent_with_tool_packs(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
        packs: Vec<Arc<dyn ToolPack>>,
    ) -> Result<Agent, SdkError>;
}
```

Existing `WhaleClient::agent` and `WhaleRuntime::agent` keep their signatures and
delegate internally with an empty pack list. No existing trait gains a required
method. `SdkError` is currently exhaustive, so this stage does not add an enum
variant or mark it `#[non_exhaustive]`; doing either would break downstream
exhaustive matches. Manifest errors use `SdkError::InvalidConfiguration`. Runtime
bind/close failures use `SdkError::Internal` with the pack ID and lifecycle phase.
Callers can use `ToolPackError` inside their implementation but are not required to
parse an SDK error string.

## Manifest and Agent composition

`ToolPack::manifest` is evaluated once when `agent_with_tool_packs` constructs the
Agent. The SDK stores that frozen value beside the factory. Later mutation of a
factory cannot change existing Agent configuration.

Validation occurs before initialization, provider inspection, Session identity
allocation, or binding:

- pack IDs are trimmed, nonempty, and unique within one Agent;
- every manifest has at least one tool;
- tool names are trimmed, nonempty, and unique across static tools and all packs;
- pack-manifest descriptions are nonempty and each pack-manifest JSON schema
  passes the existing tool schema compiler/validator; the no-pack/static path
  retains its current validation timing;
- the set of static and manifest tool names exactly equals
  `AgentDefinition.tool_names`; order is not significant.

After `bind` returns, `BoundToolPack::tools()` is called once. Its handler set must
exactly match the frozen manifest by name. Description, parameters,
`supports_parallel`, and `require_approval` must also match. The SDK orders the
handlers by manifest order, wraps each handler with frozen metadata, and allocates
a new opaque `binding_id` for the live Session. A mismatch is an invalid
configuration and includes the just-returned bound pack in reverse rollback.
Panics from `manifest`, `bind`, `tools`, or bound-handler metadata access are caught
at the SDK boundary and reported as the corresponding manifest/bind validation
failure; one pack cannot unwind the owned Session transaction.

Full metadata in the manifest is intentional. It keeps fresh Sessions deterministic
and makes persistent attach compare the same serialized definition on every live
attachment. A manifest containing names only would permit a resource factory to
silently change the durable Agent configuration during bind.

## Bind transaction

All three creation paths use one internal owned transaction:

```text
preflight
  -> allocate fresh live thread_id
  -> phase = Preparing; create Session cancellation fence
  -> bind packs in declaration order
  -> validate bound handlers and stage exact name/binding-id routes
  -> send fresh/create-persistent/attach RPC
  -> validate response identity
  -> install SessionPackOwner; phase = Open
  -> return WhaleThread
```

Preflight completes initialization, optional limit/recovery capability checks, and
`provider_ref` inspection before the first pack binds. A direct HTTP
`ProviderConfig` may still be rejected by the daemon after binding; that ordinary
Session failure runs the same rollback.

Fresh ephemeral Sessions must now enter `Preparing` before host routes are staged,
matching the existing persistent path. Incoming reverse calls for a preparing
Session are rejected and cannot execute pack code before the Session ACK.

The public creation future waits on a background-owned transaction. Dropping the
waiter does not abandon staged resources or an already-sent Session mutation. The
transaction checks whether its result receiver has closed after each bind and
before sending the RPC. If it has, it rolls back without creating a remote Session.
If the request was already sent, it settles the response. An unclaimed success is
closed through the normal Session close path; an uncertain close outcome closes the
shared connection.

The transaction selects each `bind` future against connection shutdown. On
shutdown, the current bind future is dropped and already returned packs take the
emergency rollback path. A ToolPack implementation is responsible for RAII cleanup
of resources allocated internally before `bind` returns a `BoundToolPack`; the SDK
cannot close a value it never received. `SessionBindContext::session_cancelled`
lets cooperative pack work and bound background tasks observe rollback or Session
close. This stage adds no arbitrary bind timeout.

### Bind context and persistent identity

Each successful live attachment receives a distinct `session_id` and distinct bound
pack instances:

- ordinary `create_session`: `SessionBindKind::Ephemeral`;
- `create_persistent_session`: `PersistentCreate { recovery_id }`;
- `recover_session`: `PersistentAttach { recovery_id }`.

The context never contains `RecoveryKey.secret`, Store revision, recovery epoch,
Provider credential, system prompt, or history. A factory that needs additional
application state captures it explicitly when the host constructs the pack.
Recovery bind happens once for the new live attachment and never revives the prior
bound object or an old tool invocation.

## Rollback and normal close

An internal `SessionPackOwner` holds bound leases in declaration order. Each lease
contains the frozen pack ID, its bound object, and a terminal-state flag.

Normal rollback for a failed creation performs:

1. keep the Session in `Preparing` so no new reverse callback can start;
2. cancel the Session lifetime signal;
3. remove only staged name and binding-id routes whose `Arc` identity matches the
   transaction;
4. close the just-returned invalid pack, if any, then all earlier successful packs
   in reverse order;
5. continue through every close error or panic and drop every bound object;
6. mark the provisional Session closed and remove its event hub/state.

If rollback itself fails, the SDK returns `SdkError::Internal` containing the
primary failure category and all pack IDs whose close failed. It never leaves the
Session open to make a cleanup error look recoverable.

After a daemon acknowledges `session.close`, the existing local release transaction
is refined to this order:

1. phase is `Releasing`; all new Session requests and reverse callbacks fail;
2. close the Session event hub and cancel the Session lifetime signal;
3. resolve pending Session requests so dynamic-registration tasks cannot deadlock;
4. stop all registered tool and context callbacks and await every `finished` fence;
5. await existing registration locks, then remove tool name routes, binding-id
   routes, context policy, run routes, and remaining Session-owned references;
6. take the single `SessionPackOwner` and await each pack `close` in reverse bind
   order, continuing after individual errors or panics;
7. publish the shared close outcome and enter `Closed`.

Thus a pack closes only after no SDK-owned callback future can still execute one of
its tools. Two concurrent close calls share the existing `CloseAttempt`; only its
owner takes the pack owner. Dropping any close waiter does not cancel that background
transaction. A pack close error is reported as `SdkError::Internal` after every pack
has been attempted; the remote Session and local phase remain closed.

## Emergency cleanup

Awaited close is the primary contract. Every owner also implements synchronous
Drop cleanup that requires no entered Tokio runtime:

- a lease whose async close completed does nothing on Drop;
- a lease whose normal close never began calls `emergency_close` once;
- if an in-progress close future is destroyed by task/runtime teardown, its guard
  calls `emergency_close` before dropping the bound object;
- remaining leases are emergency-closed in reverse bind order;
- panics from one emergency hook are caught so later packs still run.

Connection EOF, `WhaleClient`/`WhaleRuntime` emergency Drop, and fail-closed
connection teardown synchronously reject/cancel callbacks and detach each Session's
pack owner so these guards run. Emergency cleanup is a nonblocking stop fence, not
an async join guarantee. Implementations should set their own atomic closed state,
close synchronous handles, or notify an already-owned worker. The default method
relies on the bound object's ordinary Rust `Drop` implementation.

The SDK invokes normal `close` at most once. Emergency cleanup may follow only when
that close future did not complete because its owner task/executor was destroyed;
implementations must make the emergency fence safe after partial async cleanup.

`WhaleClient::close` and `WhaleRuntime::shutdown` remain connection-wide teardown,
not an implicit series of `session.close` RPCs. Any Session whose explicit close has
not already taken its owner uses the emergency path during that teardown. A host
that needs awaited pack-close results closes its Sessions first, then shuts down the
Runtime. Folding pack close into the Runtime's existing connection/process budget
and diagnostics is deferred with structured runtime cleanup reporting.

Dropping one `WhaleThread` clone still does not close a Session. This preserves the
current capability semantics and prevents one view holder from destroying resources
used by another clone.

## Dynamic `register_tool` boundary

Dynamic registration remains a live Session mutation with its existing immutable
binding-id and per-name serialization rules. It is not a late ToolPack bind and it
does not add a cleanup hook.

Names declared by a ToolPack manifest are reserved for that pack for the entire
live Session. `WhaleThread::register_tool` rejects an attempt to replace one of
those names locally with `SdkError::InvalidConfiguration` before sending an RPC.
This prevents a handler from being replaced while its resource owner and durable
manifest still belong to another component.

Dynamic registration of other names keeps its current behavior. Those handlers
are canceled, quiesced, and removed before pack close, but they are not closed by a
pack. For a persistent Session, their definitions continue to update durable
configuration. On the next recovery the host must reconstruct matching non-pack
bindings in `AgentDefinition` and the static tool vector, as required today. A
dynamic tool is never automatically adopted by a pack, and a pack-owned name cannot
be used to bypass manifest stability.

## Provider and context-policy boundary

ToolPack supplies only `HostTool` handlers.

- `provider_ref` still resolves through the daemon-owned startup
  `ProviderRegistry`; `ProviderConfig` still selects a daemon-side HTTP provider.
  Pack bind receives neither provider objects nor credentials.
- Provider inspection runs before pack bind when a reference is present. A later
  daemon-side provider/session error rolls back packs like any other creation
  rejection.
- `HostContextPolicy` remains attached with `Agent::with_context_policy`. It is
  staged separately, shares the callback quiescence barrier, and is removed before
  pack close. A pack cannot install or replace it.
- `SessionStore`, recovery transactions, and any future `AgentBackend` remain
  independent focused contracts.

This separation avoids a broad plugin trait and keeps ownership auditable: daemon
startup owns Providers and Stores, one live Session owns BoundToolPacks, and one
callback invocation owns its `ToolContext`.

## Panic, error, and cancellation semantics

- Manifest/schema/composition failures occur before binding and return
  `SdkError::InvalidConfiguration`.
- A `bind` error or panic is labeled with the frozen pack ID. Earlier packs roll
  back normally. The failed pack must clean any not-yet-returned partial allocation
  through its own RAII guards.
- ToolPack errors are host-visible diagnostics and must not contain credentials,
  recovery secrets, raw environment values, or other sensitive application state.
- A bound-handler/manifest mismatch includes the current bound pack in rollback.
- Pack `close` futures run one at a time in reverse order. Errors and panics are
  accumulated; neither skips an earlier pack.
- Session close cancellation is single-flight: the background owner continues even
  if every public waiter is dropped.
- Connection loss may prevent async cleanup and therefore uses the documented
  emergency path. It never sends new cleanup RPCs on a failed transport.
- Cleanup code never holds a DashMap shard, Session phase mutex, registration lock,
  or connection writer lock across user `bind`/`close` awaits.

## Wire and source compatibility

This design reuses `StartThreadParams.tools`, `session.register_tools`,
`tool.execute_host`, binding IDs, and `session.close` unchanged. No protocol type,
method, capability list, daemon router, or persisted schema changes.

Source compatibility rules are explicit:

- existing `HostTool`, `AgentDefinition`, `WhaleClient::agent`,
  `WhaleRuntime::agent`, `Agent::create_session`, recovery, and Session close
  signatures are unchanged;
- `SdkError` gains no variant or `#[non_exhaustive]` marker;
- existing static tools remain shared references with their current behavior;
- Agents with no packs allocate no pack owner and take the current fast paths;
- Python and Java SDKs need no changes and do not gate this Rust stage.

## Deterministic test seams

SDK tests use a private `ProbePack` and `ProbeBoundPack` rather than sleeping:

- atomics count manifest, bind, normal close, emergency close, and handler calls;
- `Notify`/semaphore barriers stop bind, callback execution, and close at precise
  phases;
- a shared ordered log records `bind:A`, `bind:B`, `callback:exit:B`, `close:B`,
  `close:A`;
- configurable outcomes return errors, panic, return mismatched handlers, or block
  until cancellation;
- Drop probes prove resources disappear without requiring a Tokio runtime.

The existing duplex fake peer supplies exact initialize/start/attach/close ACK
barriers and observes whether an RPC was sent. The existing in-process
`DaemonServer` fixture proves the public path executes a real reverse tool request.
Persistent tests use current recovery fake-peer helpers so they need no prebuilt
binary, network service, environment variable, or ignored test.

## Acceptance scenarios

1. Two Sessions created from one Agent call every ToolPack factory once per Session,
   receive different `session_id` values, keep resources across multiple turns, and
   share no bound handler identity.
2. Packs A and B bind, pack C fails, and cleanup observes `close:B` then `close:A`;
   no Session RPC or callback route remains.
3. A bound pack returns a mismatched manifest. That pack and earlier packs close in
   reverse order, and the daemon never sees a Session request.
4. Session close races with a blocked host callback. New callbacks are rejected,
   the blocked future is canceled and dropped, and only then do packs close in
   reverse order exactly once. Concurrent close callers see the same outcome.
5. Dropping a close waiter does not cancel cleanup. Connection-wide client/runtime
   shutdown or destroying the owner task/Runtime instead triggers reverse emergency
   cleanup without an entered Tokio runtime.
6. Persistent create and subsequent attach receive fresh bound values and bind
   kinds with the same safe `recovery_id`; neither context exposes the recovery
   secret. Rejected attach rolls back every new value.
7. Dynamic registration of a pack-owned name is rejected locally. Registration of
   an unrelated name keeps current live and persistent semantics.
8. Invalid initialization, missing recovery capability, or failed provider-ref
   inspection performs zero binds.
9. All new behavior runs under ordinary `cargo test -p whale-sdk-rust`; no test is
   ignored and no CLI/UI artifact is created.

## Deferred work

- Dependency graphs between packs. Declaration order is the bind order and reverse
  declaration order is the close order.
- Hot rebind, pack replacement, or pack-owned dynamic schema updates.
- A typed top-level Agent/session error redesign. That requires a separate
  compatibility decision because `SdkError` is currently exhaustive.
- Per-pack bind/close timeouts and structured runtime cleanup diagnostics.
- Loading ToolPacks from configuration, disk, MCP manifests, or a plugin registry.
- Exposing ToolPack in Python or Java.
