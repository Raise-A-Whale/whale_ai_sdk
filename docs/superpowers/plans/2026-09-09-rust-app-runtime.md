# Rust Application Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` task by task. Every behavior change follows RED → GREEN → focused regression. This repository already contains the uncommitted A–C5 dependency set, so write reports and ledger checkpoints but do not create partial commits.

**Goal:** Give Rust application hosts a ready-before-return, lifecycle-owned `WhaleRuntime` for embedded, managed-process, and external-UDS execution. This stage builds the SDK foundation that a CLI, TUI, desktop application, or service can consume; it does not build any of those products.

**Architecture:** Separate the cloneable `WhaleClient` connection capability from a non-cloneable connection owner before exposing `WhaleRuntime`. The owner holds the SDK reader and the owned endpoint, runs one cancellation-safe shutdown transaction, and publishes one immutable result. `WhaleRuntime::open` creates that owner immediately, then completes the existing protocol handshake against a single startup deadline.

**Tech stack:** Rust 2021, Tokio, Whale JSON-RPC v1, deterministic local stdio and UDS fixtures.

**Spec:** [Whale Rust Application SDK Design](../specs/2026-09-09-rust-app-sdk-design.md), delivery stage 1.

## Global constraints

- Change Rust runtime/SDK code and Rust-facing documentation only. Do not modify Python or Java.
- Do not create a CLI, TUI, desktop shell, project browser, product tool suite, installer, marketplace, or global discovery layer.
- Preserve the public signatures and ordinary behavior of `WhaleClient`, `Agent`, `WhaleThread`, and `RunHandle`.
- Keep one daemon-centered protocol path for all sources; do not add a second direct Engine execution path.
- Do not add a runtime wire capability. Startup and shutdown are local SDK lifecycle semantics.
- `WhaleRuntime::open` returns only after one successful `protocol.initialize` handshake.
- `startup_timeout` and `shutdown_timeout` must be positive. Validate them before spawning, connecting, or starting a server task.
- Managed commands use `Command::new`, `OsString` arguments, and child-only environment entries. Never invoke a shell and never expose environment values through `Debug` or `RuntimeInfo`.
- External UDS shutdown closes only the local connection and never terminates the external daemon.
- Stage 1 shutdown guarantees only the SDK reader, local writer, embedded `DaemonServer::run` connection loop, and direct managed child. Do not claim that detached daemon request/run/store tasks are all joined.
- Every behavioral production change must be preceded by a test that fails for the expected reason.
- Keep reports under `.superpowers/sdd/2026-09-09-rust-app-runtime/` and append exact commands/results to `progress.md`.

---

### Task 1: Split client capability from connection ownership

**Files:**

- Modify: `crates/whale-sdk-rust/src/lib.rs`
- Create: `crates/whale-sdk-rust/src/connection.rs`
- Create: `crates/whale-sdk-rust/src/connection_tests.rs`
- Modify: `crates/whale-sdk-rust/src/session_close_tests.rs` only if a legacy assertion needs a bounded wait

**Produces:** one internal owner/stop transaction used by every transport and by legacy `WhaleClient::close`.

- [x] Write RED tests that prove:
  - closing a legacy in-process client waits until its `DaemonServer::run` loop has processed EOF and removed that connection's Session;
  - two concurrent close callers and one later caller observe one cleanup transaction;
  - dropping an ordinary non-final `WhaleClient` clone does not close the connection;
  - closing a channel-backed writer drops its sender so the embedded server sees EOF.
- [x] Run the focused tests and record the expected failure against the current untracked reader/server tasks.
- [x] Introduce an internal split similar to:

  ```rust
  pub(crate) struct OpenConnection {
      pub client: WhaleClient,
      pub owner: ConnectionOwner,
      pub process_id: Option<u32>,
  }

  pub(crate) struct ConnectionOwner { /* strong command + shared completion */ }
  pub(crate) struct ConnectionStop { /* non-owning command + shared completion */ }
  ```

  Exact private names may change. The invariant may not: `ConnectionOwner` is non-cloneable and alone owns the reader plus endpoint; client clones carry only a non-owning stop handle. Legacy constructors store one compatibility owner inside their shared `ClientInner`; runtime constructors return the owner separately.
- [x] Run a single owner task for explicit close, peer EOF, compatibility close, owner Drop, and later startup rollback. Cleanup must fence `ClientState`, close the local writer, join/abort the reader, then join/abort the embedded server loop or wait/kill/reap the child. Concurrent callers wait on the same completion. Canceling a waiter cannot cancel owner cleanup.
- [x] Fix `ManagedWriter::close` so both I/O and channel targets are removed; close the nested `FrameWriter` when present.
- [x] Refactor `WhaleClient::{in_process,spawn_daemon,spawn_daemon_with_args,connect_uds}` and the private I/O constructor to use the owner without changing public signatures. `WhaleClient::close()` keeps returning `()` and ignores the structured internal outcome.
- [x] Limit timeout/forced semantics here to an internal result; public runtime types arrive in Task 2.
- [x] Run:

  ```bash
  cargo test -p whale-sdk-rust --lib connection -- --nocapture
  cargo test -p whale-sdk-rust --lib session_close -- --nocapture
  cargo test -p whale-sdk-rust --test integration_tests -- --nocapture
  ```

- [x] Append the pass counts and implementation report path to `progress.md`.

---

### Task 2: Add the public embedded application runtime

**Files:**

- Create: `crates/whale-sdk-rust/src/runtime.rs`
- Modify: `crates/whale-sdk-rust/src/lib.rs`
- Modify: `crates/whale-sdk-rust/src/initialization.rs`
- Modify: `crates/whale-sdk-rust/src/initialization_tests.rs`
- Create: `crates/whale-sdk-rust/tests/application_runtime.rs`

**Produces:** public Rust runtime types and a cancellation-safe embedded `open` path.

- [x] Write RED public API tests for `RuntimeOptions::embedded()` and an explicit by-value embedded server. Assert that:
  - `open` has completed initialization before returning;
  - `RuntimeInfo { mode: Embedded, peer, process_id: None }` matches `client.initialize()`;
  - an Agent and Session can be created through the runtime without constructing transport objects;
  - zero startup or shutdown durations fail before the retained server accepts a connection;
  - keeping a client clone does not keep the runtime owner alive after the runtime is dropped.
- [x] Run `cargo test -p whale-sdk-rust --test application_runtime embedded_ -- --nocapture` and record the compile failure because the runtime API does not exist.
- [x] Add and re-export:

  ```rust
  pub enum RuntimeMode { Embedded, ManagedProcess, ExternalUds }
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
  pub enum ShutdownDisposition { Graceful, Forced }
  pub struct RuntimeShutdown {
      pub mode: RuntimeMode,
      pub disposition: ShutdownDisposition,
  }
  pub struct WhaleRuntime { /* non-Clone */ }
  ```

- [x] Add a cloneable, comparable, structured `RuntimeError` covering invalid configuration, source/startup failure, startup timeout, shutdown phase failure, and shutdown timeout. Store strings rather than non-cloneable I/O sources so concurrent callers can observe the same outcome.
- [x] Implement redacted manual `Debug` for `RuntimeSource` and `RuntimeOptions`. Embedded debug output must not require `DaemonServer: Debug`; managed output may include executable, arguments, and environment names, never environment values.
- [x] Refactor initialization so the connection-owned single flight accepts the runtime's remaining startup deadline instead of spawning an uncancelled fixed ten-second worker. Existing `WhaleClient::initialize()` keeps the ten-second public default.
- [x] Implement `WhaleRuntime::open`, `info`, `client`, `agent`, and bounded/idempotent `shutdown`. `open` validates first, creates the connection owner immediately, and awaits initialization. Dropping or aborting `open` drops the strong owner and therefore starts emergency cleanup.
- [x] The public `shutdown_timeout` is a total budget. Reserve a final portion for forced abort/kill/reap rather than letting the graceful wait consume the whole deadline.
- [x] Run:

  ```bash
  cargo test -p whale-sdk-rust --test application_runtime embedded_ -- --nocapture
  cargo test -p whale-sdk-rust --lib initialization -- --nocapture
  ```

- [x] Append results and the review outcome to `progress.md`.

---

### Task 3: Add managed-process runtime and deterministic lifecycle tests

**Files:**

- Modify: `crates/whale-sdk-rust/src/runtime.rs`
- Modify: `crates/whale-sdk-rust/src/connection.rs`
- Modify: `crates/whale-sdk-rust/tests/application_runtime.rs`
- Create: `crates/whale-sdk-rust/tests/fixtures/runtime_peer.sh`
- Create: `crates/whale-sdk-rust/tests/support/mod.rs`
- Create: `crates/whale-sdk-rust/tests/support/process.rs`

**Produces:** exact managed commands, ready-before-return, startup rollback, and graceful/forced child reap.

- [x] Create an executable POSIX test peer that needs no prebuilt Whale binary and no opt-in environment. It must record PID, argv, one initialize request, stdin EOF, normal exit, and signal/linger behavior. Support at least `ready_after_release`, `current`, `incompatible`, `no_reply`, and `linger_after_eof`. Construct the current initialization response in Rust and pass it as one `OsString`; do not duplicate the capability list in the shell script.
- [x] Write RED tests that prove:
  - `open` remains pending after the peer receives initialize and completes only after the ACK release;
  - `RuntimeInfo.process_id` is the actual live child PID;
  - argv elements containing spaces and Unix non-UTF-8 bytes arrive as separate elements without shell parsing;
  - `Debug` includes environment names and excludes their values;
  - zero deadlines and invalid environment names create no child;
  - timeout, incompatible handshake, EOF, and an aborted `open` reap the partially started child before their cleanup completes;
  - normal shutdown observes stdin EOF and reaps the child with `Graceful`;
  - a peer that ignores EOF is killed and reaped with `Forced` within the total shutdown budget;
  - concurrent and repeated shutdown calls return the same report and do not repeat EOF/kill.
- [x] Run the managed subset and record the expected failures.
- [x] Implement `RuntimeOptions::managed`, `with_arg`, and `with_environment`. Reject empty names, `=`, and NUL. Reject user arguments that try to supply Whale's owned `--listen` option. Runtime code adds `--listen stdio` and passes every other element directly to `Command`.
- [x] Apply environment overrides only to the child. Keep inherited environment unless the caller explicitly overrides a name.
- [x] Move the child into `ConnectionOwner` immediately after extracting stdin/stdout. Any failure before that transfer must kill and await the child. Set `kill_on_drop(true)` as the last emergency fallback.
- [x] Map initialization timeout separately from incompatibility/source errors, and do not dispatch a business request after handshake failure.
- [x] Run:

  ```bash
  cargo test -p whale-sdk-rust --test application_runtime managed_ -- --nocapture
  cargo test -p whale-sdk-rust --test protocol_initialization -- --nocapture
  ```

- [x] Append results and review outcome to `progress.md`.

---

### Task 4: Add external UDS runtime and cross-source ownership tests

**Files:**

- Modify: `crates/whale-sdk-rust/src/runtime.rs`
- Modify: `crates/whale-sdk-rust/src/connection.rs`
- Modify: `crates/whale-sdk-rust/tests/application_runtime.rs`
- Create: `crates/whale-sdk-rust/tests/support/uds.rs`

**Produces:** attached UDS semantics and complete public lifecycle behavior across all sources.

- [x] Add a Tokio `UnixListener` fixture that parses the real initialization request, echoes its actual request ID, gates the ACK with a semaphore/notification, reports accept/initialize/EOF events, and accepts multiple connections.
- [x] Write RED tests that prove:
  - external `open` is pending until the UDS peer acknowledges initialization;
  - a failed or aborted `open` closes only its socket, after which the same listener accepts a successful runtime;
  - shutting down one external runtime leaves another connection and the external listener usable;
  - concurrent and sequential shutdown return the same immutable report;
  - aborting the first shutdown waiter does not cancel cleanup observed by a second waiter;
  - shutdown invalidates every previously cloned client capability;
  - two embedded runtimes created from `server.clone()` own separate connection loops, so closing one removes only its Sessions;
  - embedded shutdown waits for connection-level Session cleanup, within the Stage 1 guarantee.
- [x] Run the UDS/cross-source subset and record the expected failures.
- [x] Implement `RuntimeOptions::external_uds` and source creation under the one startup deadline. Store no remote process identity. External owner cleanup closes and joins/aborts only local SDK tasks.
- [x] Make runtime Drop send the same stop request without awaiting. It must not panic outside a Tokio context; retained client clones cannot keep managed/embedded ownership alive.
- [x] Run the full application runtime file and legacy transport regressions:

  ```bash
  cargo test -p whale-sdk-rust --test application_runtime -- --nocapture
  cargo test -p whale-sdk-rust --test integration_tests -- --nocapture
  cargo test -p whale-sdk-rust --lib session_close -- --nocapture
  ```

- [x] Append results and review outcome to `progress.md`.

---

### Task 5: Document the Rust host integration boundary and verify Stage 1

**Files:**

- Modify: `README.md`
- Modify: `docs/SDK_ARCHITECTURE_REVIEW.md`
- Create: `docs/RUST_APPLICATION_SDK.md`
- Modify: `crates/whale-sdk-rust/src/lib.rs` only to include the canonical guide in crate docs so its `no_run` example is compiled
- Modify: `docs/superpowers/plans/2026-09-09-rust-app-runtime.md`

**Produces:** one canonical host integration guide and final evidence without a product CLI implementation.

- [x] Add a compile-checked `no_run` example that opens the default embedded runtime, defines an `AgentDefinition`, creates a Session, obtains a `RunHandle`, waits for a result, closes the Session, and shuts down the Runtime. The example is library usage only.
- [x] Document all three sources, exact ownership, startup readiness, environment redaction, shutdown reports, Drop as emergency cleanup, and the Stage 1 task-join limitation.
- [x] Update the architecture assessment so the current recommendation is Rust-only application SDK work. Remove Python/Java parity and example CLI implementation from the next-step acceptance criteria.
- [x] Audit the changed files for placeholders and forbidden product work:

  ```bash
  rg -n "TODO|unimplemented!|RuntimeOwnership|runtime_lifecycle\.v1" \
    crates/whale-sdk-rust README.md docs/RUST_APPLICATION_SDK.md \
    docs/SDK_ARCHITECTURE_REVIEW.md \
    docs/superpowers/specs/2026-09-09-rust-app-sdk-design.md
  find . -path './target' -prune -o -type f \
    \( -iname '*cli*' -o -iname '*tui*' -o -iname '*desktop*' \) -newer \
    docs/superpowers/specs/2026-09-09-rust-app-sdk-design.md -print
  ```

  Matches inside explanatory documentation must be reviewed manually; there must be no new product implementation.
- [x] Run final formatting and verification:

  ```bash
  cargo fmt --all -- --check
  cargo test --workspace
  cargo test --doc -p whale-sdk-rust
  git diff --check
  ```

- [x] Record exact pass/ignore counts and independent whole-stage review findings in `progress.md` and the verification ledger below.

## Verification ledger

This section is filled with commands actually run against the final source. Planned commands and earlier-stage results are not completion evidence.

### Task 5 documentation checkpoint

- `cargo test --doc -p whale-sdk-rust` — 1 passed, 0 failed, 0 ignored; rustdoc compiled the canonical embedded Runtime → AgentDefinition → WhaleThread → RunHandle → Session close → Runtime shutdown example included from `docs/RUST_APPLICATION_SDK.md`.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- Placeholder audit — the only matches were the design's explanatory rejection of `RuntimeOwnership` and the guide's statement that no `runtime_lifecycle.v1` wire capability exists; there were no `TODO` or `unimplemented!` matches in the audited paths.
- Product-file audit — no new file whose name contains CLI, TUI, or Desktop was created after the Stage 1 design.
- `cargo test --workspace --quiet` — **408 passed, 0 failed, 14 ignored** across 57 unit, integration, and rustdoc suites. The ignored set is the existing explicitly gated fixture/environment coverage; no Stage 1 Runtime test is ignored.
- `cargo check --workspace` — passed.
- `sh -n crates/whale-sdk-rust/tests/fixtures/runtime_peer.sh` — passed.
- Independent whole-stage public API/documentation review — **0 findings** (Critical 0, Important 0, Minor 0). The reviewer also ran `cargo check -p whale-sdk-rust`, the one rustdoc test, formatting, diff checking, and verified all 71 relative Markdown links resolve.
