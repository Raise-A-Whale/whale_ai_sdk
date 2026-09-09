# Rust package and clean-consumer release hardening plan

> Scope: local Rust source-package readiness only. Do not publish, tag, push,
> build a CLI/UI, or recreate another language SDK. A later explicit user
> instruction narrowed the repository to Rust-only, so the former Python and
> Java SDK directories are deleted as part of the final boundary review.

Design: [Rust package and clean-consumer release design](../specs/2026-09-09-rust-release-hardening-design.md).

## Task 1: Freeze failing package and consumer checks

- Create `scripts/verify_rust_packages.sh` (or a Rust helper if shell cannot
  safely express archive handling) using a temporary directory.
- Add a compile-only external consumer fixture under `fixtures/rust-consumer`.
  It imports public SDK types and checks embedded/managed Runtime construction,
  Agent creation, Session creation, RunHandle use, Session close, and shutdown.
- Add a second compile-only extension fixture. It composes a custom
  `ModelProvider`/`ProviderRegistry`, `HostContextPolicy`, `ToolPack`, and
  Memory/SQLite `SessionStore` using only APIs from the packaged crates. Keep the
  owning-crate dependencies explicit unless the final public docs promise SDK
  re-exports.
- Run the verifier and record the current failures: internal path dependencies
  without versions and the SDK rustdoc include outside its package.
- The verifier must never invoke `cargo publish` and must clean temporary output.

## Task 2: Make all manifests package-complete

- Add `version = "0.1.0"` alongside every internal workspace path dependency.
- Add workspace repository metadata and inherit authors/license/repository in all
  six crates; give each crate an accurate description and package-local README.
- Declare `rust-version = "1.85"` in workspace package metadata and inherit it in
  all six crates. Run the package and consumer checks with the installed 1.85.x
  toolchain as well as the default toolchain.
- Add the Apache-2.0 `LICENSE` matching the existing workspace declaration.
- Move the SDK crate-level doc include to its package-local README.
- Do not add registry tokens, sidecar download logic, CLI code, or a new public
  Runtime abstraction.

## Task 3: Verify unpacked archives and public API closure

- Package protocol, store, adapters, core, daemon, and SDK in dependency order.
- Inspect every archive for manifest/README/license presence, local absolute paths,
  and known secret fixtures.
- Unpack archives, patch registry names to those unpacked paths in a temporary
  workspace, and `cargo check` every crate.
- Check the isolated consumer against the unpacked SDK dependency graph.
- Check the extension consumer against the unpacked SDK/Core/Daemon/Store graph.
- Run SDK doc tests, `cargo test --workspace`, default/all-feature package checks,
  `RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps`,
  `cargo fmt --all -- --check`, and `git diff --check`.
- Capture the public API surface and record the deliberate exhaustive versus
  `#[non_exhaustive]` policy for Runtime source/options/errors and SDK errors.
- Run available advisory/license tooling; if it is unavailable, record that as an
  unchecked release item rather than a pass.
- Record exact archive sizes, test counts, MSRV result, platform evidence, and any
  Cargo/rustdoc warnings in a local verification report. A macOS-only run must not
  claim Linux-tested or generic Unix release support.

## Task 4: Review the release boundary

- Confirm no publish/tag/push occurred.
- Confirm managed mode still requires a caller-provided Whale Daemon executable
  and does not accept Codex, Claude Code, OpenCode, pi, or other product CLIs.
- Confirm the package docs state that current Runtime support is Unix-only.
- Confirm the package docs distinguish Unix-targeted code from the platforms
  actually verified, and document trusted-local-peer/socket-permission assumptions
  for external UDS.
- Confirm the Python/Java SDK directories are absent and no product
  CLI/TUI/Desktop code was added. The standalone Python protocol peer fixture
  may remain because it is test infrastructure rather than a shipped SDK.
