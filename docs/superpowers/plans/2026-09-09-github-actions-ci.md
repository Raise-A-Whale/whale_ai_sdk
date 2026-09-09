# GitHub Actions CI Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add GitHub Actions workflows that reject PRs which fail Rust formatting, linting, compilation, tests, documentation, MSRV, or package-integrity checks, while keeping dependency auditing separate from the PR gate.

**Architecture:** `.github/workflows/ci.yml` is the deterministic PR gate. It uses independent jobs for formatting, stable Clippy, stable tests, rustdoc, MSRV compilation, and the existing package verifier. `.github/workflows/security.yml` runs RustSec auditing on a weekly schedule and manually, so external advisory-service availability does not block normal PR feedback.

**Tech Stack:** GitHub Actions YAML, `actions/checkout@v4`, `actions/cache@v4`, `actions-rust-lang/setup-rust-toolchain@v1`, Rust stable, Rust `1.85.1`, Cargo workspace commands, Bash package verifier, RustSec audit action.

**Spec:** `docs/superpowers/specs/2026-09-09-github-actions-ci-design.md`

## Global Constraints

- The workflows validate the Rust application SDK, daemon, protocol, adapters, core, and store only; no CLI, TUI, or desktop product is built.
- Workflow permissions are read-only: `contents: read`.
- PR checks use no secrets or provider credentials.
- Stable jobs use all targets and all features where specified by the spec.
- MSRV is exactly Rust `1.85.1`.
- The package job invokes the existing `scripts/verify_rust_packages.sh` without changing its behavior.
- Do not use `continue-on-error` for quality checks.

### Task 1: Add the PR CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `crates/whale-protocol/src/canonical.rs`
- Modify: `crates/whale-adapters/src/openai.rs`
- Modify: `crates/whale-protocol/tests/session_view_contract.rs`

**Interfaces:**
- Consumes: root `Cargo.toml`, `Cargo.lock`, all workspace crates, and `scripts/verify_rust_packages.sh`.
- Produces: six named GitHub checks: `fmt`, `clippy`, `test`, `docs`, `msrv`, and `package`.

- [x] **Step 1: Create workflow triggers, permissions, and concurrency**

Create `.github/workflows/ci.yml` with:

```yaml
name: Rust CI

on:
  pull_request:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: rust-ci-${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true
```

- [x] **Step 2: Add a shared stable Rust setup pattern**

Each stable job checks out the repository, installs stable Rust with `actions-rust-lang/setup-rust-toolchain@v1`, and restores Cargo cache with `actions/cache@v4` using the workspace lockfile:

```yaml
- uses: actions/checkout@v4
- uses: actions-rust-lang/setup-rust-toolchain@v1
  with:
    toolchain: stable
- uses: actions/cache@v4
  with:
    path: |
      ~/.cargo/registry
      ~/.cargo/git
      target
    key: ${{ runner.os }}-rust-${{ hashFiles('**/Cargo.lock') }}
```

The cache step may be repeated per job so each job remains independently runnable and diagnosable.

- [x] **Step 3: Add `fmt` and `clippy` jobs**

Use `runs-on: ubuntu-latest` and run:

```yaml
cargo fmt --all -- --check
```

and:

```yaml
cargo clippy --workspace --all-targets --all-features -- \
  -D warnings \
  -A clippy::too_many_arguments \
  -A clippy::type_complexity
```

The Clippy job must not set `continue-on-error`. The command allows only the two intentional structural lints `clippy::too_many_arguments` and `clippy::type_complexity`; all other warnings remain errors.

- [x] **Step 4: Add `test`, `docs`, and `msrv` jobs**

The stable test job runs:

```yaml
cargo test --workspace --all-features
```

The docs job runs:

```yaml
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

The MSRV job installs Rust `1.85.1` and runs:

```yaml
cargo check --workspace --all-targets
```

- [x] **Step 5: Add the package integrity job**

The package job installs stable Rust, checks out the repository, and runs:

```yaml
bash scripts/verify_rust_packages.sh
```

The job must expose the script's output directly and fail on its non-zero exit status.

- [x] **Step 6: Parse and inspect the workflow**

Run a YAML parser if available and inspect the resulting job names, trigger keys, permissions, and command strings. If the environment lacks a YAML parser, use Ruby's standard YAML parser only for syntax validation and separately inspect the `on` key because YAML 1.1 parsers may coerce it to a boolean.

Expected: six jobs are present, all jobs run on Ubuntu, stable jobs use the shared toolchain/cache pattern, and the MSRV job uses exactly `1.85.1`.

### Task 2: Add the scheduled RustSec audit workflow

**Files:**
- Create: `.github/workflows/security.yml`

**Interfaces:**
- Consumes: root `Cargo.lock` and Cargo manifests.
- Produces: a scheduled/manual `audit` check which fails when RustSec reports an advisory.

- [x] **Step 1: Create restricted triggers and permissions**

Create the workflow with:

```yaml
name: Rust Security Audit

on:
  schedule:
    - cron: '17 3 * * 1'
  workflow_dispatch:

permissions:
  contents: read
```

The workflow must not trigger on `pull_request` and must not request issue-write or package-write permissions.

- [x] **Step 2: Add the audit job**

Use `ubuntu-latest`, check out the repository, install stable Rust with `actions-rust-lang/setup-rust-toolchain@v1`, then run the maintained RustSec action:

```yaml
- uses: actions-rust-lang/audit@v1
  name: Audit Rust dependencies
  with:
    createIssues: "false"
```

Do not add advisory ignores without a repository-approved advisory ID and rationale.

- [x] **Step 3: Validate security workflow semantics**

Check that only `schedule` and `workflow_dispatch` exist under `on`, the job has `contents: read`, and no secrets or write permissions appear.

### Task 3: Keep the existing Rust code compatible with the strict lint gate

**Files:**
- Modify: `crates/whale-protocol/src/canonical.rs`
- Modify: `crates/whale-adapters/src/openai.rs`
- Modify: `crates/whale-protocol/tests/session_view_contract.rs`

**Interfaces:**
- Consumes: the public enum defaults and contract test behavior already covered by the workspace tests.
- Produces: no API behavior change; Clippy-clean derived defaults and a non-cloning one-item slice.

- [x] **Step 1: Replace derivable defaults**

Derive `Default` on `MessagePhase` and `OpenAIWireApi`, marking `FinalAnswer` and `ChatCompletions` as their default variants. Remove the equivalent manual `impl Default` blocks.

- [x] **Step 2: Remove the avoidable test clone**

Pass `std::slice::from_ref(&history_item)` to `SessionHistoryWindow::from_history` so the contract test does not clone a single item merely to construct a one-element slice.

- [x] **Step 3: Run the strict lint command**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- \
  -D warnings \
  -A clippy::too_many_arguments \
  -A clippy::type_complexity
```

Expected: formatting and Clippy both pass without any other warnings.

### Task 4: Validate and document the workflows

**Files:**
- Modify: `docs/superpowers/plans/2026-09-09-github-actions-ci.md` (checklist status only)
- Modify: PR description if the workflow checks produce new results

**Interfaces:**
- Consumes: `.github/workflows/ci.yml`, `.github/workflows/security.yml`, existing Rust verification commands.
- Produces: reproducible local validation evidence and a clean commit ready for the open PR.

- [x] **Step 1: Run static workflow checks**

Run:

```bash
git diff --check
bash -n scripts/verify_rust_packages.sh
```

Run a YAML parser or `actionlint` if installed. If neither is available, run a Python parser from an available YAML library and retain the explicit key/value inspection from Tasks 1 and 2.

- [x] **Step 2: Run the Rust checks represented by CI**

Run:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

Run the package verifier as its own command:

```bash
bash scripts/verify_rust_packages.sh
```

If local toolchains include Rust `1.85.1`, also run:

```bash
RUSTUP_TOOLCHAIN=1.85.1 cargo check --workspace --all-targets
```

- [x] **Step 3: Inspect the final diff**

Run:

```bash
git status --short
git diff --stat
git diff -- .github/workflows/ci.yml .github/workflows/security.yml
```

Expected: only the two workflow files, the implementation plan, and any intentional documentation updates are included; local editor, Codex, and CodeGraph metadata remain untracked.

- [x] **Step 4: Commit the implementation**

```bash
git add .github/workflows/ci.yml .github/workflows/security.yml docs/superpowers/plans/2026-09-09-github-actions-ci.md
git commit -m "ci: add Rust pull request quality gates"
```
