# Rust package and clean-consumer release design

Status: design for the Rust-only Agent SDK goal. It prepares reviewable local
artifacts; it does not publish crates, tags, binaries, a CLI, or another
language SDK. The former Python and Java SDK directories were later removed by
explicit user instruction.

## Goal

A Rust application outside this repository must be able to consume the packaged
Whale crates and build the public flow
`WhaleRuntime -> Agent -> WhaleThread -> RunHandle` without relying on files that
Cargo omitted from a crate archive. Every workspace crate must pass Cargo's
package checks with explicit internal dependency versions and truthful package
metadata.

The clean-consumer gate also covers the extension surfaces that make Whale a
substrate rather than only a fixed client: a custom `ModelProvider` registered
through `ProviderRegistry`, a custom `HostContextPolicy`, a `ToolPack`, and a
configured `SessionStore`. Provider and Store construction currently live in
their owning crates, so this fixture may depend explicitly on the packaged SDK,
Core, Daemon, and Store crates. The SDK documentation must describe that package
boundary accurately; a happy-path SDK-only fixture is not evidence that these
extension crates are distributable.

This stage validates source packages. It does not promise an automatically
downloaded daemon sidecar. Embedded mode remains the zero-discovery application
path; managed mode continues to require an executable supplied by the host.
Current Runtime support remains Unix-only and must be stated in the SDK package
README rather than hidden behind a successful macOS build.

The workspace declares Rust 1.85 as its initial MSRV. That is the highest
declared minimum among the currently selected direct dependencies
(`jsonschema` and `clap`) and is verified with the installed 1.85.x toolchain.
Local macOS verification alone is reported as macOS verification; the packages
cannot claim Linux-tested or Unix-wide release support until the same package,
embedded, managed-process, UDS, and SQLite matrix passes on Linux CI.

## Package graph and publication order

The dependency graph is intentionally acyclic:

```text
whale-protocol
  ├── whale-store
  └── whale-adapters
        \       /
         whale-core
             |
         whale-daemon
             |
        whale-sdk-rust
```

The actual graph also has `whale-core -> whale-store` and
`whale-daemon -> {protocol,store,adapters,core}`. Packages are checked in this
topological order: protocol, store, adapters, core, daemon, SDK.

All internal entries in `[workspace.dependencies]` retain their local `path` and
gain an exact compatible version for packaging:

```toml
whale-protocol = { version = "0.1.0", path = "crates/whale-protocol" }
```

The workspace version stays the source of truth for each package. This permits
local development while ensuring a packaged manifest can resolve dependencies
from a registry. No crate is uploaded in this stage.

## Metadata, license, and package-local documentation

Every crate inherits workspace authors and Apache-2.0 license, declares a short
description and the HTTPS repository URL, and points to a README inside its own
package directory. The root already declares `license = "Apache-2.0"`; add the
matching root `LICENSE` text so the declaration and distributed artifact agree.

The SDK currently expands `../../../docs/RUST_APPLICATION_SDK.md` from
`src/lib.rs`. That path escapes the crate root and will not exist after Cargo
unpacks the crate. Replace it with a package-local README include. The local SDK
README must state the no-CLI boundary, the three Runtime sources and their
ownership, Unix support, and link to the repository for the full architecture
documents. Other crate READMEs can stay short but must describe their public
responsibility accurately.

Package contents must not include recovery secrets, environment values, local
SQLite databases, build output, or developer-only task logs. Cargo's generated
`.crate` contents are inspected, not inferred from `.gitignore` alone.

## Verification without publishing

A repository script creates a temporary directory and runs these checks:

1. `cargo package --allow-dirty --no-verify` for all six crates in topological
   order, failing on warnings that indicate broken metadata or omitted docs.
2. unpack every generated `.crate` file and assert the manifest, README, license,
   and SDK rustdoc include targets exist;
3. create a temporary registry-shaped workspace whose `[patch.crates-io]`
   entries point at the unpacked packages, then run `cargo check` for each package;
4. create a separate minimal consumer that imports only public
   `whale-sdk-rust` APIs and type-checks embedded and managed construction plus
   Agent/Session/Run/close/shutdown calls; and
5. create an extension consumer that composes a custom Provider factory,
   ContextPolicy, ToolPack, and Memory/SQLite Store from their packaged public
   crates; and
6. scan archive file names and text for known fixture secret markers and local
   absolute workspace paths.

The consumer is compile-only and makes no model request. It proves package
closure and public API reachability without introducing a sample CLI.

Normal verification also retains `cargo test --workspace`, SDK doc tests,
`cargo doc --workspace --no-deps` with rustdoc warnings denied, default and
all-feature checks where a crate exposes features, `cargo fmt --all -- --check`,
and `git diff --check`. The report records an advisory/license scan or names the
tooling that was unavailable; absence of that tooling is never reported as a
clean supply-chain result. Package verification is run after Interaction,
Session management, and ToolPack code settles so newly added dependencies are
included in the same audit.

Before the first package is considered releasable, capture the public Rust API
surface and make an explicit extensibility decision for enums and option structs.
In particular, `RuntimeSource`, runtime shutdown/error enums, and SDK error
families must either be intentionally exhaustive for 0.1 or use
`#[non_exhaustive]`; the verifier records the decision and detects unintended
surface drift. This is a source-compatibility gate, not a promise of 1.0 API
stability.

## Explicit limits

- No `cargo publish`, git tag, release, binary upload, or registry credentials.
- No bundled/downloaded sidecar contract; that requires a separate signed
  artifact/version-discovery design.
- No Windows support claim. A later transport portability stage can cfg-gate UDS
  and add a supported Windows transport. Until Linux CI is present, release
  documents distinguish the macOS-verified host from the Unix-targeted design.
- No feature split between an attached lightweight client and embedded runtime
  in this pass. That changes the public dependency surface and needs separate
  benchmarks and compatibility tests.
- No Python or Java SDK is shipped or recreated. A deterministic Python script
  used only as an incompatible protocol peer may remain in test infrastructure.

## Acceptance

The stage is complete when every crate archive builds from its unpacked contents,
the isolated public consumer checks successfully, the SDK rustdoc no longer
references a repository-external file, package metadata matches the declared
Apache-2.0 license, the declared MSRV builds, the extension consumer checks, and
all existing Rust tests still pass. The verification report records archive
sizes, commands, platform evidence, public-API decision, and dependency-audit
coverage, but does not describe the packages as published or production-ready.
