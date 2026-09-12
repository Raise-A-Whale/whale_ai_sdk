# Releasing

This SDK is released from Git tags. Version management is handled by `.github/workflows/release.yml`:
pushing a `v*` tag validates the workspace, runs the test suite, creates a
GitHub Release with generated notes, and publishes the six workspace crates to
crates.io. The release workflow requires the `CRATES_IO_TOKEN` repository
secret.

## Cutting a release

1. Make sure `main` is green and the release-worthy changes are merged.
2. Bump `[workspace.package] version` in the root `Cargo.toml`, and update the
   six `whale-*` entries under `[workspace.dependencies]` to the same version.
   Commit to `main` (a PR is recommended).
3. Tag the release commit and push the tag:

   ```bash
   git tag v0.2.0 <commit>
   git push origin v0.2.0
   ```

4. Watch the **Release** workflow. It will:
   - check that the tag version matches the workspace version (fails fast on
     mismatch),
   - run `cargo test --workspace --all-features`,
   - create the GitHub Release with auto-generated notes.

Tags containing a hyphen (for example `v0.2.0-beta.1`) are marked as
pre-releases on GitHub.

## Consuming the SDK

Released versions are published to crates.io by the tag workflow. Consumers
can select a released version from the registry:

```toml
[dependencies]
whale-sdk-rust = "0.1.0-beta.1"
```

To consume an unreleased commit or test a repository tag directly, use a Git
dependency instead:

```toml
[dependencies]
whale-sdk-rust = { git = "https://github.com/Raise-A-Whale/whale_ai_sdk", tag = "v0.1.0-beta.1" }
```

Cargo locates the `whale-sdk-rust` crate inside the workspace automatically.
Use `branch = "main"` to track development, or `rev = "<sha>"` to pin an exact
commit. Regardless of the ref used, `Cargo.lock` records the resolved commit.

## Publishing to crates.io

The `publish-crates` release job requires the `CRATES_IO_TOKEN` repository
secret and publishes all six crates in topological dependency order. It treats
an already-published version as success and waits for registry propagation
between dependent crates. Before pushing a tag, verify that the crate names are
available, the token is configured, and `scripts/verify_rust_packages.sh`
passes. If the token is absent or another publish error occurs, the GitHub
Release may already exist while the crates.io job fails.
