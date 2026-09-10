# Releasing

This SDK is consumed **internally via git dependencies**; we do not publish to
crates.io. Version management is handled by `.github/workflows/release.yml`:
pushing a `v*` tag validates the workspace, runs the test suite, and creates a
GitHub Release with generated notes. No secrets or extra setup are required.

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

## Consuming the SDK (internal projects)

Pin a released tag in the consumer's `Cargo.toml`:

```toml
[dependencies]
whale-sdk-rust = { git = "https://github.com/Raise-A-Whale/whale_ai_sdk", tag = "v0.2.0" }
```

Cargo locates the `whale-sdk-rust` crate inside the workspace automatically.
Use `branch = "main"` to track development, or `rev = "<sha>"` to pin an exact
commit. Regardless of the ref used, `Cargo.lock` records the resolved commit;
to upgrade, bump the tag and run `cargo update -p whale-sdk-rust`.

### Private repository access

If this repository is private, consumer machines need read access:

- **SSH (recommended for local development):** use the SSH URL and let the
  system git handle keys via `~/.ssh`:

  ```toml
  whale-sdk-rust = { git = "ssh://git@github.com/Raise-A-Whale/whale_ai_sdk.git", tag = "v0.2.0" }
  ```

  ```toml
  # ~/.cargo/config.toml
  [net]
  git-fetch-with-cli = true
  ```

- **Consumer CI:** grant read access with a deploy key or a GitHub App/PAT
  configured as a git credential before `cargo build` runs.

## If we ever publish to crates.io

`ci.yml` already runs `scripts/verify_rust_packages.sh` on every PR, so the
crates stay publish-ready. Going public only means adding a `cargo publish`
job back to the release workflow plus a `CARGO_REGISTRY_TOKEN` secret — crate
names on crates.io are first-come-first-served, so decide before the project
gains outside visibility.
