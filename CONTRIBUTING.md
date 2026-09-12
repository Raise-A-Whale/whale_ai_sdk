# Contributing to Whale AI SDK

Thank you for contributing. Issues and focused pull requests are welcome.

## Development environment

- Rust 1.85 or newer; CI also checks the workspace with Rust 1.85.1.
- A Unix environment. CI runs on Ubuntu, and local validation has been performed on macOS.

Clone the repository, create a topic branch, and keep each change scoped to one problem. Do not commit generated build output, local credentials, editor state, or AI-tool configuration.

## Before submitting a pull request

Run the checks relevant to your change. For a full local verification:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D clippy::correctness
cargo test --workspace --all-features
RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace --no-deps
cargo run -q -p whale-protocol --example initialization_contract -- --check
cargo build -p whale-daemon --bins --examples
./scripts/verify_rust_packages.sh
```

Pull requests should explain the problem, the chosen solution, and how the change was verified. Add or update tests when behavior changes, and update public documentation when an API or protocol contract changes.

## Compatibility

Review the [API stability policy](docs/API_STABILITY.md) and [public API contracts](docs/API_CONTRACTS.md) before changing public Rust types or protocol behavior. Breaking changes must be explicit and justified.

## Security reports

Do not disclose suspected vulnerabilities in a public issue. Follow [SECURITY.md](SECURITY.md) instead.
