# GitHub Actions CI 门禁设计

## 目标

为 Whale AI SDK 建立可重复的 GitHub Actions PR 门禁，判断提交是否仍然可以格式化、编译、测试、生成文档并打包发布。门禁服务于 Rust application SDK、daemon、协议和存储层；不构建或集成任何 CLI、TUI 或桌面产品。

## 范围

本次新增两个 workflow：

- `.github/workflows/ci.yml`：每个 PR 和 `main` 分支 push 的必需质量检查，也支持手动触发。
- `.github/workflows/security.yml`：按周和手动触发的依赖审计，不阻塞普通 PR 的编译门禁。

workflow 只使用仓库已有的 Cargo workspace、测试、文档和 `scripts/verify_rust_packages.sh`。不引入新的运行时服务，也不要求 API key、provider 凭据或外部 agent。

## 触发与权限

`ci.yml` 在 `pull_request`、`push` 到 `main` 和 `workflow_dispatch` 时触发。`security.yml` 在每周定时任务和 `workflow_dispatch` 时触发。两个 workflow 使用 `contents: read` 的最小权限，并按 workflow、分支和 PR 编号设置并发组；新提交到同一 PR 时取消旧运行。

PR workflow 不使用 secrets。所有网络依赖只用于拉取 Rust toolchain、Cargo 依赖和 GitHub Actions；测试必须使用仓库内的 deterministic fixtures。

## CI jobs

### `fmt`

使用 stable Rust，执行 `cargo fmt --all -- --check`。任何格式变化都会使 job 失败。

### `clippy`

使用 stable Rust，执行 workspace、all targets、all features 的 Clippy，并将 warning 视为错误：

```text
cargo clippy --workspace --all-targets --all-features -- \
  -D clippy::correctness
```

The Clippy job treats correctness lints as errors. Style, complexity, and suspicious-pattern lints remain visible in the log without forcing a public API refactor or blocking otherwise valid SDK changes.

### `test`

使用 stable Rust，执行 workspace 的 all-features 测试，确保 SDK、daemon、协议、存储、fixture 和集成测试一起验证：

```text
cargo test --workspace --all-features
```

### `docs`

使用 stable Rust，在禁止 rustdoc warning 的模式下生成 workspace 文档：

```text
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

### `msrv`

使用 Rust `1.85.1`，执行 workspace、all targets 的编译检查。它验证 workspace 声明的 `rust-version` 与最低支持编译器一致，但不重复 stable 的完整测试矩阵。

### `package`

使用 stable Rust 执行仓库已有的 `scripts/verify_rust_packages.sh`。该脚本负责打包、解包、依赖元数据、许可证、敏感文件扫描和外部 consumer fixture 验证。它是 PR 发布完整性的门禁，不额外发布 crate。

## 安全审计 workflow

`security.yml` 使用 RustSec audit 检查 Cargo.lock 中的依赖漏洞，并在漏洞存在时失败。它只在每周计划任务和手动触发时运行，避免短暂的审计服务不可用阻塞每个 PR。该 workflow 同样只读，不修改仓库、标签或发布内容。

## 缓存与可诊断性

每个 Rust job 使用 Cargo target/registry 缓存，但缓存命中不能改变结果。job 名称直接表达验证边界，失败日志包含完整 Cargo 命令。workflow 不吞掉失败状态、不使用 `continue-on-error`，并为脚本和测试保留标准输出。

## 验收标准

实现完成后应满足：

1. GitHub Actions YAML 可以被解析，触发条件、权限和并发策略符合本设计。
2. PR workflow 中的 fmt、Clippy、测试、文档、MSRV 和 package jobs 均能在当前仓库通过。
3. security workflow 只包含计划/手动触发，不依赖项目运行时配置。
4. workflow 不包含 CLI/TUI/Desktop 构建或任何未授权凭据访问。
5. 本地验证覆盖 YAML 结构、shell 语法、Rust 格式、workspace 编译和测试；验证结果记录在 PR 描述中。

## 实现文件

- `.github/workflows/ci.yml`
- `.github/workflows/security.yml`

不修改 Rust API、协议、daemon 行为或 SDK 运行时实现。
