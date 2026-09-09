# Whale Rust Application SDK

`whale-sdk-rust` 是供 Rust 应用构建 Agent 产品壳的运行基座。宿主应用负责 CLI、TUI、桌面界面、工作区、身份认证和产品策略；Whale 负责模型／工具循环、会话与运行状态、宿主工具回调、协议初始化和本地连接生命周期。本阶段没有实现任何 CLI、TUI 或 Desktop 产品。

`WhaleRuntime` 是应用入口。三种 source 都经过同一套 Whale JSON-RPC、`whale-daemon` 和 `whale-core` 执行路径；embedded 模式也会进行 JSON 序列化和内存通道传输，不是绕过 Daemon 的第二套 Engine API。

## 最小应用流程

下面的 `no_run` 示例由 `cargo test --doc -p whale-sdk-rust` 编译检查。它只展示库调用；实际执行 turn 时仍需配置可用的模型和凭证。

```rust,no_run
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;

    let definition = AgentDefinition::new("application-agent", "YOUR_MODEL");
    let agent = runtime.agent(definition, Vec::new())?;
    let session = agent.create_session().await?;

    let run = session.start_turn("Explain the current task.").await?;
    let _result = run.result().await?;

    let _closed = session.close().await?;
    let _shutdown = runtime.shutdown().await?;
    Ok(())
}
```

当前公开对象链是：

```text
WhaleRuntime
  -> Agent
       -> WhaleThread + per-Session BoundToolPack owner
            -> RunHandle
```

`AgentDefinition` 保存可复用配置；`runtime.agent(definition, tools)` 将它与精确的 `Vec<Arc<dyn HostTool>>` 绑定。需要为每个 live Session 创建独立宿主资源时，使用 `runtime.agent_with_tool_packs(...)`；Agent 保存冻结 manifest 和工厂，每次 Session 创建或恢复 attach 得到新的 bound 值。`Agent::create_session` 创建独立历史并返回 `WhaleThread`。`WhaleThread::start_turn` 返回 `RunHandle`；后者提供 `result`、兼容的单消费者 `events`、`snapshot`、`cancel` 和审批处理。Rust 宿主还可以通过 `WhaleThread::watch` 与 `RunHandle::subscribe_events` 建立互不阻塞、可回放的观察流；启用 Interaction 后，可另行查询、订阅和响应挂起的 clarification、form、permission、review 或自定义请求。现有接口分别见 [ToolPack API](TOOL_PACK_API.md)、[Session View API](SESSION_VIEW_API.md)、[Interaction API](INTERACTION_API.md)、[Agent API](AGENT_API.md)、[运行 API](RUN_API.md)、[执行与模型上下文](EXECUTION_CONTEXT_API.md)、[恢复 API](RECOVERY_API.md) 和 [保留与会话预算](RETENTION_API.md)。

## Session ToolPack 示例

下面示例定义一个 Session 级工作区资源。Manifest 在 Agent 构造时冻结；`bind` 为每个
live Session 创建新的 handler。正常 Session close 会 await `close`，连接丢失或
Runtime Drop 则只能调用同步、非阻塞的 `emergency_close` fence。这个示例同样由
`cargo test --doc -p whale-sdk-rust` 编译检查。

```rust,no_run
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use whale_protocol::CanonicalToolOutput;
use whale_sdk_rust::{
    AgentDefinition, BoundToolPack, HostTool, RuntimeOptions, SessionBindContext, ToolPack,
    ToolPackError, ToolPackManifest, ToolPackTool, WhaleRuntime,
};

fn workspace_tool_manifest() -> ToolPackTool {
    ToolPackTool {
        name: "read_workspace".into(),
        description: "Read data from this Session's workspace".into(),
        parameters: json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"]
        }),
        supports_parallel: true,
        require_approval: false,
    }
}

struct WorkspaceTool {
    session_id: String,
    stopped: Arc<AtomicBool>,
}

#[async_trait]
impl HostTool for WorkspaceTool {
    fn name(&self) -> &str {
        "read_workspace"
    }

    fn description(&self) -> &str {
        "Read data from this Session's workspace"
    }

    fn parameters(&self) -> Value {
        workspace_tool_manifest().parameters
    }

    async fn execute(&self, _arguments: Value) -> Result<CanonicalToolOutput, String> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err("workspace is closed".into());
        }
        Ok(CanonicalToolOutput::text(format!(
            "workspace bound to {}",
            self.session_id
        )))
    }
}

struct WorkspaceBinding {
    tool: Arc<WorkspaceTool>,
}

#[async_trait]
impl BoundToolPack for WorkspaceBinding {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        vec![self.tool.clone()]
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        // Real code can await worker shutdown or an async resource close here.
        self.tool.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn emergency_close(&mut self) {
        // This fallback must stay synchronous and nonblocking.
        self.tool.stopped.store(true, Ordering::SeqCst);
    }
}

struct WorkspacePack;

#[async_trait]
impl ToolPack for WorkspacePack {
    fn manifest(&self) -> ToolPackManifest {
        ToolPackManifest {
            id: "workspace".into(),
            tools: vec![workspace_tool_manifest()],
        }
    }

    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        let stopped = Arc::new(AtomicBool::new(false));
        Ok(Box::new(WorkspaceBinding {
            tool: Arc::new(WorkspaceTool {
                session_id: context.session_id().to_owned(),
                stopped,
            }),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;

    let mut definition = AgentDefinition::new("workspace-agent", "YOUR_MODEL");
    definition.tool_names = vec!["read_workspace".into()];
    let agent = runtime.agent_with_tool_packs(
        definition,
        Vec::new(),
        vec![Arc::new(WorkspacePack)],
    )?;

    let session = agent.create_session().await?;
    let run = session.start_turn("Inspect the workspace.").await?;
    let _result = run.result().await?;

    // Await normal pack cleanup before connection-wide Runtime shutdown.
    let _closed = session.close().await?;
    let _shutdown = runtime.shutdown().await?;
    Ok(())
}
```

Manifest metadata and `BoundToolPack::tools()` metadata must match exactly. Pack 的 bind／rollback、
persistent attach、dynamic registration 和关闭边界见 [ToolPack API](TOOL_PACK_API.md)。

## Session View 与管理面

Stage 2.1 的 `WhaleThread::{snapshot,watch,subscribe_from}` 保留原签名和 V1 wire，适合
观察当前 writable Session 的 Run 投影。Stage 2.2 增加独立的 V2 管理面：
`WhaleClient::list_sessions` 枚举当前连接 owner 的 live records/tombstones；
`WhaleThread::session_view` 或 `WhaleClient::session_view` 创建 cloneable、非 owning 的
`SessionViewHandle`；它可以读取 snapshot/history 并观察 metadata 和 Closing/Closed，
但没有 Run、tool、metadata 或 close 写方法。`WhaleThread::replace_metadata` 仍是 write
capability，并以 V2 `view_revision` 做 whole-map compare-and-set。

```rust,no_run
use whale_sdk_rust::{
    SessionHistoryOptions, SessionListOptions, SessionManagementWatchOptions, WhaleRuntime,
};

async fn inspect_sessions(runtime: &WhaleRuntime) -> Result<(), Box<dyn std::error::Error>> {
    let page = runtime
        .client()
        .list_sessions(SessionListOptions::default())
        .await?;
    if let Some(entry) = page.sessions.first() {
        let view = runtime.client().session_view(&entry.summary.thread_id)?;
        let snapshot = view.snapshot().await?;
        let _history = view
            .history_page(SessionHistoryOptions::default())
            .await?;
        let _watch = view
            .watch(SessionManagementWatchOptions::default())
            .await?;
        assert_eq!(snapshot.summary.view_revision, snapshot.cursor.seq);
    }
    Ok(())
}
```

V1 与 V2 cursor 类型、list/history 的固定分页窗口、CAS 冲突、owner isolation、持久
reattach 的 fresh stream、Closed tombstone 以及 typed gap/error 处理见
[Session View API](SESSION_VIEW_API.md)。所有管理能力均为 optional；Rust SDK 会在发送
对应业务 RPC 前检查 capability。

## Runtime source 与所有权

Runtime 类型从 `whale_sdk_rust` crate 根导出。`RuntimeMode` 的值为 `Embedded`、`ManagedProcess` 或 `ExternalUds`；`RuntimeOptions` 的公开字段是 `source`、`startup_timeout` 和 `shutdown_timeout`。成功启动后，`RuntimeInfo` 提供 `mode`、`peer: InitializeResult` 和 `process_id: Option<u32>`。成功关闭返回 `RuntimeShutdown { mode, disposition }`，其中 `disposition` 是 `ShutdownDisposition::Graceful` 或 `ShutdownDisposition::Forced`。项目没有单独的 ownership 枚举；source mode 已经决定所有权。

| Source | 创建方式 | Runtime 拥有的资源 | 不拥有的资源 | `process_id` |
| --- | --- | --- | --- | --- |
| Embedded | `RuntimeOptions::embedded()`；或传入 `RuntimeSource::Embedded { server }` | 本地 writer、SDK reader、本连接的 `DaemonServer::run` loop | 同一 `DaemonServer` 的其他连接及 Daemon 内部脱离任务 | `None` |
| ManagedProcess | `RuntimeOptions::managed(executable)` | 本地 stdio、SDK reader、直接 child；关闭时等待或 kill 并 reap | child 自行创建且脱离的外部资源 | `Some(pid)` |
| ExternalUds | `RuntimeOptions::external_uds(path, startup_timeout, shutdown_timeout)` | 本地 Unix socket、writer 和 SDK reader | 外部 Daemon、listener、进程及其他连接 | `None` |

`RuntimeOptions::default()` 等同于 embedded。Embedded 和 managed 构造器默认使用 10 秒 startup 与 shutdown timeout；字段公开，应用可以显式调整。External UDS 要求调用者显式给出两个 timeout。当前实现面向 Unix，已在 macOS 本地验证；Linux CI 待补，Windows 不支持。

External UDS 是 trusted-local-peer 传输。`protocol.initialize` 只验证版本和 capability 兼容性，不验证 daemon 身份，也没有读取 peer credential。宿主必须把 socket 放在自己控制的目录中，限制目录与 socket 权限，并确保连接的是可信 Whale Daemon。

自定义 `ProviderRegistry` 与 `StoreRuntime` 由 `DaemonServer` 在启动期拥有。Embedded 模式先通过 `DaemonServer::with_provider_registry`／`with_store_runtime` 完成配置，再把 server 放入 `RuntimeSource::Embedded`；managed process 和 external UDS 使用其 daemon 进程已有的配置，`RuntimeOptions` 不会把这些 Rust 对象跨进程或 socket 注入。

Managed source 总是在调用者参数之前加入两个独立参数 `--listen` 和 `stdio`。`with_arg` 接受 `OsString`，不经过 shell；空格、元字符以及 Unix 非 UTF-8 参数保持原始 argv 边界。调用者不能再传 `--listen` 或 `--listen=...`。`with_environment` 只增加或覆盖 child 环境，未覆盖的父进程环境继续继承，也不会修改宿主进程环境。

Managed 环境名必须非空且不能含 `=` 或 NUL，环境值、参数和 executable 不能含 NUL。`Debug` 会显示 executable、args 和环境变量名，但不会显示环境值；因此凭证应放在环境值或其他凭证引用中，不应放进可见的参数。External UDS 的路径以及 embedded/managed 的 mode 也属于诊断信息。

内置 provider auth 目前只有 daemon 环境变量引用和显式无认证。桌面 keychain、OAuth、secret broker 与多租户异步凭证解析尚无通用 `CredentialResolver`；这类集成可先由自定义 `ModelProvider` 完成，不能把它描述为 Runtime 已内建的凭证能力。

`ManagedProcess` 只能启动实现 Whale 协议并接受 SDK 所属 `--listen stdio` 参数的 Daemon。它不是通用命令封装器，不能直接把 Codex、Claude Code、OpenCode、pi 或 DeepSeek Harness 的 CLI 当作 executable。

## Startup readiness

`WhaleRuntime::open` 先验证 options，再创建 source，并等待一次真实的 `protocol.initialize` 成功响应。返回的 Runtime 已经 ready；`runtime.info().peer` 保存协商后的服务身份、协议版本和 capabilities，再调用 `runtime.client().initialize()`会得到同一缓存结果，不会开始第二次握手。初始化是现有 wire 协议的一部分，Runtime 生命周期本身没有新增 `runtime_lifecycle.v1` capability。

`startup_timeout` 使用一个绝对 deadline。External UDS 的 connect 和初始化共享它；其他 source 的初始化只能使用 source 创建后剩余的时间。timeout 约束 readiness，不会通过丢弃已经拥有资源的 future 来逃避 rollback，因此失败后的 writer close、任务 join 或 child reap 可能使错误实际返回时间超过 startup timeout。

配置无效时不会启动 server、child 或 socket。`startup_timeout` 必须大于零；`shutdown_timeout` 当前至少为 2ns，以便为 graceful 和 forced 阶段都保留非零时间。无法表示为 Tokio deadline 的 duration 同样会在 source 创建前被拒绝。

## Capability 与 owner

`WhaleRuntime` 不实现 `Clone`，它持有唯一的 connection owner。`WhaleClient` 可以克隆，但 Runtime 路径中的 client clone 只携带请求、事件和非拥有 stop capability：

- 丢弃一个普通 client clone 不会关闭连接；
- 保留 client clone 不会让 Runtime 所拥有的 embedded loop 或 managed child 在 Runtime 关闭后继续存活；
- `runtime.client()`用于需要底层 Client API 的高级场景，`runtime.agent(...)`是通常的 Agent 装配入口；
- 对 Runtime 的 client clone 调用 `WhaleClient::close()`也会触发同一个 connection cleanup，但该旧接口返回 `()`并丢弃结构化 shutdown 结果。需要诊断时应调用 `WhaleRuntime::shutdown()`。

旧的 `WhaleClient::{in_process,spawn_daemon,spawn_daemon_with_args,connect_uds}` 保持兼容。它们在共享 client 内保留 compatibility owner，首次普通 RPC 时自动初始化，显式 `close` 使用固定 10 秒 cleanup budget。新的 Rust 应用应优先使用 `WhaleRuntime` 获得 ready-before-return 和结构化生命周期结果。

## 错误语义

`RuntimeError` 是可克隆、可比较的结构化枚举。调用者应按 variant 处理，不要解析显示字符串：

| Variant | 含义 |
| --- | --- |
| `InvalidConfiguration { field, message }` | 创建任何 source 前发现 deadline、managed argv 或环境配置无效 |
| `SourceFailure { mode, message }` | spawn、立即 UDS connect 或 transport 获取失败 |
| `InitializationFailure { mode, message }` | 协议不兼容、非法响应、EOF 等初始化失败 |
| `StartupTimeout { mode, timeout }` | source connect 或初始化耗尽 startup deadline |
| `ShutdownFailure { mode, phase, message }` | cleanup task、I/O、server loop 或 child 操作明确失败 |
| `ShutdownTimeout { mode, phase, timeout }` | Stage 1 所拥有的资源未在总 shutdown deadline 内完成 cleanup |

Shutdown 错误的 `phase` 是 `RuntimeShutdownPhase::{Deadline, OwnerTask, Writer, Reader, EmbeddedServer, ManagedChild}` 之一。`RuntimeInfo` 只包含 `mode`、初始化 peer 和 managed direct-child PID，不包含环境配置或单独的 ownership 枚举。

## Shutdown、并发与 Drop

`WhaleRuntime::shutdown(&self)`立即使共享 client state 断开，并等待同一个 owner cleanup transaction。并发调用、较晚的重复调用以及从 client 发出的 close 都汇合到该事务；放弃一个 shutdown waiter 不会取消 cleanup，其他调用者会得到同一个缓存结果。

`shutdown_timeout` 是一次 cleanup 的总预算，其中保留一部分给强制阶段。返回值为：

- `Ok(RuntimeShutdown { disposition: Graceful, .. })`：graceful window 内完成；
- `Ok(RuntimeShutdown { disposition: Forced, .. })`：graceful 阶段不足，但 writer close、reader/server abort 或 child kill/reap 在总 deadline 内完成；
- `Err(RuntimeError::ShutdownFailure { .. })`：资源任务或操作明确失败；
- `Err(RuntimeError::ShutdownTimeout { .. })`：最终 join/reap 等超过总 deadline，紧急 cleanup 仍保持启动状态。

External UDS 即使出现 `Forced`，也只表示本地 reader/writer cleanup 使用了强制阶段；SDK 不会向外部 Daemon 进程发送终止信号。

`Drop` 是无阻塞的紧急兜底：它同步 fence client state 并请求同一 owner transaction，但不等待结果，也不提供 shutdown report。Runtime shutdown 也不等价于逐个 await Session 的 ToolPack close；尚未显式关闭的 Session 只执行同步 emergency fence。需要确定资源已经释放并取得正常关闭结果时，应先显式 await 每个 Session 的 `close()`，再 await `runtime.shutdown()`。

Stage 1 的 shutdown 保证限于：关闭本地 writer；join 或 abort SDK reader；join 或 abort本连接的 embedded server loop；等待并在需要时 kill/reap managed direct child；只关闭 external UDS 的本地连接。它不保证 Daemon 中所有脱离的 request、Run、retention、Store 或其他内部任务均已 join，也不能撤销宿主工具已经产生的外部副作用。

## 当前范围与后续扩展

当前 Runtime 是 Rust-only 应用 API，面向 CLI、TUI、GUI 和后台服务宿主。界面生命周期建议映射为：打开时 `WhaleRuntime::open`，每个对话 `create_session`，每个用户回合 `start_turn` 并订阅 `RunHandle::subscribe_events` 或 `WhaleThread::watch`；需要通用人机交互时，对该 Agent 显式 opt in，再使用 `watch_interactions`；退出前 await `session.close()` 与 `runtime.shutdown()`。多观察者事件流、Session snapshot 与独立 Interaction feed 支持多个界面消费者同时渲染历史、流式增量和挂起请求，而互不阻塞结果投递。

当前 `whale-sdk-rust` 没有 attached-only 轻量 feature。即使应用运行时只选择 managed process 或 external UDS，依赖图仍会编译 embedded daemon、Core 与 Store/SQLite 等路径。拆分依赖特性需要单独的 API、构建时间和兼容性设计。

当前已提供 Stage 2.1 的权威 Session view、游标事件回放和多个 Rust 订阅者，Stage 2.2 的 owner-local list、独立 history 分页、metadata CAS、Closing/Closed 回放与 bounded tombstone，通用 Interaction 的 pending snapshot／固定窗口 feed／schema response／typed approval 共事务，以及 Rust Session-scoped ToolPack 的 manifest freeze、fresh bind、rollback、callback quiescence、normal reverse close、emergency fence 和 persistent attach。后续应用 SDK 工作包括完整协议类型生成、凭证解析、sidecar binary 发现与分发、可观测性，以及独立设计的通用插件加载／依赖／热重载能力。CLI、TUI、桌面窗口、项目浏览器、产品工具集、插件市场和隐式代码执行继续由上层产品或未来独立组件负责。

Codex、Claude Code、OpenCode、pi 和 DeepSeek Harness 是架构参照，不是当前 Runtime source。若以后需要驱动这些拥有自身模型循环、历史、工具、审批和权限语义的完整 Agent，应新增独立 `AgentBackend` 契约，而不是把它们伪装成只完成一个模型步骤的 `ModelProvider`。

Rust `0.1.x` 的 exhaustive／`#[non_exhaustive]` 选择、协议版本化规则与发布检查见 [API stability policy](API_STABILITY.md)。
