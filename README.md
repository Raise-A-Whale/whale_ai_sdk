# Whale AI SDK

Whale 是供 Rust 应用构建 Agent CLI、TUI、桌面产品和后台服务的通用 Agent Runtime 基座。`WhaleRuntime` 负责 ready-before-return、连接所有权和有界关闭；模型与工具循环沿统一的 Rust Daemon/Core 路径执行；产品界面、工作区和业务策略由宿主应用实现。本仓库不提供 CLI、TUI 或桌面产品壳。

**本阶段只交付 Rust SDK。** Python / Java 客户端已从仓库移除；跨语言协议契约仍保留在 `whale-protocol`，便于后续按需扩展。

[Rust Application SDK](docs/RUST_APPLICATION_SDK.md) · [通信协议规范](docs/PROTOCOL_SPEC.md) · [Interaction API](docs/INTERACTION_API.md) · [Session ToolPack API](docs/TOOL_PACK_API.md) · [Session View API](docs/SESSION_VIEW_API.md) · [架构评估与开源项目对比](docs/SDK_ARCHITECTURE_REVIEW.md) · [公共 API 合同](docs/API_CONTRACTS.md) · [Rust API 兼容策略](docs/API_STABILITY.md) · [连接初始化](docs/INITIALIZATION_API.md) · [Agent 装配 API](docs/AGENT_API.md) · [模型扩展](docs/MODEL_PROVIDER_API.md) · [运行 API](docs/RUN_API.md) · [执行与模型上下文](docs/EXECUTION_CONTEXT_API.md) · [会话关闭](docs/SESSION_API.md) · [持久化恢复](docs/RECOVERY_API.md) · [保留与会话预算](docs/RETENTION_API.md)

## 定位：让 CLI / TUI / GUI 快速集成 Agent

Whale 关注的是「宿主应用如何即插即用地获得完整 Agent 运行能力」，而不是再实现一个 CLI 或桌面壳。集成方通常只需要：

1. 用 `WhaleRuntime::open` 打开默认 embedded、owned managed Whale-compatible daemon process 或 attached external UDS 连接；
2. 用 `AgentDefinition` + `HostTool` / `ToolPack` 装配业务 Agent；
3. 用 `WhaleThread::start_turn` 启动回合，通过 `RunHandle`、Session View 与可选 Interaction 面消费事件、挂起请求、审批、取消和结果；
4. 在界面退出前显式关闭 Session 与 Runtime。

应用宿主常用能力已经覆盖：流式文本增量、多观察者互不阻塞订阅、有界回放、权威快照、通用 clarification／form／permission／review Interaction、审批挂起与参数替换、合作取消、deadline、工具进度上报、会话恢复与保留预算。

## 已实现

- Rust 应用 Runtime：默认 embedded、owned managed Whale-compatible daemon process 和 attached external UDS 三种 source；`open` 完成协议初始化后返回，非克隆 owner 与可克隆 client capability 分离，shutdown 有总预算、single-flight 结果和结构化错误。代码面向 Unix，当前实测平台为 macOS；Linux CI 待补，Windows 不支持。
- Canonical IR：统一消息、推理、工具调用及工具结果表示。
- 连接初始化：自动／显式单次握手、协议版本与功能检查；不兼容时清理连接及所属子进程，阻止业务派发。
- Agent 装配：可复用 `AgentDefinition`、独立会话、精确工具绑定和不可变配置副本。
- Rust Session ToolPack：Agent 构造时冻结工具 manifest，每个 fresh／recovered live Session 独立 bind；失败按反向顺序 rollback，正常 close 先等待 callback 静止再反向关闭资源，连接 teardown 使用同步 emergency fence。Pack-owned 名称不能被动态注册替换。
- 模型接入：显式协议、base URL 和凭证引用；支持 OpenAI Chat、Responses 及 Anthropic 请求与流解析，具体输入范围见 Agent API。
- 模型扩展：`ModelProvider`、启动期工厂注册、`provider_ref` 选择和能力检查；自定义非 HTTP 实现使用同一套模型／工具循环。
- 宿主工具：Rust `HostTool`；新协议按会话与绑定版本路由，同名工具和运行中的旧绑定相互隔离。
- ToolContext：执行身份、deadline、合作取消与进度上报；上下文参数不进入模型可见的工具 schema。
- 参数校验与审计：执行前及审批修改后进行 JSON Schema 校验，快照保留原始与实际执行参数。
- ContextPolicy：完整历史、近期完整回合或宿主自定义投影；每个模型步骤独立构造上下文，保留原始会话历史。
- 工具执行：并发工具的结果按调用顺序返回，独占屏障按工具注册表隔离。
- 审批：挂起、通过、拒绝、替换参数；运行快照保留待审批请求。
- 运行句柄：异步接受、独立事件与结果、查询、取消、deadline、单一外层终态。
- Rust Session View：权威快照、live attachment cursor、有界固定窗口回放、typed resync 与互不阻塞的多观察者；原有单消费者 Run event API 保持兼容。
- Rust Interaction：可选 `interactions.v1`、Agent 私有 opt-in、Session／Run pending snapshot、独立 cursor 的 live/replay 合并、通用响应 schema、自定义 kind、等价响应幂等与 typed approval 兼容；不改变既有 Agent／Run／Session 结构。
- 会话关闭：取消该会话活动运行，等待终态投递并释放运行记录、工具与上下文绑定；同一客户端的其他会话继续使用。
- 持久化恢复：可选 SessionStore、SQLite 跨进程保存、实际模型输入与逐个工具结果审计；显式持久会话关闭后保留档案，以新 ID 重新绑定，未知工具结果需显式确认且不自动重放。
- 保留与预算：默认关闭的终态 Run TTL／数量上限、受保护的 detached Store 自动回收；SessionLimits 限制新 turn 接收与模型请求大小，超限不裁掉已完成结果。
- 传输与所有权：Daemon 支持 stdio 和 UDS；Rust Runtime 可拥有 embedded connection loop、拥有并回收 stdio direct child，或只附着并关闭自己的 UDS 连接。
- 连接关闭与 EOF 清理；完整帧 writer 防止取消发送时破坏后续 JSON 消息。

普通会话和运行记录驻留内存，可通过 Session close 显式释放；显式持久会话通过配置的 Store 保留档案，跨重启需要 SQLite 等持久后端。Rust 宿主可通过 [Session View API](docs/SESSION_VIEW_API.md) 获取不依赖执行锁的快照并续接同一 live attachment 内的有界事件；通过 [Interaction API](docs/INTERACTION_API.md) 获取可恢复的 pending 请求并安全响应。持久 attach 会创建新 Session 与新 Interaction stream；Store 只保留 opt-in 配置，不保留 pending request、response 或 fingerprint。Session 级业务资源可由 [ToolPack](docs/TOOL_PACK_API.md) 管理，但应用需要显式 await Session close 才能获得正常异步关闭语义；Runtime shutdown 对仍存活的 pack 只执行同步 emergency fence。Embedded 宿主通过 `DaemonServer::with_retention_policy` 配置保留策略；managed/external source 使用 daemon 侧配置。Run TTL 从成功发送终态开始，活动运行、发送失败记录与未确认未知结果受保护。墓碑和应用持有的结果仍占用资源，字节预算不等于 token 数或进程内存硬上限。自动历史压缩、通用插件发现／安装／依赖／热重载、完整协议客户端生成、跨进程宿主 ModelProvider 回调、可观测性，以及 sidecar binary 的发现、下载和打包仍在后续计划中。`cancel()` 确认取消请求已接收；`result()` 或终态快照确认逻辑运行结束，且不能保证已经执行的宿主函数停止外部副作用。

## 仓库结构

```text
crates/
  whale-protocol/   Canonical IR、RPC 和运行事件契约
  whale-adapters/   模型请求与 SSE 适配器
  whale-core/       Agent 循环、会话、工具调度与审批
  whale-store/      事务日志、Memory/SQLite Store 与恢复状态机
  whale-daemon/     双向通信、运行管理与连接归属
  whale-sdk-rust/   Rust Application Runtime、客户端与 Agent/Session/RunHandle
scripts/
  verify_rust_packages.sh   crate 打包与仓库外消费者检查
  protocol_peer_fixture.py  初始化握手失败 peer fixture
fixtures/
  protocol/         握手响应样本
  rust-consumer/    仓库外应用／扩展消费者样例
```

## Rust 应用接入

默认 embedded source 不要求宿主先查找或启动独立 Daemon binary：

```rust
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;
    let agent = runtime.agent(AgentDefinition::new("assistant", "YOUR_MODEL"), Vec::new())?;
    let session = agent.create_session().await?;
    let run = session.start_turn("Hello").await?;
    let _result = run.result().await?;
    session.close().await?;
    runtime.shutdown().await?;
    Ok(())
}
```

完整的三种 source、所有权、初始化、错误和 shutdown 契约见 [Rust Application SDK](docs/RUST_APPLICATION_SDK.md)；通用挂起请求见 [Interaction API](docs/INTERACTION_API.md)；每 Session 资源工厂与关闭边界见 [Session ToolPack API](docs/TOOL_PACK_API.md)；应用级快照、回放和多订阅见 [Session View API](docs/SESSION_VIEW_API.md)。自定义 `ProviderRegistry` 和 `StoreRuntime` 在 embedded 模式下安装到 `DaemonServer` 后再交给 Runtime；managed/external 模式使用 daemon 侧配置，不能通过 `RuntimeOptions` 跨边界注入。External UDS 当前是 trusted-local-peer 传输，握手只确认协议兼容，宿主需要控制 socket 目录、权限与 daemon 来源。

TUI/GUI 集成时，推荐把界面生命周期映射到 Runtime/Session/Run：打开界面时 `WhaleRuntime::open`，为每个对话 `create_session`，每个用户回合 `start_turn` 并订阅 `RunHandle::subscribe_events` 或 `WhaleThread::watch`，退出前 await `session.close()` 与 `runtime.shutdown()`。

## 验证

```sh
cargo test --workspace
cargo run -q -p whale-protocol --example initialization_contract -- --check
cargo build -p whale-daemon --bins --examples
./scripts/verify_rust_packages.sh
```

`cargo test --workspace` 覆盖协议契约、Core 循环、Store、Daemon 与 SDK 单元/集成路径。少量标记 `#[ignore]` 的真实进程或外部 HTTP fixture 测试不在默认套件中执行；它们用于需要生产 Daemon、本地 HTTP 模型服务或协议 peer 的专项验收。

`verify_rust_packages.sh` 检查 workspace 内部依赖版本、SDK rustdoc include 边界、仓库外 Rust 消费者可编译，以及各 crate 可 `cargo package`。

`protocol_peer_fixture.py` 与 `fixtures/protocol/initialization-v1.json` 提供确定性的不兼容 stdio peer 与握手响应样本，供协议初始化失败路径使用。这些样本不依赖付费 API。

当前代码面向 Unix，并已在 macOS 本地验证；Linux CI 尚未交付，Windows 不支持。
