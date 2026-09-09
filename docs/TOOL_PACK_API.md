# Rust Session ToolPack API

`ToolPack` 是 `whale-sdk-rust` 的 Session 级宿主资源工厂。一个可复用
`Agent` 保存工厂和冻结后的工具 manifest；每次创建新的 live Session（包括持久
Session 的首次创建和恢复 attach）都会绑定一组全新的资源。它适合把工作区句柄、
数据库连接、MCP client 或后台 worker 的生命周期限制在一个 Session 内。

这是一项 Rust-only 的库能力，供 CLI、桌面应用和后台服务等产品壳调用。它不负责
发现或安装插件，不提供依赖解析、热重载、权限 UI、marketplace、项目浏览器或任何
CLI/TUI/Desktop 实现。

## 公开契约

相关类型从 `whale_sdk_rust` crate 根导出：

```rust
pub trait ToolPack: Send + Sync {
    fn manifest(&self) -> ToolPackManifest;
    async fn bind(
        &self,
        context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError>;
}

pub trait BoundToolPack: Send + Sync {
    fn tools(&self) -> Vec<Arc<dyn HostTool>>;
    async fn close(&mut self) -> Result<(), ToolPackError>;
    fn emergency_close(&mut self) {}
}
```

`ToolPackManifest` 包含唯一 `id` 和一个非空的 `Vec<ToolPackTool>`；每个工具声明
`name`、`description`、JSON Schema `parameters`、`supports_parallel` 和
`require_approval`。通过 `WhaleRuntime::agent_with_tool_packs` 或
`WhaleClient::agent_with_tool_packs` 装配：

```rust
let agent = runtime.agent_with_tool_packs(definition, static_tools, packs)?;
```

已有 `agent(definition, tools)`、`HostTool`、`AgentDefinition`、Session 创建／恢复和
`SdkError` 接口没有改变；无 pack 的 Agent 继续走原路径。完整、可编译检查的 embedded
示例见 [Rust Application SDK](RUST_APPLICATION_SDK.md#session-toolpack-示例)。

## Manifest 冻结与组合校验

`agent_with_tool_packs` 构造 Agent 时，每个工厂的 `manifest()` 只求值一次，SDK
保存其副本。此后工厂内部状态变化不会改变该 Agent 的工具定义。校验发生在协议初始化、
Session ID 分配和资源绑定之前：

- pack ID 必须去除首尾空白后仍非空，并且在 Agent 内唯一；
- 每个 manifest 至少声明一个工具；
- 工具名必须去除首尾空白后仍非空，并且不能与静态工具或其他 pack 重名；
- 描述必须非空，参数必须是当前工具 schema 编译器接受的 JSON Schema；
- 静态工具和所有 manifest 工具名的集合必须精确等于
  `AgentDefinition.tool_names`。

每次 `bind` 返回后，SDK 调用一次 `BoundToolPack::tools()`。返回的 handler 集合必须
与冻结 manifest 的名称和全部 metadata 精确一致。SDK 按 manifest 顺序组织 handler，
并为当前 live Session 生成新的不透明 binding ID。manifest、bind、tools 或 metadata
访问发生 panic 时，SDK 会在所有权边界捕获它，并按当前阶段回滚。

Manifest 冻结的是工具的 wire metadata 和组合关系，不是 bound 资源。Agent clone 共享
工厂和 frozen manifest，从不共享 `BoundToolPack`。

## 每个 live Session 的 bind 事务

三条 Session 创建路径使用同一套事务：

```text
preflight
  -> 分配新的 live Session ID
  -> Session 进入 Preparing
  -> 按声明顺序 bind packs
  -> 校验 handlers，建立精确 name/binding-ID routes
  -> 发送 create / persistent-create / attach 请求
  -> 校验 ACK identity
  -> 转移唯一 SessionPackOwner，Session 进入 Open
```

`Preparing` 是 readiness gate。Daemon ACK 通过前，反向工具或 context callback 不会执行
pack 代码。每个达到 bind 阶段的 live 创建尝试都会得到新的 bound 值；同一 Session 的
多个 Run 复用该 Session 的 bound 值。

`SessionBindContext` 只提供：

| 字段 | 含义 |
| --- | --- |
| `session_id()` | 本次 live attachment 的新 Session ID |
| `agent_name()` | 冻结定义中的 Agent 名称 |
| `kind()` | `Ephemeral`、`PersistentCreate { recovery_id }` 或 `PersistentAttach { recovery_id }` |
| `session_cancelled()` | rollback 或 Session close 时触发的合作取消信号 |

持久 bind 的 kind 只公开安全的 `recovery_id`。context 不包含
`RecoveryKey.secret`、Store revision/epoch、Provider credential、system prompt、历史或
旧 attachment 的 callback identity；`SessionBindContext` 也不实现 `Debug`。工厂需要的
业务依赖由应用在构造 pack 时显式捕获。

公开创建 future 的工作由 SDK 拥有。调用者取消 waiter 后，尚未发送请求的事务会回滚；
请求已经发送时，SDK 会继续结算结果。无人领取的成功 Session 进入已有的正常
`session.close` 流程；无法确定远端状态的失败会 fail closed。工厂若在 `bind` 返回前已
分配局部资源，必须用自己的 RAII guard 处理，因为 SDK 尚未取得该资源的所有权。

## Rollback 与正常关闭

后续 bind、handler 校验、请求或 ACK 校验失败时，SDK 保持 Session 不可调用，取消
Session signal，删除精确的暂存 routes，并按反向声明顺序关闭所有已返回的 bound pack。
当前刚返回但校验失败的 pack 也在 rollback 内。单个 `close` error 或 panic 不会跳过更早
的 pack；清理错误通过现有 `SdkError::Internal` 聚合返回。

成功 Session 的正常 `WhaleThread::close().await` 使用 single-flight 事务，顺序为：

1. 阻止新的 Session 请求和反向 callback，关闭 event hub，并释放 pending waiter；
2. 停止且等待已有 tool/context callback 完成；
3. 等待 dynamic registration lock，并删除精确 routes、Run 和 Session 引用；
4. 取得唯一 pack owner，按反向声明顺序逐个 await `BoundToolPack::close`；
5. 缓存关闭结果并将 Session 置为 `Closed`。

因此正常 pack close 发生时，不再有 SDK 持有的 callback future 执行它的工具。并发 close
caller 共享同一个结果；即使所有 waiter 都被取消，后台清理仍继续。正常 close 失败或
panic 后 Session 仍是 `Closed`，其他 packs 仍会被尝试，后续 close 返回 `Ok(false)`，
不会重复调用 pack。

## Emergency cleanup 与 Runtime 边界

`BoundToolPack::emergency_close` 是同步、非阻塞的 stop fence。它应设置原子关闭状态、
关闭同步 handle 或通知一个已存在的 worker；默认实现依赖 bound 对象自身的 Rust
`Drop`/RAII。SDK 在连接 EOF、fail-closed teardown 或 Runtime Drop 时，会对仍挂在
`SessionLifecycle` 上的 owner 以反向顺序执行 emergency cleanup。一个 hook 的 panic
会被隔离，后续 pack 仍会清理。如果正常 close 已经取得 owner 并进入应用实现的异步
`close` hook，connection teardown 不会抢占它，也不会对同一 pack 再调用一次
`emergency_close`。

Emergency cleanup 不等待异步任务 join，也不产生完整的 pack close 结果。阻塞或永不
返回的应用 hook 无法被 SDK 抢占。`WhaleClient::close` 与
`WhaleRuntime::shutdown` 是 connection-wide teardown，不等价于依次 await 每个 Session
的 pack close；它们对尚未进入正常 close 的 Session 使用 emergency 路径。需要确定正常
清理结果的应用应先显式 await 所有 `WhaleThread::close()`，再 await Runtime shutdown。
丢弃一个 `WhaleThread` clone 不会自动关闭 Session。

## Dynamic tool 边界

Pack manifest 声明的名字在该 live Session 内归 pack 所有。
`WhaleThread::register_tool` 尝试替换这些名字时，会在建立 registration lock、binding 或
发送 RPC 之前以 `SdkError::InvalidConfiguration` 本地失败。

其他名字的动态注册保持既有行为：它们使用不可变 binding ID 与逐名称串行化，但不会被
某个 pack 自动收养或获得 pack close hook。持久 Session 下一次 attach 时，应用仍需在新
Agent 中重建这些非 pack 动态定义与静态 handler；pack 工具则从冻结 manifest 和新的
bound 实例重建。

## Persistent create 与 attach

`create_persistent_session(&key)` 使用
`SessionBindKind::PersistentCreate { recovery_id }`；`recover_session(&key)` 使用
`PersistentAttach { recovery_id }`。每次成功 attach 都有新的 live Session ID、bound
对象和 binding ID。历史与 tool definitions 可以持久化，Rust callback 对象和 pack 资源
不会序列化或从旧进程恢复。

缺少 recovery capability 或 provider-reference preflight 失败发生在 bind 前。明确的
create/attach 拒绝按正常路径反向 rollback；Store/internal 错误、EOF、非法响应 identity
等不确定失败会关闭连接并触发 emergency fence。RecoveryKey 的保存、revision 规则和
unknown execution 语义见 [Recovery API](RECOVERY_API.md)。

## 与其他宿主扩展的所有权边界

ToolPack 只供应 `HostTool`：

- 静态 `HostTool` 仍由应用拥有，并按 Agent 共享；
- `HostContextPolicy` 通过 `Agent::with_context_policy` 单独绑定，参与 callback
  quiescence，但不属于 pack；
- `ProviderRegistry`／`ModelProvider` 和 `SessionStore` 由 Daemon 启动层拥有；
- future `AgentBackend` 承载完整外部 Agent 的运行语义，不属于 ToolPack。

ToolPack 没有新增协议方法、capability、Daemon route 或持久化 schema；不存在
`tool_packs.v1` wire capability。它复用现有 Session create、tool definitions、反向
`tool.execute_host`、binding ID 与 `session.close` 协议。

## 当前限制

- pack 依赖图、热替换、热重载和 pack-owned 动态 schema 尚未实现；绑定顺序就是声明
  顺序，关闭顺序固定为其逆序。
- SDK 没有 per-pack bind/close timeout；应用实现应响应 Session 取消并保证正常 close
  最终返回。
- emergency cleanup 只有同步 stop fence 语义；它不替代应用显式关闭 Session。
- ToolPack 是 Rust SDK 专属能力；跨语言协议扩展不在本阶段范围。
- 通用插件发现、安装、版本解析、权限与 marketplace 属于独立的未来能力。
