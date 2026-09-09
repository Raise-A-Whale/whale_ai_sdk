# Public API contracts

本文列出当前 Rust Application SDK 的兼容边界和 optional feature 规则。详细版本策略见
[Rust API stability policy](API_STABILITY.md)，各 API 的行为细节见对应专题文档。

## 冻结与可扩展边界

| Surface | 当前合同 |
| --- | --- |
| `AgentDefinition`、`StartThreadParams`、`RunSnapshot` | 继续支持既有 Rust struct literal；Interaction 不增加字段 |
| `AgentStreamEvent`、`RunEventPayload`、Session V1 snapshot/event | 不增加 Interaction variant 或改变既有 JSON |
| typed approval | `ApprovalRequested`、pending approval、resolve 方法签名和 legacy JSON 保持兼容 |
| `SdkError` | 保持 exhaustive enum；Interaction daemon 错误继续使用 `SdkError::Rpc` |
| Interaction projections/errors | 独立的 additive、`#[non_exhaustive]` 类型；消费者匹配时保留 fallback arm |
| Session durable configuration | V1/V2 一次迁移到 V3；V3 仅增加 `runtime_features.interactions_enabled` |

Interaction opt-in 是 `Agent` 私有状态。只有 opted-in Agent 创建／attach Session 时，SDK 才使用
扁平 wrapper 在原参数旁发送 `interactions_enabled: true`。未启用路径保持既有参数序列化；
`interactions.v1` 保持在 baseline `PROTOCOL_CAPABILITIES` 之外，因此旧宿主不会被迫采用新面。

## Capability 与公共入口

| Capability | Rust 入口 | 依赖 |
| --- | --- | --- |
| `interactions.v1` | `Agent::with_interactions_enabled`；Session snapshot/watch/resume；Run snapshot/respond；callback request | 只要求协议初始化和显式 Session opt-in；不要求 Session View |
| `session_views.v1` / `session_event_replay.v1` | Session V1 snapshot/watch/replay | 与 Interaction cursor/event 独立 |
| Session management 四项 V2 capability | catalog/history/metadata CAS/lifecycle replay | 与 writable Session 和 Interaction 独立 |
| `session_recovery.v1` | persistent create/inspect/attach/acknowledge/forget | 仅 StoreRuntime 启动成功时广告 |

所有 capability 在第一条业务 RPC 前由 `protocol.initialize` 协商。一个 connection 的成功协商
不能证明另一个 connection、外部 UDS peer 身份、模型模态或产品权限。

## Interaction 行为合同

- `InteractionSnapshot` 是 Session pending set 的权威状态；
  `TurnInteractionSnapshot` 只做 Run filter，但保留同一 Session cursor。
- `Requested`／`Removed` 使用专用 Session stream。固定高水位 replay 与 live merge 按 cursor
  去重；retention gap 或 attachment reset 要求换用 fresh snapshot。
- 首次有效 response 先确认 continuation 仍可接收，提交 typed Session 投影（若有）与
  Removed，再完成一次性交付；成功 RPC 等待整个事务，异步 notification 不是提交点。等价
  canonical JSON retry 等待同一最终结果且不重复交付；不同 response 冲突；schema-invalid
  response 保持 pending。
- owner、Session、Run、callback origin 和 request ID 均需匹配。错误不通过是否存在请求泄漏
  其他 connection 的状态。
- Run/Session/connection/origin 生命周期清理 pending waiter。Session close 尝试投递最后的
  removals；EOF 不承诺最后一帧，但必须完成 daemon-owned cleanup。
- response 只存在于必要的 point-to-point response RPC 与 producer 内存；snapshot、replay、
  notification、Store 和 trace 不保留它。进程内 keyed fingerprint 只用于等价比较，不出现在 wire。

完整调用方式与安全数据规则见 [Interaction API](INTERACTION_API.md)。

## Runtime source 与 owner

| Source | 连接路径 | WhaleRuntime 所有权 |
| --- | --- | --- |
| Embedded | 真实 NDJSON 序列化的内存 transport → `DaemonServer::run` | 本 connection loop 与 SDK reader/writer |
| Managed process | direct child stdio；SDK 固定添加 `--listen stdio` | child、stdio、reader/writer；shutdown 等待或 kill/reap |
| External UDS | Unix socket NDJSON | 只拥有本连接；不关闭外部 daemon/listener |

`WhaleRuntime` 是唯一 owner 且不实现 `Clone`；`WhaleClient` clone 是 capability handle。所有 source
都在 `open` 返回前完成同一 initialize，并使用同一 Agent/Session/Run/Interaction API。显式
Session close 与 Runtime shutdown 是正常异步 cleanup 边界；Drop 只提供 emergency fence。

## Store 与恢复

普通 Session 与 Interaction journal 只在内存中存在。显式 persistent Session 存储 canonical
history、Run audit、dispatch intent、结果和 V3 enablement。它不存储 live cursor、pending
Interaction、request payload、response、fingerprint 或 waiter。恢复不会续接旧 live identity；
旧活动 Run 变成 `RECOVERY_INTERRUPTED`，attach 创建新 Session 和新 stream。

Interaction 不替代 durable dispatch recovery。若外部工具副作用结果未知，宿主仍必须处理
`UnknownExecution`，不能根据 Interaction response 推断副作用是否提交。

## 发布检查

当前合同通过 protocol/Core/daemon/Store/SDK 全套测试、三种 Runtime source 的 Interaction
集成路径、仓库外 Rust struct-literal consumer、legacy golden fixtures、rustdoc、workspace check、
format 和 whitespace 检查验证。当前本地实测仅为 macOS；Linux CI 尚未交付；Windows 不支持。
