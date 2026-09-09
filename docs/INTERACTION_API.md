# Rust Interaction API

`interactions.v1` 是 Rust 应用宿主的可选、Session 级挂起请求协议。Core 工具或正在执行的
`HostTool` 可以请求 clarification、form、authentication handoff、file/network permission、
review、typed tool approval 或集成自定义输入；宿主通过一个权威快照和可续接事件流观察请求，
再把响应交回同一个 Run。这个 API 是产品基座，不规定终端、窗口、通知或凭证存储方式。

## 能力与显式 opt-in

Daemon 在 `protocol.initialize` 的附加 capabilities 中广告 `interactions.v1`。它不属于
`PROTOCOL_CAPABILITIES` 的基础集合，也不依赖 `session_views.v1`。Rust SDK 会在创建 Session
前检查该能力；不支持时返回 `SdkError::ProtocolCompatibility`，且不会发送创建业务请求。

Opt-in 只保存在 `Agent` 的私有状态中：

```rust,no_run
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;
let agent = runtime
    .agent(
        AgentDefinition::new("interactive-agent", "YOUR_MODEL"),
        Vec::new(),
    )?
    .with_interactions_enabled();
assert!(agent.interactions_enabled());
let session = agent.create_session().await?;
# session.close().await?;
# runtime.shutdown().await?;
# Ok(())
# }
```

`AgentDefinition`、`StartThreadParams` 和 `RunSnapshot` 没有新增字段。SDK 仅在 opt-in 时使用
扁平的 `StartThreadWithInteractionsParams`、`CreatePersistentSessionWithInteractionsParams`
或 `AttachRecoveryWithInteractionsParams`，在原参数旁增加必填的
`interactions_enabled: true`。未 opt-in 的 Session 保持原请求 JSON 与 typed approval 行为。

## 观察与响应

`WhaleThread::watch_interactions` 一次返回：

- `snapshot: InteractionSnapshot`：该 Session 当前全部 pending 请求和当前位置；
- `events: InteractionEventStream`：从 snapshot cursor 之后连续交付的 `Requested`／
  `Removed` envelope。

```rust,no_run
use whale_sdk_rust::{
    InteractionEventPayload, InteractionWatchOptions, WhaleThread,
};

async fn handle_pending(session: &WhaleThread) -> Result<(), Box<dyn std::error::Error>> {
    let mut watch = session
        .watch_interactions(InteractionWatchOptions::default())
        .await?;
    let mut projection = watch.snapshot;

    for pending in &projection.pending {
        // Render only the safe request payload, then obtain an application response.
        let _kind = &pending.request.kind;
    }

    while let Some(event) = watch.events.recv().await.transpose()? {
        projection.apply(&event)?;
        match event.payload {
            InteractionEventPayload::Requested { interaction } => {
                let _request_id = interaction.request_id;
            }
            InteractionEventPayload::Removed { request_id, cause, .. } => {
                let _ = (request_id, cause);
            }
            _ => {}
        }
    }
    Ok(())
}
```

`RunHandle::pending_interactions()` 返回同一 Session cursor 上只属于该 Run 的
`TurnInteractionSnapshot`。应用拿到响应后可以调用：

```rust,no_run
# use serde_json::json;
# use whale_sdk_rust::RunHandle;
# async fn respond(run: &RunHandle, request_id: &str) -> Result<(), whale_sdk_rust::SdkError> {
let result = run
    .respond_interaction(request_id, json!({"answer": "continue"}))
    .await?;
assert_eq!(result.request_id, request_id);
assert!(result.resolved);
# Ok(())
# }
```

知道完整 identity 的宿主也可以调用
`WhaleClient::respond_interaction(thread_id, turn_id, request_id, response)`。Daemon 先验证
connection owner、Session、Run 和请求 identity，再验证 64 KiB 响应上限及可选 JSON Schema。
无效响应不会消耗请求，应用可修正后重试。首次有效响应先确认 producer continuation 仍可
接收，再提交 typed Session 投影（若有）与 `Removed(cause="resolved")`，最后完成一次性交付；
成功的 response RPC 只在全部步骤完成后返回。notification 的异步到达不是提交点。相同
canonical JSON 的顺序或并发重试等待同一事务结果，成功时返回 `resolved: true`，但 continuation
只收到一次；同一 ID 的不同响应返回 `-32051` conflict。

稳定 RPC 错误码通过现有 `SdkError::Rpc { code, message }` 返回：

| Code | 含义 |
| --- | --- |
| `-32050` | request 不存在、已清理或不再保留 |
| `-32051` | request 已由不同 canonical response 解决 |
| `-32052` | response 超限或不符合 response schema |
| `-32053` | owner 不可见、Session 未启用或 Interaction route 不可用 |

错误文本用于诊断，不应用于分支判断，也不会回显响应内容。

## Cursor、回放与 resync

Interaction cursor 是 `{thread_id, stream_id, seq}`，与 Run sequence、Session V1/V2 cursor、
Store revision 和 recovery epoch 完全独立。每个 live attachment 有新的 `stream_id`。

保存 `InteractionEventStream::last_received()` 或 snapshot cursor 后，可用
`WhaleThread::subscribe_interactions_from(cursor, options)` 续接。SDK 固定第一页的
`through` 高水位，逐页拉取连续 replay，同时缓存 live notification，再按 cursor 合并和去重。
因此订阅建立期间丢失 live frame 也会被 replay 补回；慢 watcher 不阻塞 connection reader、
Run 或其他 watcher。

默认本地输出队列为 64，允许 1–4096；默认 replay 页为 128，允许 1–256。Daemon 每个
Session 保留连续后缀，最多 1024 个 envelope／4 MiB。若旧 cursor 低于 replay floor，或来自
旧 attachment，SDK 返回 `InteractionViewError::ResyncRequired { gap, snapshot }`。应用必须丢弃
旧投影并换成附带的权威 snapshot，再从它的 cursor 订阅。非法 cursor、畸形 daemon 投影和
连接关闭分别保留为不同错误。每个 watcher 独立；丢弃最后一个 watcher 会释放其弱通知 route。

## Producer 与自定义 kind

Core 的 `whale_core::ToolContext::request_interaction` 和 Rust 宿主的
`whale_sdk_rust::ToolContext::request_interaction` 使用相同请求模型。宿主回调版本只在当前
`tool.execute_host` callback 活跃、Session 已 opt-in 且 capability 已协商时可用：

```rust,no_run
# use serde_json::json;
# use whale_sdk_rust::{InteractionRequest, ToolContext};
# async fn ask(context: &ToolContext) -> Result<(), Box<dyn std::error::Error>> {
let request = InteractionRequest::new(
    "example.deploy.review",
    "Review deployment",
    json!({"environment": "staging", "digest": "sha256:..."}),
    Some(json!({
        "type": "object",
        "properties": {"decision": {"enum": ["approve", "reject"]}},
        "required": ["decision"],
        "additionalProperties": false
    })),
)?;
let response = context.request_interaction(request).await?;
let _decision = response["decision"].as_str();
# Ok(())
# }
```

这个 forward RPC 可以发生在同一连接正在处理 reverse tool callback 时。SDK 在独立 callback
task 中等待它，connection reader 继续分发响应、Run progress 和其他 Session 的消息，避免
自锁。callback 完成或取消时，它创建的精确请求会被清理。

内建命名为 `whale.tool_approval`、`whale.clarification`、`whale.form`、`whale.auth`、
`whale.permission.file`、`whale.permission.network` 和 `whale.review`。`whale.*` 由协议保留；
应用和插件应使用稳定的自有 namespace，例如 `com.example.release.review`。Daemon 只处理通用
生命周期和 schema，不根据 custom kind 推断权限或执行产品策略。

`response_schema` 使用 JSON Schema Draft 2020-12，允许本地 `#` 引用，禁用外部 schema
获取。kind 最多 128 bytes，title 最多 512 bytes；payload 和 schema 各最多 64 KiB；每个
Run 最多同时挂起 32 个请求。

## Typed approval 兼容

Opt-in Session 的传统 `ApprovalRequested` 仍按原 `AgentStreamEvent` 发布，
`RunSnapshot.pending_approvals` 也保持原形状；Daemon 同时把相同 request ID 投影为
`whale.tool_approval`。以下入口共享同一个 first-commit-wins transaction：

- `turn.respond_interaction`；
- `turn.resolve_approval`；
- 兼容方法 `approval.resolve`；
- Core typed approval resolution；
- Rust `RunHandle::resolve_approval` 和 `WhaleClient::resolve_approval`。

Rust typed 方法在 capable、opted-in Session 上走通用事务；其他 Session 保持原 wire。
approve、reject、modify_arguments 的源签名、JSON 和执行前／替换后参数校验没有变化。

`ApprovalGate::resolve_approval` 是 Core 的同步兼容入口；在 Daemon bridge 下，它的 `true` 只表示
resolver 已接受该决定，后续 projection／event 提交仍由异步任务完成。应用宿主需要可确认的
事务结果时应 await `RunHandle::resolve_approval` 或 `WhaleClient::resolve_approval`。

## 生命周期与恢复

Run cancel／deadline／terminal、origin callback 完成、Session close、connection EOF、
publication failure 和 Run retention 都会清除对应 pending state 并唤醒 producer。常见
`Removed.cause` 为 `resolved`、`cancelled`、`run_finished`、`session_closed`、
`connection_closed`、`origin_finished` 或 `publication_failed`。Session close 会尝试发布
removal，然后关闭本地 stream；EOF 无法承诺最后一帧交付，stream termination 和 Session
不可再访问是权威边界。

持久 Session 的 V3 配置只保存 `runtime_features.interactions_enabled`。pending request、
event journal、response、response fingerprint 和 waiter 都不进入 Store。进程重启会按恢复
规则把活动 Run 标记为 `RECOVERY_INTERRUPTED`；认证 attach 使用新的 live Session identity、
新的 Interaction stream 和 zero cursor，不会自动重新提示旧请求。未知外部工具执行仍由既有
dispatch-intent／`UnknownExecution` 恢复流程处理。

## 安全 payload 规则

Request payload、title、schema、Requested event 和 replay 都是可显示、可记录、可回放数据。
producer 必须只放安全摘要：精确展示路径或 host、scope、授权 URL、display code、内容摘要和
允许动作。不要放 access token、密码、cookie、私钥、完整敏感文件内容或未脱敏请求头。

Response 只沿 `turn.respond_interaction` 请求到达 daemon，并只交给精确 suspended producer。
Daemon 不把 response 放进 snapshot、event、Store 或日志；它仅在进程内保留随机 key 的
HMAC-SHA256 canonical-response fingerprint，用于判断等价重试，且不发布该 digest。协议的
`Debug` 输出会把 response 替换为 `[REDACTED]`。认证流程应优先返回宿主凭证库的 opaque
reference，让实际 credential 始终留在宿主受控存储中。

External UDS 仍是 trusted-local-peer 通道。Interaction capability 与 Session opt-in 不完成
daemon 身份认证，也不授予文件、网络、凭证或工具权限；宿主需要控制 socket 目录、权限和 peer。

## Wire 摘要

| 方向 | Method | Params | Result |
| --- | --- | --- | --- |
| client → daemon | `session.interactions.get` | `GetSessionInteractionsParams` | `InteractionSnapshot` |
| client → daemon | `session.interactions.subscribe` | `SubscribeInteractionsParams` | `SubscribeInteractionsResult` |
| client → daemon | `turn.interactions.get` | `GetTurnInteractionsParams` | `TurnInteractionSnapshot` |
| callback client → daemon | `turn.request_interaction` | `RequestInteractionParams` | `InteractionResponse` |
| client → daemon | `turn.respond_interaction` | `RespondInteractionParams` | `RespondInteractionResult` |
| daemon → client | `session.interaction_event` | `InteractionEventEnvelope` | notification |

完整 wire 字段、上限、验证器与错误常量在
[`whale_protocol::interactions`](../crates/whale-protocol/src/interactions.rs)；公共兼容边界见
[API Contracts](API_CONTRACTS.md)。
