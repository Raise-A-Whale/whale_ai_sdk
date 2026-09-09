# Rust Session View API

Session View 是 Rust 应用宿主读取和观察 Agent 会话的权威接口。CLI、TUI、
桌面应用或服务端可以用同一份快照绘制界面，再按游标消费增量事件。Whale 负责
状态投影、顺序、回放和连接生命周期；产品壳仍负责界面、工作区、身份与交互策略。

API 包含两个并行且类型隔离的版本。Stage 2.1 V1 是现有 writable `WhaleThread` 的
Run view；Stage 2.2 V2 是 owner-local 管理投影，增加 list、完整 history 分页、metadata
CAS、Closing/Closed 与 read-only tombstone handle。V2 是 additive contract，不改变 V1
签名、JSON、cursor 或 close 行为。

本接口没有实现任何 CLI、TUI 或 Desktop 页面。

## V1 快照与持续观察

`WhaleThread::snapshot()` 返回当前 Session 的权威投影。默认最多包含最近 256 个
canonical history item；`snapshot_with_history_limit` 可请求 1 到 1024 个。

`WhaleThread::watch()` 原子地组合“当前快照”和“快照之后的事件流”。SDK 会先安装
live route，再请求快照，随后做一次固定上界的 replay barrier，因此请求期间发生的
事件既不会遗漏，也不会重复返回。

```rust,no_run
use whale_sdk_rust::{AgentDefinition, RuntimeOptions, WhaleRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;
    let agent = runtime.agent(
        AgentDefinition::new("viewer-agent", "YOUR_MODEL"),
        Vec::new(),
    )?;
    let thread = agent.create_session().await?;

    let watch = thread.watch().await?;
    let mut projection = watch.snapshot;
    let mut events = watch.events;

    if let Some(next) = events.recv().await {
        let envelope = next?;
        projection.apply(&envelope)?;
        // 使用 projection 重绘界面；持久化 envelope.cursor 可在稍后续接。
    }

    thread.close().await?;
    runtime.shutdown().await?;
    Ok(())
}
```

`SessionSnapshot::apply` 是规范 reducer。它忽略已经应用的 cursor，拒绝跨 stream
事件和序列空洞，并在成功后同步更新快照 cursor 与可见 revision。应用不需要复制
daemon 的投影逻辑。

快照包含以下宿主可见状态：

- `summary`：Session ID、Agent 名称、安全 metadata、创建/更新时间和投影 revision；
- `history`：带绝对索引和容量的 canonical history 尾部窗口；
- `active_run`：当前 Run 快照及尚未完成的文本、推理或工具调用草稿；
- `last_run`：最近终态 Run 的轻量摘要；
- `cursor`：这个 live attachment 的精确事件位置。

恢复密钥、Provider 凭证、system prompt、工具 binding ID、Store revision 和内部执行
对象不会进入 Session View。

## V1 Cursor 与回放

`SessionCursor` 由 `thread_id`、`stream_id` 和单调递增的 `seq` 组成。它只属于一个
live Session attachment，与每个 Run 自己的 `RunEvent.seq` 以及 Store 的 CAS revision
相互独立。持久 Session 重新 attach 时会得到新的 `thread_id`/`stream_id`；旧 cursor
不能跨 attachment 使用。

应用可保存 `SessionEventStream::last_received()` 返回的 cursor，并用
`WhaleThread::subscribe_from` 续接：

```rust,no_run
use whale_sdk_rust::{SessionViewError, SubscriptionOptions, WhaleThread};

async fn resume(
    thread: &WhaleThread,
    cursor: whale_sdk_rust::SessionCursor,
) -> Result<(), SessionViewError> {
    let mut events = thread
        .subscribe_from(cursor, SubscriptionOptions::default())
        .await?;

    while let Some(event) = events.recv().await {
        let event = event?;
        // 按 event.cursor 顺序处理。
    }
    Ok(())
}
```

回放第一页冻结 daemon 当前 cursor 为 `through`，后续页都使用同一个上界。因此活跃
Run 不会让分页追逐不断移动的尾部；高于 `through` 的 live 事件由 SDK 缓冲并在回放
完成后合并。每个事件按 Session cursor 最多返回一次。

daemon 同时按事件数量和序列化字节数限制 journal。以下情况返回
`SessionViewError::ResyncRequired`：

- `retention`：请求 cursor 早于当前 replay floor；
- `stream_reset`：请求 cursor 属于另一个 live attachment。

收到该错误后，应丢弃旧的本地投影并重新调用 `watch()`。未来 cursor、错误的
thread ID 和非法配置分别返回 `InvalidCursor` 或 `SdkError::InvalidConfiguration`，
调用方不应解析错误字符串。

## 多观察者与 Run 过滤

同一 Session 可以创建多个独立的 `watch()` 或 `subscribe_from()`。连接 reader 只做
无等待路由；每个订阅有自己的有界输出队列、回放状态和 worker。一个未轮询、落后或
被丢弃的订阅不会阻塞其他订阅，也不会阻塞 `RunHandle::result()`。

默认订阅配置为：

| 配置 | 默认值 | 允许范围 |
| --- | ---: | ---: |
| 输出队列 `buffer_capacity` | 64 | 1–4096 |
| replay page `replay_page_size` | 128 | 1–256 |
| watch history `history_limit` | 256 | 1–1024 |

配置通过 `SubscriptionOptions::new` 和 `SessionWatchOptions::new` 创建；字段保持私有，
以后可以增加选项而不破坏调用方结构体字面量。

`RunHandle::subscribe_events()` 在同一 Session feed 上按 `turn_id` 过滤，并返回携带
Session cursor 的 `RunEventEnvelope`。它适合多个组件独立观察同一个 Run。原有
`RunHandle::events()` 仍是兼容的单消费者流，其签名和行为没有改变。

```rust,no_run
use whale_sdk_rust::{RunHandle, SessionViewError, SubscriptionOptions};

async fn observe_run(run: &RunHandle) -> Result<(), SessionViewError> {
    let mut events = run
        .subscribe_events(None, SubscriptionOptions::default())
        .await?;
    while let Some(next) = events.recv().await {
        let envelope = next?;
        // envelope.cursor 可用于诊断和续接；envelope.event 是原有 RunEvent。
    }
    Ok(())
}
```

未传 `after` 时，Run 订阅从一次新 Session 快照的 cursor 之后开始。若需要历史 Run
事件，应传入此前保存的 Session cursor。

## V1 生命周期与能力协商

新接口依赖两个可选能力：

- `session_views.v1`：`session.get`；
- `session_event_replay.v1`：`session.subscribe` 与 `session.event`。

它们没有加入基础 `PROTOCOL_CAPABILITIES`。Rust SDK 在发送业务 RPC 前检查初始化结果；
连接旧 daemon 时，宿主会得到 `UnsupportedCapability`，daemon 不会收到未知方法。

显式 `WhaleThread::close()` 成功后，所属 Session stream 的 `recv()` 返回 `None`。
意外连接断开时，每个 stream 返回一次 `SessionViewError::Sdk`，之后返回 `None`。
丢弃 stream 会取消它自己的 worker，但不会关闭 Session、其他订阅或 Run。

回放只保证同一个 live `WhaleRuntime` attachment 内的有界续接。持久恢复会从 durable
canonical state 建立新快照和新 stream，不重放旧 token delta、旧审批或外部工具副作用。

## V2 owner-local 管理面

`WhaleClient::list_sessions` 只列出当前 connection owner 已发布的 Open、Closing 和未淘汰
Closed records。Preparing 不可见，detached Store records 不是全局目录，其他连接的
Session 与未知 ID 对调用方都表现为 `SessionUnavailable`。第一页冻结成员窗口；它之后
创建的 Session 不会混入后续页。列表 entry 的 metadata/lifecycle 可以反映读页时的更新，
所以固定的是成员集合，不是跨 Session 的事务快照。

```rust,no_run
use whale_sdk_rust::{SessionListOptions, WhaleClient};

async fn list_all(client: &WhaleClient) -> Result<Vec<String>, whale_sdk_rust::SessionManagementError> {
    let mut options = SessionListOptions::default(); // 64；允许 1..=256
    let mut ids = Vec::new();
    loop {
        let page = client.list_sessions(options).await?;
        ids.extend(page.sessions.iter().map(|entry| entry.summary.thread_id.clone()));
        let Some(cursor) = page.next_cursor else { break };
        options = SessionListOptions::default().with_cursor(cursor);
    }
    Ok(ids)
}
```

`WhaleThread::session_view()` 返回 cloneable `SessionViewHandle`；已有 thread ID 也可以用
`WhaleClient::session_view(id)?` 创建。handle 只保存 client clone 和 ID，不拥有 daemon
cleanup，也不会延长 tombstone TTL。它公开 `snapshot`、`history_page`、`watch` 和
`subscribe_from`，没有 start/cancel Run、tool/context mutation、metadata replacement 或
close。`WhaleThread` 仍是 write capability。

```rust,no_run
use serde_json::Map;
use whale_sdk_rust::{
    SessionHistoryOptions, SessionManagementWatchOptions, SubscriptionOptions, WhaleThread,
};

async fn manage(thread: &WhaleThread) -> Result<(), whale_sdk_rust::SessionManagementError> {
    let view = thread.session_view();
    let snapshot = view.snapshot().await?;

    // 整体替换 metadata；任何 V2-visible Run/lifecycle 事件也会推进这个 revision。
    let changed = thread
        .replace_metadata(snapshot.summary.view_revision, Map::new())
        .await?;

    let _older = view
        .history_page(SessionHistoryOptions::default()) // 128；允许 1..=256
        .await?;
    let mut watch = view
        .watch(SessionManagementWatchOptions::default())
        .await?;
    while let Some(next) = watch.events.recv().await {
        let event = next?;
        if event.cursor.seq > changed.cursor.seq {
            // 将连续 V2 event 应用到本地 SessionSnapshotV2。
        }
    }
    Ok(())
}

async fn resume_v2(
    thread: &WhaleThread,
    cursor: whale_sdk_rust::SessionCursorV2,
) -> Result<(), whale_sdk_rust::SessionManagementError> {
    let mut events = thread
        .session_view()
        .subscribe_from(cursor, SubscriptionOptions::default())
        .await?;
    while let Some(next) = events.recv().await {
        let _event = next?;
    }
    Ok(())
}
```

V2 watch 与 V1 一样先安装 live route，再获取 snapshot，并无条件执行一次固定 `through`
的 replay barrier。每个 subscriber 有独立 worker、bounded output 和 `last_received()`；
output 或 broadcast lag 会触发 replay 修复，取消单次 `recv()` 不会取消 worker，Drop 只
终止该 stream。连接意外断开返回一次 `SessionManagementError::Sdk`，随后 `None`。

## V2 history 与 cursor 边界

V2 管理面刻意分开以下身份域：

| 类型 | 含义 | 有效期 |
| --- | --- | --- |
| `SessionCursorV2` | Run、metadata、lifecycle 的 V2 投影位置 | 当前 live attachment 或其 tombstone |
| `SessionSummaryV2.view_revision` | metadata CAS 版本，恒等于 V2 cursor seq | 当前 live attachment |
| `SessionHistoryAnchor` | 某 V2 stream 的 canonical item 绝对边界 | attachment/tombstone |
| `SessionHistoryPageCursor` | 固定 history 窗口的 opaque continuation | 五分钟 |
| `SessionListCursor` | owner catalog 固定窗口的 opaque continuation | 五分钟 |
| recovery `revision` / `epoch` | Store CAS／attachment lease | recovery record |

这些类型不能通过复制数值互换。list/history page cursor 使用 daemon-secret HMAC 绑定当前
owner、类型、窗口与 expiry；跨 owner、伪造或错误类型 token 会被拒绝。daemon 重启会更换
密钥，因此旧 opaque cursor 也会失效。

`history_page` 从固定边界向前分页，但每页 items 仍按从旧到新排列。首次请求可省略边界
以冻结当前末尾，或使用 `SessionHistoryOptions::before(anchor)?` 从 snapshot tail 之前
继续；后续请求使用 `continue_from(next_cursor)?`，不可同时传 `before`。并发 append 只会
反映在 `current_end`，不会改变该页链的 `through`。若 bounded archive 已越过请求边界，
返回 `HistoryGap`，绝不静默漏项。

## V2 metadata、关闭与持久恢复

metadata 操作是 whole-map CAS，不是 merge patch。只有 Open Session 可以写；相同 map
返回 `changed=false` 且不推进 V2 cursor。`RevisionConflict` 携带 expected/current revision
及 current cursor，宿主应读取 fresh V2 snapshot 后决定是否重试。对持久 Session，Store
提交先于 V2 event；`StorageFailure {outcome_unknown:true}` 表示响应前后的提交结果无法由
当前连接确认，应重新 inspect/attach 并读取保存的 metadata。

显式 close 的 V2 顺序是 `Closing -> 已接受 Run 的剩余终态 -> Closed -> close response`。
write fence 从 Closing 开始拒绝新 Run、metadata、tool/context mutation；read handle 仍能读取
Closing/Closed tombstone。live 或 late V2 subscriber 会交付 Closed 一次后返回 `None`；从
Closed cursor 开始则收到空 replay 页后直接结束。V1 stream 继续保持原有 close 时立即结束
的行为。

Closed tombstone 每 owner 最多 128 个／64 MiB，最长十分钟；压力按最老 Closed record
淘汰，永不淘汰 Open/Closing。淘汰后 read 返回 `TombstoneExpired`，并使旧 list cursor
过期。普通 Session 不再出现；持久 recovery record 可继续存在，但 authenticated attach
创建新的 thread ID、V1/V2 stream 与 revision。旧 replay cursor 不能恢复；history anchor
若带着旧 stream 请求新 attachment，会得到 `HistoryStreamReset`。连接 EOF 会完成取消和
durable detach 后删除整个 owner namespace。

## V2 能力、限制与错误恢复

| 可选能力 | Rust 入口 | 缺失时行为 |
| --- | --- | --- |
| `session_catalog.v1` | `WhaleClient::list_sessions` | RPC 前 `UnsupportedCapability` |
| `session_history.v1` | `SessionViewHandle::history_page` | RPC 前 `UnsupportedCapability` |
| `session_metadata_cas.v1` | `WhaleThread::replace_metadata` | RPC 前 `UnsupportedCapability` |
| `session_lifecycle_replay.v1` | V2 snapshot/watch/subscribe | RPC 前 `UnsupportedCapability` |

V2 replay 默认 128／最大 256 events，subscriber output 默认 64／最大 4096，watch history
默认 256／最大 1024。metadata replacement 最大 64 KiB serialized JSON、256 个顶层 key、
深度 16。history/list response 还受 1 MiB 上限约束；单个 oversized item 返回 typed
`ResourceLimit`。

`SessionManagementError` 由 JSON-RPC code 加 `SessionManagementErrorData.kind` 映射；
SDK 不解析诊断字符串，缺失／畸形／code-kind 不匹配的数据返回 `InvalidProjection`。典型
恢复动作如下：list cursor invalid/expired 时从第一页重启；`HistoryCursorInvalid` 重启
history 页链；`HistoryStreamReset` 获取 fresh V2 snapshot；`HistoryGap` 从返回的 floor 或
fresh snapshot 重绘；`RevisionConflict` 刷新后再决定 CAS；`SessionNotOpen` 保留读取并停止
写入；`TombstoneExpired` 删除本地 view。

## V1/V2 选择与产品边界

只需要当前 `WhaleThread` Run 投影的宿主可继续使用 V1。需要跨 Session 列表、向前翻阅
canonical history、metadata 并发更新、显式 Closing/Closed 或 close 后只读 tombstone 的
宿主使用 V2。两套 hub、cursor、reducer 和 notification route 独立；应用不能把 V1 cursor
传给 V2 API，也无需为了采用 V2 改写现有 V1 消费者。

完整的 Codex、Claude Code、OpenCode、pi 或 DeepSeek Harness Agent 仍需未来独立的
`AgentBackend` 契约；它们不能作为 `RuntimeSource::ManagedProcess` 或单步
`ModelProvider` 直接替代 Whale Daemon。
