# SessionStore / recovery / retention — 研究草稿

研究草稿，未实施 Store，不改变 C3 范围。建议**恢复历史/结果并重新绑定会话，不自动续跑中断 turn**；保证恢复器不重放调用，不宣称外部副作用 exactly-once。

**现状。** 历史仅是 Vec（[session.rs:13](../../../crates/whale-core/src/session.rs#L13)）；用户/模型/工具结果提交点为 [engine.rs:298](../../../crates/whale-core/src/engine.rs#L298)、[446](../../../crates/whale-core/src/engine.rs#L446)、[534](../../../crates/whale-core/src/engine.rs#L534)。并行工具整组返回才写结果；内存执行审计先于宿主调用（[coordinator.rs:398](../../../crates/whale-core/src/coordinator.rs#L398)、[429](../../../crates/whale-core/src/coordinator.rs#L429)）。[取消补齐](../../../crates/whale-core/src/session.rs#L175)不覆盖 SIGKILL；[RunRecord](../../../crates/whale-daemon/src/server.rs#L1134)和[终态](../../../crates/whale-daemon/src/server.rs#L1473)都未落盘，[close/EOF](../../../crates/whale-daemon/src/session_lifecycle.rs#L268)删除运行与会话。

**持久边界。** 一个事务型 `SessionStore`、SQLite/WAL 实现及兼容 MemoryStore。保存 schema/revision、session 生命周期、原始 canonical items/稳定 ID、prompt/default options、工具 schema/审批/并行声明、策略配置及 provider/secret 引用；不保存函数、连接 owner、明文密钥。run 保存参数指纹、有效 options/绝对期限、状态/result/error/usage、审批原始与最终参数、序号。每 step 保存 step ID、history revision 和**实际已验证投影**，不覆盖历史、不重算旧 host policy。

| 原子落盘点 / 崩溃窗口 | 恢复动作 |
|---|---|
| session 创建 / run 接受，必须先提交再 ACK | `(session, turn_id)` 同参数重试返回同一记录；不同参数拒绝。未提交不算接受。 |
| 模型请求前记录投影与 step；完成项逐项提交 | 未终结 run 转 `failed/RECOVERY_INTERRUPTED`；丢弃未提交 delta，不重发模型请求，不自动推进工具。 |
| 审批完成后、宿主调用前提交每个 call 的 `dispatch_intent` | 没有 intent 可标“未派发”；有 intent 无持久 result 一律“结果未知”，即使崩溃实际发生在发送前。 |
| 每个宿主返回立即提交 result，不等整组完成 | 已提交结果绝不重执行；其余未知项追加稳定 ID 的 error ToolResult，并保存独立 unknown 审计。旧审批失效；应用须显式确认未知状态才开启新 turn。 |
| history/result/终态序号同事务提交，之后发送 finished | 终态已落盘但通知丢失，恢复查询返回原结果；不再执行。恢复事务可重复运行，不重复补项。 |

Engine/Coordinator 需**await 落盘后才派发**，不能靠事件订阅/Drop。写失败停运行；工具已返回但结果提交失败仍为未知。

**重绑/关闭。** 现有 API 都是创建，不是恢复：[Rust 分配 UUID](../../../crates/whale-sdk-rust/src/agent.rs#L145)、[Python 委托创建](../../../sdks/python/src/whale_ai_sdk/agent.py#L160)、[Java 委托创建](../../../sdks/java/src/main/java/com/whale/ai/Agent.java#L30)。新增 inspect/attach，凭恢复授权（不能只凭 session ID）校验 provider/工具/策略声明，原子更新 lease/epoch 与 fresh binding IDs；SDK 预装回调，失败回滚。缺绑定/引用仅可读；旧 epoch/晚回复禁止写入。单 daemon 独占 Store 锁提供跨进程 fencing。close 释放内存及旧句柄、保留存储；显式 attach 创建新 epoch 后才能运行。delete 独立删除并禁止复活；EOF 仅脱离连接。

**Retention/验收。** 分开限制事件、结果、历史；先裁剪事件，过期游标明确要求 snapshot；历史按完整工具组裁剪。保留 run-ID 去重墓碑/未解决 unknown，活跃 lease 禁止 GC。真实重启注入：ACK 前后、并行一成一挂、副作用后/结果落盘前、终态落盘/通知间、审批/策略中、磁盘失败及 close/delete/GC 竞争。外部持久计数器验证恢复零新增调用；三 SDK 重绑后新 turn 成功，缺绑定/错 provider/旧 epoch 零派发。
