# 上游架构复核快照

核查日期：2026-09-08。只读核对现有 `docs/SDK_ARCHITECTURE_REVIEW.md`，本文件不修改其实现状态结论。以下官方页面均通过 web 实际打开；判断基于当日文档，不是性能测评，也不代表所有特性具有相同稳定性。

## 总体判断

现有定位建议成立：Whale 面向业务 Agent 的运行语义与宿主语言绑定；以 pi 为分层参照、借鉴 DeepSeek 的装配与释放、Codex 的控制协议及 OpenCode 的生成客户端。调用四者中的完整 Agent，应有独立 `AgentBackend`，不能放进只执行单个模型步骤的 `ModelProvider`。

两种边界的区别在于执行权：ModelProvider 消费已经投影的模型请求，产生模型消息/调用建议；完整 AgentBackend 自己拥有模型循环、工具、审批、历史与产品默认配置。适配后者必须保留其运行身份、终态、取消、连接归属及能力差异；不能假定它只产生 token，或让 Whale 与后端各自执行同一个工具调用。这是从下列接口归纳的架构建议。

## DeepSeek Harness

**定位与入口。** `deepseek-ai/deepseek-harness` 的 README 明确由 DeepSeek AI 开发，仍标 developer preview。Cordis 的 service/event/effect 组织模型、工具、日志和 agent loop；支持的 Node 应用由 `dsh` profile 启动。TS SDK 驱动匹配版本的完整子进程，`sdk` 带 coding persona，`sdk-minimal` 是另一套显式组合，不是用户传入的进程内 Cordis 树。[仓库](https://github.com/deepseek-ai/deepseek-harness)、[架构](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/architecture.md)、[SDK client](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/sdk/client/README.md)、[SDK profile](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/bundle/sdk-app/README.md)。

值得借鉴三点：

1. 注册附带 effect disposer，依赖通过 `inject` 声明，释放按归属执行；比仅提供全局 register 函数更完整。[Cordis primer](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/cordis-primer.md)
2. profile/bundle 装配与业务循环分开；其 stdio SDK profile 只在启动时应用 patches，避免拥有工作之后替换 transport/loop 依赖。[架构](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/architecture.md)
3. 完整外部 Agent 单独放在 `ctx.subagents`：Codex、Claude Code、ACP、DSH SDK 等后端有能力预检、start/result/dispose；注册移除只禁止新启动，已返回运行仍由持有人负责释放。这为 Whale 的 AgentBackend 与 ModelProvider 分离提供直接参照。[subagent 契约](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/subsystems/subagent.md)

**不同之处。** Whale 不必把每个内部函数改为动态插件；产品默认文件工具和 persona 应留在业务组合。插件装配受信任，其存在不等于安全隔离。DeepSeek 的可重建模型输入日志值得参考，但不能据此推断有外部工具 exactly-once 保证。[架构](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/architecture.md)、[SDK profile 的限制](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/bundle/sdk-app/README.md)

## Codex

**定位与入口。** Codex SDK 用于编程控制本地 coding agent；app-server 用于包含鉴权、会话历史、审批与事件的深度产品集成，官方文档明确其实现开源。当前同时有 TS SDK 和 Python SDK；Python 提供同步及 AsyncCodex，并固定匹配 CLI runtime。不能将其理解为独立通用模型 SDK。[Codex SDK](https://learn.chatgpt.com/docs/codex-sdk)、[app-server](https://learn.chatgpt.com/docs/app-server)。

值得借鉴三点：

1. thread/turn/item 的独立身份及双向请求、通知，便于 UI、控制调用与运行状态分离。[app-server](https://learn.chatgpt.com/docs/app-server)
2. 从所运行版本生成 TS/JSON Schema，配合初始化中的实验能力选择，降低客户端契约漂移。[app-server](https://learn.chatgpt.com/docs/app-server)
3. SDK 配套运行时和原生异步入口，把安装、进程管理及语言执行习惯纳入交付，而不只暴露几组 DTO。[Codex SDK](https://learn.chatgpt.com/docs/codex-sdk)

**不同之处。** 工作目录、代码执行与沙箱是 coding 产品职责，不能成为 Whale 业务 Agent 的必填前提。`dynamicTools`/`item/tool/call` 仍是实验接口；WebSocket 也有实验状态说明。app-server 的 Unix socket 使用 WebSocket HTTP Upgrade，不能按 Whale 原始 JSONL UDS 直接复用 transport。两者应通过显式 backend 适配。[app-server](https://learn.chatgpt.com/docs/app-server)

## Claude Agent SDK

**定位与入口。** Claude Agent SDK 把驱动 Claude Code 的 agent loop 作为 Python / TypeScript 库提供，公开 tools、permissions、hooks、sessions、subagents、skills、MCP、budget 和事件流。官方文档明确将它与交互式 Claude Code CLI、需要调用者自己实现循环的 Anthropic Client SDK、以及托管式 Agent 区分；SDK 会携带匹配的原生 Claude Code runtime。[概览](https://code.claude.com/docs/en/agent-sdk/overview)、[agent loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)。

这进一步确认 Whale 的产品边界：CLI、TUI、Desktop 和服务端产品都应调用同一 Agent 内核，界面不进入 SDK。Whale 的取舍是 Rust-first、provider-neutral，并把 embedded / owned child / attached daemon 三种部署所有权做成公开契约；Claude SDK 内置的文件、Shell、搜索、coding persona 和配置发现应继续留在 Whale 上层 ToolPack 或产品组合中，不能成为通用 Runtime 的硬依赖。

## OpenCode

**定位与入口。** 开源 coding agent 的 JS/TS SDK 是服务客户端：`createOpencode()` 启动 server/client，`createOpencodeClient()` 连接已有服务。Headless server 暴露 OpenAPI 3.1 和 SSE；另有 `opencode acp` 通过 stdio JSON-RPC 接入编辑器。[仓库](https://github.com/anomalyco/opencode)、[SDK](https://opencode.ai/docs/sdk/)、[Server](https://opencode.ai/docs/server/)、[ACP](https://opencode.ai/docs/acp/)。

值得借鉴三点：

1. TUI 与 server 分离，让另一种 UI/自动化消费者使用同一运行服务。[Server](https://opencode.ai/docs/server/)
2. SDK 类型来自 OpenAPI，并明确区分“自启服务”和“连接服务”的资源所有权。[SDK](https://opencode.ai/docs/sdk/)
3. 用事件、工具执行前后 hook 和自定义工具扩展产品行为，而非要求集成者修改循环。[Plugins](https://opencode.ai/docs/plugins/)

**不同之处。** 插件上下文明确带 project、directory、worktree 和 Bun shell；这属于 coding 应用装配。Whale 可借鉴生成与 transport 解耦，但 OpenCode SDK 本身不是可替换的通用 loop 库。外部驱动应接 HTTP/SSE 或 ACP，保留 OpenCode 对权限与工具执行的所有权。[Plugins](https://opencode.ai/docs/plugins/)、[SDK](https://opencode.ai/docs/sdk/)

## pi

**定位与入口。** 当前官方仓库为 `earendil-works/pi`，工具包分模型、Agent Core、coding-agent 与 UI 等层。业务应用可直接嵌入 Agent Core；完整 coding-agent 可通过 `createAgentSession()` 进程内嵌入，或通过 `pi --mode rpc` 子进程驱动。[仓库](https://github.com/earendil-works/pi)、[Agent Core](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)、[coding SDK](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/sdk.md)、[RPC](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/rpc.md)。

值得借鉴三点：

1. 模型 collection/provider factory/API 实现分开，按 provider 按需导入；不同应用可注入自己的模型集合，避免全局注册表和所有实现的隐含依赖。[pi-ai](https://raw.githubusercontent.com/earendil-works/pi/main/packages/ai/README.md)
2. `AgentMessage → transformContext → convertToLlm` 明确区分应用状态与模型输入；适合 Whale 保留原历史、单独构造每步 ModelRequest 的路线。[Agent Core](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)
3. 原生依赖与应用资源分层：SQLite backend 独立包，coding SDK 的 ResourceLoader/SessionManager 管资源发现与会话；避免通用内核依赖文件发现、终端或数据库实现。[Agent Core](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)、[coding SDK](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/sdk.md)

**不同之处。** 当前 Agent Core 确实依注册顺序 await 订阅者，prompt/waitForIdle 还等待 agent_end listeners；Whale 的结果与慢消费者独立契约不应照搬这一派发机制。pi 的 `turn_end` 可对应模型/工具的一步，不能仅按事件名称映射 Whale 外层 turn 终态。coding SDK 默认资源发现和文件工具也不是 Agent Core 必选依赖。[Agent Core](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)、[coding SDK](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/sdk.md)

## 对现有报告的修订建议

现有四项目表格的核心结论没有发现必须反转的事实错误；以下属于应补齐的接口范围与避免误读的限定：

- Codex 增补 Python 同步/异步 SDK、匹配运行时及与 app-server 的用途区别；不要写成只有 app-server 或只有 TS SDK。若讨论 MCP 外部驱动，当前官方已将 `codex mcp-server` 标为 deprecated。[SDK](https://learn.chatgpt.com/docs/codex-sdk)
- OpenCode 增补 ACP stdio 入口，不能把 HTTP/SSE 写成唯一外部集成方式。[ACP](https://opencode.ai/docs/acp/)
- pi 增补 Agent Core / coding AgentSession / RPC 的三种边界，并使用当前 `@earendil-works/*` 包名。保留现有 awaited listener 差异说明，它仍准确。[Agent Core](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)、[RPC](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/rpc.md)
- DeepSeek 增补 `ctx.subagents` 作为完整 Agent 后端的直接参照；保留 developer preview、默认 coding persona 和 SDK startup-only 限定。不要将“everything is a plugin”推导为任意 SDK 调用都支持 inline in-process composition。[架构](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/architecture.md)、[subagents](https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/subsystems/subagent.md)

本地辅助核对仅使用已存在的 DeepSeek checkout `dd6322d604e00eec1ba5e0c8541159906a21094a`；其 SDK bundle 主边界匹配，但官方当前文件已细化 persona/工具默认说明，所以未将本地 HEAD 当成上游最新版本。未 clone/pull、运行上游代码或修改既有源码。
