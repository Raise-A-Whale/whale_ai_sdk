# Whale AI SDK

[English](README.md) | [简体中文](README.zh-CN.md)

[![Rust CI](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/ci.yml)
[![Rust Security Audit](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/security.yml/badge.svg)](https://github.com/Raise-A-Whale/whale_ai_sdk/actions/workflows/security.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**用于构建可靠、有状态 AI Agent 的 Rust Runtime 基座。**

Whale 为 Rust 应用提供有状态的 Agent Runtime：模型和工具执行、Session、Run、审批与生命周期所有权；显式配置持久化存储后，还支持持久 Session 的跨重启恢复。它不规定产品外壳；CLI、TUI、桌面应用、服务、工作区模型和业务策略均由宿主应用自行决定，Whale 负责在底层运行 Agent。

> **项目仍处于早期阶段。** 当前公共 API 为 **0.1.x**，后续可能演进。在生产环境依赖前，请阅读 [API 兼容策略](docs/API_STABILITY.md)。

## 为什么选择 Whale？

一个 Agent 产品需要的不只是模型请求循环，还需要能流式输出、路由工具调用、暂停等待审批、恢复会话，以及可预测释放资源的运行时。Whale 通过一套强类型 Rust SDK 和统一的 Daemon/Core 执行路径，提供这些运行时能力。

| Whale 提供 | 宿主应用提供 |
| --- | --- |
| Agent 循环、Canonical 对话 IR、模型适配器、工具执行、Session 和 Run | CLI、TUI、GUI、HTTP API、工作区体验、鉴权和产品策略 |
| ready-before-return 连接生命周期和有界关闭 | 打包、部署和应用进程模型 |
| 流式事件、回放、快照、审批、恢复、保留策略和预算 | 业务工具、权限、工作流和领域数据 |

## 核心能力

- **一套 Runtime，三种连接方式。** 支持进程内 embedded、由应用拥有的 managed Whale-compatible daemon，以及附着到外部 Unix Domain Socket。
- **统一的模型执行路径。** 内置支持 OpenAI Chat Completions、OpenAI Responses 和 Anthropic Messages；自定义 ModelProvider 也复用同一套工具循环。
- **强类型且有状态的 Agent 原语。** 可复用 AgentDefinition、独立 Session、异步 Run、流式事件、取消、deadline 与确定性的终态结果。
- **由宿主掌控且具备边界的工具。** 绑定 Rust HostTool，执行前校验 JSON Schema，上报进度，协作取消，并在需要时请求审批。
- **可恢复的 Session 状态。** Session View 提供快照与有界事件回放；可选 SQLite 存储支持显式创建的持久 Session 档案和跨重启恢复。
- **具备生命周期语义的资源管理。** ToolPack 为每个 Session 建立独立资源，在初始化失败时回滚，并按明确顺序关闭。

## 架构

~~~text
你的 Rust 产品（CLI / TUI / GUI / 服务）
                  │
                  ▼
          whale-sdk-rust
                  │  强类型 JSON-RPC、所有权、生命周期
                  ▼
            whale-daemon
                  │
                  ▼
 whale-core ── whale-adapters ── 模型 Provider
      │
      ├── whale-protocol  （Canonical IR 与通信契约）
      └── whale-store     （可选的持久 Session 存储）
~~~

所有 Runtime source 使用相同的协议和执行路径。应用可以从 embedded Runtime 开始，后续迁移到独立管理的 daemon，而无需重写 Agent 集成代码。

## 快速开始

Whale 已发布到 crates.io。可以直接从 registry 添加 Rust SDK，并根据应用的兼容性策略固定版本：

~~~toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
whale-sdk-rust = "0.1.0-beta.1"
~~~

如需测试尚未发布的 commit，可以使用 Git dependency；为了保证构建可复现，应固定 tag 或 revision。

先设置模型 Provider 凭证，然后创建 embedded Runtime、Agent、Session 和 Run：

~~~sh
export OPENAI_API_KEY="..."
~~~

~~~rust
use whale_sdk_rust::{
    AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig, RuntimeOptions, WhaleRuntime,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded()).await?;

    let mut definition = AgentDefinition::new("assistant", "YOUR_MODEL");
    definition.system_prompt = Some("You are a concise, helpful assistant.".into());
    definition.provider_config = Some(ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: None,
        auth: Some(ProviderAuth::Env {
            variable: "OPENAI_API_KEY".into(),
        }),
    });

    let agent = runtime.agent(definition, Vec::new())?;
    let session = agent.create_session().await?;
    let run = session.start_turn("Hello, Whale.").await?;
    let result = run.result().await?;
    println!("{:?}", result);

    session.close().await?;
    runtime.shutdown().await?;
    Ok(())
}
~~~

Provider 配置、工具、事件与生产环境生命周期处理，请继续阅读 [Rust 应用 SDK 指南](docs/RUST_APPLICATION_SDK.md)。

## 可以构建什么？

Whale 适合作为以下产品的底层 Runtime：

- 有明确工作流的编程或研究型 CLI；
- 需要流式输出和审批交互的桌面应用或 TUI 助手；
- 将业务工作流转变为可调用工具 Agent 的内部服务；
- 配置 SQLite 存储后，可在重启后恢复显式持久化对话及保留 Run 历史的长生命周期工作区应用。

它**不会**内置一套完成品 CLI、TUI、桌面界面、工作区实现、插件市场或通用 Provider 凭证管理器。这些都是宿主应用应当拥有的产品决策。

## Runtime 能力

### Agent、工具与模型上下文

- AgentDefinition 捕获可复用、可移植的 Agent 配置；每个 Session 都有独立的历史和绑定。
- HostTool 在 Rust 宿主中执行，并通过 ToolContext 获得执行身份、deadline、协作取消和进度上报能力。
- 工具参数在执行前，以及审批期修改后都会经过校验。并发调用仍按模型可见顺序返回结果；独占工具在同一注册表中形成屏障。
- ContextPolicy 支持完整历史、最近完整回合和自定义模型投影，同时不修改 Canonical Session 历史。

### Session 与 Run

- WhaleRuntime::open 只有在 source 创建和协议初始化均成功后才会返回。
- RunHandle 提供异步接受、事件订阅、结果查询、取消、deadline 和唯一外层终态。
- Session View 提供权威快照、有界固定窗口回放，以及不会彼此阻塞的独立观察者。
- 可选 Interaction API 支持澄清、表单、权限和 review/approval 流程。Pending 状态仅在 live Session attachment 内可查询和回放，不会跨恢复持久化。

### 存储与生命周期

- 可选 SessionStore 包含进程内 MemoryStore 和持久 SQLite 存储。需要显式创建持久 Session，才能在重启后保留档案并恢复。
- 保留策略和 SessionLimits 可以限制保留的 Run 记录、新回合接收数量和模型请求大小，且不会悄悄丢弃已完成结果。
- ToolPack 工厂会为每个 live Session 绑定新资源，并定义 setup、rollback、正常关闭和 emergency fence 的行为。

## 模型 Provider

| 协议 | 默认端点 | 默认凭证环境变量 |
| --- | --- | --- |
| OpenAI Chat Completions | https://api.openai.com/v1/chat/completions | OPENAI_API_KEY |
| OpenAI Responses | https://api.openai.com/v1/responses | OPENAI_API_KEY |
| Anthropic Messages | https://api.anthropic.com/v1/messages | ANTHROPIC_API_KEY |

ProviderConfig 支持自定义 base URL，因此任何实现了上述协议的第三方端点都可以用同样方式接入：把 `base_url` 指向对应 Provider 的 URL，并指定其凭证环境变量名：

~~~rust
// Kimi（Moonshot），走 OpenAI Chat Completions 协议
definition.provider_config = Some(ProviderConfig {
    api: ProviderApi::OpenaiChatCompletions,
    base_url: Some("https://api.moonshot.cn/v1".into()),
    auth: Some(ProviderAuth::Env { variable: "KIMI_API_KEY".into() }),
});

// DeepSeek，走 OpenAI Chat Completions 协议
definition.provider_config = Some(ProviderConfig {
    api: ProviderApi::OpenaiChatCompletions,
    base_url: Some("https://api.deepseek.com/v1".into()),
    auth: Some(ProviderAuth::Env { variable: "DEEPSEEK_API_KEY".into() }),
});
~~~

提供 Anthropic 兼容端点的服务（例如 Kimi 的 Anthropic 协议服务）则改用 `ProviderApi::AnthropicMessages` 并填对应的 base URL。`ProviderAuth::None` 用于显式声明无认证的本地服务。如需非 HTTP Provider 或自定义凭证，请在 daemon 启动时注册 ModelProvider。支持的协议范围与扩展契约请阅读 [Agent API](docs/AGENT_API.md) 和 [模型扩展 API](docs/MODEL_PROVIDER_API.md)。

## Runtime 模式与平台支持

| 模式 | 适用情况 | 所有权 |
| --- | --- | --- |
| Embedded | Rust 应用应内嵌 Runtime。 | 应用拥有内存连接和本地 daemon loop。 |
| ManagedProcess | 应用会启动兼容的 daemon binary。 | 应用拥有直接 child process，并在关闭时回收它。 |
| ExternalUds | 已有受信任的本地 daemon。 | 应用只拥有自己的 Unix socket 连接。 |

当前传输实现面向 Unix。CI 在 Ubuntu 运行，本地已在 macOS 验证；Windows 暂不支持。外部 UDS peer 被视为受信任的本地 peer：协议初始化只检查兼容性，不验证 peer 身份。宿主必须控制 socket 路径及其权限。

## 仓库结构

~~~text
crates/
  whale-protocol/   Canonical IR、JSON-RPC 与事件契约
  whale-adapters/   模型请求序列化与 SSE 解析
  whale-core/       Agent 循环、Session、工具调度和审批
  whale-store/      Journal、内存/SQLite 存储和恢复
  whale-daemon/     传输、Run 管理和连接所有权
  whale-sdk-rust/   对外 Rust 应用 SDK
fixtures/           协议和仓库外消费者 fixture
scripts/            打包及消费者验证脚本
docs/               API 契约和接入指南
~~~

## 文档导航

| 从这里开始 | 深入了解 |
| --- | --- |
| [Rust 应用 SDK](docs/RUST_APPLICATION_SDK.md) | [架构](docs/ARCHITECTURE.md) |
| [Agent 装配与 Provider](docs/AGENT_API.md) | [通信协议规范](docs/PROTOCOL_SPEC.md) |
| [Run 与流式事件](docs/RUN_API.md) | [执行与模型上下文](docs/EXECUTION_CONTEXT_API.md) |
| [宿主交互与审批](docs/INTERACTION_API.md) | [Session ToolPack](docs/TOOL_PACK_API.md) |
| [Session View 与回放](docs/SESSION_VIEW_API.md) | [恢复](docs/RECOVERY_API.md) 与 [保留策略](docs/RETENTION_API.md) |
| [连接初始化契约](docs/INITIALIZATION_API.md) | [公共 API 契约](docs/API_CONTRACTS.md) |

## 开发与验证

### 环境要求

- Rust **1.85** 或更新版本（workspace 的 MSRV 为 1.85）
- Unix 环境；CI 在 Ubuntu 运行，本地已在 macOS 验证

~~~sh
cargo test --workspace --all-features
cargo run -q -p whale-protocol --example initialization_contract -- --check
cargo build -p whale-daemon --bins --examples
./scripts/verify_rust_packages.sh
~~~

默认测试套件覆盖协议契约、Core 循环、存储、daemon 行为和 SDK 集成。少量真实进程或外部 HTTP fixture 测试使用 Rust 的 `ignore` 属性标记，因为它们需要本地基础设施。

## 贡献

欢迎提交 Issue 和 Pull Request。开发流程和验证检查见 [CONTRIBUTING.md](CONTRIBUTING.md)。
如需了解公共接口的兼容性预期，请阅读 [API 兼容策略](docs/API_STABILITY.md)。
发现疑似安全漏洞时，请按照 [SECURITY.md](SECURITY.md) 私下报告。

## 许可证

Whale AI SDK 使用 [Apache License 2.0](LICENSE) 开源。
