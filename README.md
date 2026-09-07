# Whale AI SDK

**Whale AI SDK** 是面向下一代自主智能体（Autonomous Agents）的高性能、工业级分布式 SDK 与调度底座。采用 Rust 核心状态机驱动、守护进程进程隔离（Daemon-Process Boundary）、统一中间表示（Canonical IR）与反向工具 RPC（Reverse Tool RPC）架构，为 Python、Java 和 Rust 生态提供一致、强类型且极速的 Agent 运行环境。

---

## ✨ 核心特性 (Key Features)

- **统一中间表示 (Canonical IR)**: 彻底消除厂商锁定，统一映射 Anthropic Claude（Extended Thinking 签名流、Ephemeral Prompt Caching 提示词缓存）与 OpenAI（Chat Completions API、Responses API、o1/o3 Reasoning Effort）。
- **双向反向工具 RPC (Reverse Tool RPC)**: 允许在宿主语言（Python, Java, Rust）中以原生函数和反射声明工具，由 Rust 守护进程在执行循环中通过 JSON-RPC 2.0 异步反向回调，免去暴露 HTTP 端口与复杂网络配置。
- **并发调度与保序执行 (`FuturesOrdered` + `RwLock`)**: 严格保证工具执行分发顺序与模型意图保序对齐；基于读写屏障对只读工具实现高并发吞吐，对排他性工具（写文件、改表）实现独占互斥。
- **人类在环安全审批网关 (ApprovalGate HITL)**: 对敏感操作提供细粒度异步挂起机制，支持通过 RPC 进行 `Accept`（放行）、`Deny`（带原因驳回反思）与 `ModifyArguments`（动态修正参数）。
- **多语言薄客户端 (Thin SDKs)**: 提供纯净、极简且功能完备的多语言客户端（Python、Java、Rust），零 FFI/JNI 崩溃风险，支持 Stdio 子进程与 Unix Domain Socket (UDS) 高效通信。

---

## 🏛️ 项目仓库结构 (Repository Layout)

```
whale_ai_sdk/
├── crates/
│   ├── whale-protocol/    # Canonical IR、流式事件定义与 JSON-RPC 2.0 信封规范
│   ├── whale-adapters/    # Anthropic Claude 与 OpenAI 双向协议适配器及 SSE 解析器
│   ├── whale-core/        # Agent 核心状态机、Turn Loop 引擎、RwLock 屏障与 ApprovalGate
│   ├── whale-daemon/      # 守护进程二进制与服务，支持 Stdio 及 Unix Domain Socket
│   └── whale-sdk-rust/    # 原生 Rust SDK，支持进程内嵌入式与跨进程连接
├── sdks/
│   ├── python/            # Python 3.10+ 薄客户端 (支持 @client.tool 类型反射生成器)
│   └── java/              # Java 17+ 企业级客户端 (基于 Jackson & CompletableFuture)
├── docs/
│   ├── ARCHITECTURE.md    # 系统全景架构设计文档 (中英双语 / 深入分析)
│   └── PROTOCOL_SPEC.md   # 完整 JSON-RPC 2.0 通信协议契约与时序规范
└── Cargo.toml             # 根目录 Cargo Workspace
```

---

## 🚀 快速上手 (Quickstart)

### 1. Python SDK

Python SDK 支持使用类型注解直接修饰普通函数为 Host 工具：

```python
from whale_ai_sdk import WhaleClient

# 启动客户端（自动拉起 whale-daemon 子进程）
client = WhaleClient()

# 注册本地 Python 原生工具（通过反射自动生成 JSON Schema，并通过 Reverse RPC 调度）
@client.tool(description="计算给定数值数组的统计指标")
def calculate_metrics(values: list[float]) -> dict:
    mean = sum(values) / len(values)
    return {"mean": mean, "count": len(values), "max": max(values)}

# 创建多轮对话线程
thread = client.create_thread(
    model="claude-3-7-sonnet",
    system_prompt="你是一名专业的数据分析助手。"
)

# 发起任务并实时流式接收事件
print("Agent 回答: ", end="", flush=True)
for event in thread.run_turn("请计算 [12.5, 45.0, 78.2, 90.1, 15.3] 的统计指标"):
    if event.type == "text_delta":
        print(event.delta, end="", flush=True)
    elif event.type == "reasoning_delta":
        print(f"\n[思考]: {event.delta}", end="", flush=True)

client.close()
```

### 2. Java SDK

Java SDK 针对企业级应用（如 Spring Boot、Quarkus）提供流畅的 Builder 构建模式：

```java
import com.whale.ai.WhaleClient;
import com.whale.ai.AgentThread;
import com.whale.ai.Tool;
import com.whale.ai.models.CanonicalToolOutput;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AgentApp {
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // 1. 初始化客户端
        try (WhaleClient client = new WhaleClient()) {
            // 2. 声明本地 Host 工具
            Tool echoTool = Tool.builder()
                .name("sys_info")
                .description("获取宿主系统信息")
                .parametersSchema(mapper.readTree("{\"type\":\"object\"}"))
                .handler(arguments -> CanonicalToolOutput.fromText("OS: " + System.getProperty("os.name")))
                .build();
            client.registerTool(echoTool);

            // 3. 启动线程
            AgentThread thread = client.createThread("claude-3-7-sonnet", "你是一名系统运维助手。");

            // 4. 执行 Turn 并监听流式输出
            thread.runTurn("请查看当前宿主系统的操作系统信息。", event -> {
                if ("text_delta".equals(event.getType())) {
                    System.out.print(event.getDelta());
                }
            });
        }
    }
}
```

### 3. Rust 原生 SDK

Rust 客户端提供极佳的零开销体验，既支持嵌入在进程内执行，也支持与远程 Daemon 进程通信：

```rust
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::{json, Value};
use whale_sdk_rust::{HostTool, WhaleClient, DaemonServer};
use whale_protocol::canonical::CanonicalToolOutput;

struct LocalCalculator;

#[async_trait]
impl HostTool for LocalCalculator {
    fn name(&self) -> &str { "calc" }
    fn description(&self) -> &str { "执行基础四则运算" }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "expr": { "type": "string" } },
            "required": ["expr"]
        })
    }
    async fn execute(&self, _args: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("42"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 进程内嵌入式启动或者连接到现有 daemon
    let server = Arc::new(DaemonServer::default_server());
    let client = WhaleClient::in_process(server);

    let thread = client.create_thread("claude-3-7-sonnet", Some("计算专家".into())).await?;
    thread.register_tool(Arc::new(LocalCalculator)).await?;

    let (turn_res, mut event_rx) = thread.run_turn("计算 20 + 22").await?;
    while let Some(event) = event_rx.recv().await {
        println!("Stream Event: {:?}", event);
    }

    println!("Turn Completed: status={:?}", turn_res.status);
    Ok(())
}
```

---

## 🛠️ 构建与测试指南 (Building & Testing)

### 前置环境要求
- **Rust**: 1.75+ (`cargo`, `rustc`)
- **Python**: 3.10+ (`pytest`, `pip`)
- **Java**: JDK 17+, Apache Maven 3.8+

### 1. 构建 Rust Workspace 与核心 Daemon
```bash
# 编译所有 crates
cargo build --workspace

# 运行整个工作区单元测试与集成测试
cargo test --workspace

# 以 Release 模式编译 whale-daemon 二进制
cargo build --release -p whale-daemon
# 生成产物位置：./target/release/whale-daemon
```

### 2. 测试 Python SDK
```bash
cd sdks/python

# 安装为可编辑模式
pip install -e .

# 运行 Python 测试套件
pytest tests/
```

### 3. 测试 Java SDK
```bash
cd sdks/java

# 运行 Maven 测试
mvn clean test
```

---

## 📖 深度架构与协议文档

- [系统全景架构深度剖析 (docs/ARCHITECTURE.md)](docs/ARCHITECTURE.md): 涵盖 Canonical IR、Tokio Actor Loop、`FuturesOrdered` 保序队列、读写并发屏障、HITL 机制以及与 OpenAI Codex 架构的全面对比。
- [通信协议规范文档 (docs/PROTOCOL_SPEC.md)](docs/PROTOCOL_SPEC.md): 涵盖 JSON-RPC 2.0 物理帧格式、Client 与 Daemon 完整请求/响应 Schema、反向 RPC 细节以及端到端时序图。

---

## 📄 开源许可证 (License)

Apache License 2.0.
