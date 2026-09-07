# Whale AI SDK - Python Client

Python client SDK for the high-performance Whale Agent Engine and `whale-daemon`.

## Features
- **Subprocess & Socket Transports**: Seamless communication with `whale-daemon` over line-buffered Stdio or Unix Domain Sockets.
- **Bi-directional JSON-RPC 2.0**: Handles real-time event streaming and Reverse RPC callbacks.
- **Reverse RPC Host Tools**: Decorate standard Python functions with `@client.tool` or `Tool.from_function` to expose host-side computation and capabilities to the LLM agent.
- **Automatic Reflection & Schema Generation**: Inspects Python function type hints, signatures, and docstrings to synthesize JSON Schema tool definitions.
- **HITL Approval Gates**: Resolve Human-In-The-Loop tool execution approvals asynchronously.
- **Dual Model Support**: Native support for Anthropic Claude extended reasoning and OpenAI function calling.

## Quickstart

```python
from whale_ai_sdk import WhaleClient

client = WhaleClient()

@client.tool(description="Calculate average and variance of numbers")
def calculate_stats(numbers: list[float]) -> dict:
    avg = sum(numbers) / len(numbers)
    variance = sum((x - avg) ** 2 for x in numbers) / len(numbers)
    return {"mean": avg, "variance": variance, "count": len(numbers)}

thread = client.create_thread(
    model="claude-3-7-sonnet",
    provider="anthropic",
    system_prompt="You are a data science assistant."
)

for event in thread.run_turn("Compute stats for [10.5, 20.0, 30.2, 40.8, 50.0]"):
    if event.type == "text_delta":
        print(event.delta, end="", flush=True)
    elif event.type == "reasoning_delta":
        print(f"[Thinking: {event.delta}]", end="", flush=True)

client.close()
```
