#!/usr/bin/env python3
"""Data Agent Example using Whale AI SDK for Python.

Demonstrates:
1. Connecting to whale-daemon via SubprocessStdioTransport.
2. Defining a Python host tool using @client.tool with automatic schema reflection.
3. Starting a conversation thread with Anthropic or OpenAI provider.
4. Real-time streaming of extended reasoning and text deltas.
5. Bi-directional Reverse RPC execution: whale-daemon delegates calculation to Python!
"""

from __future__ import annotations

import math
import os
import sys
from typing import Any, Dict, List

# Ensure whale_ai_sdk is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from whale_ai_sdk import (
    ApprovalDecision,
    MockTransport,
    WhaleClient,
)


def main() -> None:
    print("=========================================================")
    print("     Whale AI SDK - Data Analysis Agent with Reverse RPC ")
    print("=========================================================\n")

    # Detect provider configuration from environment
    has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY"))
    has_openai = bool(os.environ.get("OPENAI_API_KEY"))

    use_mock = "--mock" in sys.argv or (not has_anthropic and not has_openai and "--real" not in sys.argv)

    if use_mock:
        print("[INFO] Running in deterministic simulated mode (pass --real or set ANTHROPIC_API_KEY to run live daemon).")
        transport = MockTransport()
        client = WhaleClient(transport=transport)

        # Setup mock transport handler to simulate daemon turn & reverse RPC
        def mock_server_logic(msg: Dict[str, Any]) -> None:
            method = msg.get("method")
            req_id = msg.get("id")

            if method == "session.start_thread":
                # Acknowledge start thread
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "thread_id": "mock_th_1001",
                        "created_at": "2026-09-07T12:00:00Z",
                    },
                })
            elif method == "thread.run_turn":
                params = msg.get("params", {})
                thread_id = params.get("thread_id", "mock_th_1001")
                turn_id = "turn_001"

                # 1. Emit turn started
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": turn_id,
                        "thread_id": thread_id,
                        "event": {
                            "type": "turn_started",
                            "turn_id": turn_id,
                            "thread_id": thread_id,
                        },
                    },
                })

                # 2. Emit reasoning delta
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": turn_id,
                        "thread_id": thread_id,
                        "event": {
                            "type": "reasoning_delta",
                            "turn_id": turn_id,
                            "delta": "User wants to compute stats on measurements. I should invoke calculate_statistics.",
                        },
                    },
                })

                # 3. Simulate Reverse RPC: Daemon calls host tool in Python!
                call_id = "call_stat_42"
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": f"reverse_{call_id}",
                    "method": "tool.execute_host",
                    "params": {
                        "call_id": call_id,
                        "name": "calculate_statistics",
                        "arguments": {
                            "data": [12.4, 15.6, 9.8, 22.1, 14.5, 18.2, 11.0, 16.7],
                        },
                    },
                })

                # 4. Stream response text deltas
                text_deltas = [
                    "\n\nBased on the analysis of your 8 measurements:\n",
                    "- **Count**: 8\n",
                    "- **Mean**: 15.04\n",
                    "- **Standard Deviation**: 3.92\n",
                    "- **Min / Max**: 9.80 / 22.10\n\n",
                    "The distribution shows moderate variance with 22.10 being the peak reading.",
                ]
                for delta in text_deltas:
                    transport.simulate_incoming({
                        "jsonrpc": "2.0",
                        "method": "turn.stream_events",
                        "params": {
                            "turn_id": turn_id,
                            "thread_id": thread_id,
                            "event": {
                                "type": "text_delta",
                                "turn_id": turn_id,
                                "delta": delta,
                            },
                        },
                    })

                # 5. Emit turn completed
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": turn_id,
                        "thread_id": thread_id,
                        "event": {
                            "type": "turn_completed",
                            "turn_id": turn_id,
                            "thread_id": thread_id,
                            "usage": {
                                "input_tokens": 140,
                                "output_tokens": 85,
                                "reasoning_tokens": 32,
                            },
                        },
                    },
                })

                # 6. Resolve run_turn request
                transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "turn_id": turn_id,
                        "thread_id": thread_id,
                        "status": "completed",
                        "items": [],
                        "usage": {
                            "input_tokens": 140,
                            "output_tokens": 85,
                            "reasoning_tokens": 32,
                        },
                    },
                })

        transport.set_send_hook(mock_server_logic)

    else:
        print("[INFO] Connecting to live whale-daemon via SubprocessStdioTransport...")
        client = WhaleClient()

    # Define Python tool using @client.tool decorator
    @client.tool(
        name="calculate_statistics",
        description="Calculates mean, variance, standard deviation, min, and max for a list of numbers.",
    )
    def calculate_statistics(data: List[float]) -> Dict[str, Any]:
        """Calculates descriptive statistics on numeric data.

        Args:
            data: List of numeric values to analyze.
        """
        print(f"\n⚡ [REVERSE RPC TRIGGERED] Python executing calculate_statistics on {len(data)} items...")
        n = len(data)
        if n == 0:
            return {"count": 0, "error": "Empty dataset"}

        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / (n - 1 if n > 1 else 1)
        std_dev = math.sqrt(variance)

        stats = {
            "count": n,
            "mean": round(mean, 4),
            "variance": round(variance, 4),
            "std_dev": round(std_dev, 4),
            "min": min(data),
            "max": max(data),
        }
        print(f"⚡ [REVERSE RPC RESULT] Computed stats: {stats}\n")
        return stats

    provider = "anthropic" if has_anthropic else ("openai" if has_openai else "anthropic")
    model = "claude-3-7-sonnet" if provider == "anthropic" else "gpt-4o"

    print(f"Creating agent thread with provider={provider}, model={model}...")
    thread = client.create_thread(
        model=model,
        provider=provider,
        system_prompt="You are an expert data analysis assistant. Use tools when calculations are needed.",
    )
    print(f"Session established! Thread ID: {thread.id}\n")

    user_prompt = (
        "Here are our sensor readings from today: [12.4, 15.6, 9.8, 22.1, 14.5, 18.2, 11.0, 16.7]. "
        "Please calculate the statistical summary using your tools and provide a brief interpretation."
    )
    print(f"User Prompt: {user_prompt}\n")
    print("--- Streaming Turn Output ---")

    stream = thread.run_turn(user_prompt)
    for event in stream:
        if event.type == "reasoning_delta" and event.delta:
            # Print reasoning in dim styling
            sys.stdout.write(f"\033[90m{event.delta}\033[0m")
            sys.stdout.flush()
        elif event.type == "text_delta" and event.delta:
            sys.stdout.write(event.delta)
            sys.stdout.flush()
        elif event.type == "turn_completed":
            print("\n\n--- Turn Completed ---")
            if event.usage:
                print(
                    f"Usage: {event.usage.input_tokens} prompt tokens, "
                    f"{event.usage.output_tokens} completion tokens, "
                    f"{event.usage.reasoning_tokens} reasoning tokens."
                )

    client.close()
    print("\nClient cleanly disconnected.")


if __name__ == "__main__":
    main()
