"""Unit tests for Whale AI SDK Python client.

Verifies:
1. Tool reflection and JSON schema parameter generation from Python type hints and docstrings.
2. Tool execution and output packaging.
3. Transport layer and JSON-RPC framing (SubprocessStdioTransport binary discovery & MockTransport).
4. Full client lifecycle: session start, turn execution with event streaming, and Reverse RPC callbacks.
5. Approval resolution flow.
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from typing import Any, Dict, List, Optional

# Add source directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from whale_ai_sdk import (
    AgentStreamEvent,
    ApprovalDecision,
    CanonicalContent,
    CanonicalItem,
    CanonicalToolOutput,
    MockTransport,
    SubprocessStdioTransport,
    Thread,
    Tool,
    WhaleClient,
)
from whale_ai_sdk.tool import _parse_docstring, _python_type_to_json_schema
from whale_ai_sdk.transport import find_daemon_binary


class TestToolReflection(unittest.TestCase):
    """Test Python function reflection to JSON Schema."""

    def test_primitive_type_schema(self) -> None:
        self.assertEqual(_python_type_to_json_schema(str), {"type": "string"})
        self.assertEqual(_python_type_to_json_schema(int), {"type": "integer"})
        self.assertEqual(_python_type_to_json_schema(float), {"type": "number"})
        self.assertEqual(_python_type_to_json_schema(bool), {"type": "boolean"})

    def test_collections_and_optional_schema(self) -> None:
        self.assertEqual(
            _python_type_to_json_schema(List[float]),
            {"type": "array", "items": {"type": "number"}},
        )
        self.assertEqual(
            _python_type_to_json_schema(Optional[str]),
            {"type": "string"},
        )
        self.assertEqual(
            _python_type_to_json_schema(Dict[str, Any]),
            {"type": "object"},
        )

    def test_docstring_parsing_google_style(self) -> None:
        doc = """Calculates metrics for given points.

        Args:
            points: List of coordinate pairs.
            normalize: Whether to normalize output.
        """
        desc, params = _parse_docstring(doc)
        self.assertIn("Calculates metrics for given points.", desc)
        self.assertEqual(params.get("points"), "List of coordinate pairs.")
        self.assertEqual(params.get("normalize"), "Whether to normalize output.")

    def test_tool_from_function(self) -> None:
        def calculate_stats(
            dataset: List[float],
            label: str = "default",
            round_digits: Optional[int] = 2,
        ) -> Dict[str, Any]:
            """Compute mean of dataset.

            Args:
                dataset: The input numerical values.
                label: Identifying label for result.
                round_digits: Decimal precision.
            """
            avg = sum(dataset) / len(dataset)
            return {"label": label, "mean": round(avg, round_digits or 2)}

        tool = Tool.from_function(calculate_stats)
        self.assertEqual(tool.name, "calculate_stats")
        self.assertIn("Compute mean of dataset", tool.description)

        props = tool.parameters["properties"]
        self.assertIn("dataset", props)
        self.assertEqual(props["dataset"]["type"], "array")
        self.assertEqual(props["dataset"]["items"]["type"], "number")
        self.assertEqual(props["dataset"]["description"], "The input numerical values.")

        self.assertIn("label", props)
        self.assertEqual(props["label"]["type"], "string")

        # Required fields: dataset has no default, label and round_digits do
        self.assertEqual(tool.parameters["required"], ["dataset"])

        # Test execution
        out = tool.execute({"dataset": [10.0, 20.0, 30.0]})
        self.assertIsInstance(out, CanonicalToolOutput)
        self.assertEqual(out.type, "structured")
        self.assertEqual(out.data["mean"], 20.0)
        self.assertEqual(out.data["label"], "default")


class TestTransportAndDaemonDiscovery(unittest.TestCase):
    """Test daemon discovery and MockTransport."""

    def test_find_daemon_binary(self) -> None:
        try:
            path = find_daemon_binary()
            self.assertTrue(path.is_file())
            self.assertIn("whale-daemon", path.name)
        except FileNotFoundError:
            self.skipTest("whale-daemon binary not built in target/debug")

    def test_mock_transport_send_recv(self) -> None:
        transport = MockTransport()
        received: List[Dict[str, Any]] = []
        transport.set_message_handler(lambda msg: received.append(msg))

        transport.send({"jsonrpc": "2.0", "id": 1, "method": "test"})
        self.assertEqual(len(transport.sent_messages), 1)
        self.assertEqual(transport.sent_messages[0]["method"], "test")

        transport.simulate_incoming({"jsonrpc": "2.0", "id": 1, "result": "ok"})
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["result"], "ok")


class TestClientAndThreadOrchestration(unittest.TestCase):
    """Test WhaleClient, Thread, EventStream, and Reverse RPC execution."""

    def setUp(self) -> None:
        self.transport = MockTransport()
        self.client = WhaleClient(transport=self.transport)

    def tearDown(self) -> None:
        self.client.close()

    def test_reverse_rpc_tool_execution(self) -> None:
        # Register a host tool in Python
        @self.client.tool(name="mock_sum")
        def add_two_numbers(a: float, b: float) -> float:
            return a + b

        # Daemon initiates Reverse RPC call: "tool.execute_host"
        call_id = "call_abc123"
        self.transport.simulate_incoming({
            "jsonrpc": "2.0",
            "id": "reverse_1",
            "method": "tool.execute_host",
            "params": {
                "call_id": call_id,
                "name": "mock_sum",
                "arguments": {"a": 15.5, "b": 24.5},
            },
        })

        # Allow worker thread to dispatch response
        import time
        time.sleep(0.05)

        # Verify client sent back Reverse RPC response
        responses = [m for m in self.transport.sent_messages if m.get("id") == "reverse_1"]
        self.assertTrue(len(responses) >= 1)
        resp = responses[0]
        self.assertIn("result", resp)
        self.assertEqual(resp["result"]["call_id"], call_id)
        self.assertFalse(resp["result"]["is_error"])
        self.assertEqual(resp["result"]["output"]["data"]["result"], 40.0)

    def test_thread_run_turn_and_event_streaming(self) -> None:
        # Setup mock responses
        def on_send(msg: Dict[str, Any]) -> None:
            method = msg.get("method")
            req_id = msg.get("id")

            if method == "session.start_thread":
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "thread_id": "th_unit_test",
                        "created_at": "2026-09-07T00:00:00Z",
                    },
                })
            elif method == "thread.run_turn":
                params = msg.get("params", {})
                thread_id = params.get("thread_id", "th_unit_test")

                # Stream some events
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": "t1",
                        "thread_id": thread_id,
                        "event": {
                            "type": "reasoning_delta",
                            "turn_id": "t1",
                            "delta": "Thinking about the answer...",
                        },
                    },
                })
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": "t1",
                        "thread_id": thread_id,
                        "event": {
                            "type": "text_delta",
                            "turn_id": "t1",
                            "delta": "Hello from Whale Agent!",
                        },
                    },
                })
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "method": "turn.stream_events",
                    "params": {
                        "turn_id": "t1",
                        "thread_id": thread_id,
                        "event": {
                            "type": "turn_completed",
                            "turn_id": "t1",
                            "thread_id": thread_id,
                            "usage": {"input_tokens": 10, "output_tokens": 8},
                        },
                    },
                })

                # Acknowledge run_turn response
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "turn_id": "t1",
                        "thread_id": thread_id,
                        "status": "completed",
                        "items": [
                            {
                                "type": "assistant_message",
                                "id": "msg_1",
                                "content": [{"type": "text", "text": "Hello from Whale Agent!"}],
                            }
                        ],
                        "usage": {"input_tokens": 10, "output_tokens": 8},
                    },
                })

        self.transport.set_send_hook(on_send)

        thread = self.client.create_thread(
            model="claude-3-7-sonnet",
            system_prompt="Test assistant",
        )
        self.assertEqual(thread.id, "th_unit_test")

        stream = thread.run_turn("Hi there!")
        events: List[AgentStreamEvent] = list(stream)

        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].type, "reasoning_delta")
        self.assertEqual(events[0].delta, "Thinking about the answer...")
        self.assertEqual(events[1].type, "text_delta")
        self.assertEqual(events[1].delta, "Hello from Whale Agent!")
        self.assertEqual(events[2].type, "turn_completed")

        res = stream.result
        self.assertIsNotNone(res)
        assert res is not None
        self.assertEqual(res.status, "completed")
        self.assertEqual(res.usage.total_tokens, 18)

    def test_approval_resolve(self) -> None:
        def on_send(msg: Dict[str, Any]) -> None:
            if msg.get("method") == "approval.resolve":
                self.transport.simulate_incoming({
                    "jsonrpc": "2.0",
                    "id": msg.get("id"),
                    "result": {
                        "resolved": True,
                        "request_id": msg["params"]["request_id"],
                    },
                })

        self.transport.set_send_hook(on_send)

        resolved = self.client.resolve_approval("req_999", ApprovalDecision.APPROVE, feedback="Approved by user")
        self.assertTrue(resolved)


if __name__ == "__main__":
    unittest.main()
