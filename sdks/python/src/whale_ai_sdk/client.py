"""Whale AI SDK Client managing connection, Reverse RPC, and thread dispatch."""

from __future__ import annotations

from concurrent.futures import Future, TimeoutError as FuturesTimeoutError
import functools
import logging
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Union
import uuid

from .tool import Tool
from .transport import SubprocessStdioTransport, Transport
from .types import (
    AgentStreamEvent,
    ApprovalDecision,
    CanonicalToolOutput,
    RunTurnResult,
)

logger = logging.getLogger("whale_ai_sdk.client")


class RpcError(Exception):
    """Exception raised when a JSON-RPC method returns an error."""

    def __init__(self, code: int, message: str, data: Optional[Any] = None) -> None:
        super().__init__(f"RPC Error [{code}]: {message}")
        self.code = code
        self.message = message
        self.data = data


class WhaleClient:
    """Whale AI SDK client for orchestrating agent threads, tools, and streaming."""

    def __init__(
        self,
        transport: Optional[Transport] = None,
        daemon_path: Optional[str] = None,
        log_level: str = "info",
    ) -> None:
        if transport is not None:
            self._transport = transport
        else:
            self._transport = SubprocessStdioTransport(daemon_path=daemon_path, log_level=log_level)

        self._pending_requests: Dict[str, Future[Any]] = {}
        self._event_queues: Dict[str, queue.Queue[Optional[AgentStreamEvent]]] = {}
        self._host_tools: Dict[str, Tool] = {}
        self._lock = threading.Lock()

        self._transport.set_message_handler(self._handle_incoming_message)

    def _handle_incoming_message(self, msg: Dict[str, Any]) -> None:
        """Processes an incoming JSON-RPC message from whale-daemon."""
        logger.debug("Client received message: %s", msg)

        # 1. Response to a client request
        if "id" in msg and ("result" in msg or "error" in msg):
            req_id = str(msg["id"])
            with self._lock:
                fut = self._pending_requests.pop(req_id, None)

            if fut is not None:
                if msg.get("error") is not None:
                    err = msg["error"]
                    fut.set_exception(
                        RpcError(
                            code=err.get("code", -32603),
                            message=err.get("message", "Internal RPC error"),
                            data=err.get("data"),
                        )
                    )
                else:
                    fut.set_result(msg.get("result"))
            return

        # 2. Notification from daemon (e.g. stream events)
        if "method" in msg and "id" not in msg:
            method = msg.get("method")
            params = msg.get("params", {}) or {}
            if method == "turn.stream_events":
                thread_id = params.get("thread_id")
                event_data = params.get("event")
                if thread_id and event_data:
                    event = AgentStreamEvent.from_dict(event_data)
                    with self._lock:
                        q = self._event_queues.get(thread_id)
                    if q:
                        q.put(event)
            return

        # 3. Request from daemon to client (Reverse RPC)
        if "method" in msg and "id" in msg:
            method = msg.get("method")
            req_id = msg.get("id")
            params = msg.get("params", {}) or {}

            if method == "tool.execute_host":
                # Spawn a worker thread to execute tool and respond
                threading.Thread(
                    target=self._execute_host_tool_and_respond,
                    args=(req_id, params),
                    daemon=True,
                ).start()
            else:
                # Unknown method
                self._transport.send({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}",
                    },
                })

    def _execute_host_tool_and_respond(self, req_id: Any, params: Dict[str, Any]) -> None:
        """Executes a host tool registered in Python and sends JSON-RPC response."""
        call_id = params.get("call_id", str(uuid.uuid4()))
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        with self._lock:
            tool = self._host_tools.get(tool_name)

        if tool is None:
            output = CanonicalToolOutput.from_text(f"Host tool '{tool_name}' not registered in Python client")
            is_error = True
        else:
            try:
                output = tool.execute(arguments)
                is_error = False
            except Exception as e:
                logger.exception("Error executing host tool '%s': %s", tool_name, e)
                output = CanonicalToolOutput.from_text(f"Tool '{tool_name}' execution failed: {e}")
                is_error = True

        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "call_id": call_id,
                "output": output.to_dict(),
                "is_error": is_error,
            },
        }

        try:
            self._transport.send(response)
        except Exception as send_err:
            logger.error("Failed to send Reverse RPC response for call_id=%s: %s", call_id, send_err)

    def request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 60.0) -> Any:
        """Sends a JSON-RPC 2.0 request and waits for the result."""
        req_id = str(uuid.uuid4())
        fut: Future[Any] = Future()

        with self._lock:
            self._pending_requests[req_id] = fut

        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
        }
        if params is not None:
            msg["params"] = params

        try:
            self._transport.send(msg)
        except Exception as e:
            with self._lock:
                self._pending_requests.pop(req_id, None)
            raise ConnectionError(f"Failed to dispatch RPC request '{method}': {e}") from e

        try:
            return fut.result(timeout=timeout)
        except FuturesTimeoutError:
            with self._lock:
                self._pending_requests.pop(req_id, None)
            raise TimeoutError(f"RPC request '{method}' timed out after {timeout} seconds")

    def register_tool(self, tool_or_func: Union[Tool, Callable[..., Any]]) -> Tool:
        """Registers a host tool function or Tool instance for Reverse RPC execution."""
        if isinstance(tool_or_func, Tool):
            tool = tool_or_func
        else:
            tool = Tool.from_function(tool_or_func)

        with self._lock:
            self._host_tools[tool.name] = tool
        return tool

    def tool(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        supports_parallel: bool = True,
        require_approval: bool = False,
    ) -> Any:
        """Decorator to register a Python function as a Reverse RPC host tool."""
        def decorator(f: Callable[..., Any]) -> Tool:
            t = Tool.from_function(
                f,
                name=name,
                description=description,
                supports_parallel=supports_parallel,
                require_approval=require_approval,
            )
            self.register_tool(t)
            return t

        if func is not None:
            return decorator(func)
        return decorator

    def create_thread(
        self,
        model: str = "claude-3-7-sonnet",
        system_prompt: Optional[str] = None,
        provider: Optional[str] = None,
        tools: Optional[List[Union[Tool, Callable[..., Any]]]] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Thread":
        """Creates a new agent conversation thread."""
        from .thread import Thread

        registered_tools: List[Tool] = []
        if tools:
            for t in tools:
                registered_tools.append(self.register_tool(t))
        else:
            with self._lock:
                registered_tools.extend(self._host_tools.values())

        tool_defs = [t.to_definition_dict() for t in registered_tools]

        params: Dict[str, Any] = {
            "model": model,
            "system_prompt": system_prompt,
            "tools": tool_defs,
            "metadata": metadata or {},
        }
        if session_id:
            params["session_id"] = session_id
        if provider:
            params["provider"] = provider

        result = self.request("session.start_thread", params)
        thread_id = result["thread_id"]

        return Thread(client=self, thread_id=thread_id)

    def resolve_approval(
        self,
        request_id: str,
        decision: Union[ApprovalDecision, str],
        feedback: Optional[str] = None,
    ) -> bool:
        """Resolves a pending HITL tool approval request."""
        dec_str = decision.value if isinstance(decision, ApprovalDecision) else str(decision)
        params: Dict[str, Any] = {
            "request_id": request_id,
            "decision": dec_str,
        }
        if feedback:
            params["feedback"] = feedback

        res = self.request("approval.resolve", params)
        return bool(res.get("resolved", False))

    def close(self) -> None:
        """Shuts down client and disconnects transport."""
        self._transport.close()

    def __enter__(self) -> "WhaleClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
