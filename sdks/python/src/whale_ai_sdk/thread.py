"""Agent conversation thread management and real-time streaming iterators."""

from __future__ import annotations

from concurrent.futures import Future
import logging
import queue
import threading
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from .tool import Tool
from .types import (
    AgentStreamEvent,
    ApprovalDecision,
    CanonicalItem,
    RunTurnResult,
    TurnStatus,
)

logger = logging.getLogger("whale_ai_sdk.thread")


class EventStream:
    """An iterable stream yielding AgentStreamEvents in real-time as the turn progresses."""

    def __init__(
        self,
        event_queue: queue.Queue[Optional[AgentStreamEvent]],
        turn_future: Future[RunTurnResult],
        cleanup_callback: Callable[[], None],
    ) -> None:
        self._queue = event_queue
        self._turn_future = turn_future
        self._cleanup = cleanup_callback
        self._result: Optional[RunTurnResult] = None
        self._closed = False

    @property
    def result(self) -> Optional[RunTurnResult]:
        """Returns the final RunTurnResult if turn has completed."""
        if self._result is not None:
            return self._result
        if self._turn_future.done():
            self._result = self._turn_future.result()
            return self._result
        return None

    def __iter__(self) -> Iterator[AgentStreamEvent]:
        try:
            while not self._closed:
                try:
                    # Check queue with a short timeout to observe future state
                    item = self._queue.get(timeout=0.1)
                except queue.Empty:
                    # If future raised an error, re-raise immediately
                    if self._turn_future.done():
                        exc = self._turn_future.exception()
                        if exc is not None:
                            raise exc
                        # Future completed and queue is empty -> done
                        break
                    continue

                if item is None:
                    # Sentinel indicating stream completion
                    break

                yield item

                # If turn finalized via event
                if item.type in ("turn_completed", "turn_failed"):
                    break

            # Await final result
            if self._result is None and self._turn_future.done():
                self._result = self._turn_future.result()
        finally:
            self._cleanup()
            self._closed = True


class Thread:
    """Represents a conversation session with the agent."""

    def __init__(self, client: Any, thread_id: str) -> None:
        self.client = client
        self.thread_id = thread_id

    @property
    def id(self) -> str:
        return self.thread_id

    def register_tool(self, tool_or_func: Union[Tool, Callable[..., Any]]) -> Tool:
        """Dynamically registers a tool with both the Python client and the daemon session."""
        tool = self.client.register_tool(tool_or_func)

        params: Dict[str, Any] = {
            "thread_id": self.thread_id,
            "tools": [tool.to_definition_dict()],
        }
        self.client.request("session.register_tools", params)
        return tool

    def run_turn(
        self,
        prompt_or_items: Union[str, List[CanonicalItem]],
        *,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        reasoning_effort: Optional[str] = None,
        timeout: float = 120.0,
    ) -> EventStream:
        """Executes a conversation turn and returns a real-time EventStream generator."""
        if isinstance(prompt_or_items, str):
            input_items = [CanonicalItem.user_text(prompt_or_items)]
        else:
            input_items = prompt_or_items

        options: Dict[str, Any] = {}
        if model is not None:
            options["model"] = model
        if temperature is not None:
            options["temperature"] = temperature
        if max_tokens is not None:
            options["max_tokens"] = max_tokens
        if reasoning_effort is not None:
            options["reasoning_effort"] = reasoning_effort

        params: Dict[str, Any] = {
            "thread_id": self.thread_id,
            "input_items": [it.to_dict() for it in input_items],
        }
        if options:
            params["options"] = options

        event_queue: queue.Queue[Optional[AgentStreamEvent]] = queue.Queue()
        with self.client._lock:
            self.client._event_queues[self.thread_id] = event_queue

        turn_future: Future[RunTurnResult] = Future()

        def _run_turn_task() -> None:
            try:
                res_dict = self.client.request("thread.run_turn", params, timeout=timeout)
                turn_res = RunTurnResult.from_dict(res_dict)
                turn_future.set_result(turn_res)
            except Exception as e:
                turn_future.set_exception(e)
            finally:
                event_queue.put(None)  # Post sentinel to release iterator

        threading.Thread(target=_run_turn_task, name=f"whale-turn-{self.thread_id}", daemon=True).start()

        def _cleanup() -> None:
            with self.client._lock:
                self.client._event_queues.pop(self.thread_id, None)

        return EventStream(event_queue, turn_future, _cleanup)

    def resolve_approval(
        self,
        request_id: str,
        decision: Union[ApprovalDecision, str],
        feedback: Optional[str] = None,
    ) -> bool:
        """Resolves a pending approval request."""
        return self.client.resolve_approval(request_id, decision, feedback=feedback)
