"""Whale AI SDK - Python Client for Whale Agent Engine."""

from .client import RpcError, WhaleClient
from .thread import EventStream, Thread
from .tool import Tool
from .transport import MockTransport, SubprocessStdioTransport, Transport
from .types import (
    AgentStreamEvent,
    ApprovalDecision,
    CanonicalContent,
    CanonicalItem,
    CanonicalToolOutput,
    MessagePhase,
    RunTurnResult,
    TurnStatus,
    UsageMetrics,
)

__version__ = "0.1.0"

__all__ = [
    "WhaleClient",
    "Thread",
    "Tool",
    "CanonicalItem",
    "CanonicalContent",
    "CanonicalToolOutput",
    "AgentStreamEvent",
    "ApprovalDecision",
    "RunTurnResult",
    "TurnStatus",
    "UsageMetrics",
    "MessagePhase",
    "EventStream",
    "RpcError",
    "Transport",
    "SubprocessStdioTransport",
    "MockTransport",
]
