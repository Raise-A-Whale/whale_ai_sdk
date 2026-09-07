"""Protocol types and canonical representations for Whale AI SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import uuid


class ApprovalDecision(str, Enum):
    """Decision for Human-in-the-loop tool execution approval."""
    APPROVE = "approve"
    REJECT = "reject"


class TurnStatus(str, Enum):
    """Terminal or pending status of an agent turn."""
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    REQUIRES_APPROVAL = "requires_approval"


class MessagePhase(str, Enum):
    """Phase of an assistant message."""
    COMMENTARY = "commentary"
    FINAL_ANSWER = "final_answer"


@dataclass
class UsageMetrics:
    """Token usage metrics for LLM generation."""
    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> UsageMetrics:
        if not data:
            return cls()
        return cls(
            input_tokens=data.get("input_tokens", 0),
            output_tokens=data.get("output_tokens", 0),
            reasoning_tokens=data.get("reasoning_tokens", 0),
            cache_creation_input_tokens=data.get("cache_creation_input_tokens", 0),
            cache_read_input_tokens=data.get("cache_read_input_tokens", 0),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "cache_creation_input_tokens": self.cache_creation_input_tokens,
            "cache_read_input_tokens": self.cache_read_input_tokens,
        }


@dataclass
class CanonicalContent:
    """Multi-modal content block supported in canonical messages."""
    type: str
    text: Optional[str] = None
    mime_type: Optional[str] = None
    data: Optional[str] = None
    uri: Optional[str] = None

    @classmethod
    def text_content(cls, text: str) -> CanonicalContent:
        return cls(type="text", text=text)

    @classmethod
    def image_uri(cls, mime_type: str, uri: str) -> CanonicalContent:
        return cls(type="image", mime_type=mime_type, uri=uri)

    @classmethod
    def image_base64(cls, mime_type: str, data: str) -> CanonicalContent:
        return cls(type="image", mime_type=mime_type, data=data)

    def to_dict(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {"type": self.type}
        if self.text is not None:
            res["text"] = self.text
        if self.mime_type is not None:
            res["mime_type"] = self.mime_type
        if self.data is not None:
            res["data"] = self.data
        if self.uri is not None:
            res["uri"] = self.uri
        return res

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CanonicalContent:
        return cls(
            type=data.get("type", "text"),
            text=data.get("text"),
            mime_type=data.get("mime_type"),
            data=data.get("data"),
            uri=data.get("uri"),
        )


@dataclass
class CanonicalToolOutput:
    """Canonical representation of tool output payload."""
    type: str  # "text", "structured", or "blocks"
    text: Optional[str] = None
    data: Optional[Any] = None
    blocks: Optional[List[CanonicalContent]] = None

    @classmethod
    def from_text(cls, text: str) -> CanonicalToolOutput:
        return cls(type="text", text=text)

    @classmethod
    def from_structured(cls, data: Any) -> CanonicalToolOutput:
        return cls(type="structured", data=data)

    @classmethod
    def from_blocks(cls, blocks: List[CanonicalContent]) -> CanonicalToolOutput:
        return cls(type="blocks", blocks=blocks)

    def to_dict(self) -> Dict[str, Any]:
        if self.type == "text":
            return {"type": "text", "text": self.text or ""}
        elif self.type == "structured":
            return {"type": "structured", "data": self.data}
        elif self.type == "blocks":
            return {
                "type": "blocks",
                "blocks": [b.to_dict() if isinstance(b, CanonicalContent) else b for b in (self.blocks or [])],
            }
        return {"type": "text", "text": str(self.text or "")}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CanonicalToolOutput:
        out_type = data.get("type", "text")
        if out_type == "text":
            return cls.from_text(data.get("text", ""))
        elif out_type == "structured":
            return cls.from_structured(data.get("data"))
        elif out_type == "blocks":
            blocks = [CanonicalContent.from_dict(b) for b in data.get("blocks", [])]
            return cls.from_blocks(blocks)
        return cls.from_text(str(data))


@dataclass
class CanonicalItem:
    """Canonical discrete item of a conversation turn."""
    type: str  # "user_message", "assistant_message", "reasoning", "tool_call", "tool_result"
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content: Optional[List[CanonicalContent]] = None
    phase: Optional[str] = None
    thinking: Optional[str] = None
    signature: Optional[str] = None
    encrypted_content: Optional[str] = None
    call_id: Optional[str] = None
    namespace: Optional[str] = None
    name: Optional[str] = None
    arguments: Optional[Dict[str, Any]] = None
    raw_arguments: Optional[str] = None
    output: Optional[CanonicalToolOutput] = None
    is_error: Optional[bool] = None

    @classmethod
    def user_text(cls, text: str) -> CanonicalItem:
        return cls(
            type="user_message",
            content=[CanonicalContent.text_content(text)],
        )

    @classmethod
    def assistant_text(cls, text: str, phase: str = "final_answer") -> CanonicalItem:
        return cls(
            type="assistant_message",
            content=[CanonicalContent.text_content(text)],
            phase=phase,
        )

    @classmethod
    def reasoning(cls, thinking: str, signature: Optional[str] = None) -> CanonicalItem:
        return cls(
            type="reasoning",
            thinking=thinking,
            signature=signature,
        )

    @classmethod
    def tool_call(
        cls,
        call_id: str,
        name: str,
        arguments: Optional[Dict[str, Any]] = None,
        raw_arguments: str = "",
        namespace: Optional[str] = None,
    ) -> CanonicalItem:
        return cls(
            type="tool_call",
            call_id=call_id,
            name=name,
            arguments=arguments,
            raw_arguments=raw_arguments,
            namespace=namespace,
        )

    @classmethod
    def tool_result(
        cls,
        call_id: str,
        output: CanonicalToolOutput,
        is_error: bool = False,
    ) -> CanonicalItem:
        return cls(
            type="tool_result",
            call_id=call_id,
            output=output,
            is_error=is_error,
        )

    def to_dict(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {"type": self.type, "id": self.id}
        if self.content is not None:
            res["content"] = [c.to_dict() if isinstance(c, CanonicalContent) else c for c in self.content]
        if self.phase is not None:
            res["phase"] = self.phase
        if self.thinking is not None:
            res["thinking"] = self.thinking
        if self.signature is not None:
            res["signature"] = self.signature
        if self.encrypted_content is not None:
            res["encrypted_content"] = self.encrypted_content
        if self.call_id is not None:
            res["call_id"] = self.call_id
        if self.namespace is not None:
            res["namespace"] = self.namespace
        if self.name is not None:
            res["name"] = self.name
        if self.arguments is not None:
            res["arguments"] = self.arguments
        if self.raw_arguments is not None:
            res["raw_arguments"] = self.raw_arguments
        if self.output is not None:
            res["output"] = self.output.to_dict() if isinstance(self.output, CanonicalToolOutput) else self.output
        if self.is_error is not None:
            res["is_error"] = self.is_error
        return res

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CanonicalItem:
        item_type = data.get("type", "user_message")
        content_raw = data.get("content")
        content = [CanonicalContent.from_dict(c) for c in content_raw] if content_raw else None

        output_raw = data.get("output")
        output = CanonicalToolOutput.from_dict(output_raw) if output_raw else None

        return cls(
            type=item_type,
            id=data.get("id", str(uuid.uuid4())),
            content=content,
            phase=data.get("phase"),
            thinking=data.get("thinking"),
            signature=data.get("signature"),
            encrypted_content=data.get("encrypted_content"),
            call_id=data.get("call_id"),
            namespace=data.get("namespace"),
            name=data.get("name"),
            arguments=data.get("arguments"),
            raw_arguments=data.get("raw_arguments"),
            output=output,
            is_error=data.get("is_error"),
        )


@dataclass
class AgentStreamEvent:
    """Real-time streaming event emitted during agent turn processing."""
    type: str
    turn_id: Optional[str] = None
    thread_id: Optional[str] = None
    item_id: Optional[str] = None
    item_type: Optional[str] = None
    phase: Optional[str] = None
    delta: Optional[str] = None
    signature: Optional[str] = None
    call_id: Optional[str] = None
    item: Optional[CanonicalItem] = None
    request_id: Optional[str] = None
    tool_call: Optional[CanonicalItem] = None
    reason: Optional[str] = None
    usage: Optional[UsageMetrics] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AgentStreamEvent:
        event_type = data.get("type", "")
        item_raw = data.get("item")
        item = CanonicalItem.from_dict(item_raw) if item_raw else None

        tool_call_raw = data.get("tool_call")
        tool_call = CanonicalItem.from_dict(tool_call_raw) if tool_call_raw else None

        usage_raw = data.get("usage")
        usage = UsageMetrics.from_dict(usage_raw) if usage_raw else None

        return cls(
            type=event_type,
            turn_id=data.get("turn_id"),
            thread_id=data.get("thread_id"),
            item_id=data.get("item_id"),
            item_type=data.get("item_type"),
            phase=data.get("phase"),
            delta=data.get("delta"),
            signature=data.get("signature"),
            call_id=data.get("call_id"),
            item=item,
            request_id=data.get("request_id"),
            tool_call=tool_call,
            reason=data.get("reason"),
            usage=usage,
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            raw=data,
        )


@dataclass
class RunTurnResult:
    """Final result of an agent turn."""
    turn_id: str
    thread_id: str
    status: Union[TurnStatus, str]
    items: List[CanonicalItem] = field(default_factory=list)
    usage: UsageMetrics = field(default_factory=UsageMetrics)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RunTurnResult:
        items_raw = data.get("items", [])
        items = [CanonicalItem.from_dict(it) for it in items_raw]
        usage = UsageMetrics.from_dict(data.get("usage"))
        return cls(
            turn_id=data.get("turn_id", ""),
            thread_id=data.get("thread_id", ""),
            status=data.get("status", TurnStatus.COMPLETED),
            items=items,
            usage=usage,
        )
