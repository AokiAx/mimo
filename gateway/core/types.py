"""
Internal Event Stream (IES) — protocol-agnostic representation.

All adapters (Anthropic / OpenAI Chat / OpenAI Responses) parse incoming
requests into InternalRequest and emit responses by streaming a sequence of
InternalEvent. The gateway's upstream call is always made in OpenAI Chat
format, but the IES sits between input and output adapters so any protocol
combination is reachable without N×N converters.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


# ────────────── Content blocks ──────────────

ContentType = Literal["text", "image", "tool_use", "tool_result"]


@dataclass
class InternalContent:
    """A single content block inside a message.

    Discriminator: ``type``. Other fields are populated only when relevant.
    """
    type: ContentType

    # text
    text: str | None = None

    # image
    image_data: str | None = None        # base64 of the raw bytes
    image_mime: str | None = None        # e.g. "image/png"

    # tool_use (assistant invoking a tool)
    tool_id: str | None = None           # invocation id, must round-trip
    tool_name: str | None = None
    tool_input: dict[str, Any] | None = None

    # tool_result (user/tool replying with output)
    tool_output: str | None = None
    tool_error: bool = False


# ────────────── Messages ──────────────

Role = Literal["system", "user", "assistant", "tool"]


@dataclass
class InternalMessage:
    role: Role
    content: list[InternalContent]
    # MiMo/OpenAI-style hidden reasoning that must round-trip when assistant
    # history contains tool calls in thinking mode.
    reasoning_content: str | None = None


# ────────────── Tool definitions (request-side) ──────────────

@dataclass
class InternalTool:
    name: str
    description: str
    input_schema: dict[str, Any]


# ────────────── Request ──────────────

@dataclass
class InternalRequest:
    model: str
    messages: list[InternalMessage]
    max_tokens: int
    stream: bool = False

    # Sampling
    temperature: float | None = None
    top_p: float | None = None
    stop: list[str] | None = None

    # Tools
    tools: list[InternalTool] | None = None
    tool_choice: str | dict[str, Any] | None = None

    # Passthrough fields the gateway must not silently drop
    metadata: dict[str, Any] = field(default_factory=dict)


# ────────────── Usage ──────────────

@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0

    # OpenAI Chat extended usage breakdown — forwarded verbatim from upstream
    # so NewAPI / direct clients see real numbers and bill cached/reasoning
    # tokens at the correct rate. MiMo's OpenAI endpoint returns these inside
    # ``prompt_tokens_details`` and ``completion_tokens_details``.
    cached_tokens: int = 0           # prompt_tokens_details.cached_tokens
    reasoning_tokens: int = 0        # completion_tokens_details.reasoning_tokens
    audio_tokens: int = 0            # prompt_tokens_details.audio_tokens
    image_tokens: int = 0            # prompt_tokens_details.image_tokens
    video_tokens: int = 0            # prompt_tokens_details.video_tokens
    # web_search_usage is a nested {tool_usage, page_usage} object — keep it
    # as dict so we don't have to track its schema separately.
    web_search_usage: dict[str, int] | None = None

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


# ────────────── Streaming events ──────────────

FinishReason = Literal["stop", "length", "tool_calls", "content_filter", "error"]


@dataclass
class InternalEvent:
    """Marker base. Adapters dispatch on concrete type via isinstance."""


@dataclass
class MessageStart(InternalEvent):
    message_id: str
    model: str


@dataclass
class ContentBlockStart(InternalEvent):
    """Begin a new content block within the assistant message."""
    index: int
    block_type: Literal["text", "tool_use"]
    tool_id: str | None = None        # tool_use only
    tool_name: str | None = None      # tool_use only


@dataclass
class TextDelta(InternalEvent):
    """Incremental text inside the current text content block."""
    index: int
    text: str


@dataclass
class ReasoningDelta(InternalEvent):
    """Incremental hidden reasoning_content for the assistant message."""
    text: str


@dataclass
class ToolCallDelta(InternalEvent):
    """Incremental JSON arguments inside a tool_use block.

    ``arguments_delta`` is a partial JSON string. Concatenating all deltas of
    the same ``index`` yields the full arguments JSON. Mirrors Anthropic's
    ``input_json_delta`` so that pass-through is cheap.
    """
    index: int
    tool_id: str
    arguments_delta: str


@dataclass
class ContentBlockEnd(InternalEvent):
    index: int


@dataclass
class MessageEnd(InternalEvent):
    finish_reason: FinishReason
    usage: Usage = field(default_factory=Usage)


@dataclass
class StreamError(InternalEvent):
    """Adapter saw a malformed upstream chunk. Recoverable=False ends the stream."""
    message: str
    recoverable: bool = False
