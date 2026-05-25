"""Gateway core: protocol-agnostic abstractions shared by adapters, routing."""
from .context import RequestContext
from .errors import (
    AdapterError,
    AuthError,
    BackendUnavailableError,
    BadRequestError,
    GatewayError,
    ModelNotFoundError,
    RateLimitError,
    UpstreamError,
    UpstreamTimeoutError,
)
from .types import (
    AudioDelta,
    ContentBlockEnd,
    ContentBlockStart,
    ContentType,
    FinishReason,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    Role,
    ReasoningDelta,
    StreamError,
    TextDelta,
    ToolCallDelta,
    Usage,
)

__all__ = [
    # context
    "RequestContext",
    # errors
    "GatewayError",
    "AuthError",
    "RateLimitError",
    "BadRequestError",
    "UpstreamError",
    "BackendUnavailableError",
    "UpstreamTimeoutError",
    "AdapterError",
    "ModelNotFoundError",
# types — request side
    "InternalRequest",
    "InternalMessage",
    "InternalContent",
    "InternalTool",
    "Usage",
    "Role",
    "ContentType",
    "FinishReason",
    # types — streaming events
    "InternalEvent",
    "MessageStart",
    "AudioDelta",
    "ContentBlockStart",
    "TextDelta",
    "ReasoningDelta",
    "ToolCallDelta",
    "ContentBlockEnd",
    "MessageEnd",
    "StreamError",
]
