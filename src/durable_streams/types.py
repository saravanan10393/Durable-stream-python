"""
Type definitions for the Durable Streams Protocol.
"""

from dataclasses import dataclass, field
from typing import Callable, Awaitable, Literal, Optional
from asyncio import Future


@dataclass
class StreamMessage:
    """A single message in a stream."""

    # The raw bytes of the message
    data: bytes

    # The offset after this message (format: "<read-seq>_<byte-offset>")
    offset: str

    # Timestamp when the message was appended (milliseconds since epoch)
    timestamp: int


@dataclass
class Stream:
    """Stream metadata and data."""

    # The stream URL path (key)
    path: str

    # Content type of the stream
    content_type: Optional[str] = None

    # Messages in the stream
    messages: list[StreamMessage] = field(default_factory=list)

    # Current offset (next offset to write to)
    current_offset: str = "0000000000000000_0000000000000000"

    # Last sequence number for writer coordination
    last_seq: Optional[str] = None

    # TTL in seconds
    ttl_seconds: Optional[int] = None

    # Absolute expiry time (ISO 8601)
    expires_at: Optional[str] = None

    # Timestamp when the stream was created (milliseconds since epoch)
    created_at: int = 0


@dataclass
class StreamLifecycleEvent:
    """Event data for stream lifecycle hooks."""

    # Type of event
    type: Literal["created", "deleted"]

    # Stream path
    path: str

    # Timestamp of the event (milliseconds since epoch)
    timestamp: int

    # Content type (only for 'created' events)
    content_type: Optional[str] = None


# Hook function called when a stream is created or deleted
StreamLifecycleHook = Callable[[StreamLifecycleEvent], Awaitable[None] | None]


@dataclass
class ServerOptions:
    """Options for creating the server."""

    # Port to listen on (default: 4437)
    port: int = 4437

    # Host to bind to (default: "127.0.0.1")
    host: str = "127.0.0.1"

    # Default long-poll timeout in milliseconds (default: 30000)
    long_poll_timeout: int = 30_000

    # Hook called when a stream is created
    on_stream_created: Optional[StreamLifecycleHook] = None

    # Hook called when a stream is deleted
    on_stream_deleted: Optional[StreamLifecycleHook] = None


@dataclass
class PendingLongPoll:
    """Pending long-poll request."""

    # Stream path
    path: str

    # Offset to wait for
    offset: str

    # Future to resolve with messages
    future: Future[list[StreamMessage]]

    # Timeout task handle
    timeout_handle: Optional[object] = None


@dataclass
class ReadResult:
    """Result of reading from a stream."""

    # Messages read from the stream
    messages: list[StreamMessage]

    # Whether the client is up-to-date (no more data available)
    up_to_date: bool


@dataclass
class WaitResult:
    """Result of waiting for new messages."""

    # Messages that arrived (empty if timed out)
    messages: list[StreamMessage]

    # Whether the wait timed out
    timed_out: bool


@dataclass
class CreateOptions:
    """Options for creating a stream."""

    # Content type of the stream
    content_type: Optional[str] = None

    # TTL in seconds
    ttl_seconds: Optional[int] = None

    # Absolute expiry time (ISO 8601)
    expires_at: Optional[str] = None

    # Initial data to append
    initial_data: Optional[bytes] = None


@dataclass
class AppendOptions:
    """Options for appending to a stream."""

    # Sequence number for writer coordination
    seq: Optional[str] = None

    # Content type (must match stream's content type)
    content_type: Optional[str] = None
