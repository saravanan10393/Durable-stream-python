"""
Durable Streams Protocol - Python Implementation

A Python implementation of the Durable Streams Protocol for creating,
appending to, and reading from durable, append-only byte streams.
"""

from .types import (
    StreamMessage,
    Stream,
    StreamLifecycleEvent,
    StreamLifecycleHook,
    ServerOptions,
    PendingLongPoll,
    ReadResult,
    WaitResult,
    CreateOptions,
    AppendOptions,
)
from .store import StreamStore
from .memory_store import MemoryStreamStore
from .sqlite_store import SQLiteStreamStore
from .server import DurableStreamServer

__version__ = "1.0.0"

__all__ = [
    # Types
    "StreamMessage",
    "Stream",
    "StreamLifecycleEvent",
    "StreamLifecycleHook",
    "ServerOptions",
    "PendingLongPoll",
    "ReadResult",
    "WaitResult",
    "CreateOptions",
    "AppendOptions",
    # Stores
    "StreamStore",
    "MemoryStreamStore",
    "SQLiteStreamStore",
    # Server
    "DurableStreamServer",
]
