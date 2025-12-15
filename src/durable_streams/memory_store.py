"""
In-memory stream storage implementation.

This is the default storage provider for testing and development.
Data is not persisted across restarts.
"""

import asyncio
import time
from typing import Optional
from copy import deepcopy

from .types import (
    Stream,
    StreamMessage,
    ReadResult,
    WaitResult,
    CreateOptions,
    AppendOptions,
    PendingLongPoll,
)
from .store import (
    StreamStore,
    StreamNotFoundError,
    StreamExistsError,
    SequenceConflictError,
    ContentTypeMismatchError,
    normalize_content_type,
)
from .json_utils import process_json_append, format_json_response


class MemoryStreamStore(StreamStore):
    """
    In-memory store for durable streams.

    This implementation stores all data in memory and is suitable for
    testing and development. For production use, consider SQLiteStreamStore.
    """

    def __init__(self):
        self._streams: dict[str, Stream] = {}
        self._pending_long_polls: list[PendingLongPoll] = []
        self._lock = asyncio.Lock()

    async def create(self, path: str, options: Optional[CreateOptions] = None) -> Stream:
        """Create a new stream or return existing if config matches."""
        options = options or CreateOptions()

        async with self._lock:
            existing = self._streams.get(path)
            if existing:
                # Check if config matches (idempotent create)
                provided_type = normalize_content_type(options.content_type) or "application/octet-stream"
                existing_type = normalize_content_type(existing.content_type) or "application/octet-stream"

                content_type_matches = provided_type == existing_type
                ttl_matches = options.ttl_seconds == existing.ttl_seconds
                expires_matches = options.expires_at == existing.expires_at

                if content_type_matches and ttl_matches and expires_matches:
                    # Idempotent success - return existing stream
                    return existing
                else:
                    # Config mismatch - conflict
                    raise StreamExistsError(
                        f"Stream already exists with different configuration: {path}"
                    )

            stream = Stream(
                path=path,
                content_type=options.content_type,
                messages=[],
                current_offset="0000000000000000_0000000000000000",
                ttl_seconds=options.ttl_seconds,
                expires_at=options.expires_at,
                created_at=int(time.time() * 1000),
            )

            # If initial data is provided, append it
            if options.initial_data and len(options.initial_data) > 0:
                self._append_to_stream(stream, options.initial_data)

            self._streams[path] = stream
            return stream

    async def get(self, path: str) -> Optional[Stream]:
        """Get a stream by path."""
        return self._streams.get(path)

    async def has(self, path: str) -> bool:
        """Check if a stream exists."""
        return path in self._streams

    async def delete(self, path: str) -> bool:
        """Delete a stream."""
        async with self._lock:
            # Cancel any pending long-polls for this stream
            await self._cancel_long_polls_for_stream(path)

            if path in self._streams:
                del self._streams[path]
                return True
            return False

    async def append(
        self, path: str, data: bytes, options: Optional[AppendOptions] = None
    ) -> StreamMessage:
        """Append data to a stream."""
        options = options or AppendOptions()

        async with self._lock:
            stream = self._streams.get(path)
            if not stream:
                raise StreamNotFoundError(f"Stream not found: {path}")

            # Check content type match using normalization
            if options.content_type and stream.content_type:
                provided_type = normalize_content_type(options.content_type)
                stream_type = normalize_content_type(stream.content_type)
                if provided_type != stream_type:
                    raise ContentTypeMismatchError(
                        f"Content-type mismatch: expected {stream.content_type}, got {options.content_type}"
                    )

            # Check sequence for writer coordination (lexicographic comparison)
            if options.seq is not None:
                if stream.last_seq is not None and options.seq <= stream.last_seq:
                    raise SequenceConflictError(
                        f"Sequence conflict: {options.seq} <= {stream.last_seq}"
                    )
                stream.last_seq = options.seq

            message = self._append_to_stream(stream, data)

        # Notify any pending long-polls (outside lock to avoid deadlock)
        await self._notify_long_polls(path)

        return message

    def _append_to_stream(self, stream: Stream, data: bytes) -> StreamMessage:
        """
        Internal method to append data to a stream.
        Must be called while holding the lock.
        """
        # Process JSON mode data (throws on invalid JSON or empty arrays)
        processed_data = data
        if normalize_content_type(stream.content_type) == "application/json":
            processed_data = process_json_append(data)

        # Parse current offset
        parts = stream.current_offset.split("_")
        read_seq = int(parts[0])
        byte_offset = int(parts[1])

        # Calculate new offset with zero-padding for lexicographic sorting
        new_byte_offset = byte_offset + len(processed_data)
        new_offset = f"{read_seq:016d}_{new_byte_offset:016d}"

        message = StreamMessage(
            data=processed_data,
            offset=new_offset,
            timestamp=int(time.time() * 1000),
        )

        stream.messages.append(message)
        stream.current_offset = new_offset

        return message

    async def read(self, path: str, offset: Optional[str] = None) -> ReadResult:
        """Read messages from a stream starting at the given offset."""
        stream = self._streams.get(path)
        if not stream:
            raise StreamNotFoundError(f"Stream not found: {path}")

        # No offset or -1 means start from beginning
        if not offset or offset == "-1":
            return ReadResult(
                messages=list(stream.messages),
                up_to_date=True,
            )

        # Find messages after the given offset using lexicographic comparison
        messages = [m for m in stream.messages if m.offset > offset]

        return ReadResult(
            messages=messages,
            up_to_date=True,
        )

    async def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        """Format messages for response."""
        stream = self._streams.get(path)
        if not stream:
            raise StreamNotFoundError(f"Stream not found: {path}")

        # Concatenate all message data
        concatenated = b"".join(m.data for m in messages)

        # For JSON mode, wrap in array brackets
        if normalize_content_type(stream.content_type) == "application/json":
            return format_json_response(concatenated)

        return concatenated

    async def wait_for_messages(
        self, path: str, offset: str, timeout_ms: int
    ) -> WaitResult:
        """Wait for new messages (long-poll)."""
        stream = self._streams.get(path)
        if not stream:
            raise StreamNotFoundError(f"Stream not found: {path}")

        # Check if there are already new messages
        result = await self.read(path, offset)
        if result.messages:
            return WaitResult(messages=result.messages, timed_out=False)

        # Create a future to wait for new messages
        loop = asyncio.get_event_loop()
        future: asyncio.Future[list[StreamMessage]] = loop.create_future()

        pending = PendingLongPoll(
            path=path,
            offset=offset,
            future=future,
        )

        # Create timeout handler
        async def timeout_handler():
            await asyncio.sleep(timeout_ms / 1000)
            if not future.done():
                self._remove_pending_long_poll(pending)
                future.set_result([])

        timeout_task = asyncio.create_task(timeout_handler())
        pending.timeout_handle = timeout_task

        async with self._lock:
            self._pending_long_polls.append(pending)

        try:
            messages = await future
            timeout_task.cancel()
            return WaitResult(
                messages=messages,
                timed_out=len(messages) == 0,
            )
        except asyncio.CancelledError:
            timeout_task.cancel()
            return WaitResult(messages=[], timed_out=True)

    async def get_current_offset(self, path: str) -> Optional[str]:
        """Get the current offset for a stream."""
        stream = self._streams.get(path)
        return stream.current_offset if stream else None

    async def clear(self) -> None:
        """Clear all streams."""
        async with self._lock:
            # Cancel all pending long-polls
            for pending in self._pending_long_polls:
                if pending.timeout_handle:
                    pending.timeout_handle.cancel()
                if not pending.future.done():
                    pending.future.set_result([])
            self._pending_long_polls = []
            self._streams.clear()

    async def cancel_all_waits(self) -> None:
        """Cancel all pending long-polls."""
        async with self._lock:
            for pending in self._pending_long_polls:
                if pending.timeout_handle:
                    pending.timeout_handle.cancel()
                if not pending.future.done():
                    pending.future.set_result([])
            self._pending_long_polls = []

    async def list_streams(self) -> list[str]:
        """Get all stream paths."""
        return list(self._streams.keys())

    async def _notify_long_polls(self, path: str) -> None:
        """Notify pending long-polls for a stream."""
        to_notify: list[PendingLongPoll] = []

        async with self._lock:
            to_notify = [p for p in self._pending_long_polls if p.path == path]

        for pending in to_notify:
            result = await self.read(path, pending.offset)
            if result.messages:
                self._remove_pending_long_poll(pending)
                if pending.timeout_handle:
                    pending.timeout_handle.cancel()
                if not pending.future.done():
                    pending.future.set_result(result.messages)

    async def _cancel_long_polls_for_stream(self, path: str) -> None:
        """Cancel pending long-polls for a specific stream."""
        to_cancel = [p for p in self._pending_long_polls if p.path == path]
        for pending in to_cancel:
            if pending.timeout_handle:
                pending.timeout_handle.cancel()
            if not pending.future.done():
                pending.future.set_result([])
            self._remove_pending_long_poll(pending)

    def _remove_pending_long_poll(self, pending: PendingLongPoll) -> None:
        """Remove a pending long-poll from the list."""
        try:
            self._pending_long_polls.remove(pending)
        except ValueError:
            pass  # Already removed
