"""
SQLite-based stream storage implementation.

This provider persists stream data to a SQLite database, making it suitable
for production use where data durability is required.
"""

import asyncio
import sqlite3
import time
import threading
from pathlib import Path
from typing import Optional

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


class SQLiteStreamStore(StreamStore):
    """
    SQLite-based store for durable streams.

    This implementation persists all data to a SQLite database file,
    providing durability across server restarts.
    """

    def __init__(self, db_path: str = "durable_streams.db"):
        """
        Initialize the SQLite store.

        Args:
            db_path: Path to the SQLite database file.
                     Use ":memory:" for an in-memory database (for testing).
        """
        self._db_path = db_path
        self._pending_long_polls: list[PendingLongPoll] = []
        self._lock = asyncio.Lock()
        self._db_lock = threading.Lock()  # For thread-safe DB access

        # For in-memory database, we need to keep a single connection alive
        # because each new connection to :memory: creates a new database
        self._is_memory = db_path == ":memory:"
        self._persistent_conn: Optional[sqlite3.Connection] = None

        # Initialize database schema
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with proper settings."""
        if self._is_memory:
            # For in-memory database, always use the persistent connection
            if self._persistent_conn is None:
                self._persistent_conn = sqlite3.connect(
                    ":memory:", check_same_thread=False
                )
                self._persistent_conn.row_factory = sqlite3.Row
            return self._persistent_conn
        else:
            # For file-based database, create a new connection
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            return conn

    def _release_connection(self, conn: sqlite3.Connection) -> None:
        """Release a connection (only closes file-based connections)."""
        if not self._is_memory:
            conn.close()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._db_lock:
            conn = self._get_connection()
            try:
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS streams (
                        path TEXT PRIMARY KEY,
                        content_type TEXT,
                        current_offset TEXT NOT NULL DEFAULT '0000000000000000_0000000000000000',
                        last_seq TEXT,
                        ttl_seconds INTEGER,
                        expires_at TEXT,
                        created_at INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stream_path TEXT NOT NULL,
                        data BLOB NOT NULL,
                        offset TEXT NOT NULL,
                        timestamp INTEGER NOT NULL,
                        FOREIGN KEY (stream_path) REFERENCES streams(path) ON DELETE CASCADE
                    );

                    CREATE INDEX IF NOT EXISTS idx_messages_stream_offset
                    ON messages(stream_path, offset);
                """)
                conn.commit()
            finally:
                self._release_connection(conn)

    async def create(self, path: str, options: Optional[CreateOptions] = None) -> Stream:
        """Create a new stream or return existing if config matches."""
        options = options or CreateOptions()

        async with self._lock:
            with self._db_lock:
                conn = self._get_connection()
                try:
                    cursor = conn.execute(
                        "SELECT * FROM streams WHERE path = ?", (path,)
                    )
                    row = cursor.fetchone()

                    if row:
                        # Check if config matches (idempotent create)
                        provided_type = normalize_content_type(options.content_type) or "application/octet-stream"
                        existing_type = normalize_content_type(row["content_type"]) or "application/octet-stream"

                        content_type_matches = provided_type == existing_type
                        ttl_matches = options.ttl_seconds == row["ttl_seconds"]
                        expires_matches = options.expires_at == row["expires_at"]

                        if content_type_matches and ttl_matches and expires_matches:
                            # Idempotent success - return existing stream
                            return self._row_to_stream(row, conn)
                        else:
                            raise StreamExistsError(
                                f"Stream already exists with different configuration: {path}"
                            )

                    # Create new stream
                    created_at = int(time.time() * 1000)
                    current_offset = "0000000000000000_0000000000000000"

                    conn.execute(
                        """INSERT INTO streams
                           (path, content_type, current_offset, ttl_seconds, expires_at, created_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (path, options.content_type, current_offset, options.ttl_seconds,
                         options.expires_at, created_at)
                    )
                    conn.commit()

                    stream = Stream(
                        path=path,
                        content_type=options.content_type,
                        messages=[],
                        current_offset=current_offset,
                        ttl_seconds=options.ttl_seconds,
                        expires_at=options.expires_at,
                        created_at=created_at,
                    )

                    # If initial data is provided, append it
                    if options.initial_data and len(options.initial_data) > 0:
                        self._append_to_stream_sync(conn, stream, options.initial_data)

                    return stream
                finally:
                    self._release_connection(conn)

    def _row_to_stream(self, row: sqlite3.Row, conn: sqlite3.Connection) -> Stream:
        """Convert a database row to a Stream object."""
        # Load messages for this stream
        cursor = conn.execute(
            "SELECT * FROM messages WHERE stream_path = ? ORDER BY offset",
            (row["path"],)
        )
        messages = [
            StreamMessage(
                data=bytes(msg_row["data"]),
                offset=msg_row["offset"],
                timestamp=msg_row["timestamp"],
            )
            for msg_row in cursor.fetchall()
        ]

        return Stream(
            path=row["path"],
            content_type=row["content_type"],
            messages=messages,
            current_offset=row["current_offset"],
            last_seq=row["last_seq"],
            ttl_seconds=row["ttl_seconds"],
            expires_at=row["expires_at"],
            created_at=row["created_at"],
        )

    async def get(self, path: str) -> Optional[Stream]:
        """Get a stream by path."""
        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT * FROM streams WHERE path = ?", (path,)
                )
                row = cursor.fetchone()
                if row:
                    return self._row_to_stream(row, conn)
                return None
            finally:
                self._release_connection(conn)

    async def has(self, path: str) -> bool:
        """Check if a stream exists."""
        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT 1 FROM streams WHERE path = ?", (path,)
                )
                return cursor.fetchone() is not None
            finally:
                self._release_connection(conn)

    async def delete(self, path: str) -> bool:
        """Delete a stream."""
        async with self._lock:
            # Cancel any pending long-polls for this stream
            await self._cancel_long_polls_for_stream(path)

            with self._db_lock:
                conn = self._get_connection()
                try:
                    cursor = conn.execute(
                        "DELETE FROM streams WHERE path = ?", (path,)
                    )
                    # Also delete associated messages
                    conn.execute(
                        "DELETE FROM messages WHERE stream_path = ?", (path,)
                    )
                    conn.commit()
                    return cursor.rowcount > 0
                finally:
                    self._release_connection(conn)

    async def append(
        self, path: str, data: bytes, options: Optional[AppendOptions] = None
    ) -> StreamMessage:
        """Append data to a stream."""
        options = options or AppendOptions()

        async with self._lock:
            with self._db_lock:
                conn = self._get_connection()
                try:
                    cursor = conn.execute(
                        "SELECT * FROM streams WHERE path = ?", (path,)
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise StreamNotFoundError(f"Stream not found: {path}")

                    # Check content type match using normalization
                    if options.content_type and row["content_type"]:
                        provided_type = normalize_content_type(options.content_type)
                        stream_type = normalize_content_type(row["content_type"])
                        if provided_type != stream_type:
                            raise ContentTypeMismatchError(
                                f"Content-type mismatch: expected {row['content_type']}, got {options.content_type}"
                            )

                    # Check sequence for writer coordination (lexicographic comparison)
                    if options.seq is not None:
                        last_seq = row["last_seq"]
                        if last_seq is not None and options.seq <= last_seq:
                            raise SequenceConflictError(
                                f"Sequence conflict: {options.seq} <= {last_seq}"
                            )
                        conn.execute(
                            "UPDATE streams SET last_seq = ? WHERE path = ?",
                            (options.seq, path)
                        )

                    # Create stream object for append
                    stream = Stream(
                        path=path,
                        content_type=row["content_type"],
                        current_offset=row["current_offset"],
                    )

                    message = self._append_to_stream_sync(conn, stream, data)
                    return message
                finally:
                    self._release_connection(conn)

        # Notify any pending long-polls (outside lock)
        await self._notify_long_polls(path)

    def _append_to_stream_sync(
        self, conn: sqlite3.Connection, stream: Stream, data: bytes
    ) -> StreamMessage:
        """
        Internal method to append data to a stream.
        Must be called while holding the db lock.
        """
        # Process JSON mode data
        processed_data = data
        if normalize_content_type(stream.content_type) == "application/json":
            processed_data = process_json_append(data)

        # Parse current offset
        parts = stream.current_offset.split("_")
        read_seq = int(parts[0])
        byte_offset = int(parts[1])

        # Calculate new offset
        new_byte_offset = byte_offset + len(processed_data)
        new_offset = f"{read_seq:016d}_{new_byte_offset:016d}"

        timestamp = int(time.time() * 1000)

        # Insert message
        conn.execute(
            """INSERT INTO messages (stream_path, data, offset, timestamp)
               VALUES (?, ?, ?, ?)""",
            (stream.path, processed_data, new_offset, timestamp)
        )

        # Update stream's current offset
        conn.execute(
            "UPDATE streams SET current_offset = ? WHERE path = ?",
            (new_offset, stream.path)
        )

        conn.commit()

        message = StreamMessage(
            data=processed_data,
            offset=new_offset,
            timestamp=timestamp,
        )

        stream.current_offset = new_offset
        return message

    async def read(self, path: str, offset: Optional[str] = None) -> ReadResult:
        """Read messages from a stream starting at the given offset."""
        with self._db_lock:
            conn = self._get_connection()
            try:
                # Check if stream exists
                cursor = conn.execute(
                    "SELECT 1 FROM streams WHERE path = ?", (path,)
                )
                if not cursor.fetchone():
                    raise StreamNotFoundError(f"Stream not found: {path}")

                # No offset or -1 means start from beginning
                if not offset or offset == "-1":
                    cursor = conn.execute(
                        "SELECT * FROM messages WHERE stream_path = ? ORDER BY offset",
                        (path,)
                    )
                else:
                    # Find messages after the given offset (lexicographic comparison)
                    cursor = conn.execute(
                        "SELECT * FROM messages WHERE stream_path = ? AND offset > ? ORDER BY offset",
                        (path, offset)
                    )

                messages = [
                    StreamMessage(
                        data=bytes(row["data"]),
                        offset=row["offset"],
                        timestamp=row["timestamp"],
                    )
                    for row in cursor.fetchall()
                ]

                return ReadResult(messages=messages, up_to_date=True)
            finally:
                self._release_connection(conn)

    async def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        """Format messages for response."""
        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT content_type FROM streams WHERE path = ?", (path,)
                )
                row = cursor.fetchone()
                if not row:
                    raise StreamNotFoundError(f"Stream not found: {path}")

                content_type = row["content_type"]
            finally:
                self._release_connection(conn)

        # Concatenate all message data
        concatenated = b"".join(m.data for m in messages)

        # For JSON mode, wrap in array brackets
        if normalize_content_type(content_type) == "application/json":
            return format_json_response(concatenated)

        return concatenated

    async def wait_for_messages(
        self, path: str, offset: str, timeout_ms: int
    ) -> WaitResult:
        """Wait for new messages (long-poll)."""
        # Check if stream exists
        if not await self.has(path):
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
        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT current_offset FROM streams WHERE path = ?", (path,)
                )
                row = cursor.fetchone()
                return row["current_offset"] if row else None
            finally:
                self._release_connection(conn)

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

            with self._db_lock:
                conn = self._get_connection()
                try:
                    conn.execute("DELETE FROM messages")
                    conn.execute("DELETE FROM streams")
                    conn.commit()
                finally:
                    self._release_connection(conn)

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
        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT path FROM streams")
                return [row["path"] for row in cursor.fetchall()]
            finally:
                self._release_connection(conn)

    async def close(self) -> None:
        """Close the store and release resources."""
        await self.cancel_all_waits()
        # Close persistent connection for in-memory database
        if self._persistent_conn is not None:
            self._persistent_conn.close()
            self._persistent_conn = None

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
