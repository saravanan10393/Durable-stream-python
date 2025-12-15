"""
Abstract base class for stream storage providers.

This module defines the interface that all storage providers must implement.
The design allows for pluggable backends (in-memory, SQLite, etc.).
"""

from abc import ABC, abstractmethod
from typing import Optional

from .types import (
    Stream,
    StreamMessage,
    ReadResult,
    WaitResult,
    CreateOptions,
    AppendOptions,
)


class StreamStoreError(Exception):
    """Base exception for stream store errors."""

    pass


class StreamNotFoundError(StreamStoreError):
    """Raised when a stream is not found."""

    pass


class StreamExistsError(StreamStoreError):
    """Raised when a stream already exists with different configuration."""

    pass


class SequenceConflictError(StreamStoreError):
    """Raised when sequence validation fails."""

    pass


class ContentTypeMismatchError(StreamStoreError):
    """Raised when content type doesn't match."""

    pass


class InvalidJsonError(StreamStoreError):
    """Raised when JSON validation fails."""

    pass


class EmptyArrayError(StreamStoreError):
    """Raised when an empty JSON array is provided."""

    pass


def normalize_content_type(content_type: Optional[str]) -> str:
    """
    Normalize content-type by extracting the media type (before any semicolon).
    Handles cases like "application/json; charset=utf-8".
    """
    if not content_type:
        return ""
    return content_type.split(";")[0].strip().lower()


class StreamStore(ABC):
    """
    Abstract base class for durable stream storage.

    All storage providers must implement this interface to be compatible
    with the DurableStreamServer.
    """

    @abstractmethod
    async def create(self, path: str, options: Optional[CreateOptions] = None) -> Stream:
        """
        Create a new stream.

        Args:
            path: The stream URL path
            options: Optional creation options

        Returns:
            The created or existing stream

        Raises:
            StreamExistsError: If stream exists with different configuration
            InvalidJsonError: If initial data is invalid JSON (for JSON streams)
            EmptyArrayError: If initial data is an empty JSON array
        """
        pass

    @abstractmethod
    async def get(self, path: str) -> Optional[Stream]:
        """
        Get a stream by path.

        Args:
            path: The stream URL path

        Returns:
            The stream, or None if not found
        """
        pass

    @abstractmethod
    async def has(self, path: str) -> bool:
        """
        Check if a stream exists.

        Args:
            path: The stream URL path

        Returns:
            True if stream exists, False otherwise
        """
        pass

    @abstractmethod
    async def delete(self, path: str) -> bool:
        """
        Delete a stream.

        Args:
            path: The stream URL path

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def append(
        self, path: str, data: bytes, options: Optional[AppendOptions] = None
    ) -> StreamMessage:
        """
        Append data to a stream.

        Args:
            path: The stream URL path
            data: The bytes to append
            options: Optional append options

        Returns:
            The created message

        Raises:
            StreamNotFoundError: If stream doesn't exist
            SequenceConflictError: If sequence validation fails
            ContentTypeMismatchError: If content type doesn't match
            InvalidJsonError: If JSON validation fails (for JSON streams)
            EmptyArrayError: If data is an empty JSON array
        """
        pass

    @abstractmethod
    async def read(self, path: str, offset: Optional[str] = None) -> ReadResult:
        """
        Read messages from a stream starting at the given offset.

        Args:
            path: The stream URL path
            offset: Start offset token (None or "-1" means start from beginning)

        Returns:
            ReadResult containing messages and up-to-date status

        Raises:
            StreamNotFoundError: If stream doesn't exist
        """
        pass

    @abstractmethod
    async def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        """
        Format messages for HTTP response.

        For JSON mode, wraps concatenated data in array brackets.

        Args:
            path: The stream URL path
            messages: Messages to format

        Returns:
            Formatted bytes for response body

        Raises:
            StreamNotFoundError: If stream doesn't exist
        """
        pass

    @abstractmethod
    async def wait_for_messages(
        self, path: str, offset: str, timeout_ms: int
    ) -> WaitResult:
        """
        Wait for new messages (long-poll).

        Args:
            path: The stream URL path
            offset: Offset to wait from
            timeout_ms: Timeout in milliseconds

        Returns:
            WaitResult containing messages or timeout status

        Raises:
            StreamNotFoundError: If stream doesn't exist
        """
        pass

    @abstractmethod
    async def get_current_offset(self, path: str) -> Optional[str]:
        """
        Get the current offset for a stream.

        Args:
            path: The stream URL path

        Returns:
            Current offset, or None if stream doesn't exist
        """
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all streams."""
        pass

    @abstractmethod
    async def cancel_all_waits(self) -> None:
        """Cancel all pending long-polls (used during shutdown)."""
        pass

    @abstractmethod
    async def list_streams(self) -> list[str]:
        """
        Get all stream paths.

        Returns:
            List of stream paths
        """
        pass

    async def close(self) -> None:
        """
        Close the store and release resources.

        Override in subclasses that need cleanup.
        """
        pass
