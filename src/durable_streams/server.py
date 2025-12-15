"""
HTTP server for the Durable Streams Protocol.

This module implements a complete HTTP server that handles all protocol
operations including create, append, read, delete, and metadata queries.
It supports both long-polling and Server-Sent Events (SSE) for live tailing.
"""

import asyncio
import base64
import json
import re
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qs, urlparse

from aiohttp import web

from .types import ServerOptions, StreamLifecycleEvent
from .store import (
    StreamStore,
    StreamNotFoundError,
    StreamExistsError,
    SequenceConflictError,
    ContentTypeMismatchError,
    InvalidJsonError,
    EmptyArrayError,
    normalize_content_type,
)
from .memory_store import MemoryStreamStore
from .types import CreateOptions, AppendOptions


# Protocol headers (aligned with PROTOCOL.md)
STREAM_OFFSET_HEADER = "Stream-Next-Offset"
STREAM_CURSOR_HEADER = "Stream-Cursor"
STREAM_UP_TO_DATE_HEADER = "Stream-Up-To-Date"
STREAM_SEQ_HEADER = "Stream-Seq"
STREAM_TTL_HEADER = "Stream-TTL"
STREAM_EXPIRES_AT_HEADER = "Stream-Expires-At"

# SSE control event fields (Protocol Section 5.7)
SSE_OFFSET_FIELD = "streamNextOffset"
SSE_CURSOR_FIELD = "streamCursor"

# Query params
OFFSET_QUERY_PARAM = "offset"
LIVE_QUERY_PARAM = "live"
CURSOR_QUERY_PARAM = "cursor"

# Valid offset pattern: "-1" or digits_digits
VALID_OFFSET_PATTERN = re.compile(r"^(-1|\d+_\d+)$")

# Valid TTL pattern: must be 0 or positive integer without leading zeros
VALID_TTL_PATTERN = re.compile(r"^(0|[1-9]\d*)$")


def encode_sse_data(payload: str) -> str:
    """
    Encode data for SSE format.
    Per SSE spec, each line in the payload needs its own "data:" prefix.
    Newlines in the payload become separate data: lines.
    """
    lines = payload.split("\n")
    return "\n".join(f"data: {line}" for line in lines) + "\n\n"


class DurableStreamServer:
    """
    HTTP server for the Durable Streams Protocol.

    Supports both in-memory and pluggable storage modes.
    """

    def __init__(
        self,
        store: Optional[StreamStore] = None,
        options: Optional[ServerOptions] = None,
    ):
        """
        Initialize the server.

        Args:
            store: The storage backend to use. Defaults to MemoryStreamStore.
            options: Server configuration options.
        """
        self.store = store or MemoryStreamStore()
        self.options = options or ServerOptions()
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._url: Optional[str] = None
        self._active_sse_responses: set[web.StreamResponse] = set()
        self._is_shutting_down = False

    async def start(self) -> str:
        """
        Start the server.

        Returns:
            The server URL.
        """
        if self._app:
            raise RuntimeError("Server already started")

        # Set large client_max_size for protocol compliance (handles large payloads)
        # Default is 1MB, but protocol tests send up to 50MB+ payloads
        self._app = web.Application(client_max_size=100 * 1024 * 1024)  # 100MB
        self._app.router.add_route("*", "/{path:.*}", self._handle_request)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(
            self._runner,
            self.options.host,
            self.options.port,
        )
        await self._site.start()

        # Get the actual port (in case 0 was specified)
        actual_port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://{self.options.host}:{actual_port}"

        return self._url

    async def stop(self) -> None:
        """Stop the server."""
        if not self._app:
            return

        # Mark as shutting down
        self._is_shutting_down = True

        # Cancel all pending long-polls and SSE waits
        await self.store.cancel_all_waits()

        # Close all active SSE connections
        for response in list(self._active_sse_responses):
            try:
                await response.write_eof()
            except Exception:
                pass  # Connection may already be closed
        self._active_sse_responses.clear()

        # Close the store
        await self.store.close()

        # Stop the server
        if self._runner:
            await self._runner.cleanup()

        self._app = None
        self._runner = None
        self._site = None
        self._url = None
        self._is_shutting_down = False

    @property
    def url(self) -> str:
        """Get the server URL."""
        if not self._url:
            raise RuntimeError("Server not started")
        return self._url

    async def clear(self) -> None:
        """Clear all streams."""
        await self.store.clear()

    # ============================================================================
    # Request handling
    # ============================================================================

    async def _handle_request(self, request: web.Request) -> web.Response:
        """Main request handler."""
        method = request.method.upper()

        # Set CORS headers
        cors_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "content-type, authorization, Stream-Seq, Stream-TTL, Stream-Expires-At",
            "Access-Control-Expose-Headers": "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, etag, content-type",
        }

        # Handle CORS preflight
        if method == "OPTIONS":
            return web.Response(status=204, headers=cors_headers)

        try:
            if method == "PUT":
                response = await self._handle_create(request)
            elif method == "HEAD":
                response = await self._handle_head(request)
            elif method == "GET":
                response = await self._handle_read(request)
            elif method == "POST":
                response = await self._handle_append(request)
            elif method == "DELETE":
                response = await self._handle_delete(request)
            else:
                response = web.Response(
                    status=405,
                    text="Method not allowed",
                    content_type="text/plain",
                )

            # Add CORS headers to response
            for key, value in cors_headers.items():
                response.headers[key] = value

            return response

        except StreamNotFoundError:
            return web.Response(
                status=404,
                text="Stream not found",
                content_type="text/plain",
                headers=cors_headers,
            )
        except StreamExistsError:
            return web.Response(
                status=409,
                text="Stream already exists with different configuration",
                content_type="text/plain",
                headers=cors_headers,
            )
        except SequenceConflictError:
            return web.Response(
                status=409,
                text="Sequence conflict",
                content_type="text/plain",
                headers=cors_headers,
            )
        except ContentTypeMismatchError:
            return web.Response(
                status=400,
                text="Content-type mismatch",
                content_type="text/plain",
                headers=cors_headers,
            )
        except InvalidJsonError:
            return web.Response(
                status=400,
                text="Invalid JSON",
                content_type="text/plain",
                headers=cors_headers,
            )
        except EmptyArrayError:
            return web.Response(
                status=400,
                text="Empty arrays are not allowed",
                content_type="text/plain",
                headers=cors_headers,
            )
        except Exception as e:
            return web.Response(
                status=500,
                text=f"Internal server error: {e}",
                content_type="text/plain",
                headers=cors_headers,
            )

    async def _handle_create(self, request: web.Request) -> web.Response:
        """Handle PUT - create stream."""
        path = "/" + request.match_info["path"]
        content_type = request.headers.get("Content-Type")

        # Sanitize content-type: if empty or invalid, use default
        if not content_type or not content_type.strip() or not re.match(r"^[\w-]+/[\w-]+", content_type):
            content_type = "application/octet-stream"

        ttl_header = request.headers.get(STREAM_TTL_HEADER)
        expires_at_header = request.headers.get(STREAM_EXPIRES_AT_HEADER)

        # Validate TTL and Expires-At headers
        if ttl_header and expires_at_header:
            return web.Response(
                status=400,
                text="Cannot specify both Stream-TTL and Stream-Expires-At",
                content_type="text/plain",
            )

        ttl_seconds: Optional[int] = None
        if ttl_header:
            # Strict TTL validation
            if not VALID_TTL_PATTERN.match(ttl_header):
                return web.Response(
                    status=400,
                    text="Invalid Stream-TTL value",
                    content_type="text/plain",
                )
            ttl_seconds = int(ttl_header)

        # Validate Expires-At timestamp format (ISO 8601)
        if expires_at_header:
            try:
                datetime.fromisoformat(expires_at_header.replace("Z", "+00:00"))
            except ValueError:
                return web.Response(
                    status=400,
                    text="Invalid Stream-Expires-At timestamp",
                    content_type="text/plain",
                )

        # Read body if present
        body = await request.read()

        is_new = not await self.store.has(path)

        options = CreateOptions(
            content_type=content_type,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at_header,
            initial_data=body if len(body) > 0 else None,
        )

        stream = await self.store.create(path, options)

        # Call lifecycle hook for new streams
        if is_new and self.options.on_stream_created:
            event = StreamLifecycleEvent(
                type="created",
                path=path,
                content_type=content_type,
                timestamp=stream.created_at,
            )
            result = self.options.on_stream_created(event)
            if asyncio.iscoroutine(result):
                await result

        # Build response headers
        headers = {
            "Content-Type": content_type,
            STREAM_OFFSET_HEADER: stream.current_offset,
        }

        # Add Location header for 201 Created responses
        if is_new:
            headers["Location"] = f"{self._url}{path}"

        return web.Response(
            status=201 if is_new else 200,
            headers=headers,
        )

    async def _handle_head(self, request: web.Request) -> web.Response:
        """Handle HEAD - get metadata."""
        path = "/" + request.match_info["path"]

        stream = await self.store.get(path)
        if not stream:
            return web.Response(status=404, content_type="text/plain")

        headers = {
            STREAM_OFFSET_HEADER: stream.current_offset,
        }

        if stream.content_type:
            headers["Content-Type"] = stream.content_type

        # Generate ETag: base64(path):offset
        path_b64 = base64.b64encode(path.encode()).decode()
        headers["ETag"] = f'"{path_b64}:{stream.current_offset}"'

        return web.Response(status=200, headers=headers)

    async def _handle_read(self, request: web.Request) -> web.Response:
        """Handle GET - read data."""
        path = "/" + request.match_info["path"]

        stream = await self.store.get(path)
        if not stream:
            return web.Response(
                status=404,
                text="Stream not found",
                content_type="text/plain",
            )

        # Parse query parameters
        offset = request.query.get(OFFSET_QUERY_PARAM)
        live = request.query.get(LIVE_QUERY_PARAM)
        cursor = request.query.get(CURSOR_QUERY_PARAM)

        # Validate offset parameter
        if offset is not None:
            # Reject empty offset
            if offset == "":
                return web.Response(
                    status=400,
                    text="Empty offset parameter",
                    content_type="text/plain",
                )

            # Reject multiple offset parameters
            all_offsets = request.query.getall(OFFSET_QUERY_PARAM)
            if len(all_offsets) > 1:
                return web.Response(
                    status=400,
                    text="Multiple offset parameters not allowed",
                    content_type="text/plain",
                )

            # Validate offset format
            if not VALID_OFFSET_PATTERN.match(offset):
                return web.Response(
                    status=400,
                    text="Invalid offset format",
                    content_type="text/plain",
                )

        # Require offset parameter for long-poll and SSE per protocol spec
        if (live == "long-poll" or live == "sse") and not offset:
            mode = "SSE" if live == "sse" else "Long-poll"
            return web.Response(
                status=400,
                text=f"{mode} requires offset parameter",
                content_type="text/plain",
            )

        # Handle SSE mode
        if live == "sse":
            return await self._handle_sse(request, path, stream, offset, cursor)

        # Read current messages
        result = await self.store.read(path, offset)
        messages = result.messages
        up_to_date = result.up_to_date

        # Only wait in long-poll if:
        # 1. long-poll mode is enabled
        # 2. Client provided an offset (not first request)
        # 3. Client's offset matches current offset (already caught up)
        # 4. No new messages
        client_is_caught_up = offset and offset == stream.current_offset
        if live == "long-poll" and client_is_caught_up and len(messages) == 0:
            wait_result = await self.store.wait_for_messages(
                path, offset, self.options.long_poll_timeout
            )

            if wait_result.timed_out:
                # Return 204 No Content on timeout
                return web.Response(
                    status=204,
                    headers={STREAM_OFFSET_HEADER: offset},
                )

            messages = wait_result.messages
            up_to_date = True

        # Build response headers
        headers = {}

        if stream.content_type:
            headers["Content-Type"] = stream.content_type

        # Set offset header to the last message's offset, or current if no messages
        last_message = messages[-1] if messages else None
        headers[STREAM_OFFSET_HEADER] = (
            last_message.offset if last_message else stream.current_offset
        )

        # Echo cursor if provided
        if cursor:
            headers[STREAM_CURSOR_HEADER] = cursor

        # Set up-to-date header
        if up_to_date:
            headers[STREAM_UP_TO_DATE_HEADER] = "true"

        # Generate ETag
        start_offset = offset or "0000000000000000_0000000000000000"
        end_offset = headers[STREAM_OFFSET_HEADER]
        path_b64 = base64.b64encode(path.encode()).decode()
        headers["ETag"] = f'"{path_b64}:{start_offset}:{end_offset}"'

        # Format response
        response_data = await self.store.format_response(path, messages)

        return web.Response(
            status=200,
            body=response_data,
            headers=headers,
        )

    async def _handle_sse(
        self,
        request: web.Request,
        path: str,
        stream,
        initial_offset: str,
        cursor: Optional[str],
    ) -> web.StreamResponse:
        """Handle SSE (Server-Sent Events) mode."""
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
            },
        )

        await response.prepare(request)

        # Track this SSE connection
        self._active_sse_responses.add(response)

        current_offset = initial_offset
        is_connected = True

        # Get content type for formatting
        is_json_stream = stream.content_type and normalize_content_type(stream.content_type) == "application/json"

        try:
            while is_connected and not self._is_shutting_down:
                # Refresh stream state
                stream = await self.store.get(path)
                if not stream:
                    break

                # Read current messages from offset
                result = await self.store.read(path, current_offset)

                # Send data events for each message
                for message in result.messages:
                    # Format data based on content type
                    if is_json_stream:
                        formatted = await self.store.format_response(path, [message])
                        data_payload = formatted.decode("utf-8")
                    else:
                        data_payload = message.data.decode("utf-8", errors="replace")

                    # Send data event
                    await response.write(f"event: data\n".encode())
                    await response.write(encode_sse_data(data_payload).encode())
                    await response.drain()

                    current_offset = message.offset

                # Compute control offset
                control_offset = (
                    result.messages[-1].offset
                    if result.messages
                    else stream.current_offset
                )

                # Send control event
                control_data = {SSE_OFFSET_FIELD: control_offset}
                if cursor:
                    control_data[SSE_CURSOR_FIELD] = cursor

                await response.write(f"event: control\n".encode())
                await response.write(
                    encode_sse_data(json.dumps(control_data)).encode()
                )

                # Drain to ensure data is sent to client before waiting
                await response.drain()

                # Update current_offset
                current_offset = control_offset

                # If caught up, wait for new messages
                if result.up_to_date:
                    wait_result = await self.store.wait_for_messages(
                        path, current_offset, self.options.long_poll_timeout
                    )

                    if self._is_shutting_down:
                        break

                    if wait_result.timed_out:
                        # Send keep-alive control event
                        keep_alive_data = {SSE_OFFSET_FIELD: current_offset}
                        if cursor:
                            keep_alive_data[SSE_CURSOR_FIELD] = cursor
                        await response.write(f"event: control\n".encode())
                        await response.write(
                            encode_sse_data(json.dumps(keep_alive_data)).encode()
                        )
                        await response.drain()

        except Exception:
            pass  # Connection closed
        finally:
            self._active_sse_responses.discard(response)

        await response.write_eof()
        return response

    async def _handle_append(self, request: web.Request) -> web.Response:
        """Handle POST - append data."""
        path = "/" + request.match_info["path"]
        content_type = request.headers.get("Content-Type")
        seq = request.headers.get(STREAM_SEQ_HEADER)

        body = await request.read()

        if len(body) == 0:
            return web.Response(
                status=400,
                text="Empty body",
                content_type="text/plain",
            )

        # Content-Type is required per protocol
        if not content_type or not content_type.strip():
            return web.Response(
                status=400,
                text="Content-Type header is required",
                content_type="text/plain",
            )

        options = AppendOptions(
            seq=seq,
            content_type=content_type,
        )

        message = await self.store.append(path, body, options)

        return web.Response(
            status=200,
            headers={STREAM_OFFSET_HEADER: message.offset},
        )

    async def _handle_delete(self, request: web.Request) -> web.Response:
        """Handle DELETE - delete stream."""
        path = "/" + request.match_info["path"]

        if not await self.store.has(path):
            return web.Response(
                status=404,
                text="Stream not found",
                content_type="text/plain",
            )

        await self.store.delete(path)

        # Call lifecycle hook
        if self.options.on_stream_deleted:
            import time
            event = StreamLifecycleEvent(
                type="deleted",
                path=path,
                timestamp=int(time.time() * 1000),
            )
            result = self.options.on_stream_deleted(event)
            if asyncio.iscoroutine(result):
                await result

        return web.Response(status=204)
