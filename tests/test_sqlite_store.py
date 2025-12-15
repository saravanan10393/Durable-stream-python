"""
Tests for the SQLite stream store implementation.
"""

import asyncio
import os
import time
import tempfile
import pytest
import httpx

from durable_streams import DurableStreamServer, SQLiteStreamStore
from durable_streams.types import ServerOptions


# Protocol headers
STREAM_OFFSET_HEADER = "stream-next-offset"
STREAM_UP_TO_DATE_HEADER = "stream-up-to-date"


@pytest.fixture
async def server():
    """Create and start a test server with SQLite store."""
    # Use in-memory SQLite for testing
    store = SQLiteStreamStore(":memory:")
    options = ServerOptions(port=0, host="127.0.0.1", long_poll_timeout=5000)
    srv = DurableStreamServer(store=store, options=options)
    url = await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
def base_url(server):
    """Get the server base URL."""
    return server.url


class TestSQLiteBasicOperations:
    """Basic operations with SQLite store."""

    async def test_create_and_read_stream(self, base_url):
        stream_path = f"/v1/stream/sqlite-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"hello world",
            )
            assert response.status_code == 201

            # Read
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "hello world"

    async def test_append_and_read(self, base_url):
        stream_path = f"/v1/stream/sqlite-append-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )

            # Append multiple
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"chunk1",
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"chunk2",
            )

            # Read
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "chunk1chunk2"

    async def test_offset_based_reading(self, base_url):
        stream_path = f"/v1/stream/sqlite-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create and append first chunk
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"first",
            )

            # Get offset
            response1 = await client.get(f"{base_url}{stream_path}")
            offset = response1.headers.get(STREAM_OFFSET_HEADER)

            # Append second chunk
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"second",
            )

            # Read from offset
            response2 = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": offset},
            )
            assert response2.text == "second"

    async def test_json_mode(self, base_url):
        stream_path = f"/v1/stream/sqlite-json-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create JSON stream
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )

            # Append JSON
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b'[{"a": 1}, {"b": 2}]',
            )

            # Read
            response = await client.get(f"{base_url}{stream_path}")
            import json
            data = json.loads(response.text)
            assert len(data) == 2
            assert data[0]["a"] == 1
            assert data[1]["b"] == 2

    async def test_delete_stream(self, base_url):
        stream_path = f"/v1/stream/sqlite-delete-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )

            # Delete
            response = await client.delete(f"{base_url}{stream_path}")
            assert response.status_code == 204

            # Verify gone
            response = await client.get(f"{base_url}{stream_path}")
            assert response.status_code == 404


class TestSQLitePersistence:
    """Test SQLite persistence capabilities."""

    async def test_file_based_persistence(self):
        """Test that data persists to file."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # Create server and add data
            store1 = SQLiteStreamStore(db_path)
            options = ServerOptions(port=0, host="127.0.0.1")
            server1 = DurableStreamServer(store=store1, options=options)
            url1 = await server1.start()

            stream_path = "/v1/stream/persistence-test"
            async with httpx.AsyncClient() as client:
                await client.put(
                    f"{url1}{stream_path}",
                    headers={"Content-Type": "text/plain"},
                    content=b"persistent data",
                )

            await server1.stop()

            # Create new server with same database
            store2 = SQLiteStreamStore(db_path)
            server2 = DurableStreamServer(store=store2, options=ServerOptions(port=0))
            url2 = await server2.start()

            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url2}{stream_path}")
                assert response.text == "persistent data"

            await server2.stop()

        finally:
            # Cleanup
            try:
                os.unlink(db_path)
                os.unlink(f"{db_path}-wal")
                os.unlink(f"{db_path}-shm")
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
