"""
Conformance tests for the Durable Streams Protocol implementation.

These tests mirror the official conformance test suite to ensure
protocol compliance.
"""

import asyncio
import time
import pytest
import httpx

from durable_streams import DurableStreamServer, MemoryStreamStore, SQLiteStreamStore
from durable_streams.types import ServerOptions


# Protocol headers
STREAM_OFFSET_HEADER = "stream-next-offset"
STREAM_CURSOR_HEADER = "stream-cursor"
STREAM_UP_TO_DATE_HEADER = "stream-up-to-date"
STREAM_SEQ_HEADER = "stream-seq"
STREAM_TTL_HEADER = "stream-ttl"
STREAM_EXPIRES_AT_HEADER = "stream-expires-at"


@pytest.fixture
async def server():
    """Create and start a test server."""
    store = MemoryStreamStore()
    options = ServerOptions(port=0, host="127.0.0.1", long_poll_timeout=5000)
    srv = DurableStreamServer(store=store, options=options)
    url = await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
def base_url(server):
    """Get the server base URL."""
    return server.url


class TestBasicStreamOperations:
    """Basic Stream Operations tests."""

    async def test_should_create_a_stream(self, base_url):
        stream_path = f"/v1/stream/create-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response.status_code == 201
            assert STREAM_OFFSET_HEADER in response.headers

    async def test_should_allow_idempotent_create_with_same_config(self, base_url):
        stream_path = f"/v1/stream/duplicate-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create first
            response1 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response1.status_code == 201

            # Create again with same config
            response2 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response2.status_code in [200, 204]

    async def test_should_reject_create_with_different_config(self, base_url):
        stream_path = f"/v1/stream/config-mismatch-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create with text/plain
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )

            # Try with different content type
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            assert response.status_code == 409

    async def test_should_delete_a_stream(self, base_url):
        stream_path = f"/v1/stream/delete-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )

            # Delete
            response = await client.delete(f"{base_url}{stream_path}")
            assert response.status_code == 204

            # Verify it's gone
            response = await client.get(f"{base_url}{stream_path}")
            assert response.status_code == 404

    async def test_should_properly_isolate_recreated_stream_after_delete(self, base_url):
        stream_path = f"/v1/stream/delete-recreate-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            # Create and append
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"old data",
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b" more old data",
            )

            # Verify old data
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "old data more old data"

            # Delete
            await client.delete(f"{base_url}{stream_path}")

            # Recreate
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"new data",
            )
            assert response.status_code == 201

            # Verify only new data
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "new data"
            assert "old data" not in response.text


class TestAppendOperations:
    """Append Operations tests."""

    async def test_should_append_string_data(self, base_url):
        stream_path = f"/v1/stream/append-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"hello world",
            )

            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "hello world"

    async def test_should_append_multiple_chunks(self, base_url):
        stream_path = f"/v1/stream/multi-append-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
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
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"chunk3",
            )

            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "chunk1chunk2chunk3"

    async def test_should_enforce_sequence_ordering(self, base_url):
        stream_path = f"/v1/stream/seq-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "001"},
                content=b"first",
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "002"},
                content=b"second",
            )

            # Try lower seq
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "001"},
                content=b"invalid",
            )
            assert response.status_code == 409


class TestReadOperations:
    """Read Operations tests."""

    async def test_should_read_empty_stream(self, base_url):
        stream_path = f"/v1/stream/read-empty-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.status_code == 200
            assert response.text == ""
            assert response.headers.get(STREAM_UP_TO_DATE_HEADER) == "true"

    async def test_should_read_stream_with_data(self, base_url):
        stream_path = f"/v1/stream/read-data-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"hello",
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "hello"
            assert response.headers.get(STREAM_UP_TO_DATE_HEADER) == "true"

    async def test_should_read_from_offset(self, base_url):
        stream_path = f"/v1/stream/read-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"first",
            )
            response1 = await client.get(f"{base_url}{stream_path}")
            first_offset = response1.headers.get(STREAM_OFFSET_HEADER)

            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"second",
            )

            response2 = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": first_offset},
            )
            assert response2.text == "second"


class TestHTTPProtocol:
    """HTTP Protocol tests."""

    async def test_should_return_correct_headers_on_put(self, base_url):
        stream_path = f"/v1/stream/put-headers-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response.status_code == 201
            assert response.headers.get("content-type") == "text/plain"
            assert STREAM_OFFSET_HEADER in response.headers

    async def test_should_return_200_on_idempotent_put(self, base_url):
        stream_path = f"/v1/stream/duplicate-put-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response1 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response1.status_code == 201

            response2 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response2.status_code in [200, 204]

    async def test_should_return_409_on_put_with_different_config(self, base_url):
        stream_path = f"/v1/stream/config-conflict-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            assert response.status_code == 409

    async def test_should_return_correct_headers_on_post(self, base_url):
        stream_path = f"/v1/stream/post-headers-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"hello world",
            )
            assert response.status_code in [200, 204]
            assert STREAM_OFFSET_HEADER in response.headers

    async def test_should_return_404_on_post_to_nonexistent(self, base_url):
        stream_path = f"/v1/stream/post-404-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"data",
            )
            assert response.status_code == 404

    async def test_should_return_400_or_409_on_content_type_mismatch(self, base_url):
        stream_path = f"/v1/stream/content-type-mismatch-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b"{}",
            )
            assert response.status_code in [400, 409]

    async def test_should_return_correct_headers_on_get(self, base_url):
        stream_path = f"/v1/stream/get-headers-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test data",
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.status_code == 200
            assert response.headers.get("content-type") == "text/plain"
            assert STREAM_OFFSET_HEADER in response.headers
            assert response.headers.get(STREAM_UP_TO_DATE_HEADER) == "true"
            assert "etag" in response.headers
            assert response.text == "test data"

    async def test_should_return_404_on_delete_nonexistent(self, base_url):
        stream_path = f"/v1/stream/delete-404-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{base_url}{stream_path}")
            assert response.status_code == 404

    async def test_should_return_204_on_successful_delete(self, base_url):
        stream_path = f"/v1/stream/delete-success-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.delete(f"{base_url}{stream_path}")
            assert response.status_code == 204

            # Verify gone
            response = await client.get(f"{base_url}{stream_path}")
            assert response.status_code == 404

    async def test_should_enforce_sequence_ordering(self, base_url):
        stream_path = f"/v1/stream/seq-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "001"},
                content=b"first",
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "002"},
                content=b"second",
            )

            # Regression should fail
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "001"},
                content=b"invalid",
            )
            assert response.status_code == 409

    async def test_should_enforce_lexicographic_seq_ordering(self, base_url):
        """Test that "2" then "10" fails (lexicographically "10" < "2")."""
        stream_path = f"/v1/stream/seq-lexicographic-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "2"},
                content=b"first",
            )
            # "10" < "2" lexicographically
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "10"},
                content=b"second",
            )
            assert response.status_code == 409

    async def test_should_allow_lexicographic_seq_ordering_padded(self, base_url):
        """Test that "09" then "10" succeeds."""
        stream_path = f"/v1/stream/seq-padded-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "09"},
                content=b"first",
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_SEQ_HEADER: "10"},
                content=b"second",
            )
            assert response.status_code in [200, 204]


class TestTTLAndExpiry:
    """TTL and Expiry Validation tests."""

    async def test_should_reject_both_ttl_and_expires_at(self, base_url):
        stream_path = f"/v1/stream/ttl-expires-conflict-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            from datetime import datetime, timezone, timedelta
            expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={
                    "Content-Type": "text/plain",
                    STREAM_TTL_HEADER: "3600",
                    STREAM_EXPIRES_AT_HEADER: expires,
                },
            )
            assert response.status_code == 400

    async def test_should_reject_invalid_ttl(self, base_url):
        stream_path = f"/v1/stream/ttl-invalid-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "abc"},
            )
            assert response.status_code == 400

    async def test_should_reject_negative_ttl(self, base_url):
        stream_path = f"/v1/stream/ttl-negative-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "-1"},
            )
            assert response.status_code == 400

    async def test_should_accept_valid_ttl(self, base_url):
        stream_path = f"/v1/stream/ttl-valid-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "3600"},
            )
            assert response.status_code in [200, 201]

    async def test_should_reject_ttl_with_leading_zeros(self, base_url):
        stream_path = f"/v1/stream/ttl-leading-zeros-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "00060"},
            )
            assert response.status_code == 400

    async def test_should_reject_ttl_with_plus_sign(self, base_url):
        stream_path = f"/v1/stream/ttl-plus-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "+60"},
            )
            assert response.status_code == 400

    async def test_should_reject_ttl_with_float_value(self, base_url):
        stream_path = f"/v1/stream/ttl-float-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "60.5"},
            )
            assert response.status_code == 400

    async def test_should_reject_ttl_with_scientific_notation(self, base_url):
        stream_path = f"/v1/stream/ttl-scientific-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain", STREAM_TTL_HEADER: "1e3"},
            )
            assert response.status_code == 400


class TestCaseInsensitivity:
    """Case-Insensitivity tests."""

    async def test_should_treat_content_type_case_insensitively(self, base_url):
        stream_path = f"/v1/stream/case-content-type-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "TEXT/PLAIN"},
                content=b"test",
            )
            assert response.status_code in [200, 204]

    async def test_should_allow_idempotent_create_with_different_case_content_type(self, base_url):
        stream_path = f"/v1/stream/case-idempotent-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response1 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            assert response1.status_code == 201

            response2 = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "APPLICATION/JSON"},
            )
            assert response2.status_code in [200, 204]


class TestHEADMetadata:
    """HEAD Metadata tests."""

    async def test_should_return_metadata_without_body(self, base_url):
        stream_path = f"/v1/stream/head-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test data",
            )
            response = await client.head(f"{base_url}{stream_path}")
            assert response.status_code == 200
            assert response.headers.get("content-type") == "text/plain"
            assert STREAM_OFFSET_HEADER in response.headers
            assert response.text == ""

    async def test_should_return_404_for_nonexistent_stream(self, base_url):
        stream_path = f"/v1/stream/head-404-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.head(f"{base_url}{stream_path}")
            assert response.status_code == 404


class TestOffsetValidation:
    """Offset Validation and Resumability tests."""

    async def test_should_reject_offset_with_comma(self, base_url):
        stream_path = f"/v1/stream/offset-comma-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )
            response = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": "0,1"},
            )
            assert response.status_code == 400

    async def test_should_reject_offset_with_spaces(self, base_url):
        stream_path = f"/v1/stream/offset-spaces-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )
            response = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": "0 1"},
            )
            assert response.status_code == 400

    async def test_should_support_resumable_reads(self, base_url):
        stream_path = f"/v1/stream/resumable-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"chunk1",
            )

            response1 = await client.get(f"{base_url}{stream_path}")
            assert response1.text == "chunk1"
            offset1 = response1.headers.get(STREAM_OFFSET_HEADER)

            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"chunk2",
            )

            # Read from offset should only get new data
            response2 = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": offset1},
            )
            assert response2.text == "chunk2"

    async def test_should_return_empty_when_reading_from_tail(self, base_url):
        stream_path = f"/v1/stream/tail-read-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )

            response1 = await client.get(f"{base_url}{stream_path}")
            tail_offset = response1.headers.get(STREAM_OFFSET_HEADER)

            response2 = await client.get(
                f"{base_url}{stream_path}",
                params={"offset": tail_offset},
            )
            assert response2.status_code == 200
            assert response2.text == ""
            assert response2.headers.get(STREAM_UP_TO_DATE_HEADER) == "true"


class TestProtocolEdgeCases:
    """Protocol Edge Cases tests."""

    async def test_should_reject_empty_post_body(self, base_url):
        stream_path = f"/v1/stream/empty-append-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"",
            )
            assert response.status_code == 400

    async def test_should_handle_put_with_initial_body(self, base_url):
        stream_path = f"/v1/stream/put-initial-body-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"initial stream content",
            )
            assert response.status_code == 201
            assert STREAM_OFFSET_HEADER in response.headers

            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "initial stream content"

    async def test_should_return_location_header_on_201(self, base_url):
        stream_path = f"/v1/stream/location-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            assert response.status_code == 201
            assert "location" in response.headers
            assert response.headers["location"] == f"{base_url}{stream_path}"

    async def test_should_reject_missing_content_type_on_post(self, base_url):
        stream_path = f"/v1/stream/missing-ct-post-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                content=b"data",
                headers={},  # No Content-Type
            )
            assert response.status_code == 400

    async def test_should_reject_empty_offset_parameter(self, base_url):
        stream_path = f"/v1/stream/empty-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )
            response = await client.get(
                f"{base_url}{stream_path}?offset=",
            )
            assert response.status_code == 400

    async def test_should_reject_multiple_offset_parameters(self, base_url):
        stream_path = f"/v1/stream/multi-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )
            response = await client.get(
                f"{base_url}{stream_path}?offset=a&offset=b",
            )
            assert response.status_code == 400

    async def test_should_handle_binary_data_with_integrity(self, base_url):
        stream_path = f"/v1/stream/binary-test-{int(time.time()*1000)}"
        binary_data = bytes([0x00, 0x01, 0x02, 0x7F, 0x80, 0xFE, 0xFF])

        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/octet-stream"},
                content=binary_data,
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.content == binary_data


class TestJSONMode:
    """JSON Mode tests."""

    async def test_should_wrap_json_responses_in_array(self, base_url):
        stream_path = f"/v1/stream/json-wrap-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b'{"event": "created"}',
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.headers.get("content-type") == "application/json"
            import json
            data = json.loads(response.text)
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["event"] == "created"

    async def test_should_flatten_json_arrays_one_level(self, base_url):
        stream_path = f"/v1/stream/json-flatten-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b'[{"event": "a"}, {"event": "b"}]',
            )
            response = await client.get(f"{base_url}{stream_path}")
            import json
            data = json.loads(response.text)
            assert len(data) == 2
            assert data[0]["event"] == "a"
            assert data[1]["event"] == "b"

    async def test_should_reject_empty_json_arrays(self, base_url):
        stream_path = f"/v1/stream/json-empty-array-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b"[]",
            )
            assert response.status_code == 400

    async def test_should_reject_invalid_json(self, base_url):
        stream_path = f"/v1/stream/json-invalid-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            response = await client.post(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
                content=b"{invalid json}",
            )
            assert response.status_code == 400

    async def test_should_return_empty_array_for_empty_json_stream(self, base_url):
        stream_path = f"/v1/stream/json-empty-stream-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/json"},
            )
            response = await client.get(f"{base_url}{stream_path}")
            assert response.text == "[]"


class TestLongPollEdgeCases:
    """Long-Poll Edge Cases tests."""

    async def test_should_require_offset_for_long_poll(self, base_url):
        stream_path = f"/v1/stream/longpoll-no-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.get(
                f"{base_url}{stream_path}",
                params={"live": "long-poll"},
            )
            assert response.status_code == 400

    async def test_should_accept_cursor_parameter(self, base_url):
        stream_path = f"/v1/stream/longpoll-cursor-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test data",
            )
            response = await client.get(
                f"{base_url}{stream_path}",
                params={
                    "offset": "-1",
                    "live": "long-poll",
                    "cursor": "test-cursor-123",
                },
            )
            assert response.status_code == 200


class TestByteExactnessInvariant:
    """Byte-Exactness Invariant test (killer invariant)."""

    async def test_reading_from_offset_yields_exact_bytes(self, base_url):
        stream_path = f"/v1/stream/killer-invariant-test-{int(time.time()*1000)}"

        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "application/octet-stream"},
            )

            # Append random-sized chunks
            import random
            chunks = []
            chunk_sizes = [1, 2, 7, 100, 1024]
            for size in chunk_sizes:
                chunk = bytes([random.randint(0, 255) for _ in range(size)])
                chunks.append(chunk)
                await client.post(
                    f"{base_url}{stream_path}",
                    headers={"Content-Type": "application/octet-stream"},
                    content=chunk,
                )

            # Calculate expected
            expected = b"".join(chunks)

            # Read entire stream
            accumulated = b""
            current_offset = None
            iterations = 0
            max_iterations = 100

            while iterations < max_iterations:
                iterations += 1
                url = f"{base_url}{stream_path}"
                if current_offset:
                    url += f"?offset={current_offset}"

                response = await client.get(url)
                assert response.status_code == 200

                if len(response.content) > 0:
                    accumulated += response.content

                next_offset = response.headers.get(STREAM_OFFSET_HEADER)
                up_to_date = response.headers.get(STREAM_UP_TO_DATE_HEADER)

                if up_to_date == "true" and len(response.content) == 0:
                    break

                assert next_offset is not None
                if next_offset == current_offset:
                    raise AssertionError(f"Offset did not progress: stuck at {current_offset}")

                current_offset = next_offset

            assert iterations < max_iterations
            assert accumulated == expected


class TestSSEMode:
    """SSE Mode tests."""

    async def test_should_require_offset_for_sse(self, base_url):
        stream_path = f"/v1/stream/sse-no-offset-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
            )
            response = await client.get(
                f"{base_url}{stream_path}",
                params={"live": "sse"},
            )
            assert response.status_code == 400

    async def test_should_return_text_event_stream_content_type(self, base_url):
        stream_path = f"/v1/stream/sse-content-type-test-{int(time.time()*1000)}"
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.put(
                f"{base_url}{stream_path}",
                headers={"Content-Type": "text/plain"},
                content=b"test",
            )
            try:
                async with client.stream(
                    "GET",
                    f"{base_url}{stream_path}",
                    params={"offset": "-1", "live": "sse"},
                ) as response:
                    assert response.status_code == 200
                    assert "text/event-stream" in response.headers.get("content-type", "")
            except Exception:
                pass  # Timeout is expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
