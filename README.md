# Durable Streams Python Implementation

A complete Python implementation of the [Durable Streams Protocol](https://github.com/durable-streams/durable-streams) - an HTTP-based protocol for creating, appending to, and reading from durable, append-only byte streams.

## Features

- **Full Protocol Compliance**: Implements the complete Durable Streams Protocol specification
- **Pluggable Storage**: Modular database provider architecture
  - In-memory storage for testing and development
  - SQLite storage for production persistence
- **Real-time Support**:
  - Long-polling for efficient live tailing
  - Server-Sent Events (SSE) for streaming updates
- **JSON Mode**: Special handling for `application/json` streams with array flattening and boundary preservation
- **Async/Await**: Built on modern Python asyncio patterns with aiohttp

## Installation

```bash
pip install -e .
```

For development with test dependencies:

```bash
pip install -e ".[dev]"
```

## Quick Start

### Starting the Server

```python
import asyncio
from durable_streams import DurableStreamServer, MemoryStreamStore

async def main():
    # Create server with in-memory storage
    server = DurableStreamServer()

    # Start server
    url = await server.start()
    print(f"Server running at {url}")

    # Keep running
    await asyncio.Event().wait()

asyncio.run(main())
```

Or use the CLI:

```bash
# Start with in-memory storage (default)
durable-streams

# Start with SQLite storage
durable-streams --db sqlite:///data/streams.db

# Custom port
durable-streams --port 8080
```

### Using the Server

```python
import httpx

# Create a stream
response = httpx.put(
    "http://localhost:4437/v1/stream/my-stream",
    headers={"Content-Type": "text/plain"}
)
print(f"Created: {response.status_code}")

# Append data
httpx.post(
    "http://localhost:4437/v1/stream/my-stream",
    headers={"Content-Type": "text/plain"},
    content=b"Hello, World!"
)

# Read data
response = httpx.get("http://localhost:4437/v1/stream/my-stream")
print(response.text)  # "Hello, World!"
```

### Resumable Reads

```python
# First read
response1 = httpx.get("http://localhost:4437/v1/stream/my-stream")
offset = response1.headers["stream-next-offset"]

# Append more data
httpx.post(
    "http://localhost:4437/v1/stream/my-stream",
    headers={"Content-Type": "text/plain"},
    content=b" More data"
)

# Resume from offset - only gets new data
response2 = httpx.get(
    "http://localhost:4437/v1/stream/my-stream",
    params={"offset": offset}
)
print(response2.text)  # " More data"
```

### JSON Mode

```python
# Create JSON stream
httpx.put(
    "http://localhost:4437/v1/stream/events",
    headers={"Content-Type": "application/json"}
)

# Append events (arrays are flattened one level)
httpx.post(
    "http://localhost:4437/v1/stream/events",
    headers={"Content-Type": "application/json"},
    content=b'[{"event": "created"}, {"event": "updated"}]'
)

# Read - returns wrapped array
response = httpx.get("http://localhost:4437/v1/stream/events")
print(response.json())  # [{"event": "created"}, {"event": "updated"}]
```

## Storage Providers

### Memory Store (Default)

```python
from durable_streams import DurableStreamServer, MemoryStreamStore

store = MemoryStreamStore()
server = DurableStreamServer(store=store)
```

### SQLite Store

```python
from durable_streams import DurableStreamServer, SQLiteStreamStore

# File-based (persistent)
store = SQLiteStreamStore("data/streams.db")

# In-memory (for testing)
store = SQLiteStreamStore(":memory:")

server = DurableStreamServer(store=store)
```

### Custom Store

Implement the `StreamStore` abstract base class:

```python
from durable_streams.store import StreamStore

class MyCustomStore(StreamStore):
    async def create(self, path, options=None):
        ...

    async def append(self, path, data, options=None):
        ...

    # ... implement all abstract methods
```

## API Reference

### HTTP Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/{path}` | Create a stream |
| POST | `/{path}` | Append to a stream |
| GET | `/{path}` | Read from a stream |
| HEAD | `/{path}` | Get stream metadata |
| DELETE | `/{path}` | Delete a stream |

### Headers

| Header | Description |
|--------|-------------|
| `Stream-Next-Offset` | Next offset for subsequent reads |
| `Stream-Up-To-Date` | Indicates client has caught up |
| `Stream-Seq` | Writer sequence for coordination |
| `Stream-TTL` | Time-to-live in seconds |
| `Stream-Expires-At` | Absolute expiry (RFC 3339) |
| `Stream-Cursor` | Cursor for CDN collapsing |

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `offset` | Start offset for reading |
| `live` | Live mode: `long-poll` or `sse` |
| `cursor` | Echo cursor for collapsing |

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run conformance tests only
pytest tests/test_conformance.py -v

# Run SQLite tests only
pytest tests/test_sqlite_store.py -v
```

## Running Against Official Conformance Tests

This implementation has been tested against the official `@durable-streams/conformance-tests` package
and **passes all 108 tests**.

1. Clone the official durable-streams repository:
```bash
git clone https://github.com/durable-streams/durable-streams.git
cd durable-streams
pnpm install && pnpm build
```

2. Start this Python server:
```bash
python run_conformance_tests.py --server-only
```

3. Create a test file `packages/server/test/python-server-conformance.test.ts`:
```typescript
import { describe } from "vitest"
import { runConformanceTests } from "@durable-streams/conformance-tests"

describe("Python Server Implementation", () => {
  runConformanceTests({
    baseUrl: "http://127.0.0.1:4437",
  })
})
```

4. Run the conformance tests:
```bash
pnpm exec vitest run packages/server/test/python-server-conformance.test.ts
```

### Test Results
```
✓ |server| packages/server/test/python-server-conformance.test.ts (108 tests)
   ✓ Python Server Implementation > Long-Poll Operations > should wait for new data with long-poll
   ✓ Python Server Implementation > Chunking and Large Payloads > should handle chunk-size pagination correctly
   ✓ Python Server Implementation > SSE Mode > should stream data events via SSE
   ... and 105 more tests

Test Files  1 passed (1)
Tests  108 passed (108)
```

## Protocol Compliance

This implementation passes all conformance tests from the official [durable-streams](https://github.com/durable-streams/durable-streams) test suite, including:

- Basic stream operations (create, append, read, delete)
- Idempotent creation with config matching
- Sequence ordering (lexicographic)
- TTL and expiry validation
- Content-type enforcement
- Offset validation and resumability
- Long-polling and SSE modes
- JSON mode with array flattening
- Binary data integrity
- Protocol edge cases

## License

MIT License - See LICENSE file for details.
