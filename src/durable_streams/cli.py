"""
Command-line interface for the Durable Streams server.
"""

import argparse
import asyncio
import signal
import sys
from typing import Optional

from .server import DurableStreamServer
from .types import ServerOptions
from .memory_store import MemoryStreamStore
from .sqlite_store import SQLiteStreamStore


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Durable Streams Protocol Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start server on default port (4437)
  durable-streams

  # Start server on custom port
  durable-streams --port 8080

  # Use SQLite for persistence
  durable-streams --db sqlite:///data/streams.db

  # Use in-memory storage (default)
  durable-streams --db memory
        """,
    )

    parser.add_argument(
        "--port",
        type=int,
        default=4437,
        help="Port to listen on (default: 4437)",
    )

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )

    parser.add_argument(
        "--db",
        type=str,
        default="memory",
        help="Database backend: 'memory' or 'sqlite:///path/to/db' (default: memory)",
    )

    parser.add_argument(
        "--long-poll-timeout",
        type=int,
        default=30000,
        help="Long-poll timeout in milliseconds (default: 30000)",
    )

    return parser.parse_args()


def create_store(db_spec: str):
    """Create a store based on the database specification."""
    if db_spec == "memory":
        return MemoryStreamStore()
    elif db_spec.startswith("sqlite:///"):
        db_path = db_spec[len("sqlite:///"):]
        return SQLiteStreamStore(db_path)
    elif db_spec.startswith("sqlite://"):
        # Handle sqlite:// without the third slash (relative path)
        db_path = db_spec[len("sqlite://"):]
        return SQLiteStreamStore(db_path)
    else:
        print(f"Unknown database backend: {db_spec}", file=sys.stderr)
        print("Use 'memory' or 'sqlite:///path/to/db'", file=sys.stderr)
        sys.exit(1)


async def run_server(args: argparse.Namespace) -> None:
    """Run the server with the given arguments."""
    store = create_store(args.db)

    options = ServerOptions(
        port=args.port,
        host=args.host,
        long_poll_timeout=args.long_poll_timeout,
    )

    server = DurableStreamServer(store=store, options=options)

    # Set up signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler():
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        url = await server.start()
        print(f"Durable Streams server started at {url}")
        print(f"Database: {args.db}")
        print("Press Ctrl+C to stop")

        # Wait for shutdown signal
        await shutdown_event.wait()

    except Exception as e:
        print(f"Failed to start server: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        print("\nShutting down...")
        await server.stop()
        print("Server stopped")


def main() -> None:
    """Main entry point."""
    args = parse_args()

    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
