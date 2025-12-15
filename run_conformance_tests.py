#!/usr/bin/env python3
"""
Script to run the official Durable Streams conformance tests against this implementation.

Usage:
    1. Start this server: python run_conformance_tests.py --server-only
    2. In another terminal, run the conformance tests from the durable-streams repo

Or use the --with-tests flag to run the Python conformance tests.
"""

import argparse
import asyncio
import signal
import sys
import subprocess

# Add src to path
sys.path.insert(0, "src")

from durable_streams import DurableStreamServer, MemoryStreamStore, SQLiteStreamStore
from durable_streams.types import ServerOptions


async def run_server(port: int, use_sqlite: bool = False):
    """Run the server and wait for shutdown."""
    if use_sqlite:
        store = SQLiteStreamStore(":memory:")
    else:
        store = MemoryStreamStore()

    options = ServerOptions(
        port=port,
        host="127.0.0.1",
        long_poll_timeout=30_000,
    )

    server = DurableStreamServer(store=store, options=options)

    shutdown_event = asyncio.Event()

    def signal_handler():
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    try:
        url = await server.start()
        print(f"Server started at {url}")
        print("Ready for conformance tests")
        print("Press Ctrl+C to stop")

        await shutdown_event.wait()
    finally:
        print("\nShutting down...")
        await server.stop()
        print("Server stopped")


def main():
    parser = argparse.ArgumentParser(description="Run conformance tests")
    parser.add_argument(
        "--port",
        type=int,
        default=4437,
        help="Port to run the server on (default: 4437)",
    )
    parser.add_argument(
        "--sqlite",
        action="store_true",
        help="Use SQLite storage instead of memory",
    )
    parser.add_argument(
        "--server-only",
        action="store_true",
        help="Only start the server (for external test runner)",
    )
    parser.add_argument(
        "--with-tests",
        action="store_true",
        help="Run Python conformance tests against the server",
    )

    args = parser.parse_args()

    if args.with_tests:
        # Run pytest with our conformance tests
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_conformance.py", "-v"],
            cwd="/home/user/Durable-stream-python",
        )
        sys.exit(result.returncode)
    else:
        # Just run the server
        asyncio.run(run_server(args.port, args.sqlite))


if __name__ == "__main__":
    main()
