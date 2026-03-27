"""Minimal health/status HTTP server for the collector.

Endpoints:
    GET /health  → 200 OK (plain text)
    GET /status  → JSON with counters, last write times, feed health
"""

import asyncio
import json
import logging
import time

from collector.config import PORT

logger = logging.getLogger(__name__)


async def health_server(snapshotter, closing_freezer, registry,
                        shutdown_event: asyncio.Event) -> None:
    """Run a tiny HTTP server for health checks and status."""

    start_time = time.time()

    async def handle(reader, writer):
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            # Drain headers
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if line in (b"\r\n", b"\n", b""):
                    break

            parts = request_line.decode("utf-8", errors="ignore").split()
            path = parts[1] if len(parts) > 1 else "/"

            if path == "/health":
                body = b"OK\n"
                header = (
                    "HTTP/1.1 200 OK\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Content-Type: text/plain\r\n"
                    "Connection: close\r\n\r\n"
                )
                writer.write(header.encode() + body)

            elif path == "/status":
                now = time.time()
                status = {
                    "uptime_s": int(now - start_time),
                    "events_tracked": len(registry.matches),
                    "snapshots_written": {
                        "market_state": snapshotter.counts.get("market_state", 0),
                        "reference_state": snapshotter.counts.get("reference_state", 0),
                        "events": snapshotter.counts.get("events", 0),
                        "closing": closing_freezer.count,
                        "skipped_dedup": snapshotter.counts.get("skipped_dedup", 0),
                    },
                    "last_write": {
                        k: time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(v)
                        )
                        for k, v in snapshotter.last_write_ts.items()
                    },
                    "storage_bytes": {
                        "market_state_poly": snapshotter._market_writers.get(
                            "polymarket", None
                        ).size_bytes if snapshotter._market_writers.get("polymarket") else 0,
                        "market_state_kalshi": snapshotter._market_writers.get(
                            "kalshi", None
                        ).size_bytes if snapshotter._market_writers.get("kalshi") else 0,
                        "reference_state": snapshotter._reference_writer.size_bytes,
                    },
                }
                body = json.dumps(status, indent=2).encode()
                header = (
                    "HTTP/1.1 200 OK\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Content-Type: application/json\r\n"
                    "Connection: close\r\n\r\n"
                )
                writer.write(header.encode() + body)

            else:
                body = b"Not Found\n"
                header = (
                    "HTTP/1.1 404 Not Found\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Connection: close\r\n\r\n"
                )
                writer.write(header.encode() + body)

            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    logger.info("Health server listening on http://0.0.0.0:%d", PORT)

    try:
        await shutdown_event.wait()
    finally:
        server.close()
        await server.wait_closed()
