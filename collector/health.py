"""Minimal health/status HTTP server for the collector.

Endpoints:
    GET /health  → 200 OK (plain text)
    GET /status  → JSON with counters, last write times, feed health
    GET /data/market_state      → download all market state JSONL (concatenated)
    GET /data/reference_state   → download all reference state JSONL
    GET /data/closing           → download all closing snapshots JSONL
    GET /data/events            → download all event snapshots JSONL
"""

import asyncio
import io
import json
import logging
import time
from pathlib import Path

from collector.config import PORT, DATA_DIR

logger = logging.getLogger(__name__)


def _collect_jsonl(category: str) -> bytes | None:
    """Concatenate all JSONL files for a category into a single byte string.

    Walks the data directory for the given category, reads all .jsonl files
    sorted by name (date order), and concatenates them.

    Supports:
        /data/market_state      → market_state/polymarket/*.jsonl + market_state/kalshi/*.jsonl
        /data/reference_state   → reference_state/pinnacle/*.jsonl
        /data/closing           → closing_snapshots/*.jsonl
        /data/events            → events/*.jsonl
    """
    base = Path(DATA_DIR) if DATA_DIR else Path(".")

    # Map URL path segments to directory names
    dir_map = {
        "market_state": "market_state",
        "reference_state": "reference_state",
        "closing": "closing_snapshots",
        "events": "events",
    }

    dir_name = dir_map.get(category)
    if not dir_name:
        return None

    target = base / dir_name
    if not target.exists():
        return None

    # Collect all .jsonl files recursively (handles platform subdirs)
    files = sorted(target.rglob("*.jsonl"))
    if not files:
        return None

    buf = io.BytesIO()
    for f in files:
        try:
            buf.write(f.read_bytes())
        except OSError:
            continue

    result = buf.getvalue()
    return result if result else None


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

            elif path.startswith("/data/"):
                # File download endpoints — concatenate all JSONL files
                # in the requested category across all dates/platforms
                category = path[len("/data/"):]
                content = _collect_jsonl(category)
                if content is not None:
                    filename = f"{category.replace('/', '_')}.jsonl"
                    header = (
                        "HTTP/1.1 200 OK\r\n"
                        f"Content-Length: {len(content)}\r\n"
                        "Content-Type: application/x-ndjson\r\n"
                        f'Content-Disposition: attachment; filename="{filename}"\r\n'
                        "Connection: close\r\n\r\n"
                    )
                    writer.write(header.encode() + content)
                else:
                    body = b"No data found for this category\n"
                    header = (
                        "HTTP/1.1 404 Not Found\r\n"
                        f"Content-Length: {len(body)}\r\n"
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
