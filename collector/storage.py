"""Partitioned JSONL storage writer.

Writes snapshots to daily-partitioned JSONL files organized by category
and platform/source:

    /data/market_state/polymarket/2026-03-27.jsonl
    /data/market_state/kalshi/2026-03-27.jsonl
    /data/reference_state/pinnacle/2026-03-27.jsonl
    /data/events/2026-03-27.jsonl
    /data/closing_snapshots/2026-03-27.jsonl

Each row is a single JSON object followed by a newline.
Files are opened in append mode with flush after every write.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from collector.config import DATA_DIR

logger = logging.getLogger(__name__)


class JsonlWriter:
    """Append-only JSONL writer with automatic daily file rotation.

    Handles:
    - Creating directories on first write
    - Rolling to a new file at midnight UTC
    - Flushing after every write (durability)
    - Best-effort: write failures are logged, never raised
    """

    def __init__(self, category: str, subcategory: str = ""):
        """Initialize writer.

        Args:
            category: Top-level partition (e.g. "market_state", "events")
            subcategory: Optional second-level partition (e.g. "polymarket", "pinnacle").
                         If empty, files go directly under category/.
        """
        self.category = category
        self.subcategory = subcategory
        self._current_date: str = ""
        self._file = None
        self._path: Path | None = None
        self.rows_written: int = 0

    def _resolve_dir(self) -> Path:
        base = Path(DATA_DIR) if DATA_DIR else Path(".")
        if self.subcategory:
            return base / self.category / self.subcategory
        return base / self.category

    def _ensure_file(self, date_str: str) -> bool:
        """Open or rotate the output file for the given UTC date.

        Returns True if file is ready, False on failure.
        """
        if self._file and self._current_date == date_str:
            return True

        # Close previous file
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass

        target_dir = self._resolve_dir()
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error("Cannot create directory %s: %s", target_dir, e)
            self._file = None
            return False

        self._path = target_dir / f"{date_str}.jsonl"
        try:
            self._file = open(self._path, "a", encoding="utf-8")
            self._current_date = date_str
            logger.info("Storage: opened %s", self._path)
            return True
        except OSError as e:
            logger.error("Cannot open %s: %s", self._path, e)
            self._file = None
            return False

    def write(self, record: dict[str, Any]) -> bool:
        """Write a single record as a JSON line.

        Returns True on success, False on failure. Never raises.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            if not self._ensure_file(date_str):
                return False
            line = json.dumps(record, separators=(",", ":"), default=str)
            self._file.write(line + "\n")
            self._file.flush()
            self.rows_written += 1
            return True
        except Exception as e:
            logger.error("Write failed (%s/%s): %s", self.category, self.subcategory, e)
            return False

    def close(self):
        """Close the underlying file."""
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None

    @property
    def current_path(self) -> str:
        return str(self._path) if self._path else ""

    @property
    def size_bytes(self) -> int:
        if self._path and self._path.exists():
            return self._path.stat().st_size
        return 0
