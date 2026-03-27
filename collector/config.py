"""Collector configuration — env vars for data collection, storage, cadence."""

import os
from pathlib import Path

# ── Data directory ────────────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "")


def data_path(filename: str) -> str:
    """Resolve a filename to DATA_DIR if set, otherwise current dir."""
    if DATA_DIR:
        return str(Path(DATA_DIR) / filename)
    return filename


# ── Snapshot cadence ──────────────────────────────────────────────────
# Heartbeat: write a snapshot even if nothing changed, every N seconds
SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "30"))
# Minimum price change (in probability units) to trigger a snapshot
# 0.005 = 0.5 cents on Polymarket
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", "0.005"))
# Standard notional sizes (USD) for VWAP computation
VWAP_SIZES = [50, 100, 250]

# ── Closing snapshot ──────────────────────────────────────────────────
# How often to check for matches approaching start (seconds)
CLOSING_CHECK_INTERVAL = int(os.getenv("CLOSING_CHECK_INTERVAL", "10"))
# Freeze the closing snapshot when match is within this many seconds of start
CLOSING_WINDOW_SECONDS = int(os.getenv("CLOSING_WINDOW_SECONDS", "60"))

# ── Registry refresh ─────────────────────────────────────────────────
REGISTRY_REFRESH_INTERVAL = int(os.getenv("REGISTRY_REFRESH_INTERVAL", "1800"))

# ── Pinnacle poll intervals (reused from live_bot) ────────────────────
PINNACLE_POLL_INTERVAL = int(os.getenv("PINNACLE_POLL_INTERVAL", "8"))
PINNACLE_LIVE_POLL_INTERVAL = int(os.getenv("PINNACLE_LIVE_POLL_INTERVAL", "4"))

# ── Health server ─────────────────────────────────────────────────────
PORT = int(os.getenv("PORT", os.getenv("COLLECTOR_PORT", "8080")))

# ── Logging ───────────────────────────────────────────────────────────
LOG_PATH = data_path("collector.log")
