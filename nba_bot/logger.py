"""Trade logger — JSONL trade log for NBA bot."""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from nba_bot.config import TRADE_LOG_PATH

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Europe/Copenhagen")


def _init_log_file() -> Path:
    path = Path(TRADE_LOG_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def log_trade(
    trade_type: str,
    match_name: str = "",
    match_id: str = "",
    platform: str = "",
    team: str = "",
    price: float = 0.0,
    edge_pct: float = 0.0,
    pinnacle_prob: float = 0.0,
    size_usd: float = 0.0,
    would_fill: bool = False,
    extra: dict | None = None,
) -> None:
    now = datetime.now(LOCAL_TZ)
    record = {
        "timestamp": now.isoformat(),
        "unix_ts": time.time(),
        "type": trade_type,
        "strategy": "NBA_REGULAR",
        "match": match_name,
        "match_id": match_id,
        "platform_a": platform,
        "team_a": team,
        "price_a": round(price, 6),
        "edge_pct": round(edge_pct, 4),
        "pinnacle_prob": round(pinnacle_prob, 6),
        "size_usd": round(size_usd, 2),
        "would_fill": would_fill,
    }
    if extra:
        record["extra"] = extra

    path = _init_log_file()
    try:
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError:
        logger.exception("Failed to write trade log")


def log_event(event_type: str, message: str, **kwargs) -> None:
    now = datetime.now(LOCAL_TZ)
    record = {
        "timestamp": now.isoformat(),
        "unix_ts": time.time(),
        "type": event_type,
        "message": message,
        **kwargs,
    }

    path = _init_log_file()
    try:
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError:
        pass

    logger.info("[%s] %s", event_type, message)
