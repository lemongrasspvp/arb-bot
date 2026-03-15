"""Trade logger — JSON lines for post-analysis."""

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from live_bot.config import TRADE_LOG_PATH

logger = logging.getLogger(__name__)

# CET timezone
CET = timezone(timedelta(hours=1))


def _init_log_file() -> Path:
    """Ensure trade log file exists."""
    path = Path(TRADE_LOG_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def log_trade(
    trade_type: str,
    strategy: str,
    match_name: str = "",
    match_id: str = "",
    platform_a: str = "",
    team_a: str = "",
    price_a: float = 0.0,
    platform_b: str = "",
    team_b: str = "",
    price_b: float = 0.0,
    combined_cost: float = 0.0,
    profit_pct: float = 0.0,
    edge_pct: float = 0.0,
    pinnacle_prob: float = 0.0,
    size_usd: float = 0.0,
    latency_ms: float = 0.0,
    simulated: bool = True,
    would_fill: bool = False,
    filled_a: bool = False,
    filled_b: bool = False,
    extra: dict | None = None,
) -> None:
    """Write a trade event to the JSON lines log file."""
    now = datetime.now(CET)
    record = {
        "timestamp": now.isoformat(),
        "unix_ts": time.time(),
        "type": trade_type,
        "strategy": strategy,
        "match": match_name,
        "match_id": match_id,
        "platform_a": platform_a,
        "team_a": team_a,
        "price_a": round(price_a, 6),
        "platform_b": platform_b,
        "team_b": team_b,
        "price_b": round(price_b, 6),
        "combined_cost": round(combined_cost, 6),
        "profit_pct": round(profit_pct, 4),
        "edge_pct": round(edge_pct, 4),
        "pinnacle_prob": round(pinnacle_prob, 6),
        "size_usd": round(size_usd, 2),
        "latency_ms": round(latency_ms, 1),
        "simulated": simulated,
        "would_fill": would_fill,
        "filled_a": filled_a,
        "filled_b": filled_b,
    }
    if extra:
        record.update(extra)

    path = _init_log_file()
    try:
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError:
        logger.exception("Failed to write trade log")

    # Also log to standard logger
    if strategy == "ARB":
        logger.info(
            "[%s] %s: %s@%s %.0f¢ + %s@%s %.0f¢ = %.1f¢ (%.2f%%) size=$%.2f latency=%.0fms %s",
            strategy, trade_type, team_a, platform_a, price_a * 100,
            team_b, platform_b, price_b * 100,
            combined_cost * 100, profit_pct, size_usd, latency_ms,
            "SIM" if simulated else "LIVE",
        )
    else:
        logger.info(
            "[%s] %s: %s@%s %.0f¢ (pinnacle=%.0f¢, edge=%.1f%%) size=$%.2f latency=%.0fms %s",
            strategy, trade_type, team_a, platform_a, price_a * 100,
            pinnacle_prob * 100, edge_pct, size_usd, latency_ms,
            "SIM" if simulated else "LIVE",
        )


def log_event(event_type: str, message: str, **kwargs) -> None:
    """Log a non-trade event (connection, error, registry update, etc.)."""
    now = datetime.now(CET)
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
