"""Persistence — save/load NBA bot positions and portfolio state."""

import json
import logging
import os
from pathlib import Path

from nba_bot.config import POSITIONS_FILE_PATH, TRADE_LOG_PATH

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path(POSITIONS_FILE_PATH)


def maybe_reset(portfolio) -> bool:
    if not os.getenv("RESET_SIMULATION"):
        return False

    balance = float(os.getenv("RESET_BALANCE", str(portfolio.starting_balance)))
    logger.warning("RESET triggered — wiping state, starting at $%.0f", balance)

    POSITIONS_FILE.write_text(json.dumps({
        "balance": balance, "starting_balance": balance,
        "total_pnl": 0.0, "daily_pnl": 0.0,
        "value_count": 0, "value_filled_count": 0,
        "value_edge_sum": 0.0, "value_pnl": 0.0,
        "pregame_value_count": 0, "pregame_value_pnl": 0.0,
        "positions": [],
    }, indent=2))

    Path(TRADE_LOG_PATH).write_text("")

    portfolio.current_balance = balance
    portfolio.starting_balance = balance
    portfolio.total_pnl = 0.0
    portfolio.daily_pnl = 0.0
    portfolio.value_count = 0
    portfolio.value_filled_count = 0
    portfolio.value_edge_sum = 0.0
    portfolio.value_pnl = 0.0
    portfolio.pregame_value_count = 0
    portfolio.pregame_value_pnl = 0.0
    portfolio.positions.clear()
    return True


def save_positions(portfolio) -> None:
    data = {
        "balance": portfolio.current_balance,
        "starting_balance": portfolio.starting_balance,
        "total_pnl": portfolio.total_pnl,
        "daily_pnl": portfolio.daily_pnl,
        "value_count": portfolio.value_count,
        "value_filled_count": portfolio.value_filled_count,
        "value_edge_sum": portfolio.value_edge_sum,
        "value_pnl": portfolio.value_pnl,
        "pregame_value_count": portfolio.pregame_value_count,
        "pregame_value_pnl": portfolio.pregame_value_pnl,
        "positions": [
            {
                "match_id": p.match_id,
                "platform": p.platform,
                "market_id": p.market_id,
                "team": p.team,
                "side": p.side,
                "price": p.price,
                "size": p.size,
                "cost_usd": p.cost_usd,
                "opened_at": p.opened_at,
                "strategy": p.strategy,
                "timing": p.timing,
                "condition_id": p.condition_id,
                "pinnacle_prob_at_entry": p.pinnacle_prob_at_entry,
                "pinnacle_prob_latest": p.pinnacle_prob_latest,
                "pinnacle_prob_pregame_close": p.pinnacle_prob_pregame_close,
            }
            for p in portfolio.positions
        ],
    }
    try:
        POSITIONS_FILE.write_text(json.dumps(data, indent=2))
        logger.debug("Saved %d positions", len(portfolio.positions))
    except OSError:
        logger.exception("Failed to save positions")


def load_positions(portfolio) -> int:
    if not POSITIONS_FILE.exists():
        logger.info("No saved positions found")
        return 0

    try:
        data = json.loads(POSITIONS_FILE.read_text())

        saved_balance = data.get("balance", portfolio.starting_balance)
        saved_starting = data.get("starting_balance", portfolio.starting_balance)
        if portfolio.starting_balance > saved_starting:
            deposit = portfolio.starting_balance - saved_starting
            saved_balance += deposit
            logger.info("Deposit: +$%.2f", deposit)

        portfolio.current_balance = saved_balance
        portfolio.total_pnl = data.get("total_pnl", 0.0)
        portfolio.daily_pnl = data.get("daily_pnl", 0.0)
        portfolio.value_count = data.get("value_count", 0)
        portfolio.value_filled_count = data.get("value_filled_count", 0)
        portfolio.value_edge_sum = data.get("value_edge_sum", 0.0)
        portfolio.value_pnl = data.get("value_pnl", 0.0)
        portfolio.pregame_value_count = data.get("pregame_value_count", 0)
        portfolio.pregame_value_pnl = data.get("pregame_value_pnl", 0.0)

        from live_bot.portfolio import Position
        for p in data.get("positions", []):
            portfolio.positions.append(Position(
                match_id=p["match_id"],
                platform=p["platform"],
                market_id=p["market_id"],
                team=p["team"],
                side=p["side"],
                price=p["price"],
                size=p["size"],
                cost_usd=p["cost_usd"],
                opened_at=p["opened_at"],
                strategy=p["strategy"],
                timing=p.get("timing", "pregame"),
                condition_id=p.get("condition_id", ""),
                pinnacle_prob_at_entry=p.get("pinnacle_prob_at_entry", 0.0),
                pinnacle_prob_latest=p.get("pinnacle_prob_latest", 0.0),
                pinnacle_prob_pregame_close=p.get("pinnacle_prob_pregame_close", 0.0),
            ))

        loaded = len(portfolio.positions)
        logger.info(
            "Loaded %d positions (balance=$%.2f, P&L=$%.2f)",
            loaded, portfolio.current_balance, portfolio.total_pnl,
        )
        return loaded

    except Exception:
        logger.exception("Failed to load positions from %s", POSITIONS_FILE)
        return 0
