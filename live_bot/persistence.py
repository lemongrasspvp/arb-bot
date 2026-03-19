"""Position persistence — save/load open positions across restarts."""

import json
import logging
from pathlib import Path

from live_bot.config import POSITIONS_FILE_PATH

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path(POSITIONS_FILE_PATH)


def save_positions(portfolio) -> None:
    """Save open positions and portfolio state to disk."""
    data = {
        "balance": portfolio.current_balance,
        "starting_balance": portfolio.starting_balance,
        "total_pnl": portfolio.total_pnl,
        "daily_pnl": portfolio.daily_pnl,
        "arb_count": portfolio.arb_count,
        "arb_pnl": portfolio.arb_pnl,
        "value_count": portfolio.value_count,
        "value_filled_count": portfolio.value_filled_count,
        "value_edge_sum": portfolio.value_edge_sum,
        "value_pnl": portfolio.value_pnl,
        "pregame_value_count": portfolio.pregame_value_count,
        "pregame_value_pnl": portfolio.pregame_value_pnl,
        "midgame_value_count": portfolio.midgame_value_count,
        "midgame_value_pnl": portfolio.midgame_value_pnl,
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
            }
            for p in portfolio.positions
        ],
    }

    try:
        POSITIONS_FILE.write_text(json.dumps(data, indent=2))
        logger.debug("Saved %d positions to %s", len(portfolio.positions), POSITIONS_FILE)
    except OSError:
        logger.exception("Failed to save positions")


def load_positions(portfolio) -> int:
    """Restore positions and portfolio state from disk.

    Returns number of positions loaded.
    """
    if not POSITIONS_FILE.exists():
        logger.info("No saved positions found")
        return 0

    try:
        data = json.loads(POSITIONS_FILE.read_text())

        # Restore portfolio stats
        portfolio.current_balance = data.get("balance", portfolio.starting_balance)
        portfolio.total_pnl = data.get("total_pnl", 0.0)
        portfolio.daily_pnl = data.get("daily_pnl", 0.0)
        portfolio.arb_count = data.get("arb_count", 0)
        portfolio.arb_pnl = data.get("arb_pnl", 0.0)
        portfolio.value_count = data.get("value_count", 0)
        portfolio.value_filled_count = data.get("value_filled_count", 0)
        portfolio.value_edge_sum = data.get("value_edge_sum", 0.0)
        portfolio.value_pnl = data.get("value_pnl", 0.0)
        portfolio.pregame_value_count = data.get("pregame_value_count", 0)
        portfolio.pregame_value_pnl = data.get("pregame_value_pnl", 0.0)
        portfolio.midgame_value_count = data.get("midgame_value_count", 0)
        portfolio.midgame_value_pnl = data.get("midgame_value_pnl", 0.0)

        # Restore open positions
        from live_bot.portfolio import Position

        for p_data in data.get("positions", []):
            pos = Position(
                match_id=p_data["match_id"],
                platform=p_data["platform"],
                market_id=p_data["market_id"],
                team=p_data["team"],
                side=p_data["side"],
                price=p_data["price"],
                size=p_data["size"],
                cost_usd=p_data["cost_usd"],
                opened_at=p_data["opened_at"],
                strategy=p_data["strategy"],
                timing=p_data.get("timing", "pregame"),
                condition_id=p_data.get("condition_id", ""),
            )
            portfolio.positions.append(pos)

        loaded = len(portfolio.positions)
        logger.info(
            "Loaded %d positions (balance=$%.2f, P&L=$%.2f, filled=%d, avg_edge=%.1f%%)",
            loaded, portfolio.current_balance, portfolio.total_pnl,
            portfolio.value_filled_count,
            portfolio.value_edge_sum / portfolio.value_filled_count if portfolio.value_filled_count else 0,
        )
        return loaded

    except Exception:
        logger.exception("Failed to load positions from %s", POSITIONS_FILE)
        return 0
