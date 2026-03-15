"""Position persistence — save/load open positions across restarts."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path("live_bot_positions.json")


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
        "value_pnl": portfolio.value_pnl,
        # 4-bucket stats
        "pregame_arb_count": portfolio.pregame_arb_count,
        "pregame_arb_pnl": portfolio.pregame_arb_pnl,
        "midgame_arb_count": portfolio.midgame_arb_count,
        "midgame_arb_pnl": portfolio.midgame_arb_pnl,
        "pregame_value_count": portfolio.pregame_value_count,
        "pregame_value_pnl": portfolio.pregame_value_pnl,
        "midgame_value_count": portfolio.midgame_value_count,
        "midgame_value_pnl": portfolio.midgame_value_pnl,
        # Shadow simulations
        "maker_arb_count": portfolio.maker_arb_count,
        "maker_arb_pnl": portfolio.maker_arb_pnl,
        "maker_value_count": portfolio.maker_value_count,
        "maker_value_pnl": portfolio.maker_value_pnl,
        "early_exit_tiers": portfolio.early_exit_tiers,
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
        portfolio.value_pnl = data.get("value_pnl", 0.0)
        # 4-bucket stats
        portfolio.pregame_arb_count = data.get("pregame_arb_count", 0)
        portfolio.pregame_arb_pnl = data.get("pregame_arb_pnl", 0.0)
        portfolio.midgame_arb_count = data.get("midgame_arb_count", 0)
        portfolio.midgame_arb_pnl = data.get("midgame_arb_pnl", 0.0)
        portfolio.pregame_value_count = data.get("pregame_value_count", 0)
        portfolio.pregame_value_pnl = data.get("pregame_value_pnl", 0.0)
        portfolio.midgame_value_count = data.get("midgame_value_count", 0)
        portfolio.midgame_value_pnl = data.get("midgame_value_pnl", 0.0)
        # Shadow simulations
        portfolio.maker_arb_count = data.get("maker_arb_count", 0)
        portfolio.maker_arb_pnl = data.get("maker_arb_pnl", 0.0)
        portfolio.maker_value_count = data.get("maker_value_count", 0)
        portfolio.maker_value_pnl = data.get("maker_value_pnl", 0.0)
        portfolio.early_exit_tiers = data.get("early_exit_tiers", {})

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
            "Loaded %d positions (balance=$%.2f, P&L=$%.2f)",
            loaded, portfolio.current_balance, portfolio.total_pnl,
        )
        return loaded

    except Exception:
        logger.exception("Failed to load positions from %s", POSITIONS_FILE)
        return 0
