"""Position persistence — save/load open positions across restarts."""

import json
import logging
import os
from pathlib import Path

from live_bot.config import POSITIONS_FILE_PATH, TRADE_LOG_PATH, INVERTED_FILE_PATH

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path(POSITIONS_FILE_PATH)
INVERTED_FILE = Path(INVERTED_FILE_PATH)


def maybe_reset_simulation(portfolio) -> bool:
    """If RESET_SIMULATION env var is set, wipe all state and start fresh.

    Set RESET_SIMULATION=1 in Railway env vars, then remove it after one deploy.
    Optionally set RESET_BALANCE to control starting balance (default: portfolio.starting_balance).
    """
    if not os.getenv("RESET_SIMULATION"):
        return False

    balance = float(os.getenv("RESET_BALANCE", str(portfolio.starting_balance)))
    logger.warning("🔄 RESET_SIMULATION triggered — wiping all state, starting at $%.0f", balance)

    # Wipe positions file
    fresh = {
        "balance": balance,
        "starting_balance": balance,
        "total_pnl": 0.0, "daily_pnl": 0.0,
        "arb_count": 0, "arb_pnl": 0.0,
        "value_count": 0, "value_filled_count": 0,
        "value_edge_sum": 0.0, "value_pnl": 0.0,
        "pregame_arb_count": 0, "pregame_arb_pnl": 0.0,
        "midgame_arb_count": 0, "midgame_arb_pnl": 0.0,
        "pregame_value_count": 0, "pregame_value_pnl": 0.0,
        "midgame_value_count": 0, "midgame_value_pnl": 0.0,
        "maker_arb_count": 0, "maker_arb_pnl": 0.0,
        "maker_value_count": 0, "maker_value_pnl": 0.0,
        "early_exit_tiers": {},
        "positions": [],
    }
    try:
        POSITIONS_FILE.write_text(json.dumps(fresh, indent=2))
    except OSError:
        logger.exception("Failed to write reset positions file")

    # Wipe trade log
    trade_log = Path(TRADE_LOG_PATH)
    try:
        trade_log.write_text("")
    except OSError:
        logger.exception("Failed to wipe trade log")

    # Reset portfolio in-memory
    portfolio.current_balance = balance
    portfolio.starting_balance = balance
    portfolio.total_pnl = 0.0
    portfolio.daily_pnl = 0.0
    portfolio.arb_count = 0
    portfolio.arb_pnl = 0.0
    portfolio.value_count = 0
    portfolio.value_filled_count = 0
    portfolio.value_edge_sum = 0.0
    portfolio.value_pnl = 0.0
    portfolio.pregame_value_count = 0
    portfolio.pregame_value_pnl = 0.0
    portfolio.midgame_value_count = 0
    portfolio.midgame_value_pnl = 0.0
    portfolio.positions.clear()

    # Wipe inverted portfolio
    try:
        if INVERTED_FILE.exists():
            INVERTED_FILE.unlink()
            logger.warning("Wiped inverted positions file")
    except OSError:
        logger.exception("Failed to wipe inverted positions file")

    logger.warning("✅ Simulation reset complete — $%.0f balance, 0 positions, trade log wiped", balance)
    return True


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
                "pinnacle_prob_at_entry": p.pinnacle_prob_at_entry,
                "pinnacle_prob_latest": p.pinnacle_prob_latest,
                "pinnacle_prob_pregame_close": p.pinnacle_prob_pregame_close,
                "shadow_exits": p.shadow_exits,
                "trade_id": p.trade_id,
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
        saved_balance = data.get("balance", portfolio.starting_balance)
        saved_starting = data.get("starting_balance", 1000.0)
        # One-time migration: if starting_balance increased (deposit), add the difference
        if portfolio.starting_balance > saved_starting:
            deposit = portfolio.starting_balance - saved_starting
            saved_balance += deposit
            logger.info("Deposit detected: +$%.2f (starting %.0f → %.0f)", deposit, saved_starting, portfolio.starting_balance)
        portfolio.current_balance = saved_balance
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
                pinnacle_prob_at_entry=p_data.get("pinnacle_prob_at_entry", 0.0),
                pinnacle_prob_latest=p_data.get("pinnacle_prob_latest", 0.0),
                pinnacle_prob_pregame_close=p_data.get("pinnacle_prob_pregame_close", 0.0),
                shadow_exits=p_data.get("shadow_exits", {}),
                trade_id=p_data.get("trade_id", ""),
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


def backfill_counters(portfolio) -> int:
    """Reconstruct filled trade counters from the JSONL trade log.

    Only backfills if portfolio counters are zero (fresh deploy or missing
    from old positions file). Skips if counters are already populated
    to avoid double-counting.

    Returns number of filled trades found.
    """
    # Don't backfill if counters are already populated (loaded from positions file)
    if portfolio.value_filled_count > 0:
        logger.info("Counters already populated (%d filled), skipping backfill",
                     portfolio.value_filled_count)
        return portfolio.value_filled_count

    log_path = Path(TRADE_LOG_PATH)
    if not log_path.exists():
        return 0

    try:
        lines = log_path.read_text().strip().split("\n")
    except OSError:
        logger.exception("Failed to read trade log for backfill")
        return 0

    filled = 0
    edge_sum = 0.0
    value_count = 0
    pregame_count = 0
    midgame_count = 0

    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Only count VALUE_BET entries (filled trades)
        if entry.get("type") != "VALUE_BET":
            continue
        if entry.get("strategy") != "VALUE":
            continue
        if not entry.get("would_fill", False):
            continue

        filled += 1
        edge_sum += entry.get("edge_pct", 0)

        timing = entry.get("extra", {}).get("timing", "pregame") if isinstance(entry.get("extra"), dict) else "pregame"
        if timing == "pregame":
            pregame_count += 1
        else:
            midgame_count += 1

    # Also count all VALUE attempts (filled + rejected)
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("strategy") == "VALUE" and entry.get("type") in ("VALUE_BET", "VALUE_REJECTED"):
            value_count += 1

    if filled > 0:
        portfolio.value_filled_count = filled
        portfolio.value_edge_sum = edge_sum
        portfolio.value_count = value_count
        portfolio.pregame_value_count = pregame_count
        portfolio.midgame_value_count = midgame_count
        avg = edge_sum / filled if filled else 0
        logger.info(
            "Backfilled from trade log: %d filled trades, %d total attempts, avg edge %.1f%%",
            filled, value_count, avg,
        )

    return filled


# ── Inverted portfolio persistence ────────────────────────────────────


def save_inverted(inv_portfolio) -> None:
    """Save inverted shadow portfolio state to disk."""
    if inv_portfolio is None:
        return
    data = {
        "starting_balance": inv_portfolio.starting_balance,
        "current_balance": inv_portfolio.current_balance,
        "total_pnl": inv_portfolio.total_pnl,
        "trade_count": inv_portfolio.trade_count,
        "settled_count": inv_portfolio.settled_count,
        "win_count": inv_portfolio.win_count,
        "loss_count": inv_portfolio.loss_count,
        "pnl_history": inv_portfolio.pnl_history,
        "created_at": inv_portfolio.created_at,
        "skipped_not_complementary": inv_portfolio.skipped_not_complementary,
        "skipped_no_price": inv_portfolio.skipped_no_price,
        "skipped_price_too_high": inv_portfolio.skipped_price_too_high,
        "skipped_fill_missed": inv_portfolio.skipped_fill_missed,
        "positions": [
            {
                "trade_id": p.trade_id,
                "linked_trade_id": p.linked_trade_id,
                "match_id": p.match_id,
                "match_name": p.match_name,
                "platform": p.platform,
                "market_id": p.market_id,
                "team": p.team,
                "price": p.price,
                "size": p.size,
                "cost_usd": p.cost_usd,
                "opened_at": p.opened_at,
                "timing": p.timing,
                "pin_prob": p.pin_prob,
            }
            for p in inv_portfolio.positions
        ],
    }
    try:
        INVERTED_FILE.write_text(json.dumps(data, indent=2))
        logger.debug("Saved %d inverted positions to %s", len(inv_portfolio.positions), INVERTED_FILE)
    except OSError:
        logger.exception("Failed to save inverted positions")


def load_inverted(inv_portfolio) -> int:
    """Restore inverted portfolio from disk. Returns count of positions loaded."""
    if not INVERTED_FILE.exists():
        logger.info("No saved inverted positions found")
        return 0

    try:
        data = json.loads(INVERTED_FILE.read_text())

        inv_portfolio.starting_balance = data.get("starting_balance", inv_portfolio.starting_balance)
        inv_portfolio.current_balance = data.get("current_balance", inv_portfolio.starting_balance)
        inv_portfolio.total_pnl = data.get("total_pnl", 0.0)
        inv_portfolio.trade_count = data.get("trade_count", 0)
        inv_portfolio.settled_count = data.get("settled_count", 0)
        inv_portfolio.win_count = data.get("win_count", 0)
        inv_portfolio.loss_count = data.get("loss_count", 0)
        inv_portfolio.pnl_history = data.get("pnl_history", [])
        inv_portfolio.created_at = data.get("created_at", inv_portfolio.created_at)
        inv_portfolio.skipped_not_complementary = data.get("skipped_not_complementary", 0)
        inv_portfolio.skipped_no_price = data.get("skipped_no_price", 0)
        inv_portfolio.skipped_price_too_high = data.get("skipped_price_too_high", 0)
        inv_portfolio.skipped_fill_missed = data.get("skipped_fill_missed", 0)

        from live_bot.portfolio import InvertedPosition
        for p in data.get("positions", []):
            inv_portfolio.positions.append(InvertedPosition(
                trade_id=p["trade_id"],
                linked_trade_id=p["linked_trade_id"],
                match_id=p["match_id"],
                match_name=p.get("match_name", ""),
                platform=p["platform"],
                market_id=p["market_id"],
                team=p["team"],
                price=p["price"],
                size=p["size"],
                cost_usd=p["cost_usd"],
                opened_at=p["opened_at"],
                timing=p.get("timing", ""),
                pin_prob=p.get("pin_prob", 0.0),
            ))

        loaded = len(inv_portfolio.positions)
        logger.info(
            "Loaded inverted portfolio: %d positions, balance=$%.2f, P&L=$%.2f",
            loaded, inv_portfolio.current_balance, inv_portfolio.total_pnl,
        )
        return loaded

    except Exception:
        logger.exception("Failed to load inverted positions from %s", INVERTED_FILE)
        return 0
