"""Shadow early-exit model — analytics only.

Tracks what P&L would be if value positions were sold at time-based
checkpoints before match start (30m, 10m, 5m, 1m). Does NOT affect
real positions or P&L. Uses executable bid-side VWAP, not mid/display.

At settlement, shadow exits are logged alongside real P&L for comparison.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Checkpoints: (label, minutes before start)
SHADOW_CHECKPOINTS = [
    ("30m", 30),
    ("10m", 10),
    ("5m", 5),
    ("1m", 1),
]


def _compute_bid_vwap(bid_levels: list[list], shares: float) -> tuple[float, float, bool]:
    """Compute executable bid-side VWAP for a given sell size.

    Args:
        bid_levels: [[price, size], ...] sorted by price descending (best bid first)
        shares: number of contracts to sell

    Returns:
        (vwap_price, executable_size, fully_executable)
    """
    if not bid_levels or shares <= 0:
        return 0.0, 0.0, False

    total_cost = 0.0
    total_filled = 0.0
    remaining = shares

    for level in bid_levels:
        if len(level) < 2:
            continue
        price, size = float(level[0]), float(level[1])
        if price <= 0 or size <= 0:
            continue

        fill = min(remaining, size)
        total_cost += fill * price
        total_filled += fill
        remaining -= fill

        if remaining <= 0:
            break

    if total_filled <= 0:
        return 0.0, 0.0, False

    vwap = total_cost / total_filled
    fully_executable = remaining <= 0
    return vwap, total_filled, fully_executable


def _kalshi_sell_fee(price: float) -> float:
    """Kalshi taker fee on sell side."""
    return 0.07 * price * (1 - price)


def check_shadow_exits(portfolio, registry, price_cache: dict) -> None:
    """Check all open VALUE positions for shadow exit checkpoints.

    Called on every Pinnacle poll cycle (~8s). For each position, checks
    if any uncaptured checkpoint has been reached (based on commence_time).
    If so, snapshots the current bid-side data.
    """
    now = time.time()
    now_dt = datetime.now(timezone.utc)

    for pos in portfolio.positions:
        if pos.strategy != "VALUE":
            continue

        # Find the match to get commence_time
        match = registry.matches.get(pos.match_id)
        if not match or not match.commence_time:
            continue

        # Parse commence_time
        try:
            ct = match.commence_time
            if ct.endswith("Z"):
                ct = ct[:-1] + "+00:00"
            commence_dt = datetime.fromisoformat(ct)
        except (ValueError, TypeError):
            continue

        # Get current market data
        cached = price_cache.get(pos.platform, {}).get(pos.market_id, {})
        best_bid = cached.get("best_bid", 0)
        bid_levels = cached.get("bid_levels", [])
        bid_ts = cached.get("timestamp", 0)

        for label, minutes in SHADOW_CHECKPOINTS:
            # Already captured this checkpoint
            if label in pos.shadow_exits:
                continue

            checkpoint_dt = commence_dt - timedelta(minutes=minutes)

            # Position was opened after this checkpoint — not applicable
            if pos.opened_at > checkpoint_dt.timestamp():
                pos.shadow_exits[label] = {"skipped": "opened_after_checkpoint"}
                continue

            # Checkpoint not reached yet
            if now_dt < checkpoint_dt:
                continue

            # Match already started — missed this checkpoint
            if now_dt >= commence_dt:
                pos.shadow_exits[label] = {"skipped": "match_already_started"}
                continue

            # --- Capture shadow exit snapshot ---
            time_to_start = (commence_dt - now_dt).total_seconds()

            # Compute bid-side VWAP at full position size
            if bid_levels:
                bid_vwap, exec_size, fully_exec = _compute_bid_vwap(bid_levels, pos.size)
            elif best_bid > 0:
                bid_vwap = best_bid
                exec_size = pos.size  # assume executable at best bid
                fully_exec = True
            else:
                # No bid data available
                pos.shadow_exits[label] = {
                    "skipped": "no_bid_data",
                    "ts": now,
                    "time_to_start": round(time_to_start),
                }
                continue

            # Calculate shadow P&L
            sell_proceeds = bid_vwap * exec_size

            # Subtract platform fees
            if pos.platform == "kalshi":
                fee = _kalshi_sell_fee(bid_vwap) * exec_size
                sell_proceeds -= fee

            # Cost basis proportional to executable size
            cost_basis = pos.price * exec_size
            shadow_pnl = sell_proceeds - cost_basis
            shadow_roi = shadow_pnl / cost_basis if cost_basis > 0 else 0.0

            # Spread at checkpoint
            best_ask = cached.get("best_ask", 0)
            spread = best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0

            # Also compute half-exit (sell 50%, hold 50%)
            half_size = pos.size / 2
            if bid_levels:
                half_vwap, half_exec, half_fully = _compute_bid_vwap(bid_levels, half_size)
            else:
                half_vwap, half_exec, half_fully = bid_vwap, half_size, True
            half_sell_proceeds = half_vwap * min(half_exec, half_size)
            if pos.platform == "kalshi":
                half_sell_proceeds -= _kalshi_sell_fee(half_vwap) * min(half_exec, half_size)
            half_cost = pos.price * min(half_exec, half_size)
            half_pnl = half_sell_proceeds - half_cost

            snapshot = {
                "ts": round(now, 2),
                "time_to_start": round(time_to_start),
                "bid_vwap": round(bid_vwap, 4),
                "best_bid": round(best_bid, 4),
                "best_ask": round(best_ask, 4),
                "spread": round(spread, 4),
                "executable_size": round(exec_size, 2),
                "fully_executable": fully_exec,
                "shadow_pnl": round(shadow_pnl, 2),
                "shadow_roi": round(shadow_roi, 4),
                "half_exit_pnl": round(half_pnl, 2),
                "bid_data_age": round(now - bid_ts, 1) if bid_ts > 0 else None,
            }

            pos.shadow_exits[label] = snapshot

            logger.info(
                "SHADOW EXIT [%s] %s@%s: bid_vwap=%.0f¢ spread=%.0f¢ "
                "shadow_pnl=$%.2f (%.1f%%) | %ds to start | %s",
                label, pos.team, pos.platform,
                bid_vwap * 100, spread * 100,
                shadow_pnl, shadow_roi * 100,
                time_to_start,
                "FULL" if fully_exec else f"PARTIAL ({exec_size:.0f}/{pos.size:.0f})",
            )
