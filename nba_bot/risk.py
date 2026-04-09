"""Risk controls for NBA bot — position limits, daily loss, Kelly sizing."""

import logging
import math
import time

from nba_bot.config import (
    NBA_BET_SIZE_USD,
    NBA_MAX_POSITIONS,
    NBA_MAX_PER_EVENT,
    MAX_DAILY_LOSS_PCT,
    MAX_EXPOSURE_PCT,
    MAX_BET_PCT,
    COOLDOWN_SECONDS,
    WALLET_CAP,
    KELLY_FRACTION,
    NBA_MIN_WIN_PROB,
)

logger = logging.getLogger(__name__)


def check_risk(portfolio, match_id: str, market_id: str, size_usd: float) -> tuple[bool, str]:
    """Check if a proposed NBA trade passes risk controls."""
    portfolio._check_daily_reset()

    if size_usd < 1.0:
        return False, f"Size ${size_usd:.2f} too small"

    if size_usd > NBA_BET_SIZE_USD * 3:
        return False, f"Size ${size_usd:.2f} exceeds 3x bet size"

    # Daily loss limit
    total_value = portfolio.total_portfolio_value
    max_daily_loss = total_value * (MAX_DAILY_LOSS_PCT / 100)
    if portfolio.daily_pnl < -max_daily_loss:
        return False, f"Daily loss limit: ${portfolio.daily_pnl:.2f}"

    # Max concurrent positions
    if len(portfolio.positions) >= NBA_MAX_POSITIONS:
        return False, f"Max {NBA_MAX_POSITIONS} positions"

    # Cooldown
    elapsed = time.time() - portfolio.last_trade_time
    if elapsed < COOLDOWN_SECONDS:
        return False, f"Cooldown: {COOLDOWN_SECONDS - elapsed:.0f}s"

    # Already in this market
    if market_id in portfolio.open_market_ids:
        return False, "Already in this market"

    # Per-event limit
    open_match_ids = [p.match_id for p in portfolio.positions]
    if open_match_ids.count(match_id) >= NBA_MAX_PER_EVENT:
        return False, f"Already {NBA_MAX_PER_EVENT} positions on this event"

    # Exposure limit
    if total_value > 0:
        deployed = sum(p.cost_usd for p in portfolio.positions)
        new_pct = (deployed + size_usd) / total_value * 100
        if new_pct > MAX_EXPOSURE_PCT:
            return False, f"Exposure {new_pct:.0f}% > {MAX_EXPOSURE_PCT:.0f}% cap"

    # Balance check
    if size_usd > portfolio.current_balance:
        return False, f"Insufficient balance: ${portfolio.current_balance:.2f}"

    return True, "OK"


def kelly_size(edge: float, win_prob: float, balance: float) -> float:
    """Half-Kelly sizing with underdog ramp."""
    if edge <= 0 or win_prob <= 0 or win_prob >= 1:
        return 0.0

    if win_prob < NBA_MIN_WIN_PROB:
        return 0.0

    market_price = win_prob / (1 + edge)
    if market_price <= 0 or market_price >= 1:
        return 0.0

    b = (1.0 / market_price) - 1.0
    q = 1.0 - win_prob
    kelly = max(0.0, (b * win_prob - q) / b)

    effective_balance = min(balance, WALLET_CAP)
    size = effective_balance * kelly * KELLY_FRACTION

    # Underdog ramp: scale down below 35% win prob
    full_size_prob = 0.35
    if win_prob < full_size_prob:
        ramp_floor = 0.20
        ramp = ramp_floor + (1.0 - ramp_floor) * (
            (win_prob - NBA_MIN_WIN_PROB) / (full_size_prob - NBA_MIN_WIN_PROB)
        )
        size *= max(ramp_floor, min(1.0, ramp))

    max_bet_usd = effective_balance * (MAX_BET_PCT / 100)
    size = min(size, NBA_BET_SIZE_USD, max_bet_usd)

    return max(0.0, size)
