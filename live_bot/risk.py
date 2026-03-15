"""Risk controls — position limits, daily loss limits, cooldowns."""

import logging
import time
from dataclasses import dataclass

from live_bot.config import (
    MAX_POSITION_USD,
    MAX_DAILY_LOSS_USD,
    MAX_CONCURRENT_POSITIONS,
    COOLDOWN_SECONDS,
    KELLY_FRACTION,
    MIN_WIN_PROB,
    TIER_MID_PROB,
    TIER_LOW_MAX_USD,
    TIER_HIGH_MAX_USD,
)

logger = logging.getLogger(__name__)


@dataclass
class ProposedTrade:
    """A trade we're considering executing."""
    strategy: str       # "ARB" or "VALUE"
    match_id: str
    market_id: str      # token_id or ticker
    size_usd: float
    edge_pct: float     # for value bets


def check_risk(portfolio, trade: ProposedTrade) -> tuple[bool, str]:
    """Check if a proposed trade passes all risk controls.

    Returns (allowed, reason). If not allowed, reason explains why.
    """
    portfolio._check_daily_reset()

    # Max position size per trade
    if trade.size_usd > MAX_POSITION_USD:
        return False, f"Size ${trade.size_usd:.2f} exceeds max ${MAX_POSITION_USD:.2f}"

    # Minimum viable size
    if trade.size_usd < 1.0:
        return False, f"Size ${trade.size_usd:.2f} too small"

    # Daily loss limit (primarily for value bets which can lose)
    if portfolio.daily_pnl < -MAX_DAILY_LOSS_USD:
        return False, f"Daily loss limit hit: ${portfolio.daily_pnl:.2f}"

    # Max concurrent open positions
    if len(portfolio.open_positions) >= MAX_CONCURRENT_POSITIONS:
        return False, f"Max {MAX_CONCURRENT_POSITIONS} concurrent positions reached"

    # Cooldown between trades
    elapsed = time.time() - portfolio.last_trade_time
    if elapsed < COOLDOWN_SECONDS:
        return False, f"Cooldown: {COOLDOWN_SECONDS - elapsed:.1f}s remaining"

    # Don't value bet on same market twice (already have exposure)
    if trade.strategy == "VALUE" and trade.market_id in portfolio.open_market_ids:
        return False, f"Already have position in {trade.market_id}"

    # Sufficient balance
    if trade.size_usd > portfolio.current_balance:
        return False, f"Insufficient balance: ${portfolio.current_balance:.2f}"

    return True, "OK"


def kelly_size(
    edge: float,
    win_prob: float,
    balance: float,
) -> float:
    """Calculate half-Kelly optimal bet size with win probability tiers.

    Tiers (configurable in .env):
        <30% win prob  → skip (too much variance)
        30-50% win prob → max $25 (medium confidence)
        >50% win prob  → max $50 (high confidence)

    Args:
        edge: (pinnacle_prob / market_price) - 1  (e.g. 0.10 = 10% edge)
        win_prob: Pinnacle's no-vig probability (0-1)
        balance: Current portfolio balance

    Returns:
        Optimal bet size in USD, capped by tier.
    """
    if edge <= 0 or win_prob <= 0 or win_prob >= 1:
        return 0.0

    # Tier 0: Skip low-probability bets entirely
    if win_prob < MIN_WIN_PROB:
        logger.debug(
            "Skipping bet: win_prob=%.0f%% below minimum %.0f%%",
            win_prob * 100, MIN_WIN_PROB * 100,
        )
        return 0.0

    # Determine tier max
    if win_prob >= TIER_MID_PROB:
        tier_max = TIER_HIGH_MAX_USD   # high confidence
        tier_name = "HIGH"
    else:
        tier_max = TIER_LOW_MAX_USD    # medium confidence
        tier_name = "MID"

    # Kelly formula: f* = (bp - q) / b
    # where b = net odds received, p = win prob, q = 1 - p
    market_price = win_prob / (1 + edge)
    if market_price <= 0 or market_price >= 1:
        return 0.0

    b = (1.0 / market_price) - 1.0  # net odds (what you win per $1 risked)
    q = 1.0 - win_prob

    kelly = (b * win_prob - q) / b
    kelly = max(0.0, kelly)

    # Half-Kelly for safety
    size = balance * kelly * KELLY_FRACTION

    # Cap by tier AND global max
    size = min(size, tier_max, MAX_POSITION_USD)

    logger.debug(
        "Kelly sizing: prob=%.0f%% edge=%.1f%% kelly=%.4f → $%.2f (tier=%s, max=$%.0f)",
        win_prob * 100, edge * 100, kelly, size, tier_name, tier_max,
    )

    # Floor at $0
    return max(0.0, size)
