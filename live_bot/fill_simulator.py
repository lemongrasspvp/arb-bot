"""Fill simulator for simulation mode — calibrated to real-money execution.

Models real-world fill behavior for market orders on prediction markets:

1. VALUE BETS — Market orders fill almost always (~97%). The only miss
   is when the price moves during the 1-3s order latency. Value edges
   are less visible to competitors, so prices are stable.

2. ARB BETS — More competitive. Other bots see the same mispricing and
   race to fill. Fill rates depend on depth and price staleness.
   Typical fill rate: 70-90% depending on depth and latency.

3. SLIPPAGE — Even when you fill, the actual execution price may be
   slightly worse than the displayed price, especially on thin books.
"""

import logging
import math
import random
import time

from live_bot.config import SIMULATE_KALSHI_WS

logger = logging.getLogger(__name__)


def simulate_arb_fill(
    price_a: float,
    price_b: float,
    depth_usd_a: float,
    depth_usd_b: float,
    price_age_a: float,
    price_age_b: float,
    platform_a: str,
    platform_b: str,
) -> tuple[bool, bool, float, float]:
    """Simulate whether both arb legs would fill in reality.

    Args:
        price_a/b: Ask price on each leg
        depth_usd_a/b: Available USD depth at that price (shares * price)
        price_age_a/b: How old the cached price is (seconds)
        platform_a/b: "polymarket" or "kalshi"

    Returns:
        (filled_a, filled_b, slip_a, slip_b)
        filled: whether the leg would have filled
        slip: price slippage in cents (added to cost)
    """
    fill_a, slip_a = _simulate_single_fill(
        depth_usd_a, price_age_a, platform_a, is_arb=True
    )
    fill_b, slip_b = _simulate_single_fill(
        depth_usd_b, price_age_b, platform_b, is_arb=True
    )
    return fill_a, fill_b, slip_a, slip_b


def simulate_value_fill(
    price: float,
    depth_usd: float,
    price_age: float,
    platform: str,
) -> tuple[bool, float]:
    """Simulate whether a value bet would fill.

    Value bets are less competitive than arbs (require a specific edge
    model + Pinnacle reference), so fill rates are higher.

    Returns:
        (filled, slippage_cents)
    """
    return _simulate_single_fill(
        depth_usd, price_age, platform, is_arb=False
    )


def _simulate_single_fill(
    depth_usd: float,
    price_age_seconds: float,
    platform: str,
    is_arb: bool,
) -> tuple[bool, float]:
    """Core fill model for a single order — calibrated to real market orders.

    VALUE BETS (is_arb=False):
      Market orders on prediction markets fill almost always. The only
      miss scenario is the price moving during the 1-3s between seeing
      the quote and the order arriving at the exchange. Value edges are
      less visible to other bots (require a model + Pinnacle reference),
      so the price is very stable.
      → Base fill rate: 97%, with a small latency penalty for stale prices.

    ARB BETS (is_arb=True):
      Arb opportunities are visible to every bot scanning the same feeds.
      Multiple bots race to fill the same depth, so competition matters.
      Fill rate depends on depth available and price staleness.
      → Fill rate: 70-95% depending on conditions.
    """
    if not is_arb:
        # ── VALUE BET: market order, ~97% base fill rate ──
        # Only miss if the price moved during order latency.
        # Small penalty for very stale prices (>10s old).
        base_fill = 0.97

        # Platform-specific staleness
        effective_age = price_age_seconds
        if platform == "kalshi" and not SIMULATE_KALSHI_WS:
            effective_age += 7.5

        # Gentle latency penalty: lose ~1% per 5s of staleness
        # At 0s age → 97%, at 5s → 96%, at 15s → 94%
        latency_penalty = effective_age * 0.002
        fill_prob = max(0.90, base_fill - latency_penalty)

    else:
        # ── ARB BET: competitive, depth and freshness matter ──
        char_depth = 800.0

        if depth_usd <= 0:
            depth_usd = 500.0

        depth_survival = 1.0 - math.exp(-depth_usd / char_depth)

        if platform == "kalshi" and not SIMULATE_KALSHI_WS:
            price_age_seconds += 7.5

        half_life = 3.0
        freshness = math.exp(-price_age_seconds * math.log(2) / half_life)

        fill_prob = depth_survival * freshness

    # Clamp to reasonable range
    fill_prob = max(0.05, min(0.98, fill_prob))

    # Roll the dice
    filled = random.random() < fill_prob

    # --- Slippage (if filled) ---
    slippage = 0.0
    if filled:
        slippage = _simulate_slippage(depth_usd, price_age_seconds, is_arb)

    logger.debug(
        "Fill sim: depth=$%.0f age=%.1fs %s %s → prob=%.0f%% → %s slip=%.1f¢",
        depth_usd, price_age_seconds, platform,
        "ARB" if is_arb else "VALUE",
        fill_prob * 100,
        "FILL" if filled else "MISS", slippage * 100,
    )

    return filled, slippage


def _simulate_slippage(
    depth_usd: float,
    price_age: float,
    is_arb: bool,
) -> float:
    """Model price slippage: how much worse is the actual fill price?

    Slippage comes from:
    1. Book depth: thin books → more slippage to fill your size
    2. Price movement: older prices → more likely to have moved

    Returns slippage as a fraction (e.g., 0.01 = 1 cent worse).
    """
    # Base slippage: random 0-1 cent
    base = random.uniform(0, 0.01)

    # Depth-based: thinner books → more slippage
    # At $100 depth: ~1.5 cents extra; at $5000: ~0.1 cents
    if depth_usd > 0:
        depth_slip = 0.015 * math.exp(-depth_usd / 500)
    else:
        depth_slip = 0.005

    # Staleness-based: older prices → more likely to have moved
    # ~0.5 cents per 5 seconds of age
    age_slip = min(0.03, price_age * 0.001)

    total = base + depth_slip + age_slip

    # Cap at 3 cents — beyond this you'd probably just not fill
    return min(total, 0.03)
