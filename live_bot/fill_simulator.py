"""Realistic fill simulator for simulation mode.

Models three real-world effects that paper trading typically ignores:

1. COMPETITION — Other bots are scanning the same prices. The probability
   that depth survives long enough for you to fill depends on how much
   dollar depth is available. $100 of depth gets sniped fast; $5000 is
   more likely to still be there when your order arrives.

2. STALENESS — Cached prices age. Polymarket WS prices are ~0.5s old;
   Kalshi REST prices are up to 15s old. The older the price, the less
   likely it still exists at that level.

3. SLIPPAGE — Even when you fill, the actual execution price may be
   slightly worse than the displayed price, especially on thin books.

The model uses exponential decay curves calibrated to esports prediction
markets where arbs are highly competitive and books are relatively thin.
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
    """Core fill model for a single order.

    Fill probability = depth_survival × freshness

    depth_survival: How likely is the depth still there given competition?
      - Modeled as 1 - exp(-depth_usd / characteristic_depth)
      - Arbs: characteristic_depth = $800 (highly competitive)
      - Value: characteristic_depth = $2000 (less competitive)
      - At $100 depth, arb fill ≈ 12%. At $1000 ≈ 71%. At $5000 ≈ 99.8%.
      - At $100 depth, value fill ≈ 49%. At $1000 ≈ 93%. At $5000 ≈ 99.9%.

    freshness: How likely is the price still valid given its age?
      - Polymarket WS: prices are ~0.5s old → high freshness
      - Kalshi REST: prices are up to 15s old → much lower freshness
      - Modeled as exp(-age / half_life)
      - Arb half-life: 3s (arb prices move fast under competition)
      - Value half-life: 15s (value edges persist longer)
    """
    # --- Depth survival (competition) ---
    if is_arb:
        # Arbs are visible to everyone → high competition
        char_depth = 800.0  # $800 characteristic depth
    else:
        # Value bets need a model → less competition
        char_depth = 2000.0

    if depth_usd <= 0:
        # No depth data available: assume moderate depth (~$500)
        # This is generous but avoids blocking all trades when depth is unknown
        depth_usd = 500.0

    depth_survival = 1.0 - math.exp(-depth_usd / char_depth)

    # --- Price freshness ---
    if is_arb:
        half_life = 3.0  # arb prices go stale fast
    else:
        half_life = 15.0  # value edges last longer

    # Platform-specific: Kalshi REST adds ~7.5s average staleness on top
    # But if we're simulating having WS access, skip this penalty
    if platform == "kalshi" and not SIMULATE_KALSHI_WS:
        price_age_seconds += 7.5  # average of 0-15s polling window

    freshness = math.exp(-price_age_seconds * math.log(2) / half_life)

    # --- Combined fill probability ---
    fill_prob = depth_survival * freshness

    # Clamp to reasonable range
    fill_prob = max(0.02, min(0.98, fill_prob))

    # Roll the dice
    filled = random.random() < fill_prob

    # --- Slippage (if filled) ---
    slippage = 0.0
    if filled:
        slippage = _simulate_slippage(depth_usd, price_age_seconds, is_arb)

    logger.debug(
        "Fill sim: depth=$%.0f age=%.1fs %s %s → prob=%.0f%% (depth_surv=%.0f%%, fresh=%.0f%%) → %s slip=%.1f¢",
        depth_usd, price_age_seconds, platform,
        "ARB" if is_arb else "VALUE",
        fill_prob * 100, depth_survival * 100, freshness * 100,
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
