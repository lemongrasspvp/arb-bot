"""Fill simulator — orderbook re-check model for post-and-cancel execution.

Instead of probabilistic fill rates, this simulator:
1. Waits a configurable delay (default 2s) to simulate order resting time
2. Re-fetches the orderbook from the exchange
3. Walks the book at the limit price to determine how many shares fill
4. Returns full, partial, or zero fill based on actual available liquidity

This replaces the old 97%/90% flat-rate model with a direct observation.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


async def simulate_value_fill_recheck(
    platform: str,
    market_id: str,
    limit_price: float,
    intended_shares: int,
    delay_seconds: float = 2.0,
) -> tuple[int, float]:
    """Simulate a limit order by re-checking the orderbook after a delay.

    1. Wait `delay_seconds` (simulates order resting time)
    2. Re-fetch the orderbook from the exchange
    3. Walk the book: count how many shares are available at or below limit_price
    4. Return (filled_shares, fill_vwap) — may be full, partial, or zero

    Args:
        platform: "polymarket" or "kalshi"
        market_id: token_id or ticker
        limit_price: max price we're willing to pay (our limit order price)
        intended_shares: how many shares we want
        delay_seconds: how long to wait before re-checking

    Returns:
        (filled_shares, fill_vwap) — 0 shares means no fill
    """
    # Wait to simulate order resting time
    await asyncio.sleep(delay_seconds)

    # Re-fetch the orderbook
    try:
        ask_levels = await _fetch_orderbook(platform, market_id)
    except Exception as exc:
        logger.warning("Fill recheck: failed to fetch book for %s — %s", market_id[:30], exc)
        return 0, 0.0

    if not ask_levels:
        logger.debug("Fill recheck: empty book for %s", market_id[:30])
        return 0, 0.0

    # Walk the book at our limit price + slippage tolerance.
    # In real trading you'd place a limit 1-2¢ above the ask to account
    # for normal book movement. Without this, the 2s recheck almost always
    # fails because the book shifts by even 1¢.
    SLIPPAGE_TOLERANCE = 0.02  # accept up to 2¢ above our target price
    effective_limit = limit_price + SLIPPAGE_TOLERANCE

    filled_shares = 0
    total_cost = 0.0

    for price, size_shares in sorted(ask_levels, key=lambda x: x[0]):
        if price > effective_limit:
            break  # Beyond our limit + tolerance

        # How many shares can we take from this level?
        remaining = intended_shares - filled_shares
        take = min(remaining, size_shares)

        filled_shares += int(take)
        total_cost += price * int(take)

        if filled_shares >= intended_shares:
            break

    if filled_shares <= 0:
        return 0, 0.0

    fill_vwap = total_cost / filled_shares
    return filled_shares, fill_vwap


async def _fetch_orderbook(platform: str, market_id: str) -> list[tuple[float, float]]:
    """Fetch current ask levels from the exchange.

    Returns list of (price, size_in_shares) tuples.
    """
    if platform == "polymarket":
        return await _fetch_poly_book(market_id)
    elif platform == "kalshi":
        return await _fetch_kalshi_book(market_id)
    return []


async def _fetch_poly_book(token_id: str) -> list[tuple[float, float]]:
    """Fetch Polymarket CLOB orderbook for a token."""
    import requests

    try:
        resp = await asyncio.to_thread(
            requests.get,
            f"https://clob.polymarket.com/book",
            params={"token_id": token_id},
            timeout=5,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        asks = data.get("asks", [])
        levels = []
        for ask in asks:
            price = float(ask.get("price", 0))
            size = float(ask.get("size", 0))
            if price > 0 and size > 0:
                levels.append((price, size))
        return levels

    except Exception as exc:
        logger.debug("Poly book fetch error: %s", exc)
        return []


async def _fetch_kalshi_book(ticker: str) -> list[tuple[float, float]]:
    """Fetch Kalshi orderbook for a ticker."""
    import requests
    from live_bot.config import KALSHI_REST_BASE

    try:
        resp = await asyncio.to_thread(
            requests.get,
            f"{KALSHI_REST_BASE}/markets/{ticker}/orderbook",
            timeout=5,
        )
        if resp.status_code != 200:
            return []

        data = resp.json().get("orderbook", {})
        asks = data.get("yes", [])  # yes asks = what we buy
        levels = []
        for ask in asks:
            price = float(ask.get("price", 0)) / 100  # Kalshi uses cents
            size = float(ask.get("quantity", 0))
            if price > 0 and size > 0:
                levels.append((price, size))
        return levels

    except Exception as exc:
        logger.debug("Kalshi book fetch error: %s", exc)
        return []


# ── Legacy arb fill simulator (kept for arb strategy if re-enabled) ──

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
    """Legacy arb fill simulation (probabilistic). Kept for arb strategy."""
    import math
    import random

    fill_a, slip_a = _legacy_single_fill(depth_usd_a, price_age_a, platform_a)
    fill_b, slip_b = _legacy_single_fill(depth_usd_b, price_age_b, platform_b)
    return fill_a, fill_b, slip_a, slip_b


def _legacy_single_fill(
    depth_usd: float,
    price_age_seconds: float,
    platform: str,
) -> tuple[bool, float]:
    """Legacy probabilistic fill for arb legs."""
    import math
    import random
    from live_bot.config import SIMULATE_KALSHI_WS

    char_depth = 800.0
    if depth_usd <= 0:
        depth_usd = 500.0

    depth_survival = 1.0 - math.exp(-depth_usd / char_depth)

    if platform == "kalshi" and not SIMULATE_KALSHI_WS:
        price_age_seconds += 7.5

    half_life = 3.0
    freshness = math.exp(-price_age_seconds * math.log(2) / half_life)

    fill_prob = depth_survival * freshness
    fill_prob = max(0.05, min(0.98, fill_prob))

    filled = random.random() < fill_prob
    slippage = 0.0

    return filled, slippage


# Also keep the old function name as alias for backward compat
def _simulate_single_fill(depth_usd, price_age_seconds, platform, is_arb=True):
    """Backward compat wrapper."""
    return _legacy_single_fill(depth_usd, price_age_seconds, platform)
