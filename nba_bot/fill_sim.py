"""Fill simulator — depth-aware orderbook recheck for NBA bot.

Simulates a taker limit order:
1. Wait a configurable delay (simulates order resting / network latency)
2. Re-fetch the LIVE orderbook from Polymarket CLOB
3. Walk the ask levels at our limit price + slippage tolerance
4. Return (filled_shares, fill_vwap) — may be full, partial, or zero

No midpoint fills. No probabilistic models. Direct book observation.
"""

import asyncio
import logging

from nba_bot.config import FILL_RECHECK_DELAY_S, SLIPPAGE_TOLERANCE

logger = logging.getLogger(__name__)


async def simulate_fill(
    market_id: str,
    limit_price: float,
    intended_shares: int,
    delay_seconds: float | None = None,
) -> tuple[int, float]:
    """Simulate a limit buy by rechecking the Polymarket orderbook.

    Args:
        market_id: Polymarket token_id
        limit_price: max price willing to pay (our limit)
        intended_shares: how many shares we want
        delay_seconds: override for recheck delay

    Returns:
        (filled_shares, fill_vwap) — 0 shares means no fill
    """
    delay = delay_seconds if delay_seconds is not None else FILL_RECHECK_DELAY_S
    await asyncio.sleep(delay)

    try:
        ask_levels = await _fetch_poly_book(market_id)
    except Exception as exc:
        logger.warning("Fill recheck failed for %s: %s", market_id[:30], exc)
        return 0, 0.0

    if not ask_levels:
        logger.debug("Empty book for %s", market_id[:30])
        return 0, 0.0

    effective_limit = limit_price + SLIPPAGE_TOLERANCE

    filled_shares = 0
    total_cost = 0.0

    for price, size_shares in sorted(ask_levels, key=lambda x: x[0]):
        if price > effective_limit:
            break

        remaining = intended_shares - filled_shares
        take = min(remaining, int(size_shares))

        filled_shares += take
        total_cost += price * take

        if filled_shares >= intended_shares:
            break

    if filled_shares <= 0:
        return 0, 0.0

    fill_vwap = total_cost / filled_shares
    return filled_shares, fill_vwap


async def _fetch_poly_book(token_id: str) -> list[tuple[float, float]]:
    """Fetch Polymarket CLOB ask levels for a token."""
    import requests

    resp = await asyncio.to_thread(
        requests.get,
        "https://clob.polymarket.com/book",
        params={"token_id": token_id},
        timeout=5,
    )
    if resp.status_code != 200:
        return []

    data = resp.json()
    levels = []
    for ask in data.get("asks", []):
        price = float(ask.get("price", 0))
        size = float(ask.get("size", 0))
        if price > 0 and size > 0:
            levels.append((price, size))
    return levels
