"""Settlement checker — resolves open positions when markets finalize."""

import asyncio
import logging
import time

import requests

from live_bot.config import KALSHI_REST_BASE, SETTLEMENT_CHECK_INTERVAL
from live_bot.logger import log_trade, log_event

logger = logging.getLogger(__name__)

# Polymarket CLOB base
POLY_CLOB_BASE = "https://clob.polymarket.com"


async def settlement_loop(
    portfolio,
    registry,
    persistence_save_fn,
    shutdown_event: asyncio.Event,
) -> None:
    """Periodically check if open positions have resolved and settle them."""
    log_event("SETTLEMENT_START", "Settlement checker started")

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=SETTLEMENT_CHECK_INTERVAL
            )
            break  # shutdown
        except asyncio.TimeoutError:
            pass  # time to check

        positions = list(portfolio.positions)
        if not positions:
            continue

        logger.info("Settlement checker: checking %d open positions...", len(positions))
        settled_count = 0

        for pos in positions:
            if shutdown_event.is_set():
                break

            try:
                won = await _check_resolution(pos, registry)
                if won is None:
                    # Not yet resolved
                    continue

                # CLV: capture Pinnacle closing line at settlement time
                pinnacle_prob_at_close = _get_pinnacle_closing_prob(pos, registry)
                clv = 0.0
                if pos.pinnacle_prob_at_entry > 0 and pinnacle_prob_at_close > 0:
                    # CLV = closing prob - entry price
                    # Positive CLV means we bought below the closing line (good)
                    clv = pinnacle_prob_at_close - pos.price

                # Settle
                pnl = portfolio.settle_position(pos.market_id, won)
                settled_count += 1

                log_trade(
                    "SETTLEMENT",
                    pos.strategy,
                    match_name=pos.match_id,
                    match_id=pos.match_id,
                    platform_a=pos.platform,
                    team_a=pos.team,
                    price_a=pos.price,
                    size_usd=pos.cost_usd,
                    simulated=True,
                    would_fill=True,
                    filled_a=True,
                    pinnacle_prob=pinnacle_prob_at_close,
                    extra={
                        "won": won,
                        "pnl": round(pnl, 2),
                        "balance": round(portfolio.current_balance, 2),
                        "pinnacle_prob_at_entry": round(pos.pinnacle_prob_at_entry, 6),
                        "pinnacle_prob_at_close": round(pinnacle_prob_at_close, 6),
                        "clv": round(clv, 4),
                    },
                )

                logger.info(
                    "SETTLED: %s %s@%s — %s | P&L=$%.2f | CLV=%.1f¢",
                    pos.team, pos.platform, pos.match_id,
                    "WON" if won else "LOST", pnl, clv * 100,
                )

            except Exception:
                logger.exception("Error checking resolution for %s", pos.market_id)

            # Rate limit between API calls
            await asyncio.sleep(0.3)

        if settled_count > 0:
            persistence_save_fn()
            log_event(
                "SETTLEMENT_BATCH",
                f"Settled {settled_count} positions",
                balance=round(portfolio.current_balance, 2),
            )


async def _check_resolution(pos, registry) -> bool | None:
    """Check if a position's market has resolved.

    Returns True (won), False (lost), or None (not yet resolved).
    """
    if pos.platform == "polymarket":
        return await _check_poly_resolution(pos, registry)
    elif pos.platform == "kalshi":
        return await _check_kalshi_resolution(pos)
    return None


async def _check_poly_resolution(pos, registry) -> bool | None:
    """Check Polymarket market resolution via CLOB API."""
    # Find the condition_id for this position
    condition_id = pos.condition_id
    if not condition_id:
        # Try to find it from the registry
        condition_id = _find_condition_id(pos, registry)
        if not condition_id:
            logger.debug("No condition_id for Poly position %s", pos.market_id)
            return None

    try:
        resp = await asyncio.to_thread(
            requests.get,
            f"{POLY_CLOB_BASE}/markets/{condition_id}",
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()

        # Market must be closed to have a resolution
        if not data.get("closed", False):
            return None

        # Check which token won
        tokens = data.get("tokens", [])
        for token in tokens:
            if token.get("token_id") == pos.market_id:
                return token.get("winner", False)

        # If our token wasn't in the response, check by outcome name
        # (fallback — shouldn't normally happen)
        return None

    except Exception:
        logger.debug("Error checking Poly resolution for %s", condition_id)
        return None


async def _check_kalshi_resolution(pos) -> bool | None:
    """Check Kalshi market resolution via public REST API."""
    ticker = pos.market_id
    if not ticker:
        return None

    try:
        resp = await asyncio.to_thread(
            requests.get,
            f"{KALSHI_REST_BASE}/markets/{ticker}",
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        market = resp.json().get("market", {})
        status = market.get("status", "")

        # Kalshi uses "finalized" for settled markets
        if status not in ("finalized", "settled"):
            return None

        result = market.get("result", "")

        # Determine if our bet won.
        # We buy YES on the outcome we're backing. If the market result
        # matches our team's outcome (result="yes"), we win.
        # For arbs: we buy YES on team A here AND YES on team B elsewhere.
        # Only one can win, which is correct — one leg wins, one loses.
        if result == "yes":
            return True
        elif result == "no":
            return False
        else:
            logger.warning("Unknown Kalshi result '%s' for %s", result, ticker)
            return None

    except Exception:
        logger.debug("Error checking Kalshi resolution for %s", ticker)
        return None


def _find_condition_id(pos, registry) -> str:
    """Look up the Polymarket condition_id from the registry for a position."""
    for match in registry.matches.values():
        if match.poly_token_id_a == pos.market_id or match.poly_token_id_b == pos.market_id:
            return match.poly_condition_id
    return ""


def _get_pinnacle_closing_prob(pos, registry) -> float:
    """Get the latest Pinnacle probability for a position's team (closing line).

    This captures the Pinnacle line at settlement time — compared against
    the entry price, this gives CLV (Closing Line Value).
    """
    for match in registry.matches.values():
        if match.poly_token_id_a == pos.market_id or (
            pos.platform == "kalshi" and match.kalshi_ticker_a == pos.market_id
        ):
            return match.pinnacle_prob_a
        if match.poly_token_id_b == pos.market_id or (
            pos.platform == "kalshi" and match.kalshi_ticker_b == pos.market_id
        ):
            return match.pinnacle_prob_b
    return 0.0
