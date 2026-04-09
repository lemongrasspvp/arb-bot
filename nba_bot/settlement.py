"""Settlement checker — resolve NBA bot positions via Polymarket Gamma API."""

import asyncio
import json as _json
import logging
import time

import requests

from nba_bot.config import SETTLEMENT_CHECK_INTERVAL
from nba_bot import logger as nba_logger
from nba_bot import skip_counters

logger = logging.getLogger(__name__)


async def settlement_loop(
    portfolio,
    registry,
    save_fn,
    shutdown_event: asyncio.Event,
) -> None:
    """Periodically check and settle resolved positions."""
    nba_logger.log_event("SETTLEMENT_START", "NBA settlement checker started")

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=SETTLEMENT_CHECK_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

        positions = list(portfolio.positions)
        if not positions:
            continue

        logger.info("Settlement: checking %d open positions...", len(positions))
        settled = 0

        for pos in positions:
            if shutdown_event.is_set():
                break
            if pos.platform != "polymarket":
                continue

            try:
                result = await _resolve_poly(pos)
                if result is None:
                    continue

                payout_per_share, mode = result

                # CLV
                pin_close = pos.pinnacle_prob_pregame_close or pos.pinnacle_prob_latest
                if pin_close <= 0:
                    pin_close = _get_pin_prob(pos, registry)
                clv_pct = (pin_close - pos.price) / pos.price if pin_close > 0 and pos.price > 0 else 0.0

                pnl = portfolio.settle_position(pos.market_id, payout_per_share=payout_per_share)
                settled += 1

                if payout_per_share >= 0.99:
                    skip_counters.inc("settled_won")
                elif payout_per_share <= 0.01:
                    skip_counters.inc("settled_lost")
                else:
                    skip_counters.inc("settled_other")

                label = "WON" if payout_per_share >= 0.99 else "LOST" if payout_per_share <= 0.01 else f"PARTIAL@{payout_per_share:.2f}"

                nba_logger.log_trade(
                    "SETTLEMENT",
                    match_name=pos.match_id,
                    match_id=pos.match_id,
                    platform=pos.platform,
                    team=pos.team,
                    price=pos.price,
                    size_usd=pos.cost_usd,
                    would_fill=True,
                    extra={
                        "won": payout_per_share >= 0.99,
                        "pnl": round(pnl, 2),
                        "balance": round(portfolio.current_balance, 2),
                        "payout_per_share": round(payout_per_share, 4),
                        "settlement_mode": mode,
                        "pinnacle_prob_at_entry": round(pos.pinnacle_prob_at_entry, 6),
                        "pinnacle_prob_at_close": round(pin_close, 6),
                        "clv_pct": round(clv_pct, 4),
                    },
                )

                logger.info(
                    "SETTLED: %s %s — %s (%.2f/share) | P&L=$%.2f | CLV=%+.1f%%",
                    pos.team, pos.match_id, label, payout_per_share, pnl, clv_pct * 100,
                )

            except Exception:
                logger.exception("Error checking resolution for %s", pos.market_id[:30])

            await asyncio.sleep(0.3)

        if settled > 0:
            save_fn()
            nba_logger.log_event(
                "SETTLEMENT_BATCH",
                f"Settled {settled} positions",
                balance=round(portfolio.current_balance, 2),
            )


async def _resolve_poly(pos) -> tuple[float, str] | None:
    """Check Polymarket Gamma API for market resolution."""
    token_id = pos.market_id
    if not token_id:
        return None

    try:
        resp = await asyncio.to_thread(
            requests.get,
            "https://gamma-api.polymarket.com/markets",
            params={"clob_token_ids": token_id},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data:
            return None

        market = data[0]
        if not market.get("closed", False):
            return None

        try:
            prices = _json.loads(market.get("outcomePrices", "[]"))
            tokens = _json.loads(market.get("clobTokenIds", "[]"))
        except (ValueError, TypeError):
            return None

        if not prices or not tokens:
            return None

        for tid, price in zip(tokens, prices):
            if tid == token_id:
                pps = float(price)
                if pps >= 0.99:
                    return (1.0, "win")
                elif pps <= 0.01:
                    return (0.0, "loss")
                elif abs(pps - 0.5) < 0.05:
                    return (pps, "split")
                else:
                    return (pps, "partial")

        return None

    except Exception:
        logger.debug("Error checking Poly resolution for %s", token_id[:30])
        return None


def _get_pin_prob(pos, registry) -> float:
    for match in registry.matches.values():
        if match.poly_token_id_a == pos.market_id:
            return match.pinnacle_prob_a
        if match.poly_token_id_b == pos.market_id:
            return match.pinnacle_prob_b
    return 0.0
