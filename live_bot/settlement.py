"""Settlement checker — resolves open positions when markets finalize.

Supports numeric payout-per-share settlement (not just binary win/loss):
  - 1.0 = full win
  - 0.0 = full loss
  - 0.5 = split / cancelled (e.g. Polymarket 50/50)
  - any value in [0, 1] for partial payouts
"""

import asyncio
import logging
import time

import requests

from live_bot.config import KALSHI_REST_BASE, SETTLEMENT_CHECK_INTERVAL
from live_bot.logger import log_trade, log_event

logger = logging.getLogger(__name__)


async def settlement_loop(
    portfolio,
    registry,
    persistence_save_fn,
    shutdown_event: asyncio.Event,
    inverted_portfolio=None,
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
                result = await _resolve_settlement_value(pos, registry)
                if result is None:
                    # Not yet resolved
                    continue

                payout_per_share, settlement_mode, settlement_source = result

                # CLV computation
                pinnacle_prob_at_close = pos.pinnacle_prob_pregame_close
                if pinnacle_prob_at_close <= 0:
                    pinnacle_prob_at_close = pos.pinnacle_prob_latest
                if pinnacle_prob_at_close <= 0:
                    pinnacle_prob_at_close = _get_pinnacle_closing_prob(pos, registry)
                clv = 0.0
                clv_pct = 0.0
                pin_drift = 0.0
                pin_drift_pct = 0.0
                if pinnacle_prob_at_close > 0 and pos.price > 0:
                    clv = pinnacle_prob_at_close - pos.price
                    clv_pct = clv / pos.price
                if pos.pinnacle_prob_at_entry > 0 and pinnacle_prob_at_close > 0:
                    pin_drift = pinnacle_prob_at_close - pos.pinnacle_prob_at_entry
                    pin_drift_pct = pin_drift / pos.pinnacle_prob_at_entry

                # Settle with numeric payout
                gross_payout = pos.size * payout_per_share
                pnl = portfolio.settle_position(
                    pos.market_id, payout_per_share=payout_per_share
                )
                settled_count += 1

                # Settlement label
                if payout_per_share >= 0.99:
                    label = "WON"
                    won = True
                elif payout_per_share <= 0.01:
                    label = "LOST"
                    won = False
                elif abs(payout_per_share - 0.5) < 0.01:
                    label = "SPLIT"
                    won = None
                else:
                    label = f"PARTIAL@{payout_per_share:.2f}"
                    won = None

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
                        "payout_per_share": round(payout_per_share, 4),
                        "gross_payout": round(gross_payout, 2),
                        "settlement_mode": settlement_mode,
                        "settlement_source": settlement_source,
                        "pinnacle_prob_at_entry": round(pos.pinnacle_prob_at_entry, 6),
                        "pinnacle_prob_at_close": round(pinnacle_prob_at_close, 6),
                        "clv": round(clv, 4),
                        "clv_pct": round(clv_pct, 4),
                        "pin_drift": round(pin_drift, 4),
                        "pin_drift_pct": round(pin_drift_pct, 4),
                        "shadow_exits": pos.shadow_exits if pos.shadow_exits else {},
                    },
                )

                logger.info(
                    "SETTLED: %s %s@%s — %s (%.2f/share) | P&L=$%.2f | CLV=%+.1f%% | Drift=%+.1f%%",
                    pos.team, pos.platform, pos.match_id,
                    label, payout_per_share, pnl, clv_pct * 100, pin_drift_pct * 100,
                )

                # Settle linked inverted position (opposite payout)
                if inverted_portfolio and getattr(pos, 'trade_id', ''):
                    try:
                        inv_pps = 1.0 - payout_per_share
                        inv_pnl = inverted_portfolio.settle_by_linked_id(
                            pos.trade_id, inv_pps,
                        )
                        # inv_pnl is None if no matching inverted position
                    except Exception:
                        logger.debug("Inverted settlement failed", exc_info=True)

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


# ── Settlement value resolvers ─────────────────────────────────────────


async def _resolve_settlement_value(pos, registry) -> tuple[float, str, str] | None:
    """Resolve a position's payout per share.

    Returns:
        (payout_per_share, settlement_mode, settlement_source) or None if
        the market is not yet terminal.

        payout_per_share: float in [0, 1]
        settlement_mode: "win" | "loss" | "split" | "partial" | "unknown_terminal"
        settlement_source: "polymarket" | "kalshi"
    """
    if pos.platform == "polymarket":
        return await _resolve_poly(pos)
    elif pos.platform == "kalshi":
        return await _resolve_kalshi(pos)
    return None


async def _resolve_poly(pos) -> tuple[float, str, str] | None:
    """Resolve Polymarket settlement via Gamma API.

    Returns numeric payout_per_share:
      - 1.0 if outcomePrices shows our token at >= 0.99
      - 0.0 if outcomePrices shows our token at <= 0.01
      - 0.5 if both outcomes near 0.5 (split / cancelled / unknown)
      - any other float if venue settles at a partial value
      - None if market not yet closed
    """
    import json as _json

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
            logger.warning("Gamma API returned %d for token %s", resp.status_code, token_id[:30])
            return None

        data = resp.json()
        if not data:
            logger.warning("Gamma API returned empty for token %s (team=%s)", token_id[:30], pos.team)
            return None

        market = data[0]

        if not market.get("closed", False):
            logger.info(
                "Market not closed yet for %s (team=%s, closed=%s)",
                token_id[:30], pos.team, market.get("closed"),
            )
            return None

        # Parse outcome prices and token IDs
        try:
            prices = _json.loads(market.get("outcomePrices", "[]"))
            clob_tokens = _json.loads(market.get("clobTokenIds", "[]"))
        except (ValueError, TypeError):
            logger.warning("Failed to parse outcomePrices/clobTokenIds for token %s", token_id[:30])
            return None

        if not prices or not clob_tokens:
            logger.warning("Empty prices/tokens for closed market, token %s", token_id[:30])
            return None

        # Match our token_id to find its settlement price
        for tid, price in zip(clob_tokens, prices):
            if tid == token_id:
                pps = float(price)

                # Classify settlement mode
                if pps >= 0.99:
                    return (1.0, "win", "polymarket")
                elif pps <= 0.01:
                    return (0.0, "loss", "polymarket")
                elif abs(pps - 0.5) < 0.05:
                    # Near 50/50 — split / cancelled / unknown resolution
                    logger.info(
                        "Polymarket SPLIT settlement: token %s settled at %.4f/share (team=%s)",
                        token_id[:30], pps, pos.team,
                    )
                    return (pps, "split", "polymarket")
                else:
                    # Some other partial payout
                    logger.info(
                        "Polymarket PARTIAL settlement: token %s settled at %.4f/share (team=%s)",
                        token_id[:30], pps, pos.team,
                    )
                    return (pps, "partial", "polymarket")

        logger.warning(
            "Poly token %s not in clobTokenIds. Market question: %s, tokens: %s",
            token_id[:40], market.get("question", "?")[:60],
            [t[:30] for t in clob_tokens],
        )
        return None

    except Exception:
        logger.exception("Error checking Poly resolution for token %s", token_id[:30])
        return None


async def _resolve_kalshi(pos) -> tuple[float, str, str] | None:
    """Resolve Kalshi settlement via public REST API.

    Returns numeric payout_per_share:
      - 1.0 if result == "yes"
      - 0.0 if result == "no"
      - None if not yet terminal or payout cannot be safely inferred

    Note: Kalshi cancelled markets may return different statuses.
    TODO: Handle venue-specific cancellation rules (e.g. voided events,
    rule 4a amendments) when Kalshi provides explicit payout values
    via their settlement/portfolio API.
    """
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

        # Not yet terminal
        if status not in ("finalized", "settled", "closed"):
            return None

        result = market.get("result", "")

        if result == "yes":
            return (1.0, "win", "kalshi")
        elif result == "no":
            return (0.0, "loss", "kalshi")
        elif result in ("", "void", "cancelled"):
            # Terminal but no clear binary result.
            # Check if Kalshi provides an explicit settlement value.
            # TODO: Query Kalshi portfolio/settlement API for explicit payout
            # when available. For now, log and leave unresolved.
            settle_value = market.get("settlement_value")
            if settle_value is not None:
                try:
                    pps = float(settle_value)
                    if 0.0 <= pps <= 1.0:
                        mode = "split" if abs(pps - 0.5) < 0.05 else "partial"
                        logger.info(
                            "Kalshi explicit settlement: %s at %.4f/share (status=%s)",
                            ticker, pps, status,
                        )
                        return (pps, mode, "kalshi")
                except (ValueError, TypeError):
                    pass

            # Voided/cancelled without explicit value — likely a full refund.
            # Kalshi typically refunds cost basis on voided markets.
            if result in ("void", "cancelled"):
                logger.info(
                    "Kalshi VOID/CANCELLED: %s (status=%s, result=%s) — "
                    "settling as refund (payout = entry price)",
                    ticker, status, result,
                )
                # Refund = you get back what you paid per share
                return (pos.price, "void_refund", "kalshi")

            logger.warning(
                "Kalshi terminal but unknown payout: %s (status=%s, result='%s') — "
                "leaving unresolved pending manual review",
                ticker, status, result,
            )
            return None
        else:
            logger.warning("Unknown Kalshi result '%s' for %s (status=%s)", result, ticker, status)
            return None

    except Exception:
        logger.debug("Error checking Kalshi resolution for %s", ticker)
        return None


# ── Helpers ────────────────────────────────────────────────────────────


def _get_pinnacle_closing_prob(pos, registry) -> float:
    """Get the latest Pinnacle probability for a position's team (closing line)."""
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
