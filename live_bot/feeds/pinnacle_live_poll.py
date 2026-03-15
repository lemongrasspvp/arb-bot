"""Pinnacle LIVE odds poller — real-time in-game price updates.

Polls Pinnacle's live/in-game moneyline odds and updates the registry
so that midgame value bets use fresh truth prices instead of stale
pre-game references.

Runs on a faster cadence than the pre-game poller (every 15s vs 30s)
because in-game odds move quickly.
"""

import asyncio
import logging
import time

from live_bot.config import PINNACLE_LIVE_POLL_INTERVAL

logger = logging.getLogger(__name__)


async def pinnacle_live_poller(
    registry,
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Poll Pinnacle LIVE odds and update the registry + price queue.

    Only fetches odds for matches that are currently in-game (isLive=True).
    Updates the registry's pinnacle_prob_a/b with fresh live no-vig probabilities,
    replacing the stale pre-game values.
    """
    from arb_scanner.clients.pinnacle import fetch_live_odds

    interval = PINNACLE_LIVE_POLL_INTERVAL

    while not (shutdown_event and shutdown_event.is_set()):
        try:
            logger.debug("Polling Pinnacle for LIVE odds...")
            live_outcomes = await asyncio.to_thread(fetch_live_odds)

            if live_outcomes:
                count = 0
                for o in live_outcomes:
                    # Update registry with fresh LIVE Pinnacle probabilities
                    # This overwrites the stale pre-game values
                    registry.update_pinnacle_price(o.outcome_name, o.sport, o.no_vig_prob)

                    # Also push into the price queue for the engine
                    await price_queue.put({
                        "platform": "pinnacle",
                        "market_id": f"{o.event_name}_{o.outcome_name}",
                        "best_ask": o.implied_prob,
                        "best_bid": 0.0,
                        "no_vig_prob": o.no_vig_prob,
                        "timestamp": time.time(),
                    })
                    count += 1

                logger.info("Pinnacle LIVE poll: %d outcomes updated", count)

        except asyncio.CancelledError:
            logger.info("Pinnacle live poller cancelled")
            return
        except Exception:
            logger.exception("Error polling Pinnacle live odds")

        # Wait for next poll
        try:
            await asyncio.wait_for(
                shutdown_event.wait() if shutdown_event else asyncio.sleep(interval),
                timeout=interval,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
