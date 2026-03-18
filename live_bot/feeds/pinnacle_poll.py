"""Pinnacle REST poller — periodic no-vig price updates as truth oracle."""

import asyncio
import logging
import time

from live_bot.config import PINNACLE_POLL_INTERVAL

logger = logging.getLogger(__name__)


async def pinnacle_poller(
    registry,
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Poll Pinnacle odds periodically and update the registry + price queue.

    Pinnacle doesn't have a WebSocket API, so we poll via REST.
    The no-vig probabilities serve as the "true price" for value betting.
    """
    from arb_scanner.clients.pinnacle import fetch_odds

    interval = PINNACLE_POLL_INTERVAL

    while not (shutdown_event and shutdown_event.is_set()):
        try:
            logger.info("Polling Pinnacle for updated odds...")
            pin_outcomes = await asyncio.to_thread(fetch_odds)

            count = 0
            for o in pin_outcomes:
                # Update registry with fresh Pinnacle probabilities
                registry.update_pinnacle_price(
                    o.outcome_name, o.sport, o.no_vig_prob,
                    event_name=o.event_name,
                )

                # Also push into the price queue for the engine
                await price_queue.put({
                    "platform": "pinnacle",
                    "market_id": f"{o.event_name}_{o.outcome_name}",
                    "best_ask": o.implied_prob,   # with-vig price (what you'd pay)
                    "best_bid": 0.0,
                    "no_vig_prob": o.no_vig_prob,  # true probability
                    "timestamp": time.time(),
                })
                count += 1

            logger.info("Pinnacle poll complete: %d outcomes updated", count)

        except asyncio.CancelledError:
            logger.info("Pinnacle poller cancelled")
            return
        except Exception:
            logger.exception("Error polling Pinnacle")

        # Wait for next poll
        try:
            await asyncio.wait_for(
                shutdown_event.wait() if shutdown_event else asyncio.sleep(interval),
                timeout=interval,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
