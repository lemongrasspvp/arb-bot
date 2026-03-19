"""Pinnacle REST poller — periodic no-vig price updates as truth oracle."""

import asyncio
import logging
import time

from live_bot.config import PINNACLE_POLL_INTERVAL

logger = logging.getLogger(__name__)

# ── Shared health state (read by dashboard) ──────────────────────────
pinnacle_health = {
    "status": "starting",      # "ok", "error", "rate_limited", "blocked", "starting"
    "last_success": 0.0,       # timestamp of last successful poll
    "last_error": "",          # last error message
    "consecutive_errors": 0,
    "consecutive_ok": 0,
    "last_outcome_count": 0,
    "total_polls": 0,
    "total_errors": 0,
}


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

            # Update health — success
            pinnacle_health["status"] = "ok"
            pinnacle_health["last_success"] = time.time()
            pinnacle_health["consecutive_ok"] += 1
            pinnacle_health["consecutive_errors"] = 0
            pinnacle_health["last_outcome_count"] = count
            pinnacle_health["total_polls"] += 1

        except asyncio.CancelledError:
            logger.info("Pinnacle poller cancelled")
            return
        except Exception as exc:
            logger.exception("Error polling Pinnacle")
            # Update health — error
            err_msg = str(exc)
            pinnacle_health["consecutive_errors"] += 1
            pinnacle_health["consecutive_ok"] = 0
            pinnacle_health["last_error"] = err_msg[:200]
            pinnacle_health["total_errors"] += 1

            if "429" in err_msg or "rate" in err_msg.lower():
                pinnacle_health["status"] = "rate_limited"
            elif "403" in err_msg or "blocked" in err_msg.lower():
                pinnacle_health["status"] = "blocked"
            elif pinnacle_health["consecutive_errors"] >= 5:
                pinnacle_health["status"] = "blocked"
            else:
                pinnacle_health["status"] = "error"

        # Wait for next poll
        try:
            await asyncio.wait_for(
                shutdown_event.wait() if shutdown_event else asyncio.sleep(interval),
                timeout=interval,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
