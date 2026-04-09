"""NBA Regular 12–24h bot — main entrypoint.

Usage:
    python -m nba_bot
"""

import asyncio
import logging
import signal
import sys
import time

from nba_bot.config import (
    SIMULATION_MODE,
    ENABLE_NBA_REGULAR,
    NBA_PLATFORM,
    NBA_SPORT,
    NBA_MIN_MINUTES,
    NBA_MAX_MINUTES,
    NBA_MIN_EDGE_PCT,
    NBA_EDGE_PERSISTENCE,
    NBA_BET_SIZE_USD,
    NBA_MAX_POSITIONS,
    DASHBOARD_PORT,
    REGISTRY_REFRESH_INTERVAL,
    PINNACLE_POLL_INTERVAL,
    SETTLEMENT_CHECK_INTERVAL,
    PERSIST_SAVE_INTERVAL,
    DATA_DIR,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def _registry_refresher(registry, shutdown_event, new_poly_tokens):
    """Periodically refresh the market registry."""
    from live_bot.registry import build_registry_from_scanner

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=REGISTRY_REFRESH_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

        try:
            logger.info("Refreshing registry...")
            new_reg = await asyncio.to_thread(build_registry_from_scanner)
            added = 0
            new_ids = []

            for mid, match in new_reg.matches.items():
                if mid not in registry.matches:
                    registry.matches[mid] = match
                    added += 1
                    if match.poly_token_id_a:
                        new_ids.append(match.poly_token_id_a)
                    if match.poly_token_id_b:
                        new_ids.append(match.poly_token_id_b)
                else:
                    existing = registry.matches[mid]
                    if match.pinnacle_prob_a > 0:
                        existing.pinnacle_prob_a = match.pinnacle_prob_a
                    if match.pinnacle_prob_b > 0:
                        existing.pinnacle_prob_b = match.pinnacle_prob_b

            registry._poly_to_match.update(new_reg._poly_to_match)

            if new_poly_tokens:
                for tid in new_ids:
                    await new_poly_tokens.put(tid)

            # Prune stale matches
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            stale = []
            for mid, m in registry.matches.items():
                ct = m.commence_time
                if not ct:
                    continue
                try:
                    if ct.endswith("Z"):
                        ct = ct[:-1] + "+00:00"
                    start = datetime.fromisoformat(ct)
                    if (now - start).total_seconds() / 3600 > 12:
                        stale.append(mid)
                except (ValueError, TypeError):
                    pass
            for mid in stale:
                m = registry.matches.pop(mid, None)
                if m:
                    for tid in [m.poly_token_id_a, m.poly_token_id_b]:
                        registry._poly_to_match.pop(tid, None)

            logger.info("Registry: %d matches (%d new, %d pruned)", len(registry.matches), added, len(stale))
        except Exception:
            logger.exception("Error refreshing registry")


async def _persist_loop(portfolio, save_fn, shutdown_event):
    """Periodically save portfolio state."""
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=PERSIST_SAVE_INTERVAL)
            break
        except asyncio.TimeoutError:
            save_fn()


async def _status_loop(portfolio, shutdown_event):
    """Periodically log portfolio summary + skip counters."""
    from nba_bot import skip_counters
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            logger.info(
                "STATUS: bal=$%.2f pnl=$%.2f open=%d | %s",
                portfolio.current_balance, portfolio.total_pnl,
                len(portfolio.positions), skip_counters.summary(),
            )


async def run():
    """Main bot loop."""
    from live_bot.registry import build_registry_from_scanner
    from live_bot.feeds.polymarket_ws import polymarket_feed
    from live_bot.feeds.pinnacle_poll import pinnacle_poller
    from live_bot.portfolio import PaperPortfolio

    from nba_bot.engine import NBAEngine
    from nba_bot.persistence import save_positions, load_positions, maybe_reset
    from nba_bot.settlement import settlement_loop
    from nba_bot.dashboard import dashboard_server

    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    logger.info("=" * 50)
    logger.info("NBA Regular 12-24h Bot — %s", mode)
    logger.info("=" * 50)
    logger.info("Platform: %s | Sport: %s | Window: %d-%dh", NBA_PLATFORM, NBA_SPORT, NBA_MIN_MINUTES // 60, NBA_MAX_MINUTES // 60)
    logger.info("Min edge: %.1f%% | Persistence: %d | Bet size: $%.0f | Max positions: %d",
                NBA_MIN_EDGE_PCT * 100, NBA_EDGE_PERSISTENCE, NBA_BET_SIZE_USD, NBA_MAX_POSITIONS)
    logger.info("Enabled: %s | Data dir: %s", ENABLE_NBA_REGULAR, DATA_DIR or "(local)")

    # Build registry
    logger.info("Building market registry...")
    registry = await asyncio.to_thread(build_registry_from_scanner)
    if not registry.matches:
        logger.error("No matched markets — nothing to trade")
        return

    nba_count = sum(1 for m in registry.matches.values() if m.sport == NBA_SPORT)
    logger.info("Registry: %d matches total, %d NBA", len(registry.matches), nba_count)

    # Initialize portfolio
    portfolio = PaperPortfolio(starting_balance=float(SIMULATION_MODE and 1000 or 1000))
    from nba_bot.config import STARTING_BALANCE
    portfolio.starting_balance = STARTING_BALANCE
    portfolio.current_balance = STARTING_BALANCE

    if maybe_reset(portfolio):
        logger.info("Simulation reset — fresh start")
    else:
        loaded = load_positions(portfolio)
        if loaded:
            logger.info("Restored %d positions from disk", loaded)

    # Engine
    engine = NBAEngine(portfolio, registry)

    # Queues
    price_queue = asyncio.Queue()
    shutdown_event = asyncio.Event()
    new_poly_tokens = asyncio.Queue()

    # Signal handling
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown_event.set())

    logger.info("Starting feeds and engine...")
    logger.info("Dashboard: http://0.0.0.0:%d", DASHBOARD_PORT)

    save_fn = lambda: save_positions(portfolio)

    tasks = {
        "polymarket_feed": polymarket_feed(
            registry.poly_token_ids, price_queue, shutdown_event, new_poly_tokens,
        ),
        "pinnacle_poller": pinnacle_poller(
            registry, price_queue, shutdown_event,
        ),
        "engine": engine.run(price_queue, shutdown_event),
        "registry_refresher": _registry_refresher(registry, shutdown_event, new_poly_tokens),
        "settlement": settlement_loop(portfolio, registry, save_fn, shutdown_event),
        "persist": _persist_loop(portfolio, save_fn, shutdown_event),
        "status": _status_loop(portfolio, shutdown_event),
        "dashboard": dashboard_server(portfolio, shutdown_event),
    }

    async_tasks = {
        name: asyncio.create_task(coro, name=name)
        for name, coro in tasks.items()
    }

    try:
        done, pending = await asyncio.wait(
            async_tasks.values(),
            return_when=asyncio.FIRST_EXCEPTION,
        )
        for task in done:
            if task.exception():
                logger.error("Task %s crashed: %s", task.get_name(), task.exception())
                shutdown_event.set()
    except asyncio.CancelledError:
        pass
    finally:
        shutdown_event.set()
        save_fn()
        logger.info("Saved positions on shutdown")

        for task in async_tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*async_tasks.values(), return_exceptions=True)
        logger.info("NBA bot stopped")


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
