"""Sports market data collector — main entrypoint.

Continuously collects market data from Polymarket, Kalshi, and Pinnacle
into partitioned JSONL files for later analysis.

Usage:
    python -m collector.main
"""

import asyncio
import logging
import signal
import time

from rich.console import Console
from rich.table import Table

from collector.config import (
    PORT,
    DATA_DIR,
    SNAPSHOT_INTERVAL,
    MIN_PRICE_CHANGE,
    CLOSING_WINDOW_SECONDS,
    REGISTRY_REFRESH_INTERVAL,
    LOG_PATH,
)

console = Console()

# Ensure data directory exists
from pathlib import Path
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def _registry_refresher(
    registry,
    shutdown_event: asyncio.Event,
    snapshotter,
    new_poly_tokens: asyncio.Queue | None = None,
    new_kalshi_tickers: asyncio.Queue | None = None,
) -> None:
    """Periodically refresh the registry and write event snapshots for new matches."""
    from live_bot.registry import build_registry_from_scanner
    from datetime import datetime, timezone
    from live_bot.config import MAX_MATCH_DURATION_HOURS

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=REGISTRY_REFRESH_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

        try:
            logger.info("Refreshing market registry...")
            new_registry = await asyncio.to_thread(build_registry_from_scanner)

            added = 0
            new_poly_ids = []
            new_kalshi_ids = []

            for match_id, match in new_registry.matches.items():
                if match_id not in registry.matches:
                    registry.matches[match_id] = match
                    added += 1
                    # Write event snapshot for new match
                    snapshotter.write_event_snapshot(match)
                    # Queue new tokens for live feeds
                    if match.poly_token_id_a:
                        new_poly_ids.append(match.poly_token_id_a)
                    if match.poly_token_id_b:
                        new_poly_ids.append(match.poly_token_id_b)
                    if match.kalshi_ticker_a:
                        new_kalshi_ids.append(match.kalshi_ticker_a)
                    if match.kalshi_ticker_b:
                        new_kalshi_ids.append(match.kalshi_ticker_b)
                else:
                    # Update Pinnacle prices for existing matches
                    existing = registry.matches[match_id]
                    if match.pinnacle_prob_a > 0:
                        existing.pinnacle_prob_a = match.pinnacle_prob_a
                    if match.pinnacle_prob_b > 0:
                        existing.pinnacle_prob_b = match.pinnacle_prob_b

            # Update reverse lookups
            registry._poly_to_match.update(new_registry._poly_to_match)
            registry._kalshi_to_match.update(new_registry._kalshi_to_match)

            # Push new tokens to feeds
            if new_poly_tokens:
                for token_id in new_poly_ids:
                    await new_poly_tokens.put(token_id)
            if new_kalshi_tickers:
                for ticker in new_kalshi_ids:
                    await new_kalshi_tickers.put(ticker)

            # Prune stale matches
            now_dt = datetime.now(timezone.utc)
            stale = []
            for mid, m in registry.matches.items():
                if not m.commence_time:
                    continue
                try:
                    ct = m.commence_time
                    if ct.endswith("Z"):
                        ct = ct[:-1] + "+00:00"
                    start = datetime.fromisoformat(ct)
                    if (now_dt - start).total_seconds() / 3600 > MAX_MATCH_DURATION_HOURS:
                        stale.append(mid)
                except (ValueError, TypeError):
                    continue
            for mid in stale:
                m = registry.matches.pop(mid, None)
                if m:
                    for tid in [m.poly_token_id_a, m.poly_token_id_b]:
                        registry._poly_to_match.pop(tid, None)
                    for tk in [m.kalshi_ticker_a, m.kalshi_ticker_b]:
                        registry._kalshi_to_match.pop(tk, None)

            logger.info(
                "Registry refreshed: %d total (%d new, %d pruned, %d poly, %d kalshi)",
                len(registry.matches), added, len(stale), len(new_poly_ids), len(new_kalshi_ids),
            )
        except Exception:
            logger.exception("Error refreshing registry")


async def run_collector() -> None:
    """Main collector loop."""
    from live_bot.registry import build_registry_from_scanner
    from live_bot.feeds.polymarket_ws import polymarket_feed
    from live_bot.feeds.kalshi_ws import kalshi_feed
    from live_bot.feeds.pinnacle_poll import pinnacle_poller
    from collector.snapshotter import Snapshotter
    from collector.closing import ClosingSnapshotFreezer
    from collector.health import health_server

    # Banner
    console.print()
    console.print("[bold cyan]═══ Sports Market Data Collector ═══[/bold cyan]")
    console.print()

    table = Table(show_header=False, border_style="dim")
    table.add_column("Setting", style="dim")
    table.add_column("Value")
    table.add_row("Mode", "DATA COLLECTION ONLY")
    table.add_row("Data directory", DATA_DIR or "(working dir)")
    table.add_row("Snapshot interval", f"{SNAPSHOT_INTERVAL}s heartbeat")
    table.add_row("Min price change", f"{MIN_PRICE_CHANGE:.3f}")
    table.add_row("Closing window", f"{CLOSING_WINDOW_SECONDS}s before start")
    table.add_row("Registry refresh", f"every {REGISTRY_REFRESH_INTERVAL}s")
    table.add_row("Health server", f"http://0.0.0.0:{PORT}")
    console.print(table)
    console.print()

    # Build registry
    console.print("[cyan]Building market registry...[/cyan]")
    registry = await asyncio.to_thread(build_registry_from_scanner)

    if not registry.matches:
        console.print("[red]No matched markets found.[/red]")
        return

    console.print(f"[green]Registry: {len(registry.matches)} tracked matches[/green]")
    console.print(f"[dim]  Polymarket tokens: {len(registry.poly_token_ids)}[/dim]")
    console.print(f"[dim]  Kalshi tickers: {len(registry.kalshi_tickers)}[/dim]")

    for mid, m in list(registry.matches.items())[:8]:
        pin_a = f"pin={m.pinnacle_prob_a:.0%}" if m.pinnacle_prob_a else "no-pin"
        platforms = []
        if m.poly_token_id_a:
            platforms.append("Poly")
        if m.kalshi_ticker_a:
            platforms.append("Kalshi")
        console.print(f"[dim]  - {m.teams[0]} vs {m.teams[1]} [{m.sport}] ({', '.join(platforms)}) [{pin_a}][/dim]")
    if len(registry.matches) > 8:
        console.print(f"[dim]  ... and {len(registry.matches) - 8} more[/dim]")
    console.print()

    # Initialize components
    snapshotter = Snapshotter(registry)
    closing_freezer = ClosingSnapshotFreezer(registry, snapshotter)

    # Write initial event snapshots
    for match in registry.matches.values():
        snapshotter.write_event_snapshot(match)
    console.print(f"[green]Wrote {snapshotter.counts['events']} event snapshots[/green]")

    # Queues
    price_queue = asyncio.Queue()
    shutdown_event = asyncio.Event()
    new_poly_tokens = asyncio.Queue()
    new_kalshi_tickers = asyncio.Queue()

    # Graceful shutdown
    def _signal_handler():
        console.print("\n[yellow]Shutting down collector...[/yellow]")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    console.print("[green]Starting feeds and collector...[/green]")
    console.print(f"[green]Health: http://0.0.0.0:{PORT}/status[/green]")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    # Seed price cache from scanner data
    seeded = 0
    for platform, prices in registry.initial_prices.items():
        for market_id, price_data in prices.items():
            await price_queue.put({
                "platform": platform,
                "market_id": market_id,
                **price_data,
            })
            seeded += 1
    console.print(f"[dim]  Seeded {seeded} initial prices[/dim]")

    # Task definitions
    task_factories = {
        "polymarket_feed": lambda: polymarket_feed(
            registry.poly_token_ids, price_queue, shutdown_event, new_poly_tokens,
        ),
        "kalshi_feed": lambda: kalshi_feed(
            registry.kalshi_tickers, price_queue, shutdown_event, new_kalshi_tickers,
        ),
        "pinnacle_poller": lambda: pinnacle_poller(
            registry, price_queue, shutdown_event,
        ),
        "snapshotter": lambda: snapshotter.run(price_queue, shutdown_event),
        "closing_freezer": lambda: closing_freezer.run(shutdown_event),
        "registry_refresher": lambda: _registry_refresher(
            registry, shutdown_event, snapshotter, new_poly_tokens, new_kalshi_tickers,
        ),
        "health": lambda: health_server(
            snapshotter, closing_freezer, registry, shutdown_event,
        ),
    }

    tasks: dict[str, asyncio.Task] = {}
    for name, factory in task_factories.items():
        tasks[name] = asyncio.create_task(factory(), name=name)

    try:
        # Monitor + restart crashed tasks
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=10)
                break
            except asyncio.TimeoutError:
                pass

            for name, task in list(tasks.items()):
                if task.done() and not shutdown_event.is_set():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        logger.error("Task '%s' crashed: %s — restarting", name, exc)
                        await asyncio.sleep(5)
                        if name in task_factories:
                            tasks[name] = asyncio.create_task(
                                task_factories[name](), name=name,
                            )
                    else:
                        logger.warning("Task '%s' completed — restarting", name)
                        if name in task_factories:
                            tasks[name] = asyncio.create_task(
                                task_factories[name](), name=name,
                            )
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)

        console.print()
        console.print("[bold cyan]═══ Collector Summary ═══[/bold cyan]")
        console.print(f"  Market snapshots: {snapshotter.counts['market_state']}")
        console.print(f"  Reference snapshots: {snapshotter.counts['reference_state']}")
        console.print(f"  Event snapshots: {snapshotter.counts['events']}")
        console.print(f"  Closing snapshots: {closing_freezer.count}")
        console.print(f"  Skipped (dedup): {snapshotter.counts['skipped_dedup']}")
        console.print()


def main():
    asyncio.run(run_collector())


if __name__ == "__main__":
    main()
