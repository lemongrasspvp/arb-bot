"""Live arb bot — main entry point.

Usage:
    python -m live_bot.main              # Run in simulation mode (default)
    python -m live_bot.main --live       # Run with real execution
    python -m live_bot.main --status     # Print portfolio summary and exit
"""

import argparse
import asyncio
import logging
import signal
import sys
import time

from rich.console import Console
from rich.table import Table

from live_bot.config import (
    SIMULATION_MODE,
    ENABLE_ARB,
    ENABLE_VALUE,
    REGISTRY_REFRESH_INTERVAL,
    PINNACLE_POLL_INTERVAL,
    MAX_POSITION_USD,
    MAX_DAILY_LOSS_USD,
    MIN_ARB_PROFIT_PCT,
    MIN_VALUE_EDGE_PCT,
    MIDGAME_VALUE_EDGE_PCT,
    KELLY_FRACTION,
    ALLOW_MIDGAME_VALUE,
    MAX_PRICE_DIVERGENCE_PCT,
    PINNACLE_LIVE_POLL_INTERVAL,
    SIMULATE_KALSHI_WS,
    MIN_ARB_DEPTH_USD,
    DASHBOARD_PORT,
    VALUE_EDGE_PERSISTENCE,
    BOT_LOG_PATH,
)

console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(BOT_LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def _registry_refresher(
    registry,
    shutdown_event: asyncio.Event,
    new_poly_tokens: asyncio.Queue | None = None,
    new_kalshi_tickers: asyncio.Queue | None = None,
) -> None:
    """Periodically refresh the market registry to pick up new events.

    When new matches are discovered, pushes their token IDs / tickers into
    the feed queues so the WS connections subscribe to them automatically.
    Also prunes stale matches that are likely over.
    """
    from live_bot.registry import build_registry_from_scanner

    interval = REGISTRY_REFRESH_INTERVAL
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break  # shutdown requested
        except asyncio.TimeoutError:
            pass  # time to refresh

        try:
            logger.info("Refreshing market registry...")
            new_registry = await asyncio.to_thread(build_registry_from_scanner)
            # Merge new matches into existing registry
            added = 0
            new_poly_ids = []
            new_kalshi_ids = []

            for match_id, match in new_registry.matches.items():
                if match_id not in registry.matches:
                    registry.matches[match_id] = match
                    added += 1
                    # Collect new token IDs / tickers for live feeds
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

            # Push new tokens to live feeds so they subscribe
            if new_poly_tokens:
                for token_id in new_poly_ids:
                    await new_poly_tokens.put(token_id)
            if new_kalshi_tickers:
                for ticker in new_kalshi_ids:
                    await new_kalshi_tickers.put(ticker)

            # ── Prune stale matches (likely over) ──
            pruned = _prune_stale_matches(registry)

            logger.info(
                "Registry refreshed: %d total matches (%d new, %d pruned, %d new tokens, %d new tickers)",
                len(registry.matches), added, pruned, len(new_poly_ids), len(new_kalshi_ids),
            )
        except Exception:
            logger.exception("Error refreshing registry")


def _prune_stale_matches(registry) -> int:
    """Remove matches from the registry that are likely over.

    A match is considered stale if its commence_time + MAX_MATCH_DURATION_HOURS
    is in the past. This prevents the registry from growing indefinitely.
    """
    from datetime import datetime, timezone
    from live_bot.config import MAX_MATCH_DURATION_HOURS

    now = datetime.now(timezone.utc)
    stale_ids = []

    for match_id, match in registry.matches.items():
        if not match.commence_time:
            continue
        try:
            ct = match.commence_time
            if ct.endswith("Z"):
                ct = ct[:-1] + "+00:00"
            start = datetime.fromisoformat(ct)
            hours_since = (now - start).total_seconds() / 3600
            if hours_since > MAX_MATCH_DURATION_HOURS:
                stale_ids.append(match_id)
        except (ValueError, TypeError):
            continue

    for match_id in stale_ids:
        match = registry.matches.pop(match_id, None)
        if match:
            # Clean up reverse lookups
            for token_id in [match.poly_token_id_a, match.poly_token_id_b]:
                registry._poly_to_match.pop(token_id, None)
            for ticker in [match.kalshi_ticker_a, match.kalshi_ticker_b]:
                registry._kalshi_to_match.pop(ticker, None)
            logger.debug("Pruned stale match: %s", match_id)

    return len(stale_ids)


async def _status_printer(portfolio, shutdown_event: asyncio.Event) -> None:
    """Print portfolio status periodically."""
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            pass

        console.print(f"\n[dim]{portfolio.summary()}[/dim]\n")


async def run_bot(live: bool = False) -> None:
    """Main bot loop."""
    from live_bot.registry import build_registry_from_scanner
    from live_bot.feeds.polymarket_ws import polymarket_feed
    from live_bot.feeds.kalshi_ws import kalshi_feed
    from live_bot.feeds.pinnacle_poll import pinnacle_poller
    from live_bot.feeds.pinnacle_live_poll import pinnacle_live_poller
    from live_bot.engine import ArbEngine
    from live_bot.execution.polymarket_exec import PolymarketExecutor
    from live_bot.execution.kalshi_exec import KalshiExecutor
    from live_bot.portfolio import PaperPortfolio
    from live_bot.settlement import settlement_loop
    from live_bot.persistence import save_positions, load_positions
    from live_bot.dashboard import dashboard_server

    simulation = not live
    mode_str = "SIMULATION" if simulation else "🔴 LIVE"

    # Banner
    console.print()
    console.print(f"[bold cyan]═══ Live Arb Bot ═══[/bold cyan]")
    console.print(f"[bold {'green' if simulation else 'red'}]Mode: {mode_str}[/bold {'green' if simulation else 'red'}]")
    console.print()

    # Config table
    table = Table(show_header=False, border_style="dim")
    table.add_column("Setting", style="dim")
    table.add_column("Value")
    table.add_row("Strategy 1 (Arb)", "✅ Enabled" if ENABLE_ARB else "❌ Disabled")
    table.add_row("Strategy 2 (Value)", "✅ Enabled" if ENABLE_VALUE else "❌ Disabled")
    table.add_row("Min arb profit", f"{MIN_ARB_PROFIT_PCT}%")
    table.add_row("Min value edge (pregame)", f"{MIN_VALUE_EDGE_PCT}%")
    table.add_row("Min value edge (midgame)", f"{MIDGAME_VALUE_EDGE_PCT}%")
    table.add_row("Edge persistence", f"{VALUE_EDGE_PERSISTENCE} checks")
    table.add_row("Value edge calc", "VWAP + full fees")
    table.add_row("Event exposure limit", "✅ Match-level")
    table.add_row("Max position", f"${MAX_POSITION_USD}")
    table.add_row("Daily loss limit", f"${MAX_DAILY_LOSS_USD}")
    table.add_row("Kelly fraction", f"{KELLY_FRACTION}")
    table.add_row("Pinnacle poll (pregame)", f"every {PINNACLE_POLL_INTERVAL}s")
    table.add_row("Pinnacle poll (live)", f"every {PINNACLE_LIVE_POLL_INTERVAL}s")
    table.add_row("Registry refresh", f"every {REGISTRY_REFRESH_INTERVAL}s")
    table.add_row("Midgame value bets", "✅ Allowed" if ALLOW_MIDGAME_VALUE else "❌ Blocked (stale ref)")
    table.add_row("Max price divergence", f"{MAX_PRICE_DIVERGENCE_PCT}%")
    table.add_row("Simulate Kalshi WS", "✅ Yes (no REST penalty)" if SIMULATE_KALSHI_WS else "❌ No (REST +7.5s)")
    table.add_row("Min arb depth", f"${MIN_ARB_DEPTH_USD}")
    table.add_row("Arb execution", "Sequential (harder leg first)")
    console.print(table)
    console.print()

    # Step 1: Build market registry
    console.print("[cyan]Building market registry...[/cyan]")
    registry = await asyncio.to_thread(build_registry_from_scanner)

    if not registry.matches:
        console.print("[red]No matched markets found — nothing to trade.[/red]")
        return

    console.print(f"[green]Registry: {len(registry.matches)} tracked matches[/green]")
    console.print(f"[dim]  Polymarket tokens: {len(registry.poly_token_ids)}[/dim]")
    console.print(f"[dim]  Kalshi tickers: {len(registry.kalshi_tickers)}[/dim]")

    # Show tracked matches
    for mid, m in list(registry.matches.items())[:10]:
        pin_a = f"pin={m.pinnacle_prob_a:.0%}" if m.pinnacle_prob_a else "no-pin"
        pin_b = f"pin={m.pinnacle_prob_b:.0%}" if m.pinnacle_prob_b else "no-pin"
        platforms = []
        if m.poly_token_id_a:
            platforms.append("Poly")
        if m.kalshi_ticker_a:
            platforms.append("Kalshi")
        console.print(
            f"[dim]  • {m.teams[0]} vs {m.teams[1]} [{m.sport}] "
            f"({', '.join(platforms)}) [{pin_a}, {pin_b}][/dim]"
        )
    if len(registry.matches) > 10:
        console.print(f"[dim]  ... and {len(registry.matches) - 10} more[/dim]")

    console.print()

    # Step 2: Initialize executors
    poly_exec = PolymarketExecutor(simulation=simulation)
    kalshi_exec = KalshiExecutor(simulation=simulation)

    # Step 3: Initialize portfolio (restore from disk if available)
    portfolio = PaperPortfolio()
    loaded = load_positions(portfolio)
    if loaded:
        console.print(f"[green]Restored {loaded} open positions from disk[/green]")
        console.print(f"[dim]  Balance: ${portfolio.current_balance:.2f} | P&L: ${portfolio.total_pnl:.2f}[/dim]")

    # Backfill counters from trade log if needed (handles deploys with old positions format)
    from live_bot.persistence import backfill_counters
    backfilled = backfill_counters(portfolio)
    if backfilled:
        avg_edge = portfolio.value_edge_sum / portfolio.value_filled_count if portfolio.value_filled_count else 0
        console.print(f"[green]Backfilled {backfilled} filled trades from log (avg edge {avg_edge:.1f}%)[/green]")

    # Step 4: Initialize engine
    engine = ArbEngine(
        registry, poly_exec, kalshi_exec, portfolio,
        on_trade_fn=lambda: save_positions(portfolio),
    )

    # Seed engine price cache with scanner data so value checks work
    # immediately, without waiting for WS to send an update.
    seeded = 0
    for platform, prices in registry.initial_prices.items():
        for market_id, price_data in prices.items():
            engine.prices[platform][market_id] = price_data
            seeded += 1
    console.print(f"[dim]  Seeded {seeded} initial prices from scanner[/dim]")

    # Step 5: Create shared queues and shutdown event
    price_queue = asyncio.Queue()
    shutdown_event = asyncio.Event()

    # Queues for feeding new tokens/tickers from registry refresh to live feeds
    new_poly_tokens = asyncio.Queue()
    new_kalshi_tickers = asyncio.Queue()

    # Handle SIGINT/SIGTERM gracefully
    def _signal_handler():
        console.print("\n[yellow]Shutting down...[/yellow]")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    console.print("[green]Starting feeds and engine...[/green]")
    console.print(f"[green]Dashboard: http://0.0.0.0:{DASHBOARD_PORT}[/green]")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    # Step 6: Define task specs for crash recovery
    # Each is (name, coroutine_factory) — factory so we can recreate on crash
    def _make_tasks():
        return {
            "polymarket_feed": lambda: polymarket_feed(
                registry.poly_token_ids, price_queue, shutdown_event, new_poly_tokens,
            ),
            "kalshi_feed": lambda: kalshi_feed(
                registry.kalshi_tickers, price_queue, shutdown_event, new_kalshi_tickers,
            ),
            "pinnacle_poller": lambda: pinnacle_poller(
                registry, price_queue, shutdown_event,
            ),
            "pinnacle_live_poller": lambda: pinnacle_live_poller(
                registry, price_queue, shutdown_event,
            ),
            "engine": lambda: engine.run(price_queue, shutdown_event),
            "registry_refresher": lambda: _registry_refresher(
                registry, shutdown_event, new_poly_tokens, new_kalshi_tickers,
            ),
            "status_printer": lambda: _status_printer(portfolio, shutdown_event),
            "settlement_checker": lambda: settlement_loop(
                portfolio, registry,
                lambda: save_positions(portfolio),
                shutdown_event,
            ),
            "dashboard": lambda: dashboard_server(portfolio, shutdown_event),
        }

    task_factories = _make_tasks()
    tasks: dict[str, asyncio.Task] = {}

    # Launch all tasks
    for name, factory in task_factories.items():
        tasks[name] = asyncio.create_task(factory(), name=name)

    try:
        # Monitor tasks — restart any that crash unexpectedly
        while not shutdown_event.is_set():
            # Check every 10 seconds for crashed tasks
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=10)
                break  # shutdown requested
            except asyncio.TimeoutError:
                pass

            for name, task in list(tasks.items()):
                if task.done() and not shutdown_event.is_set():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        logger.error(
                            "Task '%s' crashed: %s — restarting in 5s",
                            name, exc,
                        )
                        console.print(f"[red]⚠️  Task '{name}' crashed: {exc} — restarting...[/red]")
                        await asyncio.sleep(5)
                        # Restart the task
                        if name in task_factories:
                            tasks[name] = asyncio.create_task(
                                task_factories[name](), name=name,
                            )
                            logger.info("Task '%s' restarted", name)
                    else:
                        # Task completed normally (shouldn't happen for long-running tasks)
                        logger.warning("Task '%s' completed unexpectedly — restarting", name)
                        if name in task_factories:
                            tasks[name] = asyncio.create_task(
                                task_factories[name](), name=name,
                            )
    finally:
        # Cancel all tasks
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Save positions before exit
        save_positions(portfolio)
        console.print("[dim]Positions saved to disk[/dim]")

        # Final summary
        console.print()
        console.print("[bold cyan]═══ Session Summary ═══[/bold cyan]")
        console.print(portfolio.summary())
        from live_bot.config import TRADE_LOG_PATH
        console.print(f"[dim]Trade log: {TRADE_LOG_PATH}[/dim]")
        console.print()


def main():
    parser = argparse.ArgumentParser(description="Live Arb Bot")
    parser.add_argument("--live", action="store_true", help="Run with real execution (default: simulation)")
    parser.add_argument("--status", action="store_true", help="Print status and exit")
    args = parser.parse_args()

    if args.status:
        # Just print current trade log stats
        import json
        from pathlib import Path
        from live_bot.config import TRADE_LOG_PATH
        log_path = Path(TRADE_LOG_PATH)
        if not log_path.exists():
            console.print("[dim]No trade log found.[/dim]")
            return
        trades = [json.loads(line) for line in log_path.read_text().strip().split("\n") if line.strip()]
        arbs = [t for t in trades if t.get("strategy") == "ARB"]
        values = [t for t in trades if t.get("strategy") == "VALUE"]
        console.print(f"[bold]Trade Log Summary[/bold]")
        console.print(f"  Total events: {len(trades)}")
        console.print(f"  Arb trades: {len(arbs)}")
        console.print(f"  Value trades: {len(values)}")
        return

    if args.live:
        console.print("[bold red]⚠️  LIVE MODE — real money will be used![/bold red]")
        console.print("[dim]Press Ctrl+C within 5 seconds to cancel...[/dim]")
        try:
            import time
            time.sleep(5)
        except KeyboardInterrupt:
            console.print("[yellow]Cancelled.[/yellow]")
            return

    asyncio.run(run_bot(live=args.live))


if __name__ == "__main__":
    main()
