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
    POLY_MAKER_REBATE,
    MAKER_FILL_RATE,
    EARLY_EXIT_TIERS,
    DASHBOARD_PORT,
    VALUE_EDGE_PERSISTENCE,
)

console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("live_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def _registry_refresher(registry, shutdown_event: asyncio.Event) -> None:
    """Periodically refresh the market registry to pick up new events."""
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
            for match_id, match in new_registry.matches.items():
                if match_id not in registry.matches:
                    registry.matches[match_id] = match
                    added += 1
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

            logger.info("Registry refreshed: %d total matches (%d new)", len(registry.matches), added)
        except Exception:
            logger.exception("Error refreshing registry")


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
    table.add_row("Shadow: Maker sim", f"✅ (rebate {POLY_MAKER_REBATE*100:.1f}%, fill rate {MAKER_FILL_RATE*100:.0f}%)")
    tier_str = ", ".join(f"TP{int(tp*100)}/SL{int(sl*100)}" for tp, sl in EARLY_EXIT_TIERS)
    table.add_row("Shadow: Early exit", f"✅ {len(EARLY_EXIT_TIERS)} tiers ({tier_str})")
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

    # Step 4: Initialize engine
    engine = ArbEngine(
        registry, poly_exec, kalshi_exec, portfolio,
        on_trade_fn=lambda: save_positions(portfolio),
    )

    # Step 5: Create shared queue and shutdown event
    price_queue = asyncio.Queue()
    shutdown_event = asyncio.Event()

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

    # Step 6: Run everything concurrently
    tasks = [
        asyncio.create_task(
            polymarket_feed(registry.poly_token_ids, price_queue, shutdown_event),
            name="polymarket_feed",
        ),
        asyncio.create_task(
            kalshi_feed(registry.kalshi_tickers, price_queue, shutdown_event),
            name="kalshi_feed",
        ),
        asyncio.create_task(
            pinnacle_poller(registry, price_queue, shutdown_event),
            name="pinnacle_poller",
        ),
        asyncio.create_task(
            pinnacle_live_poller(registry, price_queue, shutdown_event),
            name="pinnacle_live_poller",
        ),
        asyncio.create_task(
            engine.run(price_queue, shutdown_event),
            name="engine",
        ),
        asyncio.create_task(
            _registry_refresher(registry, shutdown_event),
            name="registry_refresher",
        ),
        asyncio.create_task(
            _status_printer(portfolio, shutdown_event),
            name="status_printer",
        ),
        asyncio.create_task(
            settlement_loop(
                portfolio, registry,
                lambda: save_positions(portfolio),
                shutdown_event,
            ),
            name="settlement_checker",
        ),
        asyncio.create_task(
            engine.early_exit_loop(shutdown_event),
            name="early_exit_checker",
        ),
        asyncio.create_task(
            dashboard_server(portfolio, shutdown_event),
            name="dashboard",
        ),
    ]

    try:
        # Wait until shutdown is requested
        await shutdown_event.wait()
    finally:
        # Cancel all tasks
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Save positions before exit
        save_positions(portfolio)
        console.print("[dim]Positions saved to disk[/dim]")

        # Final summary
        console.print()
        console.print("[bold cyan]═══ Session Summary ═══[/bold cyan]")
        console.print(portfolio.summary())
        console.print(f"[dim]Trade log: live_bot_trades.jsonl[/dim]")
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
        log_path = Path("live_bot_trades.jsonl")
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
