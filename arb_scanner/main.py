"""Orchestrator — scan, match, analyze, visualize on a loop."""

import argparse
import logging
import sys
import time

from rich.console import Console

from arb_scanner.config import REFRESH_INTERVAL
from arb_scanner.clients.polymarket import fetch_markets
from arb_scanner.clients.pinnacle import fetch_odds
from arb_scanner.matcher import match_events
from arb_scanner.calculator import analyze
from arb_scanner.visualizer import render_dashboard, save_chart

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.FileHandler("arb_scanner.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_scan() -> None:
    """Execute a single scan cycle."""
    logger.info("=== Starting scan cycle ===")

    # 1. Fetch data from both platforms
    console.print("[cyan]Fetching Polymarket markets...[/cyan]")
    poly_events = fetch_markets()

    console.print("[cyan]Fetching Pinnacle odds...[/cyan]")
    pinnacle_outcomes = fetch_odds()

    if not poly_events:
        console.print("[yellow]No Polymarket events found[/yellow]")
    if not pinnacle_outcomes:
        console.print("[yellow]No Pinnacle outcomes found[/yellow]")

    # 2. Match events across platforms
    console.print("[cyan]Matching events...[/cyan]")
    matched_pairs = match_events(poly_events, pinnacle_outcomes)

    # 3. Calculate edges
    opportunities = analyze(matched_pairs)

    # 4. Render dashboard and chart
    render_dashboard(opportunities)
    save_chart(opportunities)

    logger.info("=== Scan cycle complete ===")


def main() -> None:
    """Main entry point — runs scan loop."""
    parser = argparse.ArgumentParser(description="Polymarket vs Pinnacle Arb Scanner")
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single scan and exit (saves API credits)",
    )
    args = parser.parse_args()

    console.print("[bold cyan]Polymarket vs Pinnacle Arb Scanner[/bold cyan]")
    console.print("[dim]Pinnacle direct API (LoL + CS2) — no API key needed[/dim]")
    if not args.once:
        console.print(f"[dim]Refresh interval: {REFRESH_INTERVAL}s[/dim]")
    console.print()

    if args.once:
        run_scan()
        console.print("\n[dim]Single scan complete.[/dim]")
        return

    try:
        while True:
            run_scan()
            console.print(f"\n[dim]Next scan in {REFRESH_INTERVAL}s... (Ctrl+C to stop)[/dim]")
            time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print("\n[yellow]Scanner stopped.[/yellow]")
        sys.exit(0)


if __name__ == "__main__":
    main()
