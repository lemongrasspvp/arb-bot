"""Orchestrator — fetch from all platforms, match, analyze, visualize."""

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from rich.console import Console

from arb_scanner.config import REFRESH_INTERVAL
from arb_scanner.clients.polymarket import fetch_markets as fetch_polymarket
from arb_scanner.clients.pinnacle import fetch_odds as fetch_pinnacle
from arb_scanner.clients.kalshi import fetch_markets as fetch_kalshi
from arb_scanner.matcher import MarketOutcome, match_platforms
from arb_scanner.calculator import analyze
from arb_scanner.visualizer import render_dashboard, save_chart

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.FileHandler("arb_scanner.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def _extract_teams(question: str) -> tuple[str, str] | None:
    """Extract 'Team A' and 'Team B' from an H2H question string."""
    m = re.search(
        r"(?:LoL|CS2|Dota\s*2|Valorant)?:?\s*(.+?)\s+vs\.?\s+(.+?)(?:\s*\(.*?\))?\s*(?:-|$)",
        question,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _poly_to_outcomes(poly_events) -> list[MarketOutcome]:
    """Convert Polymarket events to MarketOutcome pairs."""
    outcomes = []
    for e in poly_events:
        teams = _extract_teams(e.question)
        if not teams:
            continue
        team_a, team_b = teams
        event_name = e.question
        # YES token = first team, NO token = second team
        outcomes.append(MarketOutcome(
            platform="polymarket", event_name=event_name,
            team_name=team_a, implied_prob=e.yes_price,
            sport="lol" if "lol" in e.slug.lower() else "cs2",
            raw_id=e.condition_id,
        ))
        outcomes.append(MarketOutcome(
            platform="polymarket", event_name=event_name,
            team_name=team_b, implied_prob=e.no_price,
            sport="lol" if "lol" in e.slug.lower() else "cs2",
            raw_id=e.condition_id + "_no",
        ))
    return outcomes


def _pinnacle_to_outcomes(pin_outcomes) -> list[MarketOutcome]:
    """Convert Pinnacle outcomes to MarketOutcome list."""
    return [
        MarketOutcome(
            platform="pinnacle", event_name=o.event_name,
            team_name=o.outcome_name, implied_prob=o.no_vig_prob,
            sport=o.sport, raw_id=f"{o.event_name}_{o.outcome_name}",
        )
        for o in pin_outcomes
    ]


def _kalshi_to_outcomes(kalshi_markets) -> list[MarketOutcome]:
    """Convert Kalshi markets to MarketOutcome pairs."""
    # Kalshi has separate YES markets for each team in the same event.
    # Group by event_ticker to pair them.
    by_event: dict[str, list] = {}
    for m in kalshi_markets:
        by_event.setdefault(m.event_ticker, []).append(m)

    outcomes = []
    for event_ticker, mkts in by_event.items():
        if len(mkts) < 2:
            continue
        event_name = mkts[0].question  # "Team A vs Team B"
        for m in mkts[:2]:
            outcomes.append(MarketOutcome(
                platform="kalshi", event_name=event_name,
                team_name=m.team_name, implied_prob=m.yes_price,
                sport=m.sport, raw_id=m.ticker,
            ))
    return outcomes


def run_scan() -> None:
    """Execute a single scan cycle across all platforms."""
    logger.info("=== Starting scan cycle ===")

    # Fetch from all 3 platforms in parallel
    console.print("[cyan]Fetching from Polymarket, Pinnacle, Kalshi...[/cyan]")

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_poly = pool.submit(fetch_polymarket)
        f_pin = pool.submit(fetch_pinnacle)
        f_kalshi = pool.submit(fetch_kalshi)

    poly_raw = f_poly.result()
    pin_raw = f_pin.result()
    kalshi_raw = f_kalshi.result()

    console.print(
        f"[dim]Polymarket: {len(poly_raw)} | "
        f"Pinnacle: {len(pin_raw)} | "
        f"Kalshi: {len(kalshi_raw)}[/dim]"
    )

    # Convert to unified MarketOutcome format
    poly = _poly_to_outcomes(poly_raw)
    pin = _pinnacle_to_outcomes(pin_raw)
    kalshi = _kalshi_to_outcomes(kalshi_raw)

    # Match across all platform pairs
    console.print("[cyan]Matching events...[/cyan]")
    all_pairs = []
    all_pairs.extend(match_platforms(poly, pin, "Poly↔Pin"))
    all_pairs.extend(match_platforms(poly, kalshi, "Poly↔Kalshi"))
    all_pairs.extend(match_platforms(kalshi, pin, "Kalshi↔Pin"))

    # Analyze edges
    opportunities = analyze(all_pairs)

    # Render
    render_dashboard(opportunities)
    save_chart(opportunities)

    logger.info("=== Scan cycle complete ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="Esports Arb Scanner")
    parser.add_argument("--once", action="store_true", help="Single scan and exit")
    args = parser.parse_args()

    console.print("[bold cyan]Esports Arb Scanner[/bold cyan]")
    console.print("[dim]Polymarket + Pinnacle + Kalshi — no API keys needed[/dim]")
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
