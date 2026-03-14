"""Orchestrator — fetch from all platforms, find true cross-platform arbs."""

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
from arb_scanner.calculator import find_arbs, walk_arb_books
from arb_scanner.clients.polymarket import _fetch_book_levels as poly_book_levels, _build_session as poly_session
from arb_scanner.clients.kalshi import _fetch_book_levels as kalshi_book_levels, _build_session as kalshi_session
from arb_scanner.visualizer import render_dashboard, save_chart

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.FileHandler("arb_scanner.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def _normalize_team(name: str) -> str:
    """Normalize team name for start-time lookup."""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", name.lower().strip())).strip()


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
        slug_lower = e.slug.lower()
        if "lol" in slug_lower or "league-of-legends" in slug_lower:
            sport = "lol"
        elif "dota" in slug_lower:
            sport = "dota2"
        elif "valorant" in slug_lower:
            sport = "valorant"
        elif "call-of-duty" in slug_lower or "cod" in slug_lower:
            sport = "cod"
        else:
            sport = "cs2"
        # actual_price = ask (what you'd actually pay), not mid
        yes_ask = e.yes_ask if e.yes_ask > 0 else e.yes_price
        no_ask = e.no_ask if e.no_ask > 0 else e.no_price
        outcomes.append(MarketOutcome(
            platform="polymarket", event_name=event_name,
            team_name=team_a, implied_prob=e.yes_price,
            sport=sport, raw_id=e.condition_id,
            actual_price=yes_ask,
            token_id=e.yes_token_id,
        ))
        outcomes.append(MarketOutcome(
            platform="polymarket", event_name=event_name,
            team_name=team_b, implied_prob=e.no_price,
            sport=sport, raw_id=e.condition_id + "_no",
            actual_price=no_ask,
            token_id=e.no_token_id,
        ))
    return outcomes


def _pinnacle_to_outcomes(pin_outcomes) -> list[MarketOutcome]:
    """Convert Pinnacle outcomes to MarketOutcome list."""
    return [
        MarketOutcome(
            platform="pinnacle", event_name=o.event_name,
            team_name=o.outcome_name, implied_prob=o.no_vig_prob,
            sport=o.sport, raw_id=f"{o.event_name}_{o.outcome_name}",
            commence_time=o.commence_time,
            actual_price=o.implied_prob,  # with vig — what you'd actually pay
        )
        for o in pin_outcomes
    ]


def _kalshi_to_outcomes(kalshi_markets) -> list[MarketOutcome]:
    """Convert Kalshi markets to MarketOutcome pairs.

    Kalshi has separate YES markets per team. To bet on Team A you can:
      1. Buy YES on Team A's market (yes_ask)
      2. Buy NO on Team B's market (= 1 - Team B's yes_bid)
    We take the cheaper of the two as the actual execution price.
    """
    by_event: dict[str, list] = {}
    for m in kalshi_markets:
        by_event.setdefault(m.event_ticker, []).append(m)

    outcomes = []
    for event_ticker, mkts in by_event.items():
        if len(mkts) < 2:
            continue
        event_name = mkts[0].question
        m0, m1 = mkts[0], mkts[1]

        for m, opponent in [(m0, m1), (m1, m0)]:
            # Direct: buy YES on this team's market
            yes_ask = m.yes_ask if m.yes_ask > 0 else m.yes_price

            # Indirect: buy NO on opponent's market (= opponent loses = this team wins)
            # NO ask ≈ 1 - opponent's yes_bid
            no_via_opponent = (1.0 - opponent.yes_bid) if opponent.yes_bid > 0 else 999.0

            # Take the cheaper route
            best_price = min(yes_ask, no_via_opponent)

            if best_price != yes_ask:
                logger.debug(
                    "Kalshi %s: NO-on-opponent cheaper (%.1f¢ vs YES %.1f¢)",
                    m.team_name, no_via_opponent * 100, yes_ask * 100,
                )

            outcomes.append(MarketOutcome(
                platform="kalshi", event_name=event_name,
                team_name=m.team_name, implied_prob=m.yes_price,
                sport=m.sport, raw_id=m.ticker,
                actual_price=best_price,
                opponent_raw_id=opponent.ticker,
            ))
    return outcomes


def _get_book_levels(platform: str, raw_id: str, token_id: str,
                     price: float, p_sess, k_sess,
                     opponent_raw_id: str = "") -> list[tuple[float, float]]:
    """Fetch ask book levels for a given platform/ID.

    For sportsbooks (Pinnacle), return a synthetic level with large size
    since they offer fixed prices with high limits.
    """
    if platform == "polymarket" and token_id:
        return poly_book_levels(p_sess, token_id, side="asks")
    elif platform == "kalshi" and raw_id:
        return kalshi_book_levels(k_sess, raw_id, opponent_ticker=opponent_raw_id)
    elif platform == "pinnacle" and price > 0:
        # Sportsbooks have fixed prices — treat as deep liquidity
        # Use 5000 shares as a reasonable Pinnacle limit
        return [(price, 5000)]
    return []


def _enrich_arb_depth(arbs: list) -> None:
    """Walk both order books to find max deployable $ for each arb."""
    p_sess = poly_session()
    k_sess = kalshi_session()

    for arb in arbs:
        levels_a = _get_book_levels(
            arb.leg_a_platform, arb.leg_a_raw_id, arb.leg_a_token_id,
            arb.leg_a_price, p_sess, k_sess,
            opponent_raw_id=arb.leg_a_opponent_raw_id,
        )
        levels_b = _get_book_levels(
            arb.leg_b_platform, arb.leg_b_raw_id, arb.leg_b_token_id,
            arb.leg_b_price, p_sess, k_sess,
            opponent_raw_id=arb.leg_b_opponent_raw_id,
        )

        if not levels_a or not levels_b:
            logger.debug("Missing book data for arb: %s", arb.market_name)
            continue

        max_deploy, vwap_cost, num_shares = walk_arb_books(
            levels_a, levels_b,
            platform_a=arb.leg_a_platform,
            platform_b=arb.leg_b_platform,
        )

        arb.max_deploy = max_deploy
        arb.vwap_cost = vwap_cost

        if max_deploy > 0:
            vwap_profit = (1.0 - vwap_cost) / vwap_cost * 100
            logger.info(
                "Depth %s: $%.0f deployable (%d shares) | VWAP cost=%.1f¢ (%.2f%% profit)",
                arb.market_name, max_deploy, num_shares, vwap_cost * 100, vwap_profit,
            )
        else:
            logger.info("Depth %s: no arb in live book", arb.market_name)


def run_scan() -> None:
    """Execute a single scan cycle — find true arbs across platforms."""
    logger.info("=== Starting scan cycle ===")

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

    # Backfill start times from Pinnacle
    pin_times: dict[str, str] = {}
    for o in pin:
        if o.commence_time:
            pin_times[_normalize_team(o.team_name)] = o.commence_time

    def _backfill_time(outcomes: list[MarketOutcome]) -> None:
        for o in outcomes:
            if not o.commence_time:
                t = pin_times.get(_normalize_team(o.team_name), "")
                if t:
                    o.commence_time = t

    _backfill_time(poly)
    _backfill_time(kalshi)

    # Match across ALL platform pairs
    console.print("[cyan]Matching across platforms...[/cyan]")
    all_pairs = []
    all_pairs.extend(match_platforms(poly, pin, "Poly↔Pin"))
    all_pairs.extend(match_platforms(kalshi, pin, "Kalshi↔Pin"))
    all_pairs.extend(match_platforms(poly, kalshi, "Poly↔Kalshi"))

    # Backfill commence_time from matched pairs
    _event_times: dict[str, str] = {}
    for p in all_pairs:
        for side in [p.source_a, p.source_b, p.opponent_a, p.opponent_b]:
            if side and side.commence_time:
                _event_times[_normalize_team(side.team_name)] = side.commence_time
                _event_times[_normalize_team(side.event_name)] = side.commence_time

    for p in all_pairs:
        for side in [p.source_a, p.source_b, p.opponent_a, p.opponent_b]:
            if side and not side.commence_time:
                t = (_event_times.get(_normalize_team(side.team_name))
                     or _event_times.get(_normalize_team(side.event_name), ""))
                if t:
                    side.commence_time = t

    # Find true arbs
    arbs = find_arbs(all_pairs)

    # Enrich with order book depth
    if arbs:
        console.print(f"[cyan]Walking order books for {len(arbs)} arb(s)...[/cyan]")
        _enrich_arb_depth(arbs)

    # Render
    render_dashboard(arbs)
    save_chart(arbs)

    logger.info("=== Scan cycle complete ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="Esports Arb Scanner")
    parser.add_argument("--once", action="store_true", help="Single scan and exit")
    args = parser.parse_args()

    console.print("[bold cyan]Esports Arb Scanner[/bold cyan]")
    console.print("[dim]Polymarket × Pinnacle × Kalshi[/dim]")
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
