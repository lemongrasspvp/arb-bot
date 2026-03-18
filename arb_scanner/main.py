"""Orchestrator — fetch from all platforms, find true cross-platform arbs."""

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from rich.console import Console

from arb_scanner.config import REFRESH_INTERVAL, BETFAIR_USERNAME
from arb_scanner.clients.polymarket import fetch_markets as fetch_polymarket
from arb_scanner.clients.pinnacle import fetch_odds as fetch_pinnacle
from arb_scanner.clients.kalshi import fetch_markets as fetch_kalshi
from arb_scanner.clients.betfair import (
    fetch_markets as fetch_betfair,
    _fetch_book_levels as bf_book_levels,
    _build_client as bf_build_client,
)
from arb_scanner.matcher import MarketOutcome, match_platforms, match_totals
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


def _strip_event_prefix(name: str) -> str:
    """Strip event/tournament prefixes before a colon.

    '326: Renato Moicano' -> 'Renato Moicano'
    'Fight Night: Movsar Evloev' -> 'Movsar Evloev'
    'BNP Paribas Open: Carlos Alcaraz' -> 'Carlos Alcaraz'
    """
    if ":" in name:
        return name.split(":", 1)[1].strip()
    return name


def _extract_teams(question: str) -> tuple[str, str] | None:
    """Extract 'Team/Player A' and 'Team/Player B' from an H2H question string.

    Handles esports (LoL: Team A vs Team B (BO3) - Tournament),
    UFC (Fighter A vs. Fighter B), and tennis (Player A vs Player B).
    """
    m = re.search(
        r"(?:LoL|CS2|Dota\s*2|Valorant|UFC|ATP|WTA)?:?\s*(.+?)\s+vs\.?\s+(.+?)(?:\s*\(.*?\))?\s*(?:-|$)",
        question,
        re.IGNORECASE,
    )
    if m:
        return _strip_event_prefix(m.group(1).strip()), _strip_event_prefix(m.group(2).strip())
    # Fallback: simple "A vs B" with no suffix
    m2 = re.search(r"(.+?)\s+vs\.?\s+(.+?)$", question.strip(), re.IGNORECASE)
    if m2:
        return _strip_event_prefix(m2.group(1).strip()), _strip_event_prefix(m2.group(2).strip())
    return None


def _detect_sport(slug_lower: str, q_lower: str) -> str:
    """Detect sport from Polymarket slug and question text."""
    if "lol" in slug_lower or "league-of-legends" in slug_lower:
        return "lol"
    if "dota" in slug_lower:
        return "dota2"
    if "valorant" in slug_lower:
        return "valorant"
    if "call-of-duty" in slug_lower or "cod" in slug_lower:
        return "cod"
    if "ufc" in slug_lower or "ufc" in q_lower or "mma" in slug_lower:
        return "ufc"
    if "tennis" in slug_lower or "atp" in slug_lower or "wta" in slug_lower:
        return "tennis"
    if "ncaa" in slug_lower or "college-basketball" in slug_lower or "march-madness" in slug_lower:
        return "ncaab"
    if "euroleague" in slug_lower:
        return "euroleague"
    if "nbl" in slug_lower:
        return "nbl"
    return "cs2"


def _poly_to_outcomes(poly_events) -> list[MarketOutcome]:
    """Convert Polymarket events to MarketOutcome pairs (moneyline + totals)."""
    outcomes = []
    for e in poly_events:
        slug_lower = e.slug.lower()
        q_lower = e.question.lower()
        sport = _detect_sport(slug_lower, q_lower)

        if e.market_type == "totals":
            # Totals: YES = Over, NO = Under
            handicap = e.handicap
            handicap_str = f"{handicap:g}"
            # Extract team names for event matching.
            # Polymarket totals questions: "Team A vs. Team B: Total Sets O/U 2.5"
            # Strip the totals suffix before extracting teams.
            q_for_teams = e.question.split(":")[0] if ":" in e.question else e.question
            q_for_teams = re.sub(
                r"\s*(O/U|Over|Under|Total\s+\w+)\s*[\d.]*\s*$", "",
                q_for_teams, flags=re.IGNORECASE,
            ).strip()
            teams = _extract_teams(q_for_teams)
            event_name = f"{teams[0]} vs {teams[1]}" if teams else q_for_teams
            yes_ask = e.yes_ask if e.yes_ask > 0 else e.yes_price
            no_ask = e.no_ask if e.no_ask > 0 else e.no_price
            outcomes.append(MarketOutcome(
                platform="polymarket", event_name=event_name,
                team_name=f"Over {handicap_str}", implied_prob=e.yes_price,
                sport=sport, raw_id=e.condition_id,
                actual_price=yes_ask,
                token_id=e.yes_token_id,
                market_type="totals",
                handicap=handicap,
            ))
            outcomes.append(MarketOutcome(
                platform="polymarket", event_name=event_name,
                team_name=f"Under {handicap_str}", implied_prob=e.no_price,
                sport=sport, raw_id=e.condition_id + "_no",
                actual_price=no_ask,
                token_id=e.no_token_id,
                market_type="totals",
                handicap=handicap,
            ))
        else:
            # Moneyline (existing logic)
            teams = _extract_teams(e.question)
            if not teams:
                continue
            team_a, team_b = teams
            event_name = e.question
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
            market_type=getattr(o, "market_type", "moneyline"),
            handicap=getattr(o, "handicap", 0.0),
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


def _betfair_to_outcomes(bf_markets) -> list[MarketOutcome]:
    """Convert Betfair outcomes to MarketOutcome list."""
    return [
        MarketOutcome(
            platform="betfair", event_name=o.event_name,
            team_name=o.team_name, implied_prob=o.actual_price,
            sport=o.sport, raw_id=f"{o.market_id}_{o.selection_id}",
            actual_price=o.actual_price,
            commence_time=o.commence_time,
        )
        for o in bf_markets
    ]


def _get_book_levels(platform: str, raw_id: str, token_id: str,
                     price: float, p_sess, k_sess,
                     opponent_raw_id: str = "",
                     bf_client=None) -> list[tuple[float, float]]:
    """Fetch ask book levels for a given platform/ID.

    For sportsbooks (Pinnacle), return a synthetic level with large size
    since they offer fixed prices with high limits.
    """
    if platform == "polymarket" and token_id:
        return poly_book_levels(p_sess, token_id, side="asks")
    elif platform == "kalshi" and raw_id:
        return kalshi_book_levels(k_sess, raw_id, opponent_ticker=opponent_raw_id)
    elif platform == "betfair" and raw_id and bf_client:
        # Parse market_id and selection_id from raw_id ("1.234567_12345")
        parts = raw_id.rsplit("_", 1)
        if len(parts) == 2:
            market_id, sel_id_str = parts
            try:
                return bf_book_levels(bf_client, market_id, int(sel_id_str))
            except (ValueError, TypeError):
                pass
        return []
    elif platform == "pinnacle" and price > 0:
        # Sportsbooks have fixed prices — treat as deep liquidity
        # Use 5000 shares as a reasonable Pinnacle limit
        return [(price, 5000)]
    return []


def _enrich_arb_depth(arbs: list) -> None:
    """Walk both order books to find max deployable $ for each arb."""
    p_sess = poly_session()
    k_sess = kalshi_session()
    # Only build Betfair client if any arb involves Betfair
    bf_client = None
    if any(a.leg_a_platform == "betfair" or a.leg_b_platform == "betfair" for a in arbs):
        bf_client = bf_build_client()

    for arb in arbs:
        levels_a = _get_book_levels(
            arb.leg_a_platform, arb.leg_a_raw_id, arb.leg_a_token_id,
            arb.leg_a_price, p_sess, k_sess,
            opponent_raw_id=arb.leg_a_opponent_raw_id,
            bf_client=bf_client,
        )
        levels_b = _get_book_levels(
            arb.leg_b_platform, arb.leg_b_raw_id, arb.leg_b_token_id,
            arb.leg_b_price, p_sess, k_sess,
            opponent_raw_id=arb.leg_b_opponent_raw_id,
            bf_client=bf_client,
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

    has_betfair = bool(BETFAIR_USERNAME)
    platforms = "Polymarket, Pinnacle, Kalshi"
    if has_betfair:
        platforms += ", Betfair"
    console.print(f"[cyan]Fetching from {platforms}...[/cyan]")

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_poly = pool.submit(fetch_polymarket)
        f_pin = pool.submit(fetch_pinnacle)
        f_kalshi = pool.submit(fetch_kalshi)
        f_bf = pool.submit(fetch_betfair) if has_betfair else None

    poly_raw = f_poly.result()
    pin_raw = f_pin.result()
    kalshi_raw = f_kalshi.result()
    bf_raw = f_bf.result() if f_bf else []

    counts = (
        f"[dim]Polymarket: {len(poly_raw)} | "
        f"Pinnacle: {len(pin_raw)} | "
        f"Kalshi: {len(kalshi_raw)}"
    )
    if has_betfair:
        counts += f" | Betfair: {len(bf_raw)}"
    counts += "[/dim]"
    console.print(counts)

    # Convert to unified MarketOutcome format
    poly = _poly_to_outcomes(poly_raw)
    pin = _pinnacle_to_outcomes(pin_raw)
    kalshi = _kalshi_to_outcomes(kalshi_raw)
    bf = _betfair_to_outcomes(bf_raw) if bf_raw else []

    # Backfill start times from Pinnacle
    pin_times: dict[str, str] = {}
    for o in pin:
        if o.commence_time:
            pin_times[_normalize_team(o.team_name)] = o.commence_time

    def _backfill_time(outcomes: list[MarketOutcome]) -> None:
        for o in outcomes:
            if not o.commence_time:
                norm = _normalize_team(o.team_name)
                # Exact match first
                t = pin_times.get(norm, "")
                # Substring fallback: Pinnacle uses short names (e.g. "s2g")
                # while exchanges use full names (e.g. "s2g esports")
                if not t:
                    for pin_name, pin_time in pin_times.items():
                        if pin_name in norm or norm in pin_name:
                            t = pin_time
                            break
                if t:
                    o.commence_time = t

    _backfill_time(poly)
    _backfill_time(kalshi)
    if bf:
        _backfill_time(bf)

    # Match across ALL platform pairs (moneyline)
    console.print("[cyan]Matching across platforms...[/cyan]")
    all_pairs = []
    all_pairs.extend(match_platforms(poly, pin, "Poly↔Pin"))
    all_pairs.extend(match_platforms(kalshi, pin, "Kalshi↔Pin"))
    all_pairs.extend(match_platforms(poly, kalshi, "Poly↔Kalshi"))
    if bf:
        all_pairs.extend(match_platforms(poly, bf, "Poly↔BF"))
        all_pairs.extend(match_platforms(kalshi, bf, "Kalshi↔BF"))
        all_pairs.extend(match_platforms(bf, pin, "BF↔Pin"))

    # Match totals (over/under) across platforms
    totals_pairs = []
    totals_pairs.extend(match_totals(poly, pin, "Poly↔Pin TOTALS"))
    # Kalshi totals skipped this iteration — no known series tickers
    all_pairs.extend(totals_pairs)

    # Print sample totals matches for verification
    if totals_pairs:
        console.print(f"\n[bold green]Totals matches: {len(totals_pairs)} pairs found[/bold green]")
        for p in totals_pairs[:3]:
            console.print(
                f"  [dim]{p.pair_label}[/dim] {p.source_a.event_name} "
                f"| {p.source_a.team_name} ({p.source_a.platform}) ↔ "
                f"{p.source_b.team_name} ({p.source_b.platform}) "
                f"[dim]conf={p.confidence:.0f}%[/dim]"
            )

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
    plat_list = "Polymarket × Pinnacle × Kalshi"
    if BETFAIR_USERNAME:
        plat_list += " × Betfair"
    console.print(f"[dim]{plat_list}[/dim]")
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
