"""Market registry — tracks matched events across Polymarket, Kalshi, and Pinnacle."""

import logging
import re
import time
from dataclasses import dataclass, field

from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


@dataclass
class TrackedMatch:
    """A single H2H event matched across platforms."""
    match_id: str
    teams: tuple[str, str]     # (team_a_name, team_b_name)
    sport: str
    # Polymarket
    poly_token_id_a: str = ""  # YES token for team A
    poly_token_id_b: str = ""  # YES token for team B (= NO token for A)
    poly_condition_id: str = ""
    # Kalshi
    kalshi_ticker_a: str = ""  # Team A's market ticker
    kalshi_ticker_b: str = ""  # Team B's (opponent) ticker
    # Pinnacle reference prices (no-vig)
    pinnacle_prob_a: float = 0.0
    pinnacle_prob_b: float = 0.0
    pinnacle_key: str = ""
    # Timing
    commence_time: str = ""
    # Pinnacle freeze detection: if odds don't change across consecutive live polls,
    # the line is likely suspended (goals, set changes, red cards, etc.)
    _prev_pinnacle_prob_a: float = 0.0
    _prev_pinnacle_prob_b: float = 0.0
    pinnacle_frozen_a: bool = False
    pinnacle_frozen_b: bool = False
    pinnacle_last_update_a: float = 0.0  # timestamp of last actual change
    pinnacle_last_update_b: float = 0.0
    # Last time we received ANY Pinnacle data (even if unchanged) — for staleness
    pinnacle_last_seen_a: float = 0.0
    pinnacle_last_seen_b: float = 0.0
    # Line movement detection: if odds shifted >3% between polls, the line is
    # actively moving (steam move / sharp action) and our reference is unreliable.
    pinnacle_moving_a: bool = False
    pinnacle_moving_b: bool = False
    # Two-tier matching: DISCOVERY_OK (show on dashboard only) vs EXECUTION_OK (safe to bet)
    confidence_tier: str = "DISCOVERY_OK"


class MarketRegistry:
    """Maps markets across platforms for real-time tracking."""

    def __init__(self):
        self.matches: dict[str, TrackedMatch] = {}
        # Reverse lookups: market_id → match_id
        self._poly_to_match: dict[str, tuple[str, str]] = {}   # token_id → (match_id, "a"|"b")
        self._kalshi_to_match: dict[str, tuple[str, str]] = {}  # ticker → (match_id, "a"|"b")
        self._pinnacle_to_match: dict[str, str] = {}            # key → match_id
        # Initial ask prices from scanner — seed engine cache before WS sends updates
        # market_id → {"best_ask": float, "timestamp": float}
        self.initial_prices: dict[str, dict[str, dict]] = {"polymarket": {}, "kalshi": {}}

    @property
    def poly_token_ids(self) -> list[str]:
        """All Polymarket token IDs we're tracking."""
        ids = []
        for m in self.matches.values():
            if m.poly_token_id_a:
                ids.append(m.poly_token_id_a)
            if m.poly_token_id_b:
                ids.append(m.poly_token_id_b)
        return ids

    @property
    def kalshi_tickers(self) -> list[str]:
        """All Kalshi tickers we're tracking."""
        tickers = []
        for m in self.matches.values():
            if m.kalshi_ticker_a:
                tickers.append(m.kalshi_ticker_a)
            if m.kalshi_ticker_b:
                tickers.append(m.kalshi_ticker_b)
        return tickers

    def seed_initial_price(self, platform: str, market_id: str, ask_price: float) -> None:
        """Store an initial ask price from the scanner for engine cache seeding."""
        if ask_price > 0 and market_id:
            self.initial_prices[platform][market_id] = {
                "best_ask": ask_price,
                "best_bid": 0.0,
                "timestamp": time.time(),
            }

    def get_match_for_market(self, platform: str, market_id: str) -> tuple[TrackedMatch | None, str]:
        """Look up which TrackedMatch a given market belongs to.

        Returns (match, side) where side is "a" or "b".
        """
        if platform == "polymarket":
            entry = self._poly_to_match.get(market_id)
            if entry:
                match_id, side = entry
                return self.matches.get(match_id), side
        elif platform == "kalshi":
            entry = self._kalshi_to_match.get(market_id)
            if entry:
                match_id, side = entry
                return self.matches.get(match_id), side
        elif platform == "pinnacle":
            match_id = self._pinnacle_to_match.get(market_id)
            if match_id:
                return self.matches.get(match_id), ""
        return None, ""

    def update_pinnacle_price(
        self, team_name: str, sport: str, no_vig_prob: float,
        event_name: str = "",
    ) -> None:
        """Update Pinnacle no-vig probability for a team across all matches.

        Also detects frozen/suspended lines: if the odds haven't changed
        between consecutive polls, mark them as frozen. This catches Pinnacle
        suspending their line during live events (goals, set changes, etc.).

        For totals markets (teams like "Over 2.5"), event_name is required
        to disambiguate between different events with the same over/under line.
        """
        import time
        now = time.time()
        norm = _normalize(team_name)
        is_totals = "over" in norm or "under" in norm

        for match in self.matches.values():
            if sport and match.sport and match.sport != sport:
                continue

            # For totals: require event_name match to avoid cross-event contamination
            # (multiple events can have "Over 2.5" but with different probabilities)
            if is_totals and event_name:
                # Check if the event_name matches this match's context
                norm_event = _normalize(event_name)
                # For totals, match_id contains team names. Check if both
                # team names from the event appear in the match_id.
                event_teams = _extract_teams_from_event(event_name)
                if event_teams:
                    t1_norm = _normalize(event_teams[0])
                    t2_norm = _normalize(event_teams[1])
                    # At least one team should fuzzy-match in the match_id
                    mid_norm = _normalize(match.match_id)
                    if (fuzz.partial_ratio(t1_norm, mid_norm) < 60 and
                            fuzz.partial_ratio(t2_norm, mid_norm) < 60):
                        continue

            norm_a = _normalize(match.teams[0])
            norm_b = _normalize(match.teams[1])
            # Threshold 85 prevents partial name collisions like "FURIA" matching "FURIA fe"
            if fuzz.token_sort_ratio(norm, norm_a) > 85:
                match.pinnacle_last_seen_a = now  # always update: we got data
                # Freeze detection: same odds as last poll = likely suspended
                if match._prev_pinnacle_prob_a > 0 and abs(no_vig_prob - match._prev_pinnacle_prob_a) < 0.001:
                    match.pinnacle_frozen_a = True
                else:
                    match.pinnacle_frozen_a = False
                    match.pinnacle_last_update_a = now
                # Line movement detection: >3pp shift between polls = steam move
                if match.pinnacle_prob_a > 0:
                    shift = abs(no_vig_prob - match.pinnacle_prob_a)
                    match.pinnacle_moving_a = shift > 0.03
                match._prev_pinnacle_prob_a = match.pinnacle_prob_a
                match.pinnacle_prob_a = no_vig_prob
            elif fuzz.token_sort_ratio(norm, norm_b) > 85:
                match.pinnacle_last_seen_b = now  # always update: we got data
                if match._prev_pinnacle_prob_b > 0 and abs(no_vig_prob - match._prev_pinnacle_prob_b) < 0.001:
                    match.pinnacle_frozen_b = True
                else:
                    match.pinnacle_frozen_b = False
                    match.pinnacle_last_update_b = now
                # Line movement detection
                if match.pinnacle_prob_b > 0:
                    shift = abs(no_vig_prob - match.pinnacle_prob_b)
                    match.pinnacle_moving_b = shift > 0.03
                match._prev_pinnacle_prob_b = match.pinnacle_prob_b
                match.pinnacle_prob_b = no_vig_prob


def _extract_teams_from_event(event_name: str) -> tuple[str, str] | None:
    """Extract team names from an event name like 'G2 vs FaZe'."""
    m = re.search(r"(.+?)\s+vs\.?\s+(.+?)$", event_name.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _normalize(text: str) -> str:
    """Normalize team name for matching."""
    text = text.lower().strip()
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    text = re.sub(r"\b(esports?|gaming|team|club|org)\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_registry_from_scanner() -> MarketRegistry:
    """Run the existing scanner's fetch + match pipeline to build the registry.

    Reuses arb_scanner's fetchers and matcher to discover cross-platform pairs,
    then converts them into TrackedMatch entries.
    """
    from arb_scanner.clients.polymarket import fetch_markets as fetch_polymarket
    from arb_scanner.clients.kalshi import fetch_markets as fetch_kalshi
    from arb_scanner.clients.pinnacle import fetch_odds as fetch_pinnacle
    from arb_scanner.matcher import MarketOutcome, match_platforms, match_totals
    from arb_scanner.main import (
        _poly_to_outcomes,
        _kalshi_to_outcomes,
        _pinnacle_to_outcomes,
        _normalize_team,
    )
    from concurrent.futures import ThreadPoolExecutor

    logger.info("Building market registry from scanner...")

    # Fetch from all platforms concurrently
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_poly = pool.submit(fetch_polymarket)
        f_kalshi = pool.submit(fetch_kalshi)
        f_pin = pool.submit(fetch_pinnacle)

    poly_raw = f_poly.result()
    kalshi_raw = f_kalshi.result()
    pin_raw = f_pin.result()

    # Convert to unified format
    poly = _poly_to_outcomes(poly_raw)
    kalshi = _kalshi_to_outcomes(kalshi_raw)
    pin = _pinnacle_to_outcomes(pin_raw)

    logger.info(
        "Fetched: Polymarket=%d, Kalshi=%d, Pinnacle=%d outcomes",
        len(poly), len(kalshi), len(pin),
    )

    # Backfill start times from Pinnacle
    pin_times: dict[str, str] = {}
    for o in pin:
        if o.commence_time:
            pin_times[_normalize_team(o.team_name)] = o.commence_time

    for outcomes in [poly, kalshi]:
        for o in outcomes:
            if not o.commence_time:
                norm = _normalize_team(o.team_name)
                t = pin_times.get(norm, "")
                if not t:
                    for pin_name, pin_time in pin_times.items():
                        if pin_name in norm or norm in pin_name:
                            t = pin_time
                            break
                if t:
                    o.commence_time = t

    # Match across platform pairs
    poly_kalshi_pairs = match_platforms(poly, kalshi, "Poly↔Kalshi")
    poly_pin_pairs = match_platforms(poly, pin, "Poly↔Pin")
    kalshi_pin_pairs = match_platforms(kalshi, pin, "Kalshi↔Pin")

    logger.info(
        "Matched pairs: Poly↔Kalshi=%d, Poly↔Pin=%d, Kalshi↔Pin=%d",
        len(poly_kalshi_pairs), len(poly_pin_pairs), len(kalshi_pin_pairs),
    )

    # Build the registry
    registry = MarketRegistry()

    # Build Pinnacle lookup keyed by BOTH team names in the event.
    # This prevents the same team in different matches from getting
    # the wrong Pinnacle price (e.g. R2 Esports in two different events).
    # Key: (normalized_team_a, sport) → (prob, event_name, opponent_prob)
    pin_event_lookup: dict[str, dict[str, float]] = {}
    # Group Pinnacle outcomes by event
    pin_by_event: dict[str, list] = {}
    for o in pin:
        pin_by_event.setdefault(o.event_name, []).append(o)

    for event_name, event_outcomes in pin_by_event.items():
        if len(event_outcomes) < 2:
            continue
        for o in event_outcomes:
            # Key includes sport + event to prevent cross-match contamination
            # MarketOutcome uses team_name and implied_prob (already no-vig for Pinnacle)
            key = f"{_normalize(o.team_name)}|{o.sport}|{_normalize(event_name)}"
            pin_event_lookup[key] = {
                "prob": o.implied_prob,
                "sport": o.sport,
                "event": event_name,
            }

    # Also build simple lookup for backward compat, but only used as last resort
    pin_lookup: dict[str, tuple[float, str, str]] = {}
    for o in pin:
        pin_lookup[_normalize(o.team_name)] = (o.implied_prob, o.sport, o.event_name)

    def _find_pinnacle_prob(team_name: str, opponent_name: str, sport: str) -> float:
        """Find Pinnacle no-vig prob, matching on event context (both teams)."""
        norm_team = _normalize(team_name)
        norm_opp = _normalize(opponent_name)

        # Strategy 1: Match via Poly↔Pin or Kalshi↔Pin matched pairs
        # (most reliable — the matcher already validated both sides)
        best_prob = 0.0
        best_score = 0

        for key, info in pin_event_lookup.items():
            parts = key.split("|")
            if len(parts) < 3:
                continue
            pin_name, pin_sport, pin_event = parts[0], parts[1], parts[2]

            if sport and pin_sport and sport != pin_sport:
                continue

            team_score = fuzz.token_sort_ratio(norm_team, pin_name)
            # Also check if the opponent appears in the event name
            # Use partial_ratio for substring matching (e.g. "Players" in "FOLHA AMARELA vs Players")
            event_has_opponent = fuzz.partial_ratio(norm_opp, pin_event) > 50

            if team_score > 75 and event_has_opponent and team_score > best_score:
                best_prob = info["prob"]
                best_score = team_score

        return best_prob

    # Process Poly↔Kalshi pairs (these are the tradeable arb pairs)
    for pair in poly_kalshi_pairs:
        a = pair.source_a       # team X on platform A
        b = pair.source_b       # team X on platform B
        opp_a = pair.opponent_a  # team Y on platform A
        opp_b = pair.opponent_b  # team Y on platform B

        if not opp_a or not opp_b:
            continue

        # Determine which platform is which
        if a.platform == "polymarket":
            poly_a, poly_b = a, opp_a
            kalshi_a, kalshi_b = b, opp_b
        else:
            poly_a, poly_b = b, opp_b
            kalshi_a, kalshi_b = a, opp_a

        match_id = f"{poly_a.team_name}_vs_{poly_b.team_name}_{poly_a.sport}"
        commence = (
            a.commence_time or b.commence_time
            or opp_a.commence_time or opp_b.commence_time
        )

        # Look up Pinnacle prices using event context (both teams must match)
        pin_prob_a = _find_pinnacle_prob(
            poly_a.team_name, poly_b.team_name, poly_a.sport
        )
        pin_prob_b = _find_pinnacle_prob(
            poly_b.team_name, poly_a.team_name, poly_a.sport
        )

        match = TrackedMatch(
            match_id=match_id,
            teams=(poly_a.team_name, poly_b.team_name),
            sport=poly_a.sport or kalshi_a.sport,
            poly_token_id_a=poly_a.token_id,
            poly_token_id_b=poly_b.token_id,
            poly_condition_id=poly_a.raw_id,
            kalshi_ticker_a=kalshi_a.raw_id,
            kalshi_ticker_b=kalshi_b.raw_id,
            pinnacle_prob_a=pin_prob_a,
            pinnacle_prob_b=pin_prob_b,
            commence_time=commence,
            confidence_tier=pair.confidence_tier,
        )

        registry.matches[match_id] = match
        # Seed initial prices from scanner
        registry.seed_initial_price("polymarket", poly_a.token_id, poly_a.actual_price or poly_a.implied_prob)
        registry.seed_initial_price("polymarket", poly_b.token_id, poly_b.actual_price or poly_b.implied_prob)
        registry.seed_initial_price("kalshi", kalshi_a.raw_id, kalshi_a.actual_price or kalshi_a.implied_prob)
        registry.seed_initial_price("kalshi", kalshi_b.raw_id, kalshi_b.actual_price or kalshi_b.implied_prob)
        # Build reverse lookups
        if match.poly_token_id_a:
            registry._poly_to_match[match.poly_token_id_a] = (match_id, "a")
        if match.poly_token_id_b:
            registry._poly_to_match[match.poly_token_id_b] = (match_id, "b")
        if match.kalshi_ticker_a:
            registry._kalshi_to_match[match.kalshi_ticker_a] = (match_id, "a")
        if match.kalshi_ticker_b:
            registry._kalshi_to_match[match.kalshi_ticker_b] = (match_id, "b")

    # Also register Poly-only and Kalshi-only matches (for value betting via Pinnacle)
    _add_single_platform_matches(registry, poly, pin_event_lookup, _find_pinnacle_prob, "polymarket")
    _add_single_platform_matches(registry, kalshi, pin_event_lookup, _find_pinnacle_prob, "kalshi")

    # ── Totals (over/under) markets ──
    # Match totals across Poly↔Pin for value betting
    totals_pairs = match_totals(poly, pin, "Poly↔Pin TOTALS")
    logger.info("Totals matched: Poly↔Pin=%d pairs", len(totals_pairs))

    for pair in totals_pairs:
        over_poly = pair.source_a       # Over on Polymarket
        over_pin = pair.source_b        # Over on Pinnacle
        under_poly = pair.opponent_a    # Under on Polymarket
        under_pin = pair.opponent_b     # Under on Pinnacle

        if not over_poly or not under_poly or not over_pin or not under_pin:
            continue

        # Build match_id with handicap (use underscore instead of dot)
        handicap = over_poly.handicap
        handicap_str = f"{handicap:g}".replace(".", "_")
        # Extract team names from event for the match_id
        event_teams = _extract_teams_from_event(over_poly.event_name)
        if event_teams:
            match_id = f"{event_teams[0]}_vs_{event_teams[1]}_{over_poly.sport}_total_{handicap_str}"
        else:
            match_id = f"{over_poly.event_name}_{over_poly.sport}_total_{handicap_str}"

        if match_id in registry.matches:
            continue

        match = TrackedMatch(
            match_id=match_id,
            teams=(over_poly.team_name, under_poly.team_name),  # "Over 2.5", "Under 2.5"
            sport=over_poly.sport,
            poly_token_id_a=over_poly.token_id,
            poly_token_id_b=under_poly.token_id,
            poly_condition_id=over_poly.raw_id,
            pinnacle_prob_a=over_pin.implied_prob,
            pinnacle_prob_b=under_pin.implied_prob,
            commence_time=over_pin.commence_time or over_poly.commence_time,
            confidence_tier=pair.confidence_tier,
        )

        registry.matches[match_id] = match
        # Seed initial prices from scanner
        registry.seed_initial_price("polymarket", over_poly.token_id, over_poly.actual_price or over_poly.implied_prob)
        registry.seed_initial_price("polymarket", under_poly.token_id, under_poly.actual_price or under_poly.implied_prob)
        if match.poly_token_id_a:
            registry._poly_to_match[match.poly_token_id_a] = (match_id, "a")
        if match.poly_token_id_b:
            registry._poly_to_match[match.poly_token_id_b] = (match_id, "b")

    exec_ok = sum(1 for m in registry.matches.values() if m.confidence_tier == "EXECUTION_OK")
    logger.info(
        "Registry built: %d tracked matches (%d EXECUTION_OK, %d DISCOVERY_OK)",
        len(registry.matches), exec_ok, len(registry.matches) - exec_ok,
    )
    return registry


def _add_single_platform_matches(
    registry: MarketRegistry,
    outcomes: list,
    pin_event_lookup: dict,
    find_pin_prob_fn,
    platform: str,
) -> None:
    """Add matches that exist on only one tradeable platform (for value betting).

    These can't be arbed (need both Poly and Kalshi), but CAN be value bet
    if Pinnacle's price diverges.
    """
    # Group outcomes by event
    by_event: dict[str, list] = {}
    for o in outcomes:
        by_event.setdefault(o.event_name, []).append(o)

    for event_name, event_outcomes in by_event.items():
        if len(event_outcomes) < 2:
            continue

        team_a = event_outcomes[0]
        team_b = event_outcomes[1]
        match_id = f"{team_a.team_name}_vs_{team_b.team_name}_{team_a.sport}"

        # Skip if already registered (from Poly↔Kalshi pairs)
        if match_id in registry.matches:
            continue

        # Look up Pinnacle prices using event-aware matching
        pin_prob_a = find_pin_prob_fn(team_a.team_name, team_b.team_name, team_a.sport)
        pin_prob_b = find_pin_prob_fn(team_b.team_name, team_a.team_name, team_a.sport)

        # Only add if we have Pinnacle reference prices
        if pin_prob_a <= 0 and pin_prob_b <= 0:
            continue

        # Classify tier for single-platform matches:
        # EXECUTION_OK requires both Pinnacle probs found and summing to ~1.0
        tier = "DISCOVERY_OK"
        if pin_prob_a > 0 and pin_prob_b > 0:
            prob_sum = pin_prob_a + pin_prob_b
            if 0.90 <= prob_sum <= 1.10:
                tier = "EXECUTION_OK"

        match = TrackedMatch(
            match_id=match_id,
            teams=(team_a.team_name, team_b.team_name),
            sport=team_a.sport,
            commence_time=team_a.commence_time or team_b.commence_time,
            pinnacle_prob_a=pin_prob_a,
            pinnacle_prob_b=pin_prob_b,
            confidence_tier=tier,
        )

        if platform == "polymarket":
            match.poly_token_id_a = team_a.token_id
            match.poly_token_id_b = team_b.token_id
            match.poly_condition_id = team_a.raw_id
        else:
            match.kalshi_ticker_a = team_a.raw_id
            match.kalshi_ticker_b = team_b.raw_id

        registry.matches[match_id] = match
        # Seed initial prices from scanner
        registry.seed_initial_price(platform, team_a.token_id or team_a.raw_id, team_a.actual_price or team_a.implied_prob)
        registry.seed_initial_price(platform, team_b.token_id or team_b.raw_id, team_b.actual_price or team_b.implied_prob)
        if match.poly_token_id_a:
            registry._poly_to_match[match.poly_token_id_a] = (match_id, "a")
        if match.poly_token_id_b:
            registry._poly_to_match[match.poly_token_id_b] = (match_id, "b")
        if match.kalshi_ticker_a:
            registry._kalshi_to_match[match.kalshi_ticker_a] = (match_id, "a")
        if match.kalshi_ticker_b:
            registry._kalshi_to_match[match.kalshi_ticker_b] = (match_id, "b")
