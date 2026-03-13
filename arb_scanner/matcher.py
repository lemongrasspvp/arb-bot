"""Fuzzy match events across Polymarket and Pinnacle.

Matching strategy:
  - H2H markets: extract both team names from Polymarket question,
    find a Pinnacle matchup where BOTH teams match. This prevents
    matching "Mouz" in IEM qualifier with "Mouz" in ESL Pro League.
  - Outright/futures: match team + tournament name together.
"""

import logging
import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from arb_scanner.clients.polymarket import PolymarketEvent
from arb_scanner.clients.pinnacle import PinnacleOutcome
from arb_scanner.config import MATCH_CONFIDENCE_THRESHOLD

logger = logging.getLogger(__name__)


@dataclass
class MatchedPair:
    """A matched event across both platforms."""
    polymarket: PolymarketEvent
    pinnacle: PinnacleOutcome  # the outcome that corresponds to YES on Polymarket
    pinnacle_opponent: PinnacleOutcome | None  # the other side
    confidence: float  # 0–100 fuzzy match score
    match_method: str  # how the match was found


def _normalize(text: str) -> str:
    """Normalize text for fuzzy matching."""
    text = text.lower().strip()
    # Remove common prefixes/suffixes
    text = re.sub(r"\b(esports?|gaming|team|club|org)\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_h2h_teams(question: str) -> tuple[str, str] | None:
    """Extract two team names from a head-to-head market question.

    Handles formats like:
      'LoL: Team A vs Team B (BO3) - Tournament Group X'
      'CS2: Team A vs Team B - ESL Pro League'
    """
    # Pattern: "Game: TeamA vs TeamB (...)  - ..."
    m = re.search(
        r"(?:LoL|CS2|Dota\s*2|Valorant)?:?\s*(.+?)\s+vs\.?\s+(.+?)(?:\s*\(.*?\))?\s*(?:-|$)",
        question,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _extract_outright(question: str) -> tuple[str, str] | None:
    """Extract team + tournament from an outright/futures market.

    Handles formats like:
      'Will Mouz win Blast Open Rotterdam 2026?'
      'Will G2 Esports qualify to MSI 2026?'
      'Will JD Gaming win ESL Pro League Season 23?'
    """
    m = re.search(
        r"will\s+(.+?)\s+(?:win|qualify\s+to|make\s+it\s+to)\s+(.+?)(?:\?|$)",
        question,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _team_score(name_a: str, name_b: str) -> float:
    """Score how well two team names match (0–100)."""
    return fuzz.token_sort_ratio(_normalize(name_a), _normalize(name_b))


def match_events(
    poly_events: list[PolymarketEvent],
    pinnacle_outcomes: list[PinnacleOutcome],
) -> list[MatchedPair]:
    """Match Polymarket events to Pinnacle outcomes.

    H2H: requires BOTH teams in the matchup to fuzzy-match.
    Outrights: requires team name + tournament context to match.
    """
    if not poly_events or not pinnacle_outcomes:
        logger.warning("No events to match (poly=%d, pinnacle=%d)", len(poly_events), len(pinnacle_outcomes))
        return []

    # Group Pinnacle outcomes by event (matchup)
    # Each matchup has exactly 2 outcomes (home, away)
    pinnacle_matchups: dict[str, list[PinnacleOutcome]] = {}
    for o in pinnacle_outcomes:
        pinnacle_matchups.setdefault(o.event_name, []).append(o)

    matched: list[MatchedPair] = []
    seen: set[str] = set()

    for poly in poly_events:
        # Try H2H matching first
        h2h = _extract_h2h_teams(poly.question)
        if h2h:
            poly_team_a, poly_team_b = h2h
            best_match = _find_h2h_match(poly, poly_team_a, poly_team_b, pinnacle_matchups)
            if best_match and best_match.polymarket.condition_id not in seen:
                seen.add(best_match.polymarket.condition_id)
                matched.append(best_match)
            continue

        # Try outright matching
        outright = _extract_outright(poly.question)
        if outright:
            poly_team, poly_tournament = outright
            best_match = _find_outright_match(poly, poly_team, poly_tournament, pinnacle_outcomes)
            if best_match and best_match.polymarket.condition_id not in seen:
                seen.add(best_match.polymarket.condition_id)
                matched.append(best_match)

    logger.info("Matched %d event pairs (threshold=%d%%)", len(matched), MATCH_CONFIDENCE_THRESHOLD)
    return matched


def _find_h2h_match(
    poly: PolymarketEvent,
    poly_team_a: str,
    poly_team_b: str,
    pinnacle_matchups: dict[str, list[PinnacleOutcome]],
) -> MatchedPair | None:
    """Find a Pinnacle matchup where BOTH teams match the Polymarket H2H."""
    best: MatchedPair | None = None
    best_score = 0.0

    for event_name, outcomes in pinnacle_matchups.items():
        if len(outcomes) < 2:
            continue

        pin_teams = [o.outcome_name for o in outcomes[:2]]

        # Try both orderings: (poly_a↔pin_0, poly_b↔pin_1) and (poly_a↔pin_1, poly_b↔pin_0)
        for pin_home_idx, pin_away_idx in [(0, 1), (1, 0)]:
            score_a = _team_score(poly_team_a, pin_teams[pin_home_idx])
            score_b = _team_score(poly_team_b, pin_teams[pin_away_idx])

            # BOTH teams must meet the threshold
            if score_a < MATCH_CONFIDENCE_THRESHOLD or score_b < MATCH_CONFIDENCE_THRESHOLD:
                continue

            combined = (score_a + score_b) / 2
            if combined > best_score:
                best_score = combined
                # The first token on Polymarket is YES (first team listed).
                # Match that to the corresponding Pinnacle outcome.
                primary = outcomes[pin_home_idx]
                opponent = outcomes[pin_away_idx]
                best = MatchedPair(
                    polymarket=poly,
                    pinnacle=primary,
                    pinnacle_opponent=opponent,
                    confidence=combined,
                    match_method="h2h_both_teams",
                )

    return best


def _find_outright_match(
    poly: PolymarketEvent,
    poly_team: str,
    poly_tournament: str,
    pinnacle_outcomes: list[PinnacleOutcome],
) -> MatchedPair | None:
    """Find a Pinnacle outcome matching team + tournament context.

    For outrights like 'Will Mouz win ESL Pro League?', we need the
    Pinnacle outcome to be from the SAME tournament, not just the same team.
    Since Pinnacle H2H markets don't have tournament context, outrights
    can only match if Pinnacle has a futures/outright market for that tournament.
    In practice, Pinnacle's guest API only exposes H2H matchups, so most
    outright Polymarket markets won't have a direct Pinnacle counterpart.
    We still attempt a match but require very high confidence.
    """
    # For now, skip outrights — they almost always produce false positives
    # because Pinnacle's API only has H2H matchups, not tournament winners.
    # A "Will Mouz win ESL Pro League?" on Polymarket has no equivalent on
    # Pinnacle's matchup-level data.
    return None
