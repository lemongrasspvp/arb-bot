"""Fuzzy match events across any pair of platforms.

Matching strategy:
  - H2H markets: extract both team names, find a matchup on the other
    platform where BOTH teams match. Prevents cross-event false matches.
"""

import logging
import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from arb_scanner.config import MATCH_CONFIDENCE_THRESHOLD

logger = logging.getLogger(__name__)


@dataclass
class MarketOutcome:
    """Unified representation of a market outcome from any platform."""
    platform: str          # "polymarket", "pinnacle", "kalshi"
    event_name: str        # "Team A vs Team B" or full question
    team_name: str         # which team this outcome is for
    implied_prob: float    # 0–1 probability for this team winning
    sport: str             # "lol" or "cs2"
    raw_id: str            # platform-specific ID (condition_id, ticker, etc.)


@dataclass
class MatchedPair:
    """A matched H2H event across two platforms."""
    source_a: MarketOutcome    # first platform's outcome for team A
    source_b: MarketOutcome    # second platform's outcome for same team
    opponent_a: MarketOutcome | None  # first platform's other side
    opponent_b: MarketOutcome | None  # second platform's other side
    confidence: float          # 0–100 fuzzy match score
    pair_label: str            # e.g. "Poly↔Pin", "Poly↔Kalshi"


def _normalize(text: str) -> str:
    """Normalize text for fuzzy matching."""
    text = text.lower().strip()
    text = re.sub(r"\b(esports?|gaming|team|club|org)\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _team_score(name_a: str, name_b: str) -> float:
    """Score how well two team names match (0–100)."""
    return fuzz.token_sort_ratio(_normalize(name_a), _normalize(name_b))


def _group_by_event(outcomes: list[MarketOutcome]) -> dict[str, list[MarketOutcome]]:
    """Group outcomes by event_name so each group has both sides of a matchup."""
    groups: dict[str, list[MarketOutcome]] = {}
    for o in outcomes:
        groups.setdefault(o.event_name, []).append(o)
    return groups


def match_platforms(
    platform_a: list[MarketOutcome],
    platform_b: list[MarketOutcome],
    label: str,
) -> list[MatchedPair]:
    """Match H2H events between two platforms.

    Requires BOTH teams in a matchup to fuzzy-match above threshold.
    """
    if not platform_a or not platform_b:
        return []

    groups_a = _group_by_event(platform_a)
    groups_b = _group_by_event(platform_b)

    matched: list[MatchedPair] = []
    seen: set[tuple[str, str]] = set()

    for event_a, outcomes_a in groups_a.items():
        if len(outcomes_a) < 2:
            continue
        team_a1 = outcomes_a[0].team_name
        team_a2 = outcomes_a[1].team_name
        if not team_a1 or not team_a2:
            continue

        best: MatchedPair | None = None
        best_score = 0.0

        for event_b, outcomes_b in groups_b.items():
            if len(outcomes_b) < 2:
                continue

            team_b1 = outcomes_b[0].team_name
            team_b2 = outcomes_b[1].team_name
            if not team_b1 or not team_b2:
                continue

            # Try both orderings
            for idx1, idx2 in [(0, 1), (1, 0)]:
                s1 = _team_score(team_a1, outcomes_b[idx1].team_name)
                s2 = _team_score(team_a2, outcomes_b[idx2].team_name)

                if s1 < MATCH_CONFIDENCE_THRESHOLD or s2 < MATCH_CONFIDENCE_THRESHOLD:
                    continue

                combined = (s1 + s2) / 2
                if combined > best_score:
                    best_score = combined
                    best = MatchedPair(
                        source_a=outcomes_a[0],
                        source_b=outcomes_b[idx1],
                        opponent_a=outcomes_a[1],
                        opponent_b=outcomes_b[idx2],
                        confidence=combined,
                        pair_label=label,
                    )

        if best:
            pair_key = (best.source_a.raw_id, best.source_b.raw_id)
            if pair_key not in seen:
                seen.add(pair_key)
                matched.append(best)

    logger.info("Matched %d pairs for %s (threshold=%d%%)", len(matched), label, MATCH_CONFIDENCE_THRESHOLD)
    return matched
