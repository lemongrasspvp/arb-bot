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
    implied_prob: float    # 0–1 probability for this team winning (no-vig for Pinnacle)
    sport: str             # "lol" or "cs2"
    raw_id: str            # platform-specific ID (condition_id, ticker, etc.)
    commence_time: str = ""  # ISO start time (from Pinnacle)
    actual_price: float = 0.0  # what you'd actually pay (with vig); 0 = same as implied_prob
    token_id: str = ""         # Polymarket token ID (for order book lookup)
    opponent_raw_id: str = ""  # Kalshi: opponent's ticker (for NO-via-opponent book)
    market_type: str = "moneyline"  # "moneyline" or "totals"
    handicap: float = 0.0           # the line (e.g. 2.5) — only for totals


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
    """Normalize text for fuzzy matching.

    Strips:
      - Esports org suffixes (Esports, Gaming, Team, Club)
      - UFC event prefixes (Fight Night:, 326:, UFC:)
      - Tennis tournament prefixes (BNP Paribas Open:, Phoenix:, etc.)
      - Any prefix before a colon (catches unknown tournament/event labels)
    """
    text = text.lower().strip()
    # Strip everything before a colon (tournament/event prefix)
    # e.g. "Fight Night: Movsar Evloev" -> "Movsar Evloev"
    # e.g. "BNP Paribas Open: Carlos Alcaraz" -> "Carlos Alcaraz"
    # e.g. "326: Renato Moicano" -> "Renato Moicano"
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    text = re.sub(r"\b(esports?|gaming|team|club|org)\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _team_score(name_a: str, name_b: str) -> float:
    """Score how well two team names match (0–100).

    Uses token_sort_ratio as primary metric. Falls back to token_set_ratio
    (which handles subset matching like "Bradley" vs "Bradley Braves") but
    discounts it slightly to avoid false positives.
    """
    na, nb = _normalize(name_a), _normalize(name_b)
    sort_score = fuzz.token_sort_ratio(na, nb)
    if sort_score >= 75:
        return sort_score

    # Subset matching: "Bradley" ⊂ "Bradley Braves" → token_set_ratio = 100
    # Discount by 15% to penalize partial matches (so 100 → 85, still above 75 threshold).
    # This catches college team + mascot mismatches without false positives.
    set_score = fuzz.token_set_ratio(na, nb) * 0.85
    return max(sort_score, set_score)


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
    Only matches moneyline markets — totals go through match_totals().
    """
    # Filter out totals outcomes (they use match_totals() instead)
    platform_a = [o for o in platform_a if o.market_type == "moneyline"]
    platform_b = [o for o in platform_b if o.market_type == "moneyline"]

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

            # Sport must match (prevent LoL G2 matching CS2 G2)
            if outcomes_a[0].sport and outcomes_b[0].sport:
                if outcomes_a[0].sport != outcomes_b[0].sport:
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


def match_totals(
    platform_a: list[MarketOutcome],
    platform_b: list[MarketOutcome],
    label: str,
) -> list[MatchedPair]:
    """Match over/under (totals) events across two platforms.

    Unlike moneyline matching (which pairs team names), totals matching:
      1. Filters to market_type == "totals" only
      2. Fuzzy-matches event names (which contain team names like "G2 vs FaZe")
      3. Requires exact handicap match (2.5 == 2.5, not 2.5 vs 3.5)
      4. Pairs Over↔Over and Under↔Under across platforms

    For totals, each event has exactly 2 outcomes: "Over X" and "Under X".
    """
    # Filter to totals only
    totals_a = [o for o in platform_a if o.market_type == "totals" and o.handicap > 0]
    totals_b = [o for o in platform_b if o.market_type == "totals" and o.handicap > 0]

    if not totals_a or not totals_b:
        return []

    # Group by (event_name, handicap) — each group should have Over + Under
    def _group_totals(outcomes: list[MarketOutcome]) -> dict[tuple[str, float], list[MarketOutcome]]:
        groups: dict[tuple[str, float], list[MarketOutcome]] = {}
        for o in outcomes:
            groups.setdefault((o.event_name, o.handicap), []).append(o)
        return groups

    groups_a = _group_totals(totals_a)
    groups_b = _group_totals(totals_b)

    matched: list[MatchedPair] = []
    seen: set[tuple[str, str]] = set()

    for (event_a, hcap_a), outcomes_a in groups_a.items():
        if len(outcomes_a) < 2:
            continue

        # Find the Over and Under outcomes
        over_a = next((o for o in outcomes_a if "over" in o.team_name.lower()), None)
        under_a = next((o for o in outcomes_a if "under" in o.team_name.lower()), None)
        if not over_a or not under_a:
            continue

        best: MatchedPair | None = None
        best_score = 0.0

        for (event_b, hcap_b), outcomes_b in groups_b.items():
            # Handicap must match exactly
            if abs(hcap_a - hcap_b) > 0.01:
                continue

            if len(outcomes_b) < 2:
                continue

            # Sport must match
            if over_a.sport and outcomes_b[0].sport:
                if over_a.sport != outcomes_b[0].sport:
                    continue

            over_b = next((o for o in outcomes_b if "over" in o.team_name.lower()), None)
            under_b = next((o for o in outcomes_b if "under" in o.team_name.lower()), None)
            if not over_b or not under_b:
                continue

            # Fuzzy-match event names (which contain team names)
            score = fuzz.token_sort_ratio(
                _normalize(event_a), _normalize(event_b)
            )

            if score < MATCH_CONFIDENCE_THRESHOLD:
                continue

            if score > best_score:
                best_score = score
                best = MatchedPair(
                    source_a=over_a,
                    source_b=over_b,
                    opponent_a=under_a,
                    opponent_b=under_b,
                    confidence=score,
                    pair_label=label,
                )

        if best:
            pair_key = (best.source_a.raw_id, best.source_b.raw_id)
            if pair_key not in seen:
                seen.add(pair_key)
                matched.append(best)

    logger.info("Matched %d totals pairs for %s (threshold=%d%%)", len(matched), label, MATCH_CONFIDENCE_THRESHOLD)
    return matched
