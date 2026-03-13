"""Fuzzy match events across Polymarket and Pinnacle."""

import logging
import re
from dataclasses import dataclass

from rapidfuzz import fuzz, process

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
    """Normalize event text for better matching."""
    text = text.lower().strip()
    # Remove common filler words
    text = re.sub(r"\b(will|the|to|win|vs\.?|versus|match|game|fight)\b", " ", text)
    # Remove punctuation
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_teams_from_question(question: str) -> list[str]:
    """Try to extract team/entity names from a Polymarket question."""
    # Common patterns: "Will X win Y?", "X vs Y", "Will X beat Y?"
    patterns = [
        r"will (.+?) (?:win|beat|defeat)",
        r"(.+?) vs\.? (.+?)(?:\?|$)",
        r"will (.+?) (?:make|reach|qualify)",
    ]
    entities = []
    for pattern in patterns:
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            entities.extend(match.groups())
    return [e.strip() for e in entities if e.strip()]


def match_events(
    poly_events: list[PolymarketEvent],
    pinnacle_outcomes: list[PinnacleOutcome],
) -> list[MatchedPair]:
    """Fuzzy-match Polymarket events to Pinnacle outcomes.

    Uses multiple matching strategies:
    1. Direct question-to-event name matching
    2. Team name extraction and matching
    """
    if not poly_events or not pinnacle_outcomes:
        logger.warning("No events to match (poly=%d, pinnacle=%d)", len(poly_events), len(pinnacle_outcomes))
        return []

    # Build lookup: normalized pinnacle event name -> list of outcomes
    pinnacle_by_event: dict[str, list[PinnacleOutcome]] = {}
    for outcome in pinnacle_outcomes:
        key = _normalize(outcome.event_name)
        pinnacle_by_event.setdefault(key, []).append(outcome)

    # Also index by individual outcome name (team name)
    pinnacle_by_team: dict[str, list[PinnacleOutcome]] = {}
    for outcome in pinnacle_outcomes:
        key = _normalize(outcome.outcome_name)
        pinnacle_by_team.setdefault(key, []).append(outcome)

    pinnacle_event_names = list(pinnacle_by_event.keys())
    pinnacle_team_names = list(pinnacle_by_team.keys())

    matched: list[MatchedPair] = []
    seen_pairs: set[tuple[str, str]] = set()

    for poly in poly_events:
        norm_question = _normalize(poly.question)

        # Strategy 1: Match full question against event names
        if pinnacle_event_names:
            result = process.extractOne(
                norm_question,
                pinnacle_event_names,
                scorer=fuzz.token_sort_ratio,
            )
            if result and result[1] >= MATCH_CONFIDENCE_THRESHOLD:
                best_name, score, _ = result
                outcomes = pinnacle_by_event[best_name]
                _add_match(matched, seen_pairs, poly, outcomes, score, "event_name")
                continue

        # Strategy 2: Extract teams and match individually
        teams = _extract_teams_from_question(poly.question)
        for team in teams:
            norm_team = _normalize(team)
            if pinnacle_team_names:
                result = process.extractOne(
                    norm_team,
                    pinnacle_team_names,
                    scorer=fuzz.token_sort_ratio,
                )
                if result and result[1] >= MATCH_CONFIDENCE_THRESHOLD:
                    best_name, score, _ = result
                    outcomes = pinnacle_by_team[best_name]
                    _add_match(matched, seen_pairs, poly, outcomes, score, "team_name")
                    break

    logger.info("Matched %d event pairs (threshold=%d%%)", len(matched), MATCH_CONFIDENCE_THRESHOLD)
    return matched


def _add_match(
    matched: list[MatchedPair],
    seen: set[tuple[str, str]],
    poly: PolymarketEvent,
    pinnacle_outcomes: list[PinnacleOutcome],
    score: float,
    method: str,
) -> None:
    """Add a matched pair, avoiding duplicates."""
    if not pinnacle_outcomes:
        return

    primary = pinnacle_outcomes[0]
    pair_key = (poly.condition_id, primary.event_name)
    if pair_key in seen:
        return
    seen.add(pair_key)

    # Find the opponent outcome from the same event
    opponent = None
    for o in pinnacle_outcomes:
        if o.outcome_name != primary.outcome_name and o.event_name == primary.event_name:
            opponent = o
            break

    matched.append(MatchedPair(
        polymarket=poly,
        pinnacle=primary,
        pinnacle_opponent=opponent,
        confidence=score,
        match_method=method,
    ))
