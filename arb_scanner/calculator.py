"""Compute edges between matched platform pairs and flag arbitrage opportunities."""

import logging
from dataclasses import dataclass

from arb_scanner.matcher import MatchedPair
from arb_scanner.config import EDGE_THRESHOLD

logger = logging.getLogger(__name__)


@dataclass
class ArbOpportunity:
    """A potential arbitrage opportunity between two platforms."""
    market_name: str
    platform_a: str             # e.g. "polymarket"
    platform_b: str             # e.g. "pinnacle"
    pair_label: str             # e.g. "Poly↔Pin"
    price_a: float              # platform A implied prob for team
    price_b: float              # platform B implied prob for same team
    edge: float                 # price_a - price_b
    abs_edge: float
    match_confidence: float
    sport: str
    is_arb: bool
    signal: str                 # "BUY_A", "BUY_B", or "NO_EDGE"


def analyze(pairs: list[MatchedPair]) -> list[ArbOpportunity]:
    """Compute edge for each matched pair and flag arbitrage opportunities."""
    opportunities: list[ArbOpportunity] = []

    for pair in pairs:
        a = pair.source_a
        b = pair.source_b

        # Edge = platform A prob - platform B prob for the same team
        # Positive: A overprices → buy on B
        # Negative: A underprices → buy on A
        edge = a.implied_prob - b.implied_prob
        abs_edge = abs(edge)
        is_arb = abs_edge > EDGE_THRESHOLD

        if edge > EDGE_THRESHOLD:
            signal = f"BUY_{b.platform.upper()}"
        elif edge < -EDGE_THRESHOLD:
            signal = f"BUY_{a.platform.upper()}"
        else:
            signal = "NO_EDGE"

        opp = ArbOpportunity(
            market_name=a.event_name,
            platform_a=a.platform,
            platform_b=b.platform,
            pair_label=pair.pair_label,
            price_a=a.implied_prob,
            price_b=b.implied_prob,
            edge=edge,
            abs_edge=abs_edge,
            match_confidence=pair.confidence,
            sport=a.sport,
            is_arb=is_arb,
            signal=signal,
        )
        opportunities.append(opp)

        if is_arb:
            logger.info(
                "ARB FOUND [%s]: %s | edge=%.2f%% | %s",
                pair.pair_label, a.event_name[:50], edge * 100, signal,
            )

    opportunities.sort(key=lambda o: o.abs_edge, reverse=True)

    arb_count = sum(1 for o in opportunities if o.is_arb)
    logger.info(
        "Analysis complete: %d opportunities, %d arbs (threshold=%.1f%%)",
        len(opportunities), arb_count, EDGE_THRESHOLD * 100,
    )
    return opportunities
