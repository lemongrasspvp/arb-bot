"""Convert odds to implied probability and find arbitrage gaps."""

import logging
from dataclasses import dataclass

from arb_scanner.matcher import MatchedPair
from arb_scanner.config import EDGE_THRESHOLD

logger = logging.getLogger(__name__)


@dataclass
class ArbOpportunity:
    """A potential arbitrage opportunity between platforms."""
    poly_question: str
    poly_yes_price: float       # Polymarket YES price (implied prob)
    pinnacle_outcome: str       # Pinnacle outcome name
    pinnacle_no_vig_prob: float  # Pinnacle no-vig implied probability
    edge: float                 # poly_yes - pinnacle_no_vig (positive = poly overpriced)
    abs_edge: float
    match_confidence: float
    sport: str
    is_arb: bool                # abs_edge > threshold
    signal: str                 # "BUY_POLY_YES", "BUY_POLY_NO", or "NO_EDGE"


def analyze(pairs: list[MatchedPair]) -> list[ArbOpportunity]:
    """Compute edge for each matched pair and flag arbitrage opportunities."""
    opportunities: list[ArbOpportunity] = []

    for pair in pairs:
        poly = pair.polymarket
        pin = pair.pinnacle

        # Edge = Polymarket YES price - Pinnacle no-vig probability
        # Positive edge: Polymarket overprices YES -> bet NO on Polymarket, YES on Pinnacle
        # Negative edge: Polymarket underprices YES -> bet YES on Polymarket, NO on Pinnacle
        edge = poly.yes_price - pin.no_vig_prob
        abs_edge = abs(edge)
        is_arb = abs_edge > EDGE_THRESHOLD

        if edge > EDGE_THRESHOLD:
            signal = "BUY_POLY_NO"
        elif edge < -EDGE_THRESHOLD:
            signal = "BUY_POLY_YES"
        else:
            signal = "NO_EDGE"

        opp = ArbOpportunity(
            poly_question=poly.question,
            poly_yes_price=poly.yes_price,
            pinnacle_outcome=pin.outcome_name,
            pinnacle_no_vig_prob=pin.no_vig_prob,
            edge=edge,
            abs_edge=abs_edge,
            match_confidence=pair.confidence,
            sport=pin.sport,
            is_arb=is_arb,
            signal=signal,
        )
        opportunities.append(opp)

        if is_arb:
            logger.info(
                "ARB FOUND: %s | edge=%.2f%% | signal=%s",
                poly.question[:60], edge * 100, signal,
            )

    # Sort by absolute edge descending
    opportunities.sort(key=lambda o: o.abs_edge, reverse=True)

    arb_count = sum(1 for o in opportunities if o.is_arb)
    logger.info(
        "Analysis complete: %d opportunities, %d arbs (threshold=%.1f%%)",
        len(opportunities), arb_count, EDGE_THRESHOLD * 100,
    )
    return opportunities
