"""True arbitrage detector — find risk-free cross-platform arbs."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from arb_scanner.matcher import MatchedPair
from arb_scanner.config import EDGE_THRESHOLD

logger = logging.getLogger(__name__)

# Minimum ROI to display (filters noise)
MIN_ROI_PCT = 0.75


def _kalshi_fee(price: float) -> float:
    """Kalshi taker fee per contract: 0.07 * p * (1-p), max ~1.75¢ at 50¢."""
    return 0.07 * price * (1.0 - price)


def _hours_until(commence_time: str) -> float | None:
    """Return hours until match start, or None if unknown."""
    if not commence_time:
        return None
    try:
        start = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        return (start - datetime.now(timezone.utc)).total_seconds() / 3600
    except (ValueError, TypeError):
        return None


@dataclass
class TrueArb:
    """A risk-free arbitrage across two platforms."""
    market_name: str            # e.g. "GRINGOS vs. OG"
    pair_label: str             # e.g. "Poly↔Kalshi", "Kalshi↔Pin"
    sport: str
    # Leg A: buy this team on this platform
    leg_a_platform: str
    leg_a_team: str
    leg_a_price: float          # ask price (0–1)
    # Leg B: buy opponent on other platform
    leg_b_platform: str
    leg_b_team: str
    leg_b_price: float          # ask price (0–1)
    # Optional fields (all have defaults)
    leg_a_raw_id: str = ""
    leg_a_token_id: str = ""
    leg_b_raw_id: str = ""
    leg_b_token_id: str = ""
    # Arb metrics
    total_cost: float = 0.0     # leg_a + leg_b (< 1.0 = arb)
    profit_pct: float = 0.0     # (1 - total_cost) / total_cost * 100
    commence_time: str = ""
    match_confidence: float = 0.0
    # Depth (filled after book walk)
    max_deploy: float = 0.0     # max $ deployable while arb holds
    vwap_cost: float = 0.0      # volume-weighted combined cost


def walk_arb_books(
    levels_a: list[tuple[float, float]],
    levels_b: list[tuple[float, float]],
    platform_a: str = "",
    platform_b: str = "",
) -> tuple[float, float, int]:
    """Walk both order books to find max $ deployable while arb holds.

    Both books are ask books sorted cheapest first: [(price, size), ...].
    We need to buy equal shares on both sides. Walk through levels,
    buying at the current cheapest level on each side. Stop when
    combined price per share (including fees) ≥ $1.00.

    Returns:
        (max_deploy_dollars, vwap_combined_cost, num_shares)
        - max_deploy_dollars: total $ spent across both legs
        - vwap_combined_cost: blended cost per share (both legs, incl fees)
        - num_shares: total shares/contracts bought per side
    """
    if not levels_a or not levels_b:
        return 0.0, 0.0, 0

    idx_a, idx_b = 0, 0
    remaining_a = levels_a[0][1] if levels_a else 0
    remaining_b = levels_b[0][1] if levels_b else 0

    total_cost = 0.0
    total_shares = 0.0

    while idx_a < len(levels_a) and idx_b < len(levels_b):
        price_a = levels_a[idx_a][0]
        price_b = levels_b[idx_b][0]

        fee_a = _kalshi_fee(price_a) if platform_a == "kalshi" else 0.0
        fee_b = _kalshi_fee(price_b) if platform_b == "kalshi" else 0.0
        combined = price_a + fee_a + price_b + fee_b
        if combined >= 1.0:
            break  # no more arb at these prices

        # Buy the minimum of what's available on both sides
        can_buy = min(remaining_a, remaining_b)
        cost = combined * can_buy
        total_cost += cost
        total_shares += can_buy

        remaining_a -= can_buy
        remaining_b -= can_buy

        # Advance to next level if exhausted
        if remaining_a <= 0:
            idx_a += 1
            if idx_a < len(levels_a):
                remaining_a = levels_a[idx_a][1]
        if remaining_b <= 0:
            idx_b += 1
            if idx_b < len(levels_b):
                remaining_b = levels_b[idx_b][1]

    if total_shares == 0:
        return 0.0, 0.0, 0

    vwap_cost = total_cost / total_shares
    return total_cost, vwap_cost, int(total_shares)


def find_arbs(pairs: list[MatchedPair]) -> list[TrueArb]:
    """Find true arbitrage opportunities across platform pairs.

    For each matched H2H event, check if buying Team A on one platform
    + Team B on the other costs < $1.00 total. If so, guaranteed profit.
    """
    arbs: list[TrueArb] = []

    for pair in pairs:
        a = pair.source_a       # Team X on Platform A
        b = pair.source_b       # Team X on Platform B
        opp_a = pair.opponent_a  # Team Y on Platform A
        opp_b = pair.opponent_b  # Team Y on Platform B

        if not opp_a or not opp_b:
            continue

        commence = a.commence_time or b.commence_time or opp_a.commence_time or opp_b.commence_time

        # Get actual ask prices (what you'd pay)
        price_a = a.actual_price if a.actual_price > 0 else a.implied_prob
        price_b = b.actual_price if b.actual_price > 0 else b.implied_prob
        price_opp_a = opp_a.actual_price if opp_a.actual_price > 0 else opp_a.implied_prob
        price_opp_b = opp_b.actual_price if opp_b.actual_price > 0 else opp_b.implied_prob

        # Compute Kalshi fees for each leg (0 if not Kalshi)
        fee_a = _kalshi_fee(price_a) if a.platform == "kalshi" else 0.0
        fee_b = _kalshi_fee(price_b) if b.platform == "kalshi" else 0.0
        fee_opp_a = _kalshi_fee(price_opp_a) if opp_a.platform == "kalshi" else 0.0
        fee_opp_b = _kalshi_fee(price_opp_b) if opp_b.platform == "kalshi" else 0.0

        # Check both directions (include fees in cost):
        # Dir 1: Team X on Platform A + Team Y on Platform B
        cost_1 = price_a + fee_a + price_opp_b + fee_opp_b
        # Dir 2: Team X on Platform B + Team Y on Platform A
        cost_2 = price_b + fee_b + price_opp_a + fee_opp_a

        # Take the cheaper direction (or both if both are arbs)
        candidates = []
        if cost_1 < 1.0:
            candidates.append((cost_1, a, opp_b))
        if cost_2 < 1.0:
            candidates.append((cost_2, b, opp_a))

        for total_cost, leg_a, leg_b in candidates:
            la_price = leg_a.actual_price if leg_a.actual_price > 0 else leg_a.implied_prob
            lb_price = leg_b.actual_price if leg_b.actual_price > 0 else leg_b.implied_prob
            profit_pct = (1.0 - total_cost) / total_cost * 100

            if profit_pct < MIN_ROI_PCT:
                continue

            arb = TrueArb(
                market_name=a.event_name or b.event_name,
                pair_label=pair.pair_label,
                sport=a.sport or b.sport,
                leg_a_platform=leg_a.platform,
                leg_a_team=leg_a.team_name,
                leg_a_price=la_price,
                leg_a_raw_id=leg_a.raw_id,
                leg_a_token_id=leg_a.token_id,
                leg_b_platform=leg_b.platform,
                leg_b_team=leg_b.team_name,
                leg_b_price=lb_price,
                leg_b_raw_id=leg_b.raw_id,
                leg_b_token_id=leg_b.token_id,
                total_cost=total_cost,
                profit_pct=profit_pct,
                commence_time=commence,
                match_confidence=pair.confidence,
            )
            arbs.append(arb)

            logger.info(
                "ARB [%s]: %s@%s %.0f¢ + %s@%s %.0f¢ = %.1f¢ (%.2f%% profit)",
                pair.pair_label,
                leg_a.team_name, leg_a.platform, la_price * 100,
                leg_b.team_name, leg_b.platform, lb_price * 100,
                total_cost * 100, profit_pct,
            )

    # Sort by profit % descending
    arbs.sort(key=lambda a: -a.profit_pct)

    logger.info("Arb analysis: %d true arbs found", len(arbs))
    return arbs
