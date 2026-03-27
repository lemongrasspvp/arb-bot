"""Canonical schemas for collected sports market data.

All timestamps are UTC. All prices are in probability units (0-1).
All USD amounts are in dollars. All IDs are strings.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any
import hashlib


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _stable_event_id(team_a: str, team_b: str, sport: str) -> str:
    """Create a stable canonical event ID from sorted team names + sport.

    Sorting ensures the same match always gets the same ID regardless
    of which platform listed which team first.
    """
    sorted_teams = sorted([team_a.strip(), team_b.strip()], key=str.lower)
    return f"{sorted_teams[0]}_vs_{sorted_teams[1]}_{sport}".replace(" ", "_")


def _event_id_hash(event_id: str) -> str:
    """Short 8-char hash of event_id for compact joins."""
    return hashlib.sha256(event_id.encode()).hexdigest()[:8]


@dataclass
class EventSnapshot:
    """Identity snapshot for a matched cross-platform event."""
    ts_utc: str
    event_id: str
    event_id_hash: str
    sport: str
    team_a: str
    team_b: str
    commence_time_utc: str

    # Raw source IDs (exactly as received)
    poly_token_a: str = ""
    poly_token_b: str = ""
    poly_condition_id: str = ""
    kalshi_ticker_a: str = ""
    kalshi_ticker_b: str = ""

    # Raw source naming (for debugging mis-matches)
    poly_name_a: str = ""
    poly_name_b: str = ""
    kalshi_name_a: str = ""
    kalshi_name_b: str = ""
    pinnacle_name_a: str = ""
    pinnacle_name_b: str = ""

    # Mapping quality
    match_confidence: float = 0.0
    confidence_tier: str = "DISCOVERY_OK"
    pinnacle_prob_sum: float = 0.0
    platforms_matched: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MarketStateSnapshot:
    """Timestamped market state from a single platform/side."""
    ts_utc: str
    ts_epoch: float
    event_id: str
    platform: str
    side: str           # "a" or "b"
    team: str

    # Raw platform identifiers (exactly as received from source)
    raw_market_id: str       # token_id (poly) or ticker (kalshi)
    raw_condition_id: str    # poly condition_id, empty for kalshi
    raw_event_name: str      # event/market title as the platform names it

    # Book state
    best_bid: float = 0.0
    best_ask: float = 0.0
    mid_price: float = 0.0
    spread: float = 0.0
    bid_depth_usd: float = 0.0
    ask_depth_usd: float = 0.0
    bid_levels_count: int = 0
    ask_levels_count: int = 0

    # VWAP at standard sizes (keyed by USD amount)
    bid_vwap_50: float = 0.0
    bid_vwap_100: float = 0.0
    bid_vwap_250: float = 0.0
    ask_vwap_50: float = 0.0
    ask_vwap_100: float = 0.0
    ask_vwap_250: float = 0.0

    # Timing
    minutes_to_start: float = -1.0
    is_live: bool = False

    # Derived (populated when Pinnacle reference is available)
    pinnacle_prob: float = 0.0
    pinnacle_margin: float = 0.0
    edge_at_ask: float = 0.0
    edge_at_mid: float = 0.0

    # Movement since previous snapshot
    prev_best_ask: float = 0.0
    prev_best_bid: float = 0.0
    price_move: float = 0.0
    ref_move: float = 0.0
    market_minus_ref_move: float = 0.0

    # Quality flags
    data_quality: str = "ok"
    snapshot_trigger: str = "initial"  # "price_change" | "heartbeat" | "initial"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReferenceStateSnapshot:
    """Timestamped Pinnacle reference state for a matched event."""
    ts_utc: str
    ts_epoch: float
    event_id: str

    pinnacle_prob_a: float = 0.0
    pinnacle_prob_b: float = 0.0
    pinnacle_implied_a: float = 0.0
    pinnacle_implied_b: float = 0.0
    pinnacle_margin: float = 0.0

    pinnacle_frozen_a: bool = False
    pinnacle_frozen_b: bool = False
    pinnacle_moving_a: bool = False
    pinnacle_moving_b: bool = False

    is_live: bool = False
    minutes_to_start: float = -1.0

    # Movement since previous snapshot
    prev_prob_a: float = 0.0
    prev_prob_b: float = 0.0
    prob_a_move: float = 0.0
    prob_b_move: float = 0.0

    data_quality: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ClosingSnapshot:
    """Pre-game closing state frozen just before match start.

    Captures the last known market + reference state across all platforms
    right before commence_time. This is the most valuable label for
    strategy research — the "closing line."
    """
    ts_utc: str
    event_id: str
    commence_time_utc: str
    seconds_before_start: float

    # Last Polymarket state (side A)
    poly_a_best_bid: float = 0.0
    poly_a_best_ask: float = 0.0
    poly_a_mid: float = 0.0
    poly_a_spread: float = 0.0
    poly_a_bid_depth_usd: float = 0.0
    poly_a_ask_depth_usd: float = 0.0
    # Last Polymarket state (side B)
    poly_b_best_bid: float = 0.0
    poly_b_best_ask: float = 0.0
    poly_b_mid: float = 0.0
    poly_b_spread: float = 0.0
    poly_b_bid_depth_usd: float = 0.0
    poly_b_ask_depth_usd: float = 0.0

    # Last Kalshi state (side A)
    kalshi_a_best_bid: float = 0.0
    kalshi_a_best_ask: float = 0.0
    kalshi_a_mid: float = 0.0
    # Last Kalshi state (side B)
    kalshi_b_best_bid: float = 0.0
    kalshi_b_best_ask: float = 0.0
    kalshi_b_mid: float = 0.0

    # Last Pinnacle reference
    pinnacle_prob_a: float = 0.0
    pinnacle_prob_b: float = 0.0
    pinnacle_implied_a: float = 0.0
    pinnacle_implied_b: float = 0.0
    pinnacle_margin: float = 0.0

    # Closing edges
    poly_a_edge_at_ask: float = 0.0
    poly_b_edge_at_ask: float = 0.0
    kalshi_a_edge_at_ask: float = 0.0
    kalshi_b_edge_at_ask: float = 0.0

    data_quality: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
