"""Snapshotter — consumes price updates, deduplicates, computes derived fields, writes to storage.

Sits between the raw feeds (price_queue) and the JSONL storage layer.
For each update:
  1. Look up the event/match in the registry
  2. Check dedup (skip if price hasn't changed enough and heartbeat not due)
  3. Compute derived fields (VWAP, edge, movement, quality flags)
  4. Write a MarketStateSnapshot or ReferenceStateSnapshot row

Also maintains an in-memory cache of the latest state per market for:
  - Movement calculation (price_move, ref_move)
  - Closing snapshot freezer (reads last known state)
  - Health reporting (last update times)
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

from collector.config import SNAPSHOT_INTERVAL, MIN_PRICE_CHANGE, VWAP_SIZES
from collector.schema import (
    MarketStateSnapshot,
    ReferenceStateSnapshot,
    EventSnapshot,
    _utc_now_iso,
    _stable_event_id,
    _event_id_hash,
)
from collector.storage import JsonlWriter

logger = logging.getLogger(__name__)


def _compute_vwap(levels: list[tuple[float, float]], size_usd: float) -> float:
    """Walk order book levels to compute VWAP for a target USD size.

    Args:
        levels: [(price, size_in_shares), ...] sorted best-first
        size_usd: target notional to fill

    Returns VWAP price, or 0.0 if no levels / can't fill anything.
    """
    if not levels or size_usd <= 0:
        return 0.0

    remaining = size_usd
    total_shares = 0.0
    total_cost = 0.0

    for price, shares in levels:
        if remaining <= 0 or price <= 0:
            break
        level_usd = price * shares
        if level_usd <= remaining:
            total_shares += shares
            total_cost += level_usd
            remaining -= level_usd
        else:
            partial = remaining / price
            total_shares += partial
            total_cost += remaining
            remaining = 0.0

    if total_shares <= 0:
        return 0.0
    return total_cost / total_shares


def _compute_edge(pin_prob: float, price: float) -> float:
    """Compute edge = (pin_prob / price) - 1. Returns 0 if inputs invalid."""
    if pin_prob <= 0 or price <= 0:
        return 0.0
    return (pin_prob / price) - 1.0


def _minutes_to_start(commence_time: str) -> float:
    """Compute minutes until match start. Returns -1 if can't parse."""
    if not commence_time:
        return -1.0
    try:
        ct = commence_time
        if ct.endswith("Z"):
            ct = ct[:-1] + "+00:00"
        start = datetime.fromisoformat(ct)
        now = datetime.now(timezone.utc)
        return (start - now).total_seconds() / 60.0
    except (ValueError, TypeError):
        return -1.0


class Snapshotter:
    """Consumes price updates from the queue, writes snapshots to storage."""

    def __init__(self, registry):
        self.registry = registry

        # Storage writers (partitioned by category + platform)
        self._market_writers: dict[str, JsonlWriter] = {
            "polymarket": JsonlWriter("market_state", "polymarket"),
            "kalshi": JsonlWriter("market_state", "kalshi"),
        }
        self._reference_writer = JsonlWriter("reference_state", "pinnacle")
        self._event_writer = JsonlWriter("events")

        # In-memory cache: latest state per market_id
        # platform:market_id → {best_ask, best_bid, ts, ...}
        self.market_cache: dict[str, dict] = {}
        # event_id → {pinnacle_prob_a, pinnacle_prob_b, ts, ...}
        self.reference_cache: dict[str, dict] = {}
        # Dedup: market_id → last snapshot timestamp
        self._last_snap_ts: dict[str, float] = {}

        # Counters for health
        self.counts = {
            "market_state": 0,
            "reference_state": 0,
            "events": 0,
            "skipped_dedup": 0,
        }
        self.last_write_ts: dict[str, float] = {}

    async def run(self, price_queue: asyncio.Queue,
                  shutdown_event: asyncio.Event) -> None:
        """Main loop: drain price_queue and write snapshots."""
        logger.info("Snapshotter started")
        while not shutdown_event.is_set():
            try:
                update = await asyncio.wait_for(
                    price_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                self._process_update(update)
            except Exception:
                logger.debug("Snapshotter error processing update", exc_info=True)

        # Cleanup
        for w in self._market_writers.values():
            w.close()
        self._reference_writer.close()
        self._event_writer.close()
        logger.info("Snapshotter stopped")

    def _process_update(self, update: dict) -> None:
        """Route an update to the correct handler."""
        platform = update.get("platform", "")
        if platform == "pinnacle":
            self._handle_pinnacle(update)
        elif platform in ("polymarket", "kalshi"):
            self._handle_market(update, platform)

    def _handle_market(self, update: dict, platform: str) -> None:
        """Process a Polymarket or Kalshi price update."""
        market_id = update.get("market_id", "")
        if not market_id:
            return

        now = time.time()
        best_ask = update.get("best_ask", 0)
        best_bid = update.get("best_bid", 0)

        # Look up the event
        match, side = self.registry.get_match_for_market(platform, market_id)
        if not match:
            return

        cache_key = f"{platform}:{market_id}"

        # Dedup check
        prev = self.market_cache.get(cache_key, {})
        prev_ask = prev.get("best_ask", 0)
        prev_bid = prev.get("best_bid", 0)
        last_snap = self._last_snap_ts.get(cache_key, 0)

        price_changed = (
            abs(best_ask - prev_ask) >= MIN_PRICE_CHANGE
            or abs(best_bid - prev_bid) >= MIN_PRICE_CHANGE
        )
        heartbeat_due = (now - last_snap) >= SNAPSHOT_INTERVAL

        if not price_changed and not heartbeat_due:
            self.counts["skipped_dedup"] += 1
            # Still update cache for closing snapshots
            self.market_cache[cache_key] = {
                "best_ask": best_ask,
                "best_bid": best_bid,
                "ts": now,
                "update": update,
                "match": match,
                "side": side,
            }
            return

        # Compute derived fields
        mid = (best_ask + best_bid) / 2.0 if best_ask > 0 and best_bid > 0 else 0.0
        spread = best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0

        ask_levels = update.get("ask_levels", [])
        bid_levels = update.get("bid_levels", [])

        # Sort levels: asks low→high, bids high→low
        sorted_asks = sorted(ask_levels, key=lambda x: x[0]) if ask_levels else []
        sorted_bids = sorted(bid_levels, key=lambda x: x[0], reverse=True) if bid_levels else []

        # Pinnacle reference for this side
        event_id = _stable_event_id(match.teams[0], match.teams[1], match.sport)
        ref = self.reference_cache.get(event_id, {})
        pin_prob = ref.get(f"pinnacle_prob_{side}", 0.0) if side in ("a", "b") else 0.0
        pin_margin = ref.get("pinnacle_margin", 0.0)

        # Minutes to start
        mts = _minutes_to_start(match.commence_time)

        # Determine raw event name from the match
        raw_event_name = f"{match.teams[0]} vs {match.teams[1]}"

        # Determine trigger
        trigger = "initial" if not prev else ("price_change" if price_changed else "heartbeat")

        # Data quality
        quality = "ok"
        if pin_prob > 0 and (now - ref.get("ts", 0)) > 300:
            quality = "stale_reference"
        if best_ask <= 0:
            quality = "no_ask"

        snap = MarketStateSnapshot(
            ts_utc=_utc_now_iso(),
            ts_epoch=now,
            event_id=event_id,
            platform=platform,
            side=side,
            team=match.teams[0] if side == "a" else match.teams[1] if side == "b" else "",
            raw_market_id=market_id,
            raw_condition_id=match.poly_condition_id if platform == "polymarket" else "",
            raw_event_name=raw_event_name,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=round(mid, 6),
            spread=round(spread, 6),
            bid_depth_usd=update.get("bid_depth_usd", 0),
            ask_depth_usd=update.get("ask_depth_usd", 0),
            bid_levels_count=len(bid_levels),
            ask_levels_count=len(ask_levels),
            bid_vwap_50=round(_compute_vwap(sorted_bids, 50), 6),
            bid_vwap_100=round(_compute_vwap(sorted_bids, 100), 6),
            bid_vwap_250=round(_compute_vwap(sorted_bids, 250), 6),
            ask_vwap_50=round(_compute_vwap(sorted_asks, 50), 6),
            ask_vwap_100=round(_compute_vwap(sorted_asks, 100), 6),
            ask_vwap_250=round(_compute_vwap(sorted_asks, 250), 6),
            minutes_to_start=round(mts, 1),
            is_live=mts < 0 and mts != -1.0,
            pinnacle_prob=round(pin_prob, 6),
            pinnacle_margin=round(pin_margin, 6),
            edge_at_ask=round(_compute_edge(pin_prob, best_ask), 6),
            edge_at_mid=round(_compute_edge(pin_prob, mid), 6) if mid > 0 else 0.0,
            prev_best_ask=round(prev_ask, 6),
            prev_best_bid=round(prev_bid, 6),
            price_move=round(best_ask - prev_ask, 6) if prev_ask > 0 else 0.0,
            ref_move=0.0,  # filled below
            market_minus_ref_move=0.0,
            data_quality=quality,
            snapshot_trigger=trigger,
        )

        # Reference movement (compare to last ref snapshot's prob for this side)
        prev_ref = prev.get("pin_prob_at_snap", 0)
        if prev_ref > 0 and pin_prob > 0:
            snap.ref_move = round(pin_prob - prev_ref, 6)
            snap.market_minus_ref_move = round(snap.price_move - snap.ref_move, 6)

        # Write
        writer = self._market_writers.get(platform)
        if writer and writer.write(snap.to_dict()):
            self.counts["market_state"] += 1
            self.last_write_ts["market_state"] = now

        # Update cache
        self.market_cache[cache_key] = {
            "best_ask": best_ask,
            "best_bid": best_bid,
            "ts": now,
            "update": update,
            "match": match,
            "side": side,
            "pin_prob_at_snap": pin_prob,
        }
        self._last_snap_ts[cache_key] = now

    def _handle_pinnacle(self, update: dict) -> None:
        """Process a Pinnacle reference price update.

        Pinnacle updates come per-outcome (one team at a time), but we write
        reference snapshots per-event (both sides together). We accumulate
        updates and write when we have a fresh pair.
        """
        market_id = update.get("market_id", "")
        no_vig_prob = update.get("no_vig_prob", 0)
        implied_prob = update.get("implied_prob", update.get("best_ask", 0))
        if not market_id or no_vig_prob <= 0:
            return

        now = time.time()

        # Find the event this Pinnacle update belongs to
        # Pinnacle market_ids are "{event_name}_{outcome_name}"
        # The registry's update_pinnacle_price already did the fuzzy matching
        # and updated the match object. We scan the registry to find which
        # events have fresh Pinnacle data.
        #
        # Since Pinnacle updates arrive per-team, we write a reference snapshot
        # when BOTH sides have been updated in the current poll cycle.
        # We track this via the registry's pinnacle_last_seen timestamps.

        for match_id, match in self.registry.matches.items():
            if match.pinnacle_prob_a <= 0 or match.pinnacle_prob_b <= 0:
                continue

            event_id = _stable_event_id(match.teams[0], match.teams[1], match.sport)

            prev_ref = self.reference_cache.get(event_id, {})
            prev_ts = prev_ref.get("ts", 0)

            # Only write if Pinnacle data is fresher than last snapshot
            latest_seen = max(
                getattr(match, "pinnacle_last_seen_a", 0),
                getattr(match, "pinnacle_last_seen_b", 0),
            )
            if latest_seen <= prev_ts:
                continue

            margin = 0.0
            if match.pinnacle_implied_a > 0 and match.pinnacle_implied_b > 0:
                margin = match.pinnacle_implied_a + match.pinnacle_implied_b - 1.0

            mts = _minutes_to_start(match.commence_time)

            quality = "ok"
            if match.pinnacle_frozen_a or match.pinnacle_frozen_b:
                quality = "frozen_line"
            if match.pinnacle_moving_a or match.pinnacle_moving_b:
                quality = "moving_line"

            snap = ReferenceStateSnapshot(
                ts_utc=_utc_now_iso(),
                ts_epoch=now,
                event_id=event_id,
                pinnacle_prob_a=round(match.pinnacle_prob_a, 6),
                pinnacle_prob_b=round(match.pinnacle_prob_b, 6),
                pinnacle_implied_a=round(match.pinnacle_implied_a, 6),
                pinnacle_implied_b=round(match.pinnacle_implied_b, 6),
                pinnacle_margin=round(margin, 6),
                pinnacle_frozen_a=match.pinnacle_frozen_a,
                pinnacle_frozen_b=match.pinnacle_frozen_b,
                pinnacle_moving_a=match.pinnacle_moving_a,
                pinnacle_moving_b=match.pinnacle_moving_b,
                is_live=mts < 0 and mts != -1.0,
                minutes_to_start=round(mts, 1),
                prev_prob_a=round(prev_ref.get("pinnacle_prob_a", 0), 6),
                prev_prob_b=round(prev_ref.get("pinnacle_prob_b", 0), 6),
                prob_a_move=round(
                    match.pinnacle_prob_a - prev_ref.get("pinnacle_prob_a", 0), 6
                ) if prev_ref.get("pinnacle_prob_a", 0) > 0 else 0.0,
                prob_b_move=round(
                    match.pinnacle_prob_b - prev_ref.get("pinnacle_prob_b", 0), 6
                ) if prev_ref.get("pinnacle_prob_b", 0) > 0 else 0.0,
                data_quality=quality,
            )

            if self._reference_writer.write(snap.to_dict()):
                self.counts["reference_state"] += 1
                self.last_write_ts["reference_state"] = now

            # Update reference cache
            self.reference_cache[event_id] = {
                "pinnacle_prob_a": match.pinnacle_prob_a,
                "pinnacle_prob_b": match.pinnacle_prob_b,
                "pinnacle_implied_a": match.pinnacle_implied_a,
                "pinnacle_implied_b": match.pinnacle_implied_b,
                "pinnacle_margin": margin,
                "ts": now,
            }

    def write_event_snapshot(self, match) -> None:
        """Write an event identity snapshot for a tracked match."""
        event_id = _stable_event_id(match.teams[0], match.teams[1], match.sport)
        eid_hash = _event_id_hash(event_id)

        prob_sum = 0.0
        if match.pinnacle_prob_a > 0 and match.pinnacle_prob_b > 0:
            prob_sum = match.pinnacle_prob_a + match.pinnacle_prob_b

        platforms = []
        if match.poly_token_id_a or match.poly_token_id_b:
            platforms.append("polymarket")
        if match.kalshi_ticker_a or match.kalshi_ticker_b:
            platforms.append("kalshi")
        if match.pinnacle_prob_a > 0 or match.pinnacle_prob_b > 0:
            platforms.append("pinnacle")

        snap = EventSnapshot(
            ts_utc=_utc_now_iso(),
            event_id=event_id,
            event_id_hash=eid_hash,
            sport=match.sport,
            team_a=match.teams[0],
            team_b=match.teams[1],
            commence_time_utc=match.commence_time or "",
            poly_token_a=match.poly_token_id_a,
            poly_token_b=match.poly_token_id_b,
            poly_condition_id=getattr(match, "poly_condition_id", ""),
            kalshi_ticker_a=match.kalshi_ticker_a,
            kalshi_ticker_b=match.kalshi_ticker_b,
            # Raw names: team names as stored on the match (from the platform that created the match)
            poly_name_a=match.teams[0] if match.poly_token_id_a else "",
            poly_name_b=match.teams[1] if match.poly_token_id_b else "",
            kalshi_name_a=match.teams[0] if match.kalshi_ticker_a else "",
            kalshi_name_b=match.teams[1] if match.kalshi_ticker_b else "",
            pinnacle_name_a=match.teams[0] if match.pinnacle_prob_a > 0 else "",
            pinnacle_name_b=match.teams[1] if match.pinnacle_prob_b > 0 else "",
            match_confidence=getattr(match, "match_confidence", 0.0),
            confidence_tier=match.confidence_tier,
            pinnacle_prob_sum=round(prob_sum, 4),
            platforms_matched=platforms,
        )

        if self._event_writer.write(snap.to_dict()):
            self.counts["events"] += 1
            self.last_write_ts["events"] = time.time()
