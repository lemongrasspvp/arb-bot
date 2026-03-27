"""Pre-game closing snapshot freezer.

Monitors tracked matches and writes a closing snapshot when a match
is within CLOSING_WINDOW_SECONDS of its commence_time. This captures
the last known market + reference state across all platforms just
before the game starts — the "closing line."

Each event gets exactly one closing snapshot. Once written, the event
is marked as closed to avoid duplicates.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

from collector.config import CLOSING_CHECK_INTERVAL, CLOSING_WINDOW_SECONDS
from collector.schema import ClosingSnapshot, _utc_now_iso, _stable_event_id
from collector.storage import JsonlWriter

logger = logging.getLogger(__name__)


def _compute_edge(pin_prob: float, price: float) -> float:
    if pin_prob <= 0 or price <= 0:
        return 0.0
    return round((pin_prob / price) - 1.0, 6)


class ClosingSnapshotFreezer:
    """Background task that freezes pre-game closing snapshots."""

    def __init__(self, registry, snapshotter):
        self.registry = registry
        self.snapshotter = snapshotter
        self._writer = JsonlWriter("closing_snapshots")
        self._closed_events: set[str] = set()
        self.count = 0

    async def run(self, shutdown_event: asyncio.Event) -> None:
        logger.info("Closing snapshot freezer started")
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=CLOSING_CHECK_INTERVAL
                )
                break
            except asyncio.TimeoutError:
                pass

            try:
                self._check_matches()
            except Exception:
                logger.debug("Closing snapshot error", exc_info=True)

        self._writer.close()
        logger.info("Closing snapshot freezer stopped (%d snapshots)", self.count)

    def _check_matches(self) -> None:
        now_dt = datetime.now(timezone.utc)

        for match_id, match in self.registry.matches.items():
            if not match.commence_time:
                continue

            event_id = _stable_event_id(match.teams[0], match.teams[1], match.sport)
            if event_id in self._closed_events:
                continue

            try:
                ct = match.commence_time
                if ct.endswith("Z"):
                    ct = ct[:-1] + "+00:00"
                start = datetime.fromisoformat(ct)
                seconds_until = (start - now_dt).total_seconds()
            except (ValueError, TypeError):
                continue

            # Within closing window and not yet started
            if 0 < seconds_until <= CLOSING_WINDOW_SECONDS:
                self._freeze(match, event_id, seconds_until)
            # Clean up events that started more than 5 min ago
            elif seconds_until < -300:
                self._closed_events.discard(event_id)

    def _freeze(self, match, event_id: str, seconds_before: float) -> None:
        """Build and write the closing snapshot from cached state."""
        cache = self.snapshotter.market_cache
        ref_cache = self.snapshotter.reference_cache

        # Helper to get last cached market state for a platform + side
        def _get_market(platform: str, token_or_ticker: str) -> dict:
            key = f"{platform}:{token_or_ticker}"
            c = cache.get(key, {})
            return {
                "best_bid": c.get("best_bid", 0),
                "best_ask": c.get("best_ask", 0),
                "mid": (c.get("best_ask", 0) + c.get("best_bid", 0)) / 2.0
                       if c.get("best_ask", 0) > 0 and c.get("best_bid", 0) > 0 else 0.0,
                "spread": c.get("best_ask", 0) - c.get("best_bid", 0)
                          if c.get("best_ask", 0) > 0 and c.get("best_bid", 0) > 0 else 0.0,
                "bid_depth_usd": c.get("update", {}).get("bid_depth_usd", 0) if c.get("update") else 0,
                "ask_depth_usd": c.get("update", {}).get("ask_depth_usd", 0) if c.get("update") else 0,
            }

        poly_a = _get_market("polymarket", match.poly_token_id_a) if match.poly_token_id_a else {}
        poly_b = _get_market("polymarket", match.poly_token_id_b) if match.poly_token_id_b else {}
        kalshi_a = _get_market("kalshi", match.kalshi_ticker_a) if match.kalshi_ticker_a else {}
        kalshi_b = _get_market("kalshi", match.kalshi_ticker_b) if match.kalshi_ticker_b else {}

        ref = ref_cache.get(event_id, {})
        pin_a = ref.get("pinnacle_prob_a", 0)
        pin_b = ref.get("pinnacle_prob_b", 0)
        pin_ia = ref.get("pinnacle_implied_a", 0)
        pin_ib = ref.get("pinnacle_implied_b", 0)
        pin_margin = ref.get("pinnacle_margin", 0)

        snap = ClosingSnapshot(
            ts_utc=_utc_now_iso(),
            event_id=event_id,
            commence_time_utc=match.commence_time or "",
            seconds_before_start=round(seconds_before, 1),
            poly_a_best_bid=poly_a.get("best_bid", 0),
            poly_a_best_ask=poly_a.get("best_ask", 0),
            poly_a_mid=round(poly_a.get("mid", 0), 6),
            poly_a_spread=round(poly_a.get("spread", 0), 6),
            poly_a_bid_depth_usd=poly_a.get("bid_depth_usd", 0),
            poly_a_ask_depth_usd=poly_a.get("ask_depth_usd", 0),
            poly_b_best_bid=poly_b.get("best_bid", 0),
            poly_b_best_ask=poly_b.get("best_ask", 0),
            poly_b_mid=round(poly_b.get("mid", 0), 6),
            poly_b_spread=round(poly_b.get("spread", 0), 6),
            poly_b_bid_depth_usd=poly_b.get("bid_depth_usd", 0),
            poly_b_ask_depth_usd=poly_b.get("ask_depth_usd", 0),
            kalshi_a_best_bid=kalshi_a.get("best_bid", 0),
            kalshi_a_best_ask=kalshi_a.get("best_ask", 0),
            kalshi_a_mid=round(kalshi_a.get("mid", 0), 6),
            kalshi_b_best_bid=kalshi_b.get("best_bid", 0),
            kalshi_b_best_ask=kalshi_b.get("best_ask", 0),
            kalshi_b_mid=round(kalshi_b.get("mid", 0), 6),
            pinnacle_prob_a=round(pin_a, 6),
            pinnacle_prob_b=round(pin_b, 6),
            pinnacle_implied_a=round(pin_ia, 6),
            pinnacle_implied_b=round(pin_ib, 6),
            pinnacle_margin=round(pin_margin, 6),
            poly_a_edge_at_ask=_compute_edge(pin_a, poly_a.get("best_ask", 0)),
            poly_b_edge_at_ask=_compute_edge(pin_b, poly_b.get("best_ask", 0)),
            kalshi_a_edge_at_ask=_compute_edge(pin_a, kalshi_a.get("best_ask", 0)),
            kalshi_b_edge_at_ask=_compute_edge(pin_b, kalshi_b.get("best_ask", 0)),
            data_quality="ok",
        )

        if self._writer.write(snap.to_dict()):
            self._closed_events.add(event_id)
            self.count += 1
            logger.info(
                "Closing snapshot: %s — %.0fs before start (poly_a=%.0f¢/%.0f¢, pin=%.0f%%/%.0f%%)",
                event_id, seconds_before,
                poly_a.get("best_bid", 0) * 100, poly_a.get("best_ask", 0) * 100,
                pin_a * 100, pin_b * 100,
            )
