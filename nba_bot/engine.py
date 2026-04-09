"""NBA Regular 12–24h strategy engine.

Receives price updates from feeds, evaluates the narrow NBA strategy,
and creates positions when all filters pass.
"""

import asyncio
import logging
import time
from collections import defaultdict

from live_bot.portfolio import PaperPortfolio, Position, Trade

from nba_bot.config import (
    ENABLE_NBA_REGULAR,
    NBA_PLATFORM,
    NBA_SPORT,
    NBA_MIN_MINUTES,
    NBA_MAX_MINUTES,
    NBA_MIN_EDGE_PCT,
    NBA_EDGE_PERSISTENCE,
    NBA_MIN_WIN_PROB,
    NBA_MAX_WIN_PROB,
    NBA_MAX_PINNACLE_AGE_S,
    NBA_BET_SIZE_USD,
    SIMULATION_MODE,
)
from nba_bot import skip_counters
from nba_bot import logger as nba_logger
from nba_bot.fill_sim import simulate_fill
from nba_bot.risk import check_risk, kelly_size

logger = logging.getLogger(__name__)


def _parse_commence(ct: str) -> float:
    """Parse commence_time string to epoch seconds."""
    if not ct:
        return 0.0
    try:
        from datetime import datetime, timezone
        if ct.endswith("Z"):
            ct = ct[:-1] + "+00:00"
        return datetime.fromisoformat(ct).timestamp()
    except (ValueError, TypeError):
        return 0.0


class NBAEngine:
    """Evaluates NBA Regular 12–24h value opportunities."""

    def __init__(self, portfolio: PaperPortfolio, registry):
        self.portfolio = portfolio
        self.registry = registry
        self._edge_persist_counts: dict[str, int] = defaultdict(int)
        self._price_cache: dict[str, dict] = {}

    async def run(self, price_queue: asyncio.Queue, shutdown_event: asyncio.Event):
        """Main loop: consume price updates and evaluate strategy."""
        logger.info("NBA engine started (enabled=%s, sim=%s)", ENABLE_NBA_REGULAR, SIMULATION_MODE)

        while not shutdown_event.is_set():
            try:
                update = await asyncio.wait_for(price_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            if shutdown_event.is_set():
                break

            try:
                await self._on_price_update(update)
            except Exception:
                logger.exception("Error processing price update")

    async def _on_price_update(self, update: dict):
        """Process a single price update from the feed."""
        platform = update.get("platform", "")
        market_id = update.get("market_id", "")

        # Cache the latest price data
        self._price_cache[market_id] = update

        # Find the match in registry
        match, _side = self.registry.get_match_for_market(platform, market_id)
        if not match:
            return

        skip_counters.inc("signals_seen")

        # ── Filter cascade ────────────────────────────────────────────

        # 1. Strategy enabled?
        if not ENABLE_NBA_REGULAR:
            skip_counters.inc("skipped_disabled")
            return

        # 2. Platform filter
        if platform != NBA_PLATFORM:
            skip_counters.inc("skipped_platform")
            return

        # 3. Sport filter
        if match.sport != NBA_SPORT:
            skip_counters.inc("skipped_sport")
            return

        # 4. Pregame only
        now = time.time()
        commence_ts = _parse_commence(match.commence_time)
        if commence_ts <= 0:
            skip_counters.inc("skipped_live")
            return
        minutes_to_start = (commence_ts - now) / 60
        if minutes_to_start <= 0:
            skip_counters.inc("skipped_live")
            return

        # 5. Time window: 12–24h
        if minutes_to_start < NBA_MIN_MINUTES or minutes_to_start > NBA_MAX_MINUTES:
            skip_counters.inc("skipped_time_window")
            return

        # Determine which side this update is for
        if market_id == match.poly_token_id_a:
            side = "a"
            pin_prob = match.pinnacle_prob_a
            team = match.teams[0]
        elif market_id == match.poly_token_id_b:
            side = "b"
            pin_prob = match.pinnacle_prob_b
            team = match.teams[1]
        else:
            return

        # 6. Pinnacle data freshness
        pin_last = match.pinnacle_last_seen_a if side == "a" else match.pinnacle_last_seen_b
        pin_age = now - pin_last if pin_last > 0 else 9999
        if pin_age > NBA_MAX_PINNACLE_AGE_S:
            skip_counters.inc("skipped_pinnacle_stale")
            return

        # 7. Pinnacle frozen / moving
        if side == "a" and match.pinnacle_frozen_a:
            skip_counters.inc("skipped_pinnacle_frozen")
            return
        if side == "b" and match.pinnacle_frozen_b:
            skip_counters.inc("skipped_pinnacle_frozen")
            return

        # 8. Edge calculation
        best_ask = update.get("best_ask", 0)
        if best_ask <= 0 or pin_prob <= 0:
            skip_counters.inc("skipped_no_edge")
            return

        edge = (pin_prob / best_ask) - 1.0
        if edge < NBA_MIN_EDGE_PCT:
            self._edge_persist_counts[market_id] = 0
            skip_counters.inc("skipped_no_edge")
            return

        # 9. Edge persistence
        self._edge_persist_counts[market_id] += 1
        if self._edge_persist_counts[market_id] < NBA_EDGE_PERSISTENCE:
            skip_counters.inc("skipped_edge_persistence")
            return

        # 10. Win probability range
        if pin_prob < NBA_MIN_WIN_PROB or pin_prob > NBA_MAX_WIN_PROB:
            skip_counters.inc("skipped_win_prob")
            return

        # ── Size and risk ─────────────────────────────────────────────

        size_usd = kelly_size(edge, pin_prob, self.portfolio.current_balance)
        if size_usd < 1.0:
            size_usd = NBA_BET_SIZE_USD
        size_usd = min(size_usd, NBA_BET_SIZE_USD)

        ok, reason = check_risk(self.portfolio, match.match_id, market_id, size_usd)
        if not ok:
            skip_counters.inc("skipped_risk")
            logger.debug("Risk blocked: %s — %s", team, reason)
            return

        # ── Execution ─────────────────────────────────────────────────

        shares = int(size_usd / best_ask)
        if shares < 1:
            skip_counters.inc("skipped_no_edge")
            return

        logger.info(
            "NBA SIGNAL: %s %s — ask=%.0fc pin=%.0fc edge=%.1f%% size=$%.0f persist=%d mts=%.0f",
            team, match.match_id, best_ask * 100, pin_prob * 100,
            edge * 100, size_usd, self._edge_persist_counts[market_id],
            minutes_to_start,
        )

        # Fill simulation
        filled_shares, fill_vwap = await simulate_fill(
            market_id=market_id,
            limit_price=best_ask,
            intended_shares=shares,
        )

        would_fill = filled_shares > 0
        if not would_fill:
            skip_counters.inc("skipped_fill_missed")
            nba_logger.log_trade(
                "NBA_BET",
                match_name=match.match_id,
                match_id=match.match_id,
                platform="polymarket",
                team=team,
                price=best_ask,
                edge_pct=edge * 100,
                pinnacle_prob=pin_prob,
                size_usd=size_usd,
                would_fill=False,
                extra={"minutes_to_start": round(minutes_to_start, 1)},
            )
            logger.info("Fill MISSED: %s — book moved", team)
            return

        # ── Record position ───────────────────────────────────────────

        actual_cost = filled_shares * fill_vwap
        trade = Trade(
            timestamp=time.time(),
            strategy="VALUE",
            match_id=match.match_id,
            match_name=match.match_id,
            platform_a="polymarket",
            team_a=team,
            price_a=fill_vwap,
            platform_b="",
            team_b="",
            price_b=0,
            size_usd=actual_cost,
            profit_pct=0,
            edge_pct=edge * 100,
            pinnacle_prob=pin_prob,
            timing="pregame",
            simulated=SIMULATION_MODE,
            would_fill=True,
            filled_a=True,
        )

        self.portfolio.record_value_trade(
            trade,
            market_id=market_id,
            condition_id=match.poly_condition_id,
        )

        # Update Pinnacle close tracking
        for pos in self.portfolio.positions:
            if pos.market_id == market_id:
                pos.pinnacle_prob_at_entry = pin_prob
                break

        skip_counters.inc("created")
        self._edge_persist_counts[market_id] = 0

        nba_logger.log_trade(
            "NBA_BET",
            match_name=match.match_id,
            match_id=match.match_id,
            platform="polymarket",
            team=team,
            price=fill_vwap,
            edge_pct=edge * 100,
            pinnacle_prob=pin_prob,
            size_usd=actual_cost,
            would_fill=True,
            extra={
                "filled_shares": filled_shares,
                "intended_shares": shares,
                "fill_vwap": round(fill_vwap, 6),
                "best_ask": best_ask,
                "minutes_to_start": round(minutes_to_start, 1),
                "balance_after": round(self.portfolio.current_balance, 2),
            },
        )

        logger.info(
            "NBA CREATED: %s %s — %d/%d shares @ %.0fc (vwap) | $%.2f | bal=$%.2f",
            team, match.match_id, filled_shares, shares,
            fill_vwap * 100, actual_cost, self.portfolio.current_balance,
        )
