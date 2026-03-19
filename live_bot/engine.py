"""Core engine — arb detection + value detection + execution coordination."""

import asyncio
import logging
import time

from datetime import datetime, timezone

from live_bot.config import (
    ENABLE_ARB,
    ENABLE_VALUE,
    MIN_ARB_PROFIT_PCT,
    MIN_VALUE_EDGE_PCT,
    MIDGAME_VALUE_EDGE_PCT,
    KALSHI_FEE_RATE,
    SIMULATION_MODE,
    ALLOW_MIDGAME_VALUE,
    MAX_PRICE_DIVERGENCE_PCT,
    MIN_ARB_DEPTH_USD,
    POLY_MAKER_REBATE,
    MAKER_FILL_RATE,
    MAKER_PRICE_IMPROVEMENT,
    EARLY_EXIT_TIERS,
    EARLY_EXIT_SPREAD_COST,
    VALUE_EDGE_PERSISTENCE,
    MAX_VALUE_EDGE_PCT,
    MAX_PINNACLE_AGE_SECONDS,
    MAX_PREGAME_HOURS,
    MAX_MATCH_DURATION_HOURS,
    MAX_POSITION_USD,
)
from live_bot.registry import MarketRegistry, TrackedMatch
from live_bot.portfolio import PaperPortfolio, Trade
from live_bot.risk import check_risk, kelly_size, ProposedTrade
from live_bot.logger import log_trade, log_event
from live_bot.fill_simulator import simulate_arb_fill, simulate_value_fill

import random

logger = logging.getLogger(__name__)


def _kalshi_fee(price: float) -> float:
    """Kalshi taker fee per contract: 0.07 * p * (1-p)."""
    return KALSHI_FEE_RATE * price * (1.0 - price)


def _max_size_for_edge(
    ask_levels: list[tuple[float, float]],
    pin_prob: float,
    min_edge: float,
    fee_fn=None,
) -> float:
    """Find the maximum USD size that can be filled while keeping edge above min_edge.

    Walks the order book level by level, computing the running VWAP.
    Stops when adding the next level would push the VWAP (after fees)
    above the price where the edge drops below min_edge.

    Returns the max safe USD size. If even the best ask doesn't have
    enough edge, returns 0. If book data isn't available, returns inf
    (meaning "no cap — we don't know the book").
    """
    if not ask_levels:
        return float("inf")  # No book data → don't cap (use Kelly only)

    # The worst effective price we can tolerate and still have min_edge
    # edge = (pin_prob / effective_price) - 1 >= min_edge
    # → effective_price <= pin_prob / (1 + min_edge)
    max_tolerable_price = pin_prob / (1.0 + min_edge)

    sorted_asks = sorted(ask_levels, key=lambda x: x[0])

    total_shares = 0.0
    total_cost = 0.0

    for price, size_shares in sorted_asks:
        level_usd = price * size_shares

        # Compute VWAP if we take this entire level
        new_shares = total_shares + size_shares
        new_cost = total_cost + level_usd
        new_vwap = new_cost / new_shares

        # Apply fee to check effective price
        effective_vwap = new_vwap
        if fee_fn:
            effective_vwap += fee_fn(new_vwap)

        if effective_vwap > max_tolerable_price:
            # This level would push us over — figure out how much of it we can take
            # Solve: (total_cost + p * s) / (total_shares + s) <= max_tolerable_price (ignoring fee change)
            # → s <= (max_tolerable_price * total_shares - total_cost) / (price - max_tolerable_price)
            if price <= max_tolerable_price and total_shares > 0:
                denom = price - max_tolerable_price
                if denom > 0:
                    partial_shares = (max_tolerable_price * total_shares - total_cost) / denom
                    partial_shares = max(0, min(partial_shares, size_shares))
                    total_cost += price * partial_shares
                    total_shares += partial_shares
            break

        total_shares += size_shares
        total_cost += level_usd

    if total_shares <= 0:
        return 0.0

    return total_cost  # total USD we can safely spend


def _compute_vwap(ask_levels: list[tuple[float, float]], size_usd: float) -> float:
    """Compute volume-weighted average price for a given USD size across book levels.

    Walks the order book from best to worst, filling the target USD amount.
    Returns the VWAP — the true cost per share if you buy `size_usd` worth.

    If book is too thin to fill the entire size, returns the VWAP of what's available
    plus a penalty for the unfilled portion (worst price + 1¢).
    """
    if not ask_levels or size_usd <= 0:
        return 0.0

    # Sort asks low→high (best first)
    sorted_asks = sorted(ask_levels, key=lambda x: x[0])

    remaining_usd = size_usd
    total_shares = 0.0
    total_cost = 0.0

    for price, size_shares in sorted_asks:
        if remaining_usd <= 0:
            break
        level_usd = price * size_shares
        if level_usd <= remaining_usd:
            # Take entire level
            total_shares += size_shares
            total_cost += level_usd
            remaining_usd -= level_usd
        else:
            # Partial fill at this level
            shares_needed = remaining_usd / price
            total_shares += shares_needed
            total_cost += remaining_usd
            remaining_usd = 0.0

    if total_shares <= 0:
        return sorted_asks[0][0] if sorted_asks else 0.0

    vwap = total_cost / total_shares

    # If we couldn't fill the full size, the remaining would walk even deeper
    if remaining_usd > 0 and sorted_asks:
        worst_price = sorted_asks[-1][0] + 0.01  # 1¢ beyond deepest level
        unfilled_shares = remaining_usd / worst_price
        total_cost += remaining_usd
        total_shares += unfilled_shares
        vwap = total_cost / total_shares

    return vwap


class ArbEngine:
    """Processes price updates and triggers arb/value trades."""

    def __init__(
        self,
        registry: MarketRegistry,
        poly_exec,
        kalshi_exec,
        portfolio: PaperPortfolio,
        on_trade_fn=None,
    ):
        self.registry = registry
        self.poly_exec = poly_exec
        self.kalshi_exec = kalshi_exec
        self.portfolio = portfolio
        self._on_trade = on_trade_fn  # called after each trade for persistence

        # Price cache: platform → market_id → {best_ask, best_bid, timestamp}
        self.prices: dict[str, dict[str, dict]] = {
            "polymarket": {},
            "kalshi": {},
            "pinnacle": {},
        }

        # Track recent arbs to avoid re-triggering (match_id → timestamp)
        self._recent_arbs: dict[str, float] = {}
        self._recent_values: dict[str, float] = {}

        # Edge persistence: track consecutive edge sightings
        # key = val_key → {"first_seen": timestamp, "last_edge": float}
        self._edge_persistence: dict[str, dict] = {}

        # Throttle full-registry scans on Pinnacle polls
        self._last_pinnacle_scan: float = 0.0

    @staticmethod
    def _get_match_timing(match: TrackedMatch) -> str:
        """Determine if a match is 'pregame' or 'midgame' based on commence_time."""
        if not match.commence_time:
            return "pregame"  # assume pregame if no time available
        try:
            # Parse ISO 8601 commence time
            ct = match.commence_time
            if ct.endswith("Z"):
                ct = ct[:-1] + "+00:00"
            start = datetime.fromisoformat(ct)
            now = datetime.now(timezone.utc)
            if now >= start:
                return "midgame"
            return "pregame"
        except (ValueError, TypeError):
            return "pregame"

    async def run(self, price_queue: asyncio.Queue, shutdown_event: asyncio.Event | None = None) -> None:
        """Main engine loop — consume price updates and check for opportunities."""
        log_event("ENGINE_START", f"Engine started (arb={ENABLE_ARB}, value={ENABLE_VALUE}, sim={SIMULATION_MODE})")

        while not (shutdown_event and shutdown_event.is_set()):
            try:
                update = await asyncio.wait_for(price_queue.get(), timeout=5.0)
                await self._on_price_update(update)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("Engine cancelled")
                return
            except Exception:
                logger.exception("Error processing price update")

    async def early_exit_loop(self, shutdown_event: asyncio.Event | None = None) -> None:
        """Shadow simulation: check if open value positions would benefit from early exit."""
        from live_bot.config import EARLY_EXIT_CHECK_INTERVAL
        while not (shutdown_event and shutdown_event.is_set()):
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=EARLY_EXIT_CHECK_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass

            self._check_early_exits()

    def _check_early_exits(self) -> None:
        """Check shadow positions for early exit across all tier thresholds.

        Each position is tracked independently per tier. A position can be
        "exited" in one tier (tight TP) but still open in another (wide TP).
        """
        for pos in self.portfolio.early_exit_positions:
            cached = self.prices.get(pos["platform"], {}).get(pos["market_id"], {})
            current_bid = cached.get("best_bid", 0)
            if current_bid <= 0:
                continue

            entry = pos["entry_price"]
            sell_price = current_bid - EARLY_EXIT_SPREAD_COST
            price_move = sell_price - entry
            pnl = price_move * pos["shares"]

            # Check each tier independently
            for tp, sl in EARLY_EXIT_TIERS:
                label = f"TP{int(tp*100)}/SL{int(sl*100)}"

                # Skip if this position already exited in this tier
                exited_tiers = pos.get("exited_tiers", set())
                if label in exited_tiers:
                    continue

                # Initialize tier stats if needed
                if label not in self.portfolio.early_exit_tiers:
                    self.portfolio.early_exit_tiers[label] = {"count": 0, "pnl": 0.0}

                tier = self.portfolio.early_exit_tiers[label]

                if price_move >= tp:
                    tier["count"] += 1
                    tier["pnl"] += pnl
                    exited_tiers.add(label)
                    pos["exited_tiers"] = exited_tiers
                    logger.info(
                        "   [EARLY EXIT %s] TP %s: %.0f¢→%.0f¢ = $%.2f",
                        label, pos["team"], entry * 100, sell_price * 100, pnl,
                    )
                elif price_move <= -sl:
                    tier["count"] += 1
                    tier["pnl"] += pnl
                    exited_tiers.add(label)
                    pos["exited_tiers"] = exited_tiers
                    logger.info(
                        "   [EARLY EXIT %s] SL %s: %.0f¢→%.0f¢ = $%.2f",
                        label, pos["team"], entry * 100, sell_price * 100, pnl,
                    )

        # Clean up positions where ALL tiers have exited or position is > 24h old
        all_labels = {f"TP{int(tp*100)}/SL{int(sl*100)}" for tp, sl in EARLY_EXIT_TIERS}
        now = time.time()
        max_age = 86400  # 24 hours
        self.portfolio.early_exit_positions = [
            pos for pos in self.portfolio.early_exit_positions
            if (not pos.get("exited_tiers", set()) >= all_labels)
            and (now - pos.get("opened_at", now) < max_age)
        ]

    async def _on_price_update(self, update: dict) -> None:
        """Process a single price update from any feed."""
        platform = update["platform"]
        market_id = update["market_id"]

        # Merge into price cache (don't overwrite non-zero with zero)
        existing = self.prices[platform].get(market_id, {})
        if update["best_ask"] > 0:
            existing["best_ask"] = update["best_ask"]
        if update["best_bid"] > 0:
            existing["best_bid"] = update["best_bid"]
        # Track available size at best ask/bid
        if update.get("ask_size", 0) > 0:
            existing["ask_size"] = update["ask_size"]
        if update.get("bid_size", 0) > 0:
            existing["bid_size"] = update["bid_size"]
        # Full order book depth in USD (across all price levels)
        if update.get("ask_depth_usd", 0) > 0:
            existing["ask_depth_usd"] = update["ask_depth_usd"]
        if update.get("bid_depth_usd", 0) > 0:
            existing["bid_depth_usd"] = update["bid_depth_usd"]
        # Raw order book levels for VWAP computation
        if update.get("ask_levels"):
            existing["ask_levels"] = update["ask_levels"]
        if update.get("bid_levels"):
            existing["bid_levels"] = update["bid_levels"]
        if update.get("no_vig_prob", 0) > 0:
            existing["no_vig_prob"] = update["no_vig_prob"]
        existing["timestamp"] = update["timestamp"]
        self.prices[platform][market_id] = existing

        # Find which tracked match this belongs to
        match, side = self.registry.get_match_for_market(platform, market_id)

        if platform == "pinnacle":
            # Pinnacle updates don't map to specific matches via reverse lookup.
            # On Pinnacle poll, re-check ALL matches with cached market prices.
            # This advances persistence timers that started from WS updates but
            # stalled because the WS price didn't change.
            # Throttle: only do the full scan once per 5 seconds max.
            now = time.time()
            if ENABLE_VALUE and now - self._last_pinnacle_scan > 5.0:
                self._last_pinnacle_scan = now
                for m in self.registry.matches.values():
                    await self._check_value(m)
                # Also check 48h shadow bets approaching start time
                self._check_48h_prestart()
            return

        if not match:
            return

        # Strategy 1: Cross-platform arb
        if ENABLE_ARB:
            await self._check_arb(match)

        # Strategy 2: Pinnacle value betting
        if ENABLE_VALUE:
            await self._check_value(match)

    async def _check_arb(self, match: TrackedMatch) -> None:
        """Check for cross-platform arb opportunity (Poly × Kalshi)."""
        # Need both platforms
        if not match.poly_token_id_a or not match.kalshi_ticker_a:
            return

        # Get prices for all 4 endpoints
        poly_a = self.prices["polymarket"].get(match.poly_token_id_a, {})
        poly_b = self.prices["polymarket"].get(match.poly_token_id_b, {})
        kalshi_a = self.prices["kalshi"].get(match.kalshi_ticker_a, {})
        kalshi_b = self.prices["kalshi"].get(match.kalshi_ticker_b, {})

        # Check all 4 arb directions:
        # 1. Buy team A on Poly + Buy team B on Kalshi
        # 2. Buy team B on Poly + Buy team A on Kalshi
        # 3. Buy team A on Kalshi + Buy team B on Poly
        # 4. Buy team B on Kalshi + Buy team A on Poly
        # (3 & 4 are same as 1 & 2 but checking both orderings)

        combos = []

        # Direction 1: Team A on Poly + Team B on Kalshi
        pa = poly_a.get("best_ask", 0)
        kb = kalshi_b.get("best_ask", 0)
        if pa > 0 and kb > 0:
            cost = pa + kb + _kalshi_fee(kb)
            if cost < 1.0:
                profit = (1.0 - cost) / cost * 100
                # Available shares at this price on each side
                size_a = poly_a.get("ask_size", 0)
                size_b = kalshi_b.get("ask_size", 0)
                combos.append(("poly_a_kalshi_b", pa, kb, cost, profit,
                               "polymarket", match.teams[0], match.poly_token_id_a,
                               "kalshi", match.teams[1], match.kalshi_ticker_b,
                               size_a, size_b))

        # Direction 2: Team B on Poly + Team A on Kalshi
        pb = poly_b.get("best_ask", 0)
        ka = kalshi_a.get("best_ask", 0)
        if pb > 0 and ka > 0:
            cost = pb + ka + _kalshi_fee(ka)
            if cost < 1.0:
                profit = (1.0 - cost) / cost * 100
                size_a = poly_b.get("ask_size", 0)
                size_b = kalshi_a.get("ask_size", 0)
                combos.append(("poly_b_kalshi_a", pb, ka, cost, profit,
                               "polymarket", match.teams[1], match.poly_token_id_b,
                               "kalshi", match.teams[0], match.kalshi_ticker_a,
                               size_a, size_b))

        # Direction 3: Team A on Kalshi + Team B on Poly
        if ka > 0 and pb > 0:
            cost = ka + _kalshi_fee(ka) + pb
            if cost < 1.0:
                profit = (1.0 - cost) / cost * 100
                size_a = kalshi_a.get("ask_size", 0)
                size_b = poly_b.get("ask_size", 0)
                combos.append(("kalshi_a_poly_b", ka, pb, cost, profit,
                               "kalshi", match.teams[0], match.kalshi_ticker_a,
                               "polymarket", match.teams[1], match.poly_token_id_b,
                               size_a, size_b))

        # Direction 4: Team B on Kalshi + Team A on Poly
        if kb > 0 and pa > 0:
            cost = kb + _kalshi_fee(kb) + pa
            if cost < 1.0:
                profit = (1.0 - cost) / cost * 100
                size_a = kalshi_b.get("ask_size", 0)
                size_b = poly_a.get("ask_size", 0)
                combos.append(("kalshi_b_poly_a", kb, pa, cost, profit,
                               "kalshi", match.teams[1], match.kalshi_ticker_b,
                               "polymarket", match.teams[0], match.poly_token_id_a,
                               size_a, size_b))

        if not combos:
            return

        # Take the most profitable direction
        combos.sort(key=lambda c: -c[4])
        best = combos[0]
        (direction, price_a, price_b, cost, profit,
         plat_a, team_a, id_a, plat_b, team_b, id_b,
         avail_size_a, avail_size_b) = best

        if profit < MIN_ARB_PROFIT_PCT:
            return

        # Deduplicate: don't re-trigger same arb within cooldown
        arb_key = f"{match.match_id}_{direction}"
        last = self._recent_arbs.get(arb_key, 0)
        if time.time() - last < 30:  # 30s cooldown per arb direction
            return

        # Determine size — capped by available liquidity on BOTH sides
        size_usd = min(MAX_POSITION_USD, self.portfolio.current_balance / 2)

        # Cap by available liquidity (shares * price = USD available)
        if avail_size_a > 0:
            usd_avail_a = avail_size_a * price_a
            size_usd = min(size_usd, usd_avail_a)
        if avail_size_b > 0:
            usd_avail_b = avail_size_b * price_b
            size_usd = min(size_usd, usd_avail_b)

        if size_usd < 1.0:
            logger.debug(
                "Arb too thin: %s — only $%.2f available (need $1+)",
                match.match_id, size_usd,
            )
            return

        # Risk check
        proposed = ProposedTrade("ARB", match.match_id, id_a, size_usd, profit)
        allowed, reason = check_risk(self.portfolio, proposed)
        if not allowed:
            logger.debug("Arb blocked by risk: %s", reason)
            return

        self._recent_arbs[arb_key] = time.time()

        timing = self._get_match_timing(match)

        # Gather depth and staleness for fill simulation
        now = time.time()
        cache_a = self.prices[plat_a].get(id_a, {})
        cache_b = self.prices[plat_b].get(id_b, {})
        # Prefer full book depth_usd if available, else fall back to top-of-book estimate
        depth_usd_a = cache_a.get("ask_depth_usd", 0) or (avail_size_a * price_a if avail_size_a > 0 else 0.0)
        depth_usd_b = cache_b.get("ask_depth_usd", 0) or (avail_size_b * price_b if avail_size_b > 0 else 0.0)
        age_a = now - cache_a.get("timestamp", now)
        age_b = now - cache_b.get("timestamp", now)

        # Minimum depth check: don't take arbs on paper-thin books
        if depth_usd_a > 0 and depth_usd_a < MIN_ARB_DEPTH_USD:
            logger.debug(
                "Arb skip (thin depth A): %s — $%.0f < $%.0f min",
                match.match_id, depth_usd_a, MIN_ARB_DEPTH_USD,
            )
            return
        if depth_usd_b > 0 and depth_usd_b < MIN_ARB_DEPTH_USD:
            logger.debug(
                "Arb skip (thin depth B): %s — $%.0f < $%.0f min",
                match.match_id, depth_usd_b, MIN_ARB_DEPTH_USD,
            )
            return

        # Execute
        await self._execute_arb(
            match, direction, price_a, price_b, cost, profit,
            plat_a, team_a, id_a, plat_b, team_b, id_b, size_usd, timing,
            depth_usd_a, depth_usd_b, age_a, age_b,
        )

    async def _execute_arb(
        self, match, direction, price_a, price_b, cost, profit,
        plat_a, team_a, id_a, plat_b, team_b, id_b, size_usd, timing="pregame",
        depth_usd_a=0.0, depth_usd_b=0.0, age_a=0.0, age_b=0.0,
    ) -> None:
        """Execute an arb using sequential leg strategy.

        Strategy: Place the HARDER leg first (thinner depth = less likely to fill).
        If it fills, immediately place the easier leg. If the easier leg fails,
        sell back the first leg at market price (lose spread, avoid exposure).
        """
        start = time.time()
        shares = int(size_usd / cost)  # how many contracts at this combined cost
        if shares < 1:
            return

        # Decide leg order: place the thinner-depth leg first
        # If leg A has less depth, it's harder to fill → go first
        if depth_usd_a <= depth_usd_b:
            first_leg = ("a", plat_a, team_a, id_a, price_a, depth_usd_a, age_a)
            second_leg = ("b", plat_b, team_b, id_b, price_b, depth_usd_b, age_b)
        else:
            first_leg = ("b", plat_b, team_b, id_b, price_b, depth_usd_b, age_b)
            second_leg = ("a", plat_a, team_a, id_a, price_a, depth_usd_a, age_a)

        logger.info(
            "🎯 ARB DETECTED: %s — %s@%s %.0f¢ + %s@%s %.0f¢ = %.1f¢ (%.2f%%) — %d shares "
            "(depth: $%.0f/$%.0f, age: %.1fs/%.1fs) [%s leg first]",
            match.match_id, team_a, plat_a, price_a * 100,
            team_b, plat_b, price_b * 100, cost * 100, profit, shares,
            depth_usd_a, depth_usd_b, age_a, age_b,
            first_leg[0].upper(),
        )

        if SIMULATION_MODE:
            # SEQUENTIAL fill simulation: harder leg first, then easier leg
            from live_bot.fill_simulator import _simulate_single_fill

            # Leg 1: the harder fill
            filled_1, slip_1 = _simulate_single_fill(
                first_leg[5], first_leg[6], first_leg[1], is_arb=True
            )

            if not filled_1:
                # First leg missed → no risk, just skip
                logger.info("   ↳ First leg (%s) missed — no fill, no risk", first_leg[0].upper())
                trade_type = "ARB_REJECTED"
                filled_a = False
                filled_b = False
            else:
                # First leg filled! Now try the easier leg
                filled_2, slip_2 = _simulate_single_fill(
                    second_leg[5], second_leg[6], second_leg[1], is_arb=True
                )

                if filled_2:
                    # Both legs filled — full arb!
                    # Assign slippage back to correct legs
                    if first_leg[0] == "a":
                        slip_a, slip_b = slip_1, slip_2
                    else:
                        slip_a, slip_b = slip_2, slip_1

                    # Check if slippage kills profit
                    actual_cost = (price_a + slip_a) + (price_b + slip_b) + _kalshi_fee(
                        price_b + slip_b if plat_b == "kalshi" else price_b
                    )
                    if actual_cost >= 1.0:
                        logger.info(
                            "   ↳ Slippage killed arb: cost %.1f¢ + %.1f¢ slip → %.1f¢ (≥100¢)",
                            cost * 100, (slip_a + slip_b) * 100, actual_cost * 100,
                        )
                        trade_type = "ARB_REJECTED"
                        filled_a = False
                        filled_b = False
                    else:
                        actual_profit = (1.0 - actual_cost) / actual_cost * 100
                        profit = actual_profit
                        price_a += slip_a
                        price_b += slip_b
                        cost = actual_cost
                        trade_type = "ARB_SUCCESS"
                        filled_a = True
                        filled_b = True
                        logger.info(
                            "   ✅ Both legs filled! profit=%.2f%% (after %.1f¢ slippage)",
                            profit, (slip_a + slip_b) * 100,
                        )
                else:
                    # PARTIAL: first leg filled but second missed
                    # In real trading: sell back first leg at market (lose spread)
                    spread_loss_per_share = 0.02  # ~2 cents typical spread
                    unwind_loss = spread_loss_per_share * shares
                    # Debit the loss from portfolio
                    self.portfolio.total_pnl -= unwind_loss
                    self.portfolio.daily_pnl -= unwind_loss
                    self.portfolio.current_balance -= unwind_loss
                    logger.info(
                        "   ↳ Second leg (%s) missed — unwinding first leg (loss $%.2f = %d shares × %.0f¢ spread)",
                        second_leg[0].upper(), unwind_loss, shares, spread_loss_per_share * 100,
                    )
                    trade_type = "ARB_UNWOUND"
                    filled_a = False
                    filled_b = False

            latency = (time.time() - start) * 1000
        else:
            # LIVE sequential execution: harder leg first
            latency_start = time.time()

            # Place first leg
            if first_leg[1] == "polymarket":
                result_1 = await self.poly_exec.place_order(first_leg[3], first_leg[4], shares, "BUY")
            else:
                result_1 = await self.kalshi_exec.place_order(first_leg[3], "yes", first_leg[4], shares)

            filled_1 = isinstance(result_1, tuple) and result_1[0]

            if not filled_1:
                logger.info("   ↳ First leg (%s) rejected — no risk", first_leg[0].upper())
                trade_type = "ARB_REJECTED"
                filled_a = False
                filled_b = False
            else:
                # First leg filled — immediately place second leg
                if second_leg[1] == "polymarket":
                    result_2 = await self.poly_exec.place_order(second_leg[3], second_leg[4], shares, "BUY")
                else:
                    result_2 = await self.kalshi_exec.place_order(second_leg[3], "yes", second_leg[4], shares)

                filled_2 = isinstance(result_2, tuple) and result_2[0]

                if filled_2:
                    trade_type = "ARB_SUCCESS"
                    filled_a = True
                    filled_b = True
                else:
                    # PARTIAL: sell back the first leg at market
                    logger.warning(
                        "⚠️ Second leg (%s) failed — unwinding first leg at market!",
                        second_leg[0].upper(),
                    )
                    if first_leg[1] == "polymarket":
                        await self.poly_exec.place_order(first_leg[3], first_leg[4], shares, "SELL")
                    else:
                        await self.kalshi_exec.place_order(first_leg[3], "no", first_leg[4], shares)

                    trade_type = "ARB_UNWOUND"
                    filled_a = False
                    filled_b = False

            latency = (time.time() - latency_start) * 1000

        trade = Trade(
            timestamp=time.time(),
            strategy="ARB",
            match_id=match.match_id,
            match_name=f"{match.teams[0]} vs {match.teams[1]}",
            platform_a=plat_a,
            team_a=team_a,
            price_a=price_a,
            platform_b=plat_b,
            team_b=team_b,
            price_b=price_b,
            size_usd=shares * cost,
            profit_pct=profit,
            edge_pct=0,
            pinnacle_prob=0,
            timing=timing,
            simulated=SIMULATION_MODE,
            would_fill=filled_a and filled_b,
            filled_a=filled_a,
            filled_b=filled_b,
            latency_ms=latency,
        )

        # Only record as position if both legs filled
        if filled_a and filled_b:
            cid_a = match.poly_condition_id if plat_a == "polymarket" else ""
            cid_b = match.poly_condition_id if plat_b == "polymarket" else ""

            self.portfolio.record_arb_trade(
                trade,
                market_id_a=id_a,
                market_id_b=id_b,
                condition_id_a=cid_a,
                condition_id_b=cid_b,
            )

        log_trade(
            trade_type, "ARB",
            match_name=trade.match_name, match_id=match.match_id,
            platform_a=plat_a, team_a=team_a, price_a=price_a,
            platform_b=plat_b, team_b=team_b, price_b=price_b,
            combined_cost=cost, profit_pct=profit,
            size_usd=trade.size_usd, latency_ms=latency,
            simulated=SIMULATION_MODE, would_fill=trade.would_fill,
            filled_a=filled_a, filled_b=filled_b,
            extra={"timing": timing},
        )

        # --- SHADOW: Maker order simulation ---
        # What if we posted a limit order on the Poly leg instead of taking?
        # Maker gets rebate instead of paying fees + gets 1¢ price improvement
        if SIMULATION_MODE and profit > 0:
            self._shadow_maker_arb(
                plat_a, plat_b, price_a, price_b, shares, cost, match,
            )

        if self._on_trade:
            self._on_trade()

    def _shadow_maker_arb(
        self, plat_a, plat_b, price_a, price_b, shares, taker_cost, match,
    ):
        """Shadow simulation: what would this arb look like with maker orders?"""
        # Maker order on the Polymarket leg: better price + rebate
        maker_price_a = price_a - MAKER_PRICE_IMPROVEMENT if plat_a == "polymarket" else price_a
        maker_price_b = price_b - MAKER_PRICE_IMPROVEMENT if plat_b == "polymarket" else price_b

        # Rebate on the Poly leg
        rebate = 0.0
        if plat_a == "polymarket":
            rebate += maker_price_a * shares * POLY_MAKER_REBATE
        if plat_b == "polymarket":
            rebate += maker_price_b * shares * POLY_MAKER_REBATE

        maker_cost = maker_price_a + maker_price_b
        if plat_a == "kalshi":
            maker_cost += _kalshi_fee(maker_price_a)
        if plat_b == "kalshi":
            maker_cost += _kalshi_fee(maker_price_b)

        if maker_cost >= 1.0:
            return

        maker_profit_per_share = 1.0 - maker_cost
        total_profit = maker_profit_per_share * shares + rebate

        # But maker orders only fill MAKER_FILL_RATE of the time
        if random.random() < MAKER_FILL_RATE:
            self.portfolio.maker_arb_count += 1
            self.portfolio.maker_arb_pnl += total_profit
            logger.info(
                "   [MAKER SIM] Arb would profit $%.2f (vs taker $%.2f) — rebate $%.3f",
                total_profit, (1.0 - taker_cost) * shares, rebate,
            )

    async def _check_value(self, match: TrackedMatch) -> None:
        """Check for value betting opportunity using Pinnacle as truth."""
        # Need Pinnacle reference prices
        if match.pinnacle_prob_a <= 0 and match.pinnacle_prob_b <= 0:
            return


        timing = self._get_match_timing(match)

        # Countermeasure 1: Skip midgame value bets entirely (unless allowed).
        # Pinnacle pre-game odds are stale once the match starts — a team
        # losing 0-3 still shows 60% pre-game prob, making everything look +EV.
        if timing == "midgame" and not ALLOW_MIDGAME_VALUE:
            return

        # ── Sanity: Pinnacle probabilities should sum to ~1.0 ──
        # If they don't, the fuzzy matcher likely linked the wrong teams.
        if match.pinnacle_prob_a > 0 and match.pinnacle_prob_b > 0:
            prob_sum = match.pinnacle_prob_a + match.pinnacle_prob_b
            if prob_sum < 0.85 or prob_sum > 1.15:
                logger.debug(
                    "Value skip (prob sanity): %s — pin_a=%.0f%% + pin_b=%.0f%% = %.0f%% (expected ~100%%)",
                    match.match_id,
                    match.pinnacle_prob_a * 100, match.pinnacle_prob_b * 100,
                    prob_sum * 100,
                )
                return

        # ── Timing guards: skip matches too far away or likely over ──
        shadow_48h = False
        shadow_hours = 0.0
        if match.commence_time:
            try:
                ct = match.commence_time
                if ct.endswith("Z"):
                    ct = ct[:-1] + "+00:00"
                start = datetime.fromisoformat(ct)
                now_dt = datetime.now(timezone.utc)
                hours_until = (start - now_dt).total_seconds() / 3600

                # Too far in the future — odds will shift, not worth betting yet
                if hours_until > MAX_PREGAME_HOURS:
                    if hours_until <= 48:
                        # Let it continue through the full pipeline as a 48h shadow
                        shadow_48h = True
                        shadow_hours = hours_until
                    else:
                        return

                # Match likely over — commence + duration exceeded
                if hours_until < 0 and abs(hours_until) > MAX_MATCH_DURATION_HOURS:
                    logger.debug(
                        "Value skip (match likely over): %s — started %.0fh ago (max %.0fh)",
                        match.match_id, abs(hours_until), MAX_MATCH_DURATION_HOURS,
                    )
                    return
            except (ValueError, TypeError):
                pass  # can't parse, continue with other checks

        # Check each team on each platform
        checks = []
        if match.pinnacle_prob_a > 0:
            if match.poly_token_id_a:
                checks.append(("a", "polymarket", match.poly_token_id_a,
                               match.teams[0], match.pinnacle_prob_a))
            if match.kalshi_ticker_a:
                checks.append(("a", "kalshi", match.kalshi_ticker_a,
                               match.teams[0], match.pinnacle_prob_a))
        if match.pinnacle_prob_b > 0:
            if match.poly_token_id_b:
                checks.append(("b", "polymarket", match.poly_token_id_b,
                               match.teams[1], match.pinnacle_prob_b))
            if match.kalshi_ticker_b:
                checks.append(("b", "kalshi", match.kalshi_ticker_b,
                               match.teams[1], match.pinnacle_prob_b))

        for team_side, platform, market_id, team_name, pin_prob in checks:
            # Skip if Pinnacle odds are frozen (suspended line during live play)
            if timing == "midgame":
                frozen = match.pinnacle_frozen_a if team_side == "a" else match.pinnacle_frozen_b
                if frozen:
                    logger.debug(
                        "Value skip (Pinnacle frozen): %s %s — line likely suspended",
                        team_name, platform,
                    )
                    continue

            # ── Pinnacle line movement check ──
            # If the Pinnacle line shifted >3pp between polls, sharp money is moving it.
            # Our reference is unreliable until the line stabilizes.
            moving = match.pinnacle_moving_a if team_side == "a" else match.pinnacle_moving_b
            if moving:
                logger.debug(
                    "Value skip (Pinnacle moving): %s %s — line shifted >3pp, waiting for stabilization",
                    team_name, platform,
                )
                continue

            # ── Pinnacle data age check ──
            # If we haven't received ANY Pinnacle data recently, the reference is stale
            # (API errors, rate limits, etc. — freeze detection won't catch this)
            last_seen = match.pinnacle_last_seen_a if team_side == "a" else match.pinnacle_last_seen_b
            now = time.time()
            if last_seen > 0:
                pin_age = now - last_seen
                if pin_age > MAX_PINNACLE_AGE_SECONDS:
                    logger.debug(
                        "Value skip (Pinnacle stale): %s %s — last data %.0fs ago (max %.0fs)",
                        team_name, platform, pin_age, MAX_PINNACLE_AGE_SECONDS,
                    )
                    continue

            cached = self.prices[platform].get(market_id, {})
            market_ask = cached.get("best_ask", 0)
            if market_ask <= 0:
                # Only log for matches with significant Pinnacle prob
                if pin_prob > 0.10:
                    logger.info(
                        "Value skip (no cached price): %s %s market_id=%s",
                        team_name, platform, market_id[:30],
                    )
                continue

            # --- VWAP-based cost calculation ---
            # Use order book levels if available to get true executable cost
            ask_levels = cached.get("ask_levels", [])
            intended_size_usd = min(MAX_POSITION_USD, self.portfolio.current_balance / 2)

            if ask_levels and intended_size_usd > 0:
                vwap_price = _compute_vwap(ask_levels, intended_size_usd)
                effective_price = vwap_price if vwap_price > 0 else market_ask
            else:
                effective_price = market_ask

            # Add fee for Kalshi
            if platform == "kalshi":
                effective_price += _kalshi_fee(effective_price)

            # Countermeasure 2: Detect stale Pinnacle reference.
            # If market price diverges massively from Pinnacle (e.g., pin=60%, market=15%),
            # the reference is likely outdated. This catches matches that started between
            # Pinnacle polls or slow-moving odds after match start.
            divergence_pct = abs(pin_prob - effective_price) * 100
            if divergence_pct > MAX_PRICE_DIVERGENCE_PCT:
                logger.debug(
                    "Value skip (stale ref): %s %s — pin=%.0f¢ vs market=%.0f¢ (divergence=%.0f%%)",
                    team_name, platform, pin_prob * 100, effective_price * 100, divergence_pct,
                )
                continue

            # Calculate edge using VWAP-inclusive effective price
            edge = (pin_prob / effective_price) - 1.0

            # Dynamic edge threshold: stricter for midgame
            min_edge = MIDGAME_VALUE_EDGE_PCT / 100 if timing == "midgame" else MIN_VALUE_EDGE_PCT / 100

            if edge < min_edge:
                continue

            logger.info(
                "Value edge found: %s %s pin=%.0f¢ mkt=%.0f¢ edge=%.1f%%",
                team_name, platform, pin_prob * 100, effective_price * 100, edge * 100,
            )

            # Sanity cap: edges above 20% are almost certainly stale refs or bad matches
            if edge > MAX_VALUE_EDGE_PCT / 100:
                logger.debug(
                    "Value skip (edge too high = likely stale): %s %s edge=%.1f%% > %.0f%% cap",
                    team_name, platform, edge * 100, MAX_VALUE_EDGE_PCT,
                )
                continue

            # --- Edge persistence check (time-based) ---
            # Edge must exist for at least 3 seconds before we bet.
            # Filters out momentary glitches and stale-reference false positives.
            # Executes as soon as 3s passes — doesn't wait for next Pinnacle poll.
            MIN_PERSISTENCE_SECONDS = 3.0

            val_key = f"{match.match_id}_{team_side}_{platform}"
            now = time.time()
            persistence = self._edge_persistence.get(val_key)

            if persistence is None:
                # First sighting — start the timer
                self._edge_persistence[val_key] = {
                    "first_seen": now,
                    "last_edge": edge,
                }
                logger.info(
                    "Value persistence [new]: %s %s edge=%.1f%% — waiting %.0fs",
                    team_name, platform, edge * 100, MIN_PERSISTENCE_SECONDS,
                )
                continue
            else:
                age = now - persistence["first_seen"]
                # Too old (>90s) — edge disappeared and came back, reset
                if age > 90:
                    self._edge_persistence[val_key] = {
                        "first_seen": now,
                        "last_edge": edge,
                    }
                    continue

                persistence["last_edge"] = edge

                # Not enough time elapsed yet
                if age < MIN_PERSISTENCE_SECONDS:
                    continue

            # Edge persisted! Clear persistence tracker
            self._edge_persistence.pop(val_key, None)

            # Deduplicate
            last = self._recent_values.get(val_key, 0)
            if now - last < 300:  # 5 min cooldown per value opportunity
                continue

            # Size using Kelly criterion
            size = kelly_size(edge, pin_prob, self.portfolio.current_balance)
            if size < 1.0:
                continue

            # ── Depth-aware size cap ──
            # If the orderbook is available, cap the bet to the max USD
            # that can be filled without slippage eating the edge.
            # This matters as wallet grows and Kelly wants to bet $200+.
            ask_levels = cached.get("ask_levels", [])
            fee_fn = _kalshi_fee if platform == "kalshi" else None
            max_depth_size = _max_size_for_edge(ask_levels, pin_prob, min_edge, fee_fn)
            if max_depth_size < 1.0:
                logger.info(
                    "Value skip (book too thin): %s %s — even best ask kills edge",
                    team_name, platform,
                )
                continue
            if size > max_depth_size and max_depth_size != float("inf"):
                logger.info(
                    "Value bet depth-capped: %s %s — Kelly=$%.0f → capped to $%.0f (book limit)",
                    team_name, platform, size, max_depth_size,
                )
                size = max_depth_size

            # Risk check
            proposed = ProposedTrade("VALUE", match.match_id, market_id, size, edge * 100)
            allowed, reason = check_risk(self.portfolio, proposed)
            if not allowed:
                logger.debug("Value bet blocked: %s", reason)
                continue

            self._recent_values[val_key] = now

            # Gather depth and staleness for fill simulation
            depth_usd = cached.get("ask_depth_usd", 0) or (cached.get("ask_size", 0) * market_ask)
            # Kalshi REST only reports top-of-book size, not full depth.
            # Full orderbook is typically 5-20× deeper than best level.
            # Assume $2000 minimum so the fill simulator doesn't reject
            # every Kalshi bet due to apparent thin books.
            if platform == "kalshi" and depth_usd < 2000:
                depth_usd = max(depth_usd, 2000.0)
            price_age = now - cached.get("timestamp", now)

            # ── 48h shadow: log instead of executing ──
            if shadow_48h:
                bet_key = f"{match.match_id}_{team_side}_{platform}"
                existing = [b for b in self.portfolio.early_48h_bets if b.get("key") == bet_key]
                if not existing:
                    entry = {
                        "key": bet_key,
                        "match_id": match.match_id,
                        "team": team_name,
                        "sport": match.sport,
                        "platform": platform,
                        "market_id": market_id,
                        "pin_prob": round(pin_prob, 4),
                        "entry_ask": round(effective_price, 4),
                        "edge_pct": round(edge * 100, 1),
                        "size_usd": round(size, 2),
                        "hours_until": round(shadow_hours, 1),
                        "timestamp": time.time(),
                        "commence_time": match.commence_time,
                        "pre_match_edge_pct": None,
                        "pre_match_ask": None,
                    }
                    self.portfolio.early_48h_bets.append(entry)
                    logger.info(
                        "SHADOW 48h BET: %s %s — pin=%.0f¢ ask=%.0f¢ edge=%.1f%% size=$%.0f hours=%.0fh",
                        team_name, platform, pin_prob * 100, effective_price * 100,
                        edge * 100, size, shadow_hours,
                    )
                continue

            # Execute (pass effective_price which includes VWAP + fees)
            await self._execute_value(
                match, platform, market_id, team_name, effective_price,
                pin_prob, edge, size, timing, depth_usd, price_age,
            )

    def _check_48h_prestart(self) -> None:
        """Check 48h shadow bets that are ~1h from start — record current edge.

        Called periodically from the Pinnacle scan loop.
        """
        now_dt = datetime.now(timezone.utc)
        for bet in self.portfolio.early_48h_bets:
            if bet.get("pre_match_edge_pct") is not None:
                continue  # already checked

            ct = bet.get("commence_time", "")
            if not ct:
                continue
            try:
                if ct.endswith("Z"):
                    ct = ct[:-1] + "+00:00"
                start = datetime.fromisoformat(ct)
                hours_left = (start - now_dt).total_seconds() / 3600
            except (ValueError, TypeError):
                continue

            # Check when match is 0.5-1.5h away
            if hours_left > 1.5 or hours_left < 0:
                continue

            # Get current market price
            platform = bet["platform"]
            mid = bet["market_id"]
            cached = self.prices[platform].get(mid, {})
            ask = cached.get("best_ask", 0)
            if ask <= 0:
                continue

            pin_prob = bet["pin_prob"]
            current_edge = (pin_prob / ask) - 1.0

            bet["pre_match_ask"] = round(ask, 4)
            bet["pre_match_edge_pct"] = round(current_edge * 100, 1)
            bet["pre_match_timestamp"] = time.time()

            logger.info(
                "SHADOW 48h PRE-START: %s — entry_edge=%.1f%% now_edge=%.1f%% (ask %.0f¢→%.0f¢) hours_left=%.1fh",
                bet["team"], bet["edge_pct"], current_edge * 100,
                bet["entry_ask"] * 100, ask * 100, hours_left,
            )

    async def _execute_value(
        self, match, platform, market_id, team_name,
        market_price, pin_prob, edge, size_usd, timing="pregame",
        depth_usd=0.0, price_age=0.0,
    ) -> None:
        """Execute a value bet — single leg."""
        start = time.time()
        shares = int(size_usd / market_price)
        if shares < 1:
            return

        logger.info(
            "📊 VALUE BET: %s %s@%s at %.0f¢ (pinnacle=%.0f¢, edge=%.1f%%) — %d shares ($%.2f) "
            "(depth: $%.0f, age: %.1fs)",
            team_name, platform, match.match_id,
            market_price * 100, pin_prob * 100, edge * 100,
            shares, size_usd, depth_usd, price_age,
        )

        if SIMULATION_MODE:
            # Fill simulation — slippage is already in the VWAP price from the engine
            filled, _slippage = simulate_value_fill(
                market_price, depth_usd, price_age, platform
            )
            if not filled:
                logger.info("   ↳ Value bet missed fill (depth=$%.0f, age=%.1fs)", depth_usd, price_age)
        else:
            if platform == "polymarket":
                filled, details = await self.poly_exec.place_order(
                    market_id, market_price, shares, "BUY"
                )
            else:
                filled, details = await self.kalshi_exec.place_order(
                    market_id, "yes", market_price, shares
                )

        latency = (time.time() - start) * 1000

        trade = Trade(
            timestamp=time.time(),
            strategy="VALUE",
            match_id=match.match_id,
            match_name=f"{match.teams[0]} vs {match.teams[1]}",
            platform_a=platform,
            team_a=team_name,
            price_a=market_price,
            platform_b="",
            team_b="",
            price_b=0,
            size_usd=shares * market_price,
            profit_pct=0,
            edge_pct=edge * 100,
            pinnacle_prob=pin_prob,
            timing=timing,
            simulated=SIMULATION_MODE,
            would_fill=filled,
            filled_a=filled,
            filled_b=False,
            latency_ms=latency,
        )

        # Pass market_id and condition_id for settlement tracking
        self.portfolio.record_value_trade(
            trade,
            market_id=market_id,
            condition_id=match.poly_condition_id if platform == "polymarket" else "",
        )
        log_trade(
            "VALUE_BET" if filled else "VALUE_REJECTED", "VALUE",
            match_name=trade.match_name, match_id=match.match_id,
            platform_a=platform, team_a=team_name, price_a=market_price,
            edge_pct=edge * 100, pinnacle_prob=pin_prob,
            size_usd=trade.size_usd, latency_ms=latency,
            simulated=SIMULATION_MODE, would_fill=filled,
            filled_a=filled,
            extra={
                "timing": timing,
                "pinnacle_prob_at_entry": round(pin_prob, 6),
                "effective_price_vwap": round(market_price, 6),
            },
        )

        # --- SHADOW: Maker order simulation ---
        if SIMULATION_MODE and filled and platform == "polymarket":
            maker_price = market_price - MAKER_PRICE_IMPROVEMENT
            rebate = maker_price * shares * POLY_MAKER_REBATE
            maker_edge = (pin_prob / maker_price) - 1.0
            if maker_edge > 0 and random.random() < MAKER_FILL_RATE:
                # Shadow P&L uses expected value: edge * size + rebate
                ev_profit = maker_edge * shares * maker_price + rebate
                self.portfolio.maker_value_count += 1
                self.portfolio.maker_value_pnl += ev_profit
                logger.info(
                    "   [MAKER SIM] Value EV $%.2f (edge %.1f%% + rebate $%.3f)",
                    ev_profit, maker_edge * 100, rebate,
                )

        # --- SHADOW: Early exit tracking ---
        # Add a shadow copy of this position so we can check price movement later
        if SIMULATION_MODE and filled:
            self.portfolio.early_exit_positions.append({
                "market_id": market_id,
                "platform": platform,
                "entry_price": market_price,
                "shares": shares,
                "cost_usd": shares * market_price,
                "opened_at": time.time(),
                "team": team_name,
                "match_name": f"{match.teams[0]} vs {match.teams[1]}",
            })

        if self._on_trade:
            self._on_trade()
