"""Observer signal logger — logs value signals and tracks markout P&L.

Self-contained module: no imports from engine.py to avoid circular deps.
All file writes are best-effort — failures never crash the bot.
All timestamps are UTC.

NOTE ON SAMPLING: Pre-start exit tracking (profitable windows, best/last
exit prices, gap closure) is sampled at the markout tracker interval
(~10 seconds). These are NOT exact tick-by-tick values — the true best
exit may occur between samples and be missed. All timing fields
(first_profitable_exit_ts, seconds_until_best_prestart_exit, etc.) have
the same ~10s resolution.
"""

import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from live_bot.config import (
    SIGNAL_LOG_PATH,
    MARKOUT_LOG_PATH,
    SIGNAL_LOG_STDOUT,
    KALSHI_FEE_RATE,
    OBSERVER_PRESTART_TRADEABLE_CUTOFF_MINUTES,
)

logger = logging.getLogger(__name__)

# ── Markout horizons (seconds) ──────────────────────────────────────
# "event_start" is a special sentinel handled separately.
MARKOUT_HORIZONS = [30, 120, 300, 600, 1800, "event_start"]

# In-memory pending markouts. Each entry tracks one signal forward.
_pending_markouts: list[dict] = []

# ── Constants ────────────────────────────────────────────────────────

# Gap closure / division-by-zero guard: if the entry-to-fair gap is
# smaller than this (in price units, i.e. 0.5¢), gap_closed_pct is
# reported as None rather than producing a huge or unstable number.
_GAP_EPSILON = 0.005

# "Early" window for converged_only_late_prestart: profitable exit
# appearing within this many seconds of signal is considered "early".
_EARLY_CONVERGENCE_SECONDS = 600  # 10 minutes


# ── VWAP helpers (self-contained, no engine.py import) ───────────────

def _compute_ask_vwap(ask_levels: list[tuple[float, float]], size_usd: float) -> float:
    """Walk the ask book (low→high) and compute VWAP for a buy of `size_usd`.

    Returns 0.0 if no book data or size <= 0.
    """
    if not ask_levels or size_usd <= 0:
        return 0.0
    sorted_asks = sorted(ask_levels, key=lambda x: x[0])
    remaining = size_usd
    total_shares = 0.0
    total_cost = 0.0
    for price, shares in sorted_asks:
        if remaining <= 0:
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
        return sorted_asks[0][0] if sorted_asks else 0.0
    return total_cost / total_shares


def _compute_bid_vwap(bid_levels: list[tuple[float, float]], size_usd: float) -> float:
    """Walk the bid book (high→low) and compute VWAP for a sell of `size_usd`.

    For markout exit calculations: what price would you get selling into the bid.
    Returns 0.0 if no book data or size <= 0.
    """
    if not bid_levels or size_usd <= 0:
        return 0.0
    sorted_bids = sorted(bid_levels, key=lambda x: x[0], reverse=True)
    remaining = size_usd
    total_shares = 0.0
    total_cost = 0.0
    for price, shares in sorted_bids:
        if remaining <= 0:
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
        return sorted_bids[0][0] if sorted_bids else 0.0
    return total_cost / total_shares


def _kalshi_fee(price: float) -> float:
    """Kalshi taker fee per contract."""
    return KALSHI_FEE_RATE * price * (1.0 - price)


def _is_profitable_exit(bid_price: float, entry_price: float, platform: str) -> bool:
    """Check if selling at `bid_price` yields a profit after fees."""
    if bid_price <= 0 or entry_price <= 0:
        return False
    exit_fee = _kalshi_fee(bid_price) if platform == "kalshi" else 0.0
    return (bid_price - exit_fee) > entry_price


def _compute_bid_depth_usd(bid_levels: list[tuple[float, float]]) -> float:
    """Total USD depth on the bid side of the order book."""
    if not bid_levels:
        return 0.0
    return sum(price * shares for price, shares in bid_levels)


def _fillable_at_bid(bid_levels: list[tuple[float, float]], target_usd: float) -> float:
    """How much of `target_usd` can actually be filled on the bid side."""
    if not bid_levels or target_usd <= 0:
        return 0.0
    available = _compute_bid_depth_usd(bid_levels)
    return min(target_usd, available)


# ── File writing helpers ─────────────────────────────────────────────

_first_write_done: set[str] = set()


def _write_jsonl(path: str, record: dict) -> None:
    """Append one JSON line to a file. Best-effort, flushes after write."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
            f.flush()
        if path not in _first_write_done:
            _first_write_done.add(path)
            logger.info("First successful write to %s (size=%d bytes)", path, p.stat().st_size)
    except Exception:
        logger.error("FAILED to write to %s", path, exc_info=True)

    if SIGNAL_LOG_STDOUT:
        try:
            print(json.dumps(record, default=str), file=sys.stdout, flush=True)
        except Exception:
            pass


# ── Commence time helper ─────────────────────────────────────────────

def _parse_commence_time(commence_time: str | None) -> datetime | None:
    """Parse a commence_time string to a timezone-aware datetime, or None."""
    if not commence_time:
        return None
    try:
        ct = commence_time
        if ct.endswith("Z"):
            ct = ct[:-1] + "+00:00"
        return datetime.fromisoformat(ct)
    except (ValueError, TypeError):
        return None


# ── Signal logging ───────────────────────────────────────────────────

def log_signal(
    *,
    match,
    team_side: str,
    team_name: str,
    platform: str,
    market_id: str,
    pin_prob: float,
    pin_margin: float,
    effective_price: float,
    best_ask: float,
    best_bid: float,
    edge: float,
    min_edge: float,
    timing: str,
    cached: dict,
    ask_levels: list,
    last_seen: float,
    now: float,
    intended_size_usd: float,
    price_cache: dict,
    match_registry=None,
) -> str | None:
    """Log a detected value signal (edge >= min_edge) to signal_events.jsonl.

    Returns signal_id (uuid4 string) for linkage, or None on failure.
    """
    try:
        signal_id = str(uuid.uuid4())
        ts = time.time()

        # Compute bid-side VWAP for exit analysis
        bid_levels = cached.get("bid_levels", [])
        bid_vwap = _compute_bid_vwap(bid_levels, intended_size_usd) if bid_levels else best_bid
        ask_vwap = _compute_ask_vwap(ask_levels, intended_size_usd) if ask_levels else best_ask

        mid_price = (best_ask + best_bid) / 2.0 if best_bid > 0 else best_ask
        spread = best_ask - best_bid if best_bid > 0 else 0.0

        # Minutes to start
        minutes_to_start = None
        if match.commence_time:
            try:
                ct = match.commence_time
                if ct.endswith("Z"):
                    ct = ct[:-1] + "+00:00"
                start = datetime.fromisoformat(ct)
                minutes_to_start = round((start - datetime.now(timezone.utc)).total_seconds() / 60, 1)
            except (ValueError, TypeError):
                pass

        # Other side Pinnacle prob
        pin_prob_other = match.pinnacle_prob_b if team_side == "a" else match.pinnacle_prob_a

        # Orderbook depth summary
        ask_depth = cached.get("ask_depth_usd", 0)
        bid_depth = cached.get("bid_depth_usd", 0)

        record = {
            "signal_id": signal_id,
            "signal_phase": "detected",
            "ts": ts,
            "ts_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "match_id": match.match_id,
            "teams": list(match.teams) if hasattr(match.teams, '__iter__') else [str(match.teams)],
            "commence_time": match.commence_time,
            "sport": getattr(match, "sport", ""),
            "timing": timing,
            "team_side": team_side,
            "team_name": team_name,
            "platform": platform,
            "market_id": market_id,
            "pin_prob": round(pin_prob, 6),
            "pin_prob_other": round(pin_prob_other, 6),
            "pin_implied_a": round(getattr(match, "pinnacle_implied_a", 0), 6),
            "pin_implied_b": round(getattr(match, "pinnacle_implied_b", 0), 6),
            "pin_margin": round(pin_margin, 6),
            "best_ask": round(best_ask, 6),
            "best_bid": round(best_bid, 6),
            "mid_price": round(mid_price, 6),
            "spread": round(spread, 6),
            "ask_vwap_at_target_size": round(ask_vwap, 6),
            "bid_vwap_at_target_size": round(bid_vwap, 6),
            "intended_bet_size_usd": round(intended_size_usd, 2),
            "effective_price": round(effective_price, 6),
            "edge": round(edge, 6),
            "min_edge_required": round(min_edge, 6),
            "passed_persistence": False,
            "minutes_to_start": minutes_to_start,
            "is_live": timing == "midgame",
            "pin_frozen": bool(
                match.pinnacle_frozen_a if team_side == "a" else match.pinnacle_frozen_b
            ),
            "pin_moving": bool(
                match.pinnacle_moving_a if team_side == "a" else match.pinnacle_moving_b
            ),
            "pin_last_update_age_s": round(now - last_seen, 1) if last_seen > 0 else None,
            "mkt_last_update_age_s": round(now - cached.get("timestamp", now), 1),
            "ask_depth_usd": round(ask_depth, 2),
            "bid_depth_usd": round(bid_depth, 2),
            "ask_levels_count": len(ask_levels),
            "bid_levels_count": len(bid_levels),
            "confidence_tier": getattr(match, "confidence_tier", ""),
            "seeded": bool(cached.get("seeded", False)),
            "persistence_count": 0,
        }

        _write_jsonl(SIGNAL_LOG_PATH, record)

        # Register for markout tracking
        _pending_markouts.append({
            "signal_id": signal_id,
            "signal_ts": ts,
            "platform": platform,
            "market_id": market_id,
            "match_id": match.match_id,
            "commence_time": match.commence_time,
            "effective_price": effective_price,
            "pin_prob": pin_prob,
            "intended_size_usd": intended_size_usd,
            "team_side": team_side,
            "team_name": team_name,
            # Horizon snapshots: horizon_label -> snapshot dict
            "horizons": {},
            # MFE/MAE tracking (based on executable exit = bid-side)
            "mfe_pct": 0.0,       # max favorable excursion as % of entry
            "mae_pct": 0.0,       # max adverse excursion as % of entry
            "mfe_ts": None,
            "mae_ts": None,
            "mfe_bid": effective_price,  # best bid seen (initialize to entry)
            "mae_bid": effective_price,  # worst bid seen
            # ── Pre-start exit tracking state ──
            "prestart": {
                # Best executable exit seen before start
                "best_bid": None,
                "best_bid_vwap": None,
                "best_mid": None,
                "best_bid_ts": None,
                "best_bid_used_vwap": False,      # did the best exit use VWAP or fallback best_bid?
                "best_bid_depth_usd": None,
                "best_bid_levels_count": None,
                "best_bid_size_fillable_usd": None,
                "best_pnl_at_bid_pct": None,      # PnL if sold at best_bid
                "best_pnl_at_bid_vwap_pct": None,  # PnL if sold at best_bid_vwap
                # Last snapshot before start
                "last_bid": None,
                "last_bid_vwap": None,
                "last_mid": None,
                "last_ts": None,
                "last_pnl_at_bid_pct": None,
                "last_pnl_at_bid_vwap_pct": None,
                # MFE/MAE before start (bid-side + bid VWAP)
                "mfe_bid": None,
                "mae_bid": None,
                "mfe_bid_vwap": None,
                "mae_bid_vwap": None,
                # Profitable exit tracking
                "first_profitable_ts": None,
                "profitable_window_count": 0,
                "profitable_window_total_s": 0.0,
                "profitable_at_last_prestart": False,
                "last_profitable_exit_price": None,
                # Gap convergence
                "entry_to_fair_gap": None,   # pin_prob - entry_price at signal time
                "max_gap_closed_pct": None,
                # Pinnacle reference drift (updated each tick)
                "pin_prob_at_first_profitable": None,
                "pin_prob_at_best_exit": None,
                "last_pin_prob_seen": pin_prob,
                # Whether match started (to finalize pre-start section)
                "match_started": False,
            },
        })

        logger.debug("Signal logged: %s %s %s edge=%.1f%%", signal_id[:8], team_name, platform, edge * 100)
        return signal_id

    except Exception:
        logger.warning("Failed to log signal", exc_info=True)
        return None


def log_signal_persisted(
    *,
    signal_id: str | None,
    match,
    team_name: str,
    platform: str,
    market_id: str,
    edge: float,
    pin_prob: float,
    cached: dict,
    kelly_size_usd: float = 0.0,
    depth_capped_size_usd: float = 0.0,
    risk_passed: bool = True,
) -> None:
    """Log a persistence-confirmed signal to signal_events.jsonl.

    Uses the same signal_id as the detection row so they can be linked.
    """
    try:
        ts = time.time()
        best_ask = cached.get("best_ask", 0)
        best_bid = cached.get("best_bid", 0)

        record = {
            "signal_id": signal_id,
            "signal_phase": "persisted",
            "ts": ts,
            "ts_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "match_id": match.match_id,
            "team_name": team_name,
            "platform": platform,
            "market_id": market_id,
            "edge": round(edge, 6),
            "pin_prob": round(pin_prob, 6),
            "best_ask": round(best_ask, 6),
            "best_bid": round(best_bid, 6),
            "passed_persistence": True,
            "kelly_size_usd": round(kelly_size_usd, 2),
            "depth_capped_size_usd": round(depth_capped_size_usd, 2),
            "risk_passed": risk_passed,
        }

        _write_jsonl(SIGNAL_LOG_PATH, record)
        logger.debug("Signal persisted: %s %s %s", (signal_id or "?")[:8], team_name, platform)

    except Exception:
        logger.warning("Failed to log persisted signal", exc_info=True)


# ── Markout tracker (async background task) ──────────────────────────

async def markout_tracker(
    price_cache: dict,
    shutdown_event: asyncio.Event,
    match_registry=None,
) -> None:
    """Background task: track pending signals and capture price snapshots at horizons.

    Reads from the shared price_cache (same dict reference as engine.prices).
    Optionally reads from match_registry to get current Pinnacle probs for drift.
    Writes completed markout rows to signal_markouts.jsonl.
    """
    # Verify paths are writable at startup
    for label, fpath in [("signals", SIGNAL_LOG_PATH), ("markouts", MARKOUT_LOG_PATH)]:
        p = Path(fpath)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            # Test write
            with open(p, "a") as f:
                f.flush()
            logger.info("Markout tracker: %s path OK → %s (exists=%s, dir=%s)",
                        label, fpath, p.exists(), p.parent.exists())
        except Exception:
            logger.error("Markout tracker: %s path FAILED → %s", label, fpath, exc_info=True)

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
            break  # shutdown
        except asyncio.TimeoutError:
            pass  # normal tick

        now = time.time()
        completed = []

        for i, entry in enumerate(_pending_markouts):
            try:
                elapsed = now - entry["signal_ts"]
                platform = entry["platform"]
                market_id = entry["market_id"]
                entry_price = entry["effective_price"]

                # Get current price data
                mkt = price_cache.get(platform, {}).get(market_id, {})
                if not mkt:
                    # Market gone from cache — if old enough, finalize
                    if elapsed > 7200:
                        completed.append(i)
                    continue

                current_bid = mkt.get("best_bid", 0)
                current_ask = mkt.get("best_ask", 0)
                bid_levels = mkt.get("bid_levels", [])
                ask_levels = mkt.get("ask_levels", [])

                target_size = entry["intended_size_usd"]

                # Update MFE/MAE based on executable exit (bid-side)
                exit_price = current_bid
                if bid_levels and target_size > 0:
                    bid_vwap = _compute_bid_vwap(bid_levels, target_size)
                    if bid_vwap > 0:
                        exit_price = bid_vwap

                if exit_price > 0 and entry_price > 0:
                    excursion_pct = (exit_price - entry_price) / entry_price

                    if excursion_pct > entry["mfe_pct"]:
                        entry["mfe_pct"] = excursion_pct
                        entry["mfe_ts"] = now
                        entry["mfe_bid"] = exit_price

                    if excursion_pct < -entry["mae_pct"]:
                        # mae_pct stored as positive magnitude
                        entry["mae_pct"] = abs(excursion_pct)
                        entry["mae_ts"] = now
                        entry["mae_bid"] = exit_price

                # ── Pre-start exit tracking ──────────────────────────
                ps = entry["prestart"]
                if not ps["match_started"]:
                    _update_prestart(
                        ps, entry, mkt, platform, now,
                        current_bid, current_ask, bid_levels,
                        entry_price, target_size, match_registry,
                    )

                # Check each numeric horizon
                for h in MARKOUT_HORIZONS:
                    if h == "event_start":
                        continue  # handled below
                    h_label = f"{h}s"
                    if h_label in entry["horizons"]:
                        continue  # already captured
                    if elapsed >= h:
                        # First snapshot at or after this horizon
                        entry["horizons"][h_label] = _capture_snapshot(
                            mkt, entry_price, target_size, platform, now,
                        )

                # Event start horizon
                if "event_start" not in entry["horizons"] and entry.get("commence_time"):
                    try:
                        ct = entry["commence_time"]
                        if ct.endswith("Z"):
                            ct = ct[:-1] + "+00:00"
                        start_dt = datetime.fromisoformat(ct)
                        if datetime.now(timezone.utc) >= start_dt:
                            entry["horizons"]["event_start"] = _capture_snapshot(
                                mkt, entry_price, target_size, platform, now,
                            )
                    except (ValueError, TypeError):
                        pass

                # Check if all horizons captured
                expected = {f"{h}s" for h in MARKOUT_HORIZONS if h != "event_start"}
                expected.add("event_start")
                all_done = expected.issubset(entry["horizons"].keys())

                # Also finalize if signal is >2h old
                if all_done or elapsed > 7200:
                    _write_markout(entry)
                    completed.append(i)

            except Exception:
                logger.warning("Markout tracker error for entry %d", i, exc_info=True)
                # If entry is very old, drop it to avoid memory leak
                if now - entry.get("signal_ts", now) > 14400:
                    completed.append(i)

        # Remove completed entries (reverse order to preserve indices)
        for i in sorted(completed, reverse=True):
            try:
                _pending_markouts.pop(i)
            except IndexError:
                pass

    # On shutdown, flush remaining entries
    for entry in _pending_markouts:
        try:
            _write_markout(entry)
        except Exception:
            pass
    _pending_markouts.clear()
    logger.info("Markout tracker stopped — flushed remaining entries")


# ── Pre-start exit update (called each tick while match is pregame) ───

def _update_prestart(
    ps: dict,
    entry: dict,
    mkt: dict,
    platform: str,
    now: float,
    current_bid: float,
    current_ask: float,
    bid_levels: list,
    entry_price: float,
    target_size: float,
    match_registry,
) -> None:
    """Update running pre-start exit state for one pending signal.

    Called every ~10s while the match has not yet started.
    """
    # Check if match has started
    start_dt = _parse_commence_time(entry.get("commence_time"))
    if start_dt and datetime.now(timezone.utc) >= start_dt:
        ps["match_started"] = True
        # Capture final pre-start snapshot
        _prestart_snapshot(ps, entry, mkt, platform, now, current_bid,
                           current_ask, bid_levels, entry_price, target_size,
                           match_registry, is_final=True)
        return

    _prestart_snapshot(ps, entry, mkt, platform, now, current_bid,
                       current_ask, bid_levels, entry_price, target_size,
                       match_registry, is_final=False)


def _prestart_snapshot(
    ps: dict,
    entry: dict,
    mkt: dict,
    platform: str,
    now: float,
    current_bid: float,
    current_ask: float,
    bid_levels: list,
    entry_price: float,
    target_size: float,
    match_registry,
    *,
    is_final: bool,
) -> None:
    """Capture one pre-start snapshot and update running best/last/MFE/MAE."""
    if current_bid <= 0 and current_ask <= 0:
        return  # no price data this tick

    # Compute executable exit price (prefer VWAP when depth available)
    bid_vwap = 0.0
    used_vwap = False
    if bid_levels and target_size > 0:
        bid_vwap = _compute_bid_vwap(bid_levels, target_size)
        if bid_vwap > 0:
            used_vwap = True
    if not used_vwap:
        bid_vwap = current_bid  # fallback

    mid = (current_ask + current_bid) / 2.0 if current_bid > 0 and current_ask > 0 else (current_ask or current_bid)

    # ── Pinnacle reference drift ──
    current_pin_prob = entry["pin_prob"]  # default to signal-time
    if match_registry is not None:
        try:
            match_obj = match_registry.matches.get(entry["match_id"])
            if match_obj:
                if entry["team_side"] == "a":
                    p = match_obj.pinnacle_prob_a
                else:
                    p = match_obj.pinnacle_prob_b
                if p > 0:
                    current_pin_prob = p
        except Exception:
            pass
    ps["last_pin_prob_seen"] = current_pin_prob

    # ── PnL at this tick ──
    pnl_at_bid = None
    pnl_at_bid_vwap = None
    if entry_price > 0:
        if current_bid > 0:
            pnl_at_bid = (current_bid - entry_price) / entry_price
        if bid_vwap > 0:
            pnl_at_bid_vwap = (bid_vwap - entry_price) / entry_price

    # ── Profitable exit check ──
    # Use VWAP price if available, otherwise best bid
    exit_check_price = bid_vwap if used_vwap else current_bid
    is_profitable = _is_profitable_exit(exit_check_price, entry_price, platform)

    # Profitable window tracking (~10s per tick)
    if is_profitable:
        ps["profitable_window_count"] += 1
        ps["profitable_window_total_s"] += 10.0  # approximate: tracker interval
        ps["last_profitable_exit_price"] = exit_check_price
        if ps["first_profitable_ts"] is None:
            ps["first_profitable_ts"] = now
            ps["pin_prob_at_first_profitable"] = current_pin_prob

    if is_final:
        ps["profitable_at_last_prestart"] = is_profitable

    # ── Last pre-start snapshot (overwrite each tick) ──
    ps["last_bid"] = current_bid
    ps["last_bid_vwap"] = bid_vwap
    ps["last_mid"] = mid
    ps["last_ts"] = now
    ps["last_pnl_at_bid_pct"] = pnl_at_bid
    ps["last_pnl_at_bid_vwap_pct"] = pnl_at_bid_vwap

    # ── Best pre-start exit (track highest executable bid) ──
    # "Best" = highest executable exit price seen (VWAP when available, else bid)
    best_candidate = bid_vwap if used_vwap else current_bid
    current_best = ps["best_bid_vwap"] if ps.get("best_bid_used_vwap") else ps["best_bid"]

    if best_candidate > 0 and (current_best is None or best_candidate > current_best):
        ps["best_bid"] = current_bid
        ps["best_bid_vwap"] = bid_vwap
        ps["best_mid"] = mid
        ps["best_bid_ts"] = now
        ps["best_bid_used_vwap"] = used_vwap
        ps["best_bid_depth_usd"] = _compute_bid_depth_usd(bid_levels) if bid_levels else 0.0
        ps["best_bid_levels_count"] = len(bid_levels)
        ps["best_bid_size_fillable_usd"] = _fillable_at_bid(bid_levels, target_size)
        if entry_price > 0:
            ps["best_pnl_at_bid_pct"] = (current_bid - entry_price) / entry_price if current_bid > 0 else None
            ps["best_pnl_at_bid_vwap_pct"] = (bid_vwap - entry_price) / entry_price if bid_vwap > 0 else None
        ps["pin_prob_at_best_exit"] = current_pin_prob

    # ── MFE/MAE before start (bid and VWAP separately) ──
    if current_bid > 0:
        if ps["mfe_bid"] is None or current_bid > ps["mfe_bid"]:
            ps["mfe_bid"] = current_bid
        if ps["mae_bid"] is None or current_bid < ps["mae_bid"]:
            ps["mae_bid"] = current_bid

    if bid_vwap > 0:
        if ps["mfe_bid_vwap"] is None or bid_vwap > ps["mfe_bid_vwap"]:
            ps["mfe_bid_vwap"] = bid_vwap
        if ps["mae_bid_vwap"] is None or bid_vwap < ps["mae_bid_vwap"]:
            ps["mae_bid_vwap"] = bid_vwap

    # ── Gap convergence ──
    signal_pin_prob = entry["pin_prob"]
    gap = signal_pin_prob - entry_price  # positive = we bought below fair
    if ps["entry_to_fair_gap"] is None:
        ps["entry_to_fair_gap"] = gap

    # Gap closed = how much of the original gap the bid has recovered
    if abs(gap) >= _GAP_EPSILON and current_bid > 0:
        gap_closed = (current_bid - entry_price) / gap
        gap_closed_pct = max(0.0, gap_closed)  # clamp at 0 (negative = moved away)
        if ps["max_gap_closed_pct"] is None or gap_closed_pct > ps["max_gap_closed_pct"]:
            ps["max_gap_closed_pct"] = gap_closed_pct


def _capture_snapshot(
    mkt: dict,
    entry_price: float,
    target_size: float,
    platform: str,
    now: float,
) -> dict:
    """Capture a price snapshot for one markout horizon."""
    best_bid = mkt.get("best_bid", 0)
    best_ask = mkt.get("best_ask", 0)
    mid = (best_ask + best_bid) / 2.0 if best_bid > 0 else best_ask

    bid_levels = mkt.get("bid_levels", [])
    ask_levels = mkt.get("ask_levels", [])

    bid_vwap = _compute_bid_vwap(bid_levels, target_size) if bid_levels else best_bid
    ask_vwap = _compute_ask_vwap(ask_levels, target_size) if ask_levels else best_ask

    # PnL calculations (selling to exit a long position)
    pnl_at_bid = (best_bid - entry_price) / entry_price if entry_price > 0 and best_bid > 0 else None
    pnl_at_mid = (mid - entry_price) / entry_price if entry_price > 0 and mid > 0 else None
    pnl_at_vwap = (bid_vwap - entry_price) / entry_price if entry_price > 0 and bid_vwap > 0 else None

    # Exit feasibility: can you sell at bid for profit after fees?
    profitable_exit = _is_profitable_exit(best_bid, entry_price, platform)

    return {
        "ts": now,
        "ts_utc": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "best_bid": round(best_bid, 6),
        "best_ask": round(best_ask, 6),
        "mid_price": round(mid, 6),
        "spread": round(best_ask - best_bid, 6) if best_bid > 0 else None,
        "bid_vwap_at_target_size": round(bid_vwap, 6) if bid_vwap else None,
        "ask_vwap_at_target_size": round(ask_vwap, 6) if ask_vwap else None,
        "ask_depth_usd": round(mkt.get("ask_depth_usd", 0), 2),
        "bid_depth_usd": round(mkt.get("bid_depth_usd", 0), 2),
        "pnl_at_bid_pct": round(pnl_at_bid, 6) if pnl_at_bid is not None else None,
        "pnl_at_mid_pct": round(pnl_at_mid, 6) if pnl_at_mid is not None else None,
        "pnl_at_vwap_pct": round(pnl_at_vwap, 6) if pnl_at_vwap is not None else None,
        "profitable_exit_at_bid": profitable_exit,
    }


# ── Markout output ───────────────────────────────────────────────────

def _write_markout(entry: dict) -> None:
    """Write a completed markout row to signal_markouts.jsonl."""
    ts = time.time()
    ps = entry.get("prestart", {})
    entry_price = entry["effective_price"]
    signal_pin_prob = entry["pin_prob"]

    # ── Derive pre-start summary fields ──

    # Gap closure at best bid and last bid
    gap = ps.get("entry_to_fair_gap")
    gap_closed_at_best = None
    gap_closed_at_last = None
    if gap is not None and abs(gap) >= _GAP_EPSILON:
        best_bid = ps.get("best_bid")
        if best_bid is not None and best_bid > 0 and entry_price > 0:
            gap_closed_at_best = max(0.0, (best_bid - entry_price) / gap)
        last_bid = ps.get("last_bid")
        if last_bid is not None and last_bid > 0 and entry_price > 0:
            gap_closed_at_last = max(0.0, (last_bid - entry_price) / gap)

    # MFE/MAE as percentages relative to entry
    mfe_before_start_bid = None
    mae_before_start_bid = None
    mfe_before_start_bid_vwap = None
    mae_before_start_bid_vwap = None
    if entry_price > 0:
        if ps.get("mfe_bid") is not None:
            mfe_before_start_bid = (ps["mfe_bid"] - entry_price) / entry_price
        if ps.get("mae_bid") is not None:
            mae_before_start_bid = (ps["mae_bid"] - entry_price) / entry_price
        if ps.get("mfe_bid_vwap") is not None:
            mfe_before_start_bid_vwap = (ps["mfe_bid_vwap"] - entry_price) / entry_price
        if ps.get("mae_bid_vwap") is not None:
            mae_before_start_bid_vwap = (ps["mae_bid_vwap"] - entry_price) / entry_price

    # Profitable exit before start?
    profitable_exit_before_start = ps.get("first_profitable_ts") is not None

    # Seconds from signal to first profitable exit
    seconds_until_first_profitable = None
    if ps.get("first_profitable_ts") is not None:
        seconds_until_first_profitable = round(ps["first_profitable_ts"] - entry["signal_ts"], 1)

    # Seconds from signal to best pre-start exit
    seconds_until_best_prestart_exit = None
    if ps.get("best_bid_ts") is not None:
        seconds_until_best_prestart_exit = round(ps["best_bid_ts"] - entry["signal_ts"], 1)

    # Minutes to start at various points
    minutes_to_start_at_signal = None
    minutes_to_start_at_first_profitable = None
    minutes_to_start_at_best_exit = None
    start_dt = _parse_commence_time(entry.get("commence_time"))
    if start_dt:
        signal_dt = datetime.fromtimestamp(entry["signal_ts"], tz=timezone.utc)
        minutes_to_start_at_signal = round((start_dt - signal_dt).total_seconds() / 60, 1)

        if ps.get("first_profitable_ts") is not None:
            fp_dt = datetime.fromtimestamp(ps["first_profitable_ts"], tz=timezone.utc)
            minutes_to_start_at_first_profitable = round((start_dt - fp_dt).total_seconds() / 60, 1)

        if ps.get("best_bid_ts") is not None:
            be_dt = datetime.fromtimestamp(ps["best_bid_ts"], tz=timezone.utc)
            minutes_to_start_at_best_exit = round((start_dt - be_dt).total_seconds() / 60, 1)

    # converged_only_late_prestart:
    #   true  = no profitable exit in first 10 min after signal, but one appeared later before start
    #   false = profitable exit appeared early, or never appeared at all
    converged_only_late_prestart = False
    if profitable_exit_before_start and seconds_until_first_profitable is not None:
        converged_only_late_prestart = seconds_until_first_profitable > _EARLY_CONVERGENCE_SECONDS

    # ── Reference drift ──
    pin_prob_at_signal = signal_pin_prob
    pin_prob_at_event_start = ps.get("last_pin_prob_seen", signal_pin_prob)
    reference_move_pct = None
    market_move_minus_ref_pct = None
    if signal_pin_prob > 0:
        reference_move_pct = (pin_prob_at_event_start - signal_pin_prob) / signal_pin_prob
        # Market move = how much the bid moved relative to entry
        last_bid = ps.get("last_bid")
        if last_bid is not None and last_bid > 0 and entry_price > 0:
            market_move_pct = (last_bid - entry_price) / entry_price
            market_move_minus_ref_pct = market_move_pct - reference_move_pct

    # ── would_have_been_tradeable_before_start label ──
    # True only if ALL of:
    #   1. A profitable executable exit appeared before start
    #   2. Sufficient depth for the intended size at that exit
    #   3. The profitable exit was not ultra-late (before cutoff)
    cutoff_s = OBSERVER_PRESTART_TRADEABLE_CUTOFF_MINUTES * 60
    would_have_been_tradeable = False
    if profitable_exit_before_start and start_dt:
        # Check depth at best exit
        fillable = ps.get("best_bid_size_fillable_usd") or 0
        target = entry["intended_size_usd"]
        has_depth = fillable >= target * 0.5  # at least 50% of intended size fillable

        # Check timing: was ANY profitable exit before the cutoff?
        # Use the last profitable exit timestamp for timing check
        last_profitable_ts = ps.get("first_profitable_ts")  # at minimum, first profitable
        if ps.get("profitable_at_last_prestart"):
            # Still profitable at the final pre-start tick
            last_profitable_ts = ps.get("last_ts") or last_profitable_ts

        not_too_late = False
        if last_profitable_ts and start_dt:
            lp_dt = datetime.fromtimestamp(last_profitable_ts, tz=timezone.utc)
            seconds_before_start = (start_dt - lp_dt).total_seconds()
            not_too_late = seconds_before_start >= cutoff_s

        would_have_been_tradeable = has_depth and not_too_late

    record = {
        "signal_id": entry["signal_id"],
        "completed_ts": ts,
        "completed_ts_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "match_id": entry["match_id"],
        "platform": entry["platform"],
        "market_id": entry["market_id"],
        "team_side": entry["team_side"],
        "team_name": entry["team_name"],
        "signal_ts": entry["signal_ts"],
        "signal_ts_utc": datetime.fromtimestamp(entry["signal_ts"], tz=timezone.utc).isoformat(),
        "signal_effective_price": round(entry_price, 6),
        "signal_pin_prob": round(signal_pin_prob, 6),
        "intended_size_usd": round(entry["intended_size_usd"], 2),
        "horizons": entry["horizons"],
        # ── Existing MFE/MAE (overall) ──
        "max_favorable_excursion_pct": round(entry["mfe_pct"], 6),
        "max_adverse_excursion_pct": round(entry["mae_pct"], 6),
        "mfe_ts": entry["mfe_ts"],
        "mfe_ts_utc": datetime.fromtimestamp(entry["mfe_ts"], tz=timezone.utc).isoformat() if entry["mfe_ts"] else None,
        "mae_ts": entry["mae_ts"],
        "mae_ts_utc": datetime.fromtimestamp(entry["mae_ts"], tz=timezone.utc).isoformat() if entry["mae_ts"] else None,
        "mfe_exit_price": round(entry.get("mfe_bid", 0), 6),
        "mae_exit_price": round(entry.get("mae_bid", 0), 6),
        "horizons_captured": list(entry["horizons"].keys()),
        "tracking_duration_s": round(ts - entry["signal_ts"], 1),
        # ══════════════════════════════════════════════════════════
        # ── NEW: Pre-start exit summary ──
        # ══════════════════════════════════════════════════════════
        # Core pre-start exit prices
        "best_bid_before_start": _r(ps.get("best_bid")),
        "best_bid_vwap_before_start": _r(ps.get("best_bid_vwap")),
        "best_mid_before_start": _r(ps.get("best_mid")),
        "last_bid_before_start": _r(ps.get("last_bid")),
        "last_bid_vwap_before_start": _r(ps.get("last_bid_vwap")),
        "last_mid_before_start": _r(ps.get("last_mid")),
        "timestamp_of_best_bid_before_start": ps.get("best_bid_ts"),
        "timestamp_of_best_bid_before_start_utc": (
            datetime.fromtimestamp(ps["best_bid_ts"], tz=timezone.utc).isoformat()
            if ps.get("best_bid_ts") else None
        ),
        "timestamp_of_last_prestart_snapshot": ps.get("last_ts"),
        "timestamp_of_last_prestart_snapshot_utc": (
            datetime.fromtimestamp(ps["last_ts"], tz=timezone.utc).isoformat()
            if ps.get("last_ts") else None
        ),
        # Profitability / timing
        "profitable_exit_before_start": profitable_exit_before_start,
        "first_profitable_exit_ts": ps.get("first_profitable_ts"),
        "first_profitable_exit_ts_utc": (
            datetime.fromtimestamp(ps["first_profitable_ts"], tz=timezone.utc).isoformat()
            if ps.get("first_profitable_ts") else None
        ),
        "seconds_until_first_profitable_exit": seconds_until_first_profitable,
        "seconds_until_best_prestart_exit": seconds_until_best_prestart_exit,
        "profitable_exit_window_count": ps.get("profitable_window_count", 0),
        "profitable_exit_window_total_seconds": round(ps.get("profitable_window_total_s", 0), 1),
        "profitable_exit_still_open_at_start": ps.get("profitable_at_last_prestart", False),
        # Gap / convergence
        "entry_to_fair_gap_at_signal": _r(gap),
        "max_gap_closed_before_start_pct": _r(ps.get("max_gap_closed_pct")),
        "gap_closed_at_best_bid_pct": _r(gap_closed_at_best),
        "gap_closed_at_last_prestart_bid_pct": _r(gap_closed_at_last),
        # Excursion / path (before start)
        "max_favorable_excursion_before_start_bid": _r(mfe_before_start_bid),
        "max_adverse_excursion_before_start_bid": _r(mae_before_start_bid),
        "max_favorable_excursion_before_start_bid_vwap": _r(mfe_before_start_bid_vwap),
        "max_adverse_excursion_before_start_bid_vwap": _r(mae_before_start_bid_vwap),
        # Exit PnL summary
        "best_pnl_at_bid_before_start_pct": _r(ps.get("best_pnl_at_bid_pct")),
        "best_pnl_at_bid_vwap_before_start_pct": _r(ps.get("best_pnl_at_bid_vwap_pct")),
        "last_pnl_at_bid_before_start_pct": _r(ps.get("last_pnl_at_bid_pct")),
        "last_pnl_at_bid_vwap_before_start_pct": _r(ps.get("last_pnl_at_bid_vwap_pct")),
        # ── Reference / Pinnacle drift ──
        "pin_prob_at_signal": _r(pin_prob_at_signal),
        "pin_prob_at_first_profitable_exit": _r(ps.get("pin_prob_at_first_profitable")),
        "pin_prob_at_best_prestart_exit": _r(ps.get("pin_prob_at_best_exit")),
        "pin_prob_at_event_start": _r(pin_prob_at_event_start),
        "reference_move_from_signal_pct": _r(reference_move_pct),
        "market_move_minus_reference_move_pct": _r(market_move_minus_ref_pct),
        # ── Order book / executability at best exit ──
        "best_exit_bid_depth_usd": _r(ps.get("best_bid_depth_usd")),
        "best_exit_bid_levels_count": ps.get("best_bid_levels_count"),
        "best_exit_size_fillable_usd": _r(ps.get("best_bid_size_fillable_usd")),
        "best_exit_used_vwap": ps.get("best_bid_used_vwap", False),
        # ── Time-to-start / late convergence ──
        "minutes_to_start_at_signal": minutes_to_start_at_signal,
        "minutes_to_start_at_first_profitable_exit": minutes_to_start_at_first_profitable,
        "minutes_to_start_at_best_exit": minutes_to_start_at_best_exit,
        "converged_only_late_prestart": converged_only_late_prestart,
        # ── Future model label ──
        "would_have_been_tradeable_before_start": would_have_been_tradeable,
    }

    _write_jsonl(MARKOUT_LOG_PATH, record)


def _r(val, digits: int = 6):
    """Round a value if it's a float, pass through None."""
    if val is None:
        return None
    if isinstance(val, float):
        return round(val, digits)
    return val
