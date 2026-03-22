"""Observer signal logger — logs value signals and tracks markout P&L.

Self-contained module: no imports from engine.py to avoid circular deps.
All file writes are best-effort — failures never crash the bot.
All timestamps are UTC.
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
)

logger = logging.getLogger(__name__)

# ── Markout horizons (seconds) ──────────────────────────────────────
# "event_start" is a special sentinel handled separately.
MARKOUT_HORIZONS = [30, 120, 300, 600, 1800, "event_start"]

# In-memory pending markouts. Each entry tracks one signal forward.
_pending_markouts: list[dict] = []


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


# ── File writing helpers ─────────────────────────────────────────────

def _write_jsonl(path: str, record: dict) -> None:
    """Append one JSON line to a file. Best-effort, flushes after write."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
            f.flush()
    except Exception:
        logger.warning("Failed to write to %s", path, exc_info=True)

    if SIGNAL_LOG_STDOUT:
        try:
            print(json.dumps(record, default=str), file=sys.stdout, flush=True)
        except Exception:
            pass


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
) -> None:
    """Background task: track pending signals and capture price snapshots at horizons.

    Reads from the shared price_cache (same dict reference as engine.prices).
    Writes completed markout rows to signal_markouts.jsonl.
    """
    logger.info(
        "Markout tracker started — signals: %s, markouts: %s",
        SIGNAL_LOG_PATH, MARKOUT_LOG_PATH,
    )

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
    exit_fee = _kalshi_fee(best_bid) if platform == "kalshi" else 0.0
    profitable_exit = best_bid > 0 and (best_bid - exit_fee) > entry_price

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


def _write_markout(entry: dict) -> None:
    """Write a completed markout row to signal_markouts.jsonl."""
    ts = time.time()
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
        "signal_effective_price": round(entry["effective_price"], 6),
        "signal_pin_prob": round(entry["pin_prob"], 6),
        "intended_size_usd": round(entry["intended_size_usd"], 2),
        "horizons": entry["horizons"],
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
    }

    _write_jsonl(MARKOUT_LOG_PATH, record)
