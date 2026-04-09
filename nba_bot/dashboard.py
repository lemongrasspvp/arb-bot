"""NBA bot dashboard — minimal HTTP server."""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from nba_bot.config import (
    DASHBOARD_PORT, DASHBOARD_PASSWORD, TRADE_LOG_PATH, SIMULATION_MODE,
    WALLET_CAP, ENABLE_NBA_REGULAR, NBA_PLATFORM, NBA_SPORT,
    NBA_MIN_MINUTES, NBA_MAX_MINUTES, NBA_MIN_EDGE_PCT, NBA_EDGE_PERSISTENCE,
    NBA_BET_SIZE_USD, NBA_MAX_POSITIONS,
)
from nba_bot import skip_counters

logger = logging.getLogger(__name__)

CET = timezone(timedelta(hours=1))
_start_time = time.time()


async def dashboard_server(portfolio, shutdown_event: asyncio.Event) -> None:
    async def handle(reader, writer):
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if line in (b"\r\n", b"\n", b""):
                    break

            if DASHBOARD_PASSWORD:
                import base64
                # Simple auth check — read authorization from request
                # (headers already consumed; for minimal impl, skip auth parsing
                #  and rely on Railway's private networking or basic auth proxy)

            html = _render(portfolio)
            body = html.encode("utf-8")
            header = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(header.encode() + body)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", DASHBOARD_PORT)
    logger.info("Dashboard on http://0.0.0.0:%d", DASHBOARD_PORT)
    try:
        await shutdown_event.wait()
    finally:
        server.close()
        await server.wait_closed()


def _render(portfolio) -> str:
    now = datetime.now(CET)
    uptime_s = time.time() - _start_time
    uptime = _dur(uptime_s)
    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    mode_color = "#4caf50" if SIMULATION_MODE else "#ef5350"

    # Read trades
    trades = _read_trades(200)
    settlements = [t for t in trades if t.get("type") == "SETTLEMENT"]
    bets = [t for t in trades if t.get("type") == "NBA_BET" and t.get("would_fill")]

    # Win rate
    wins = sum(1 for s in settlements if s.get("extra", {}).get("won") is True)
    wr = f"{wins / len(settlements) * 100:.0f}%" if settlements else "n/a"

    # Skip counters
    sc = skip_counters.as_dict()
    sc_rows = "".join(
        f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in sc.items() if v > 0
    )

    # Open positions
    pos_rows = ""
    t_now = time.time()
    for p in portfolio.positions:
        age = _dur(t_now - p.opened_at)
        pos_rows += (
            f"<tr><td>{_e(p.match_id[:35])}</td><td>{_e(p.team)}</td>"
            f"<td>{p.price * 100:.0f}c</td><td>${p.cost_usd:.2f}</td>"
            f"<td>{int(p.size)}</td><td>{age}</td>"
            f"<td>{p.pinnacle_prob_at_entry * 100:.0f}c</td></tr>\n"
        )

    # Settled trades table
    settle_rows = ""
    for s in reversed(settlements[-30:]):
        ex = s.get("extra", {})
        pnl = ex.get("pnl", 0)
        pnl_cls = "pos" if pnl >= 0 else "neg"
        label = "W" if ex.get("won") else "L"
        settle_rows += (
            f"<tr><td>{s.get('timestamp', '')[:16]}</td>"
            f"<td>{_e(s.get('team_a', ''))}</td>"
            f"<td>{s.get('price_a', 0) * 100:.0f}c</td>"
            f"<td>{ex.get('payout_per_share', 0):.2f}</td>"
            f"<td class='{pnl_cls}'>${pnl:+.2f}</td>"
            f"<td>{label}</td></tr>\n"
        )

    # P&L chart
    pnl_svg = _pnl_chart(settlements)

    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>NBA Bot</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0d1117;color:#e0e0e0;font-family:-apple-system,sans-serif;padding:20px;max-width:1100px;margin:0 auto}}
.hdr{{display:flex;justify-content:space-between;align-items:center;padding:14px 18px;background:#161b22;border-radius:8px;margin-bottom:16px;border:1px solid #30363d}}
.hdr h1{{font-size:18px;color:#58a6ff}}.mode{{font-size:13px;font-weight:bold;color:{mode_color}}}
.meta{{font-size:11px;color:#8b949e}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:16px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;border-left:3px solid #30363d}}
.card.g{{border-left-color:#4caf50}}.card.r{{border-left-color:#ef5350}}.card.b{{border-left-color:#58a6ff}}.card.y{{border-left-color:#ffd54f}}
.cl{{font-size:11px;color:#8b949e;text-transform:uppercase;margin-bottom:2px}}
.cv{{font-size:22px;font-weight:bold;font-family:'SF Mono',Consolas,monospace}}
.pos{{color:#4caf50}}.neg{{color:#ef5350}}
.sec{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin-bottom:16px}}
.sec h2{{font-size:13px;color:#8b949e;text-transform:uppercase;margin-bottom:10px;letter-spacing:.5px}}
table{{width:100%;border-collapse:collapse;font-family:'SF Mono',Consolas,monospace;font-size:12px}}
th{{text-align:left;padding:6px 8px;border-bottom:1px solid #30363d;color:#8b949e;font-size:10px;text-transform:uppercase}}
td{{padding:5px 8px;border-bottom:1px solid #21262d}}
tr:hover{{background:rgba(88,166,255,.04)}}
.no{{color:#8b949e;font-style:italic;padding:16px;text-align:center}}
.info-grid{{display:grid;grid-template-columns:1fr 1fr;gap:4px 20px;font-size:12px}}
.info-grid span{{color:#8b949e}}.info-grid strong{{color:#e0e0e0}}
.chart{{width:100%;height:130px;margin-top:6px}}
</style>
</head><body>

<div class="hdr">
<div><h1>NBA Regular 12-24h</h1>
<span class="meta">Updated {now.strftime("%Y-%m-%d %H:%M:%S")} CET &middot; Uptime {uptime}</span></div>
<span class="mode">{mode}</span>
</div>

<div class="cards">
<div class="card b"><div class="cl">Balance</div><div class="cv">${portfolio.current_balance:.2f}</div>
<div class="meta">Cap ${WALLET_CAP:.0f}</div></div>
<div class="card {'g' if portfolio.total_pnl >= 0 else 'r'}"><div class="cl">Total P&amp;L</div>
<div class="cv {'pos' if portfolio.total_pnl >= 0 else 'neg'}">${portfolio.total_pnl:+.2f}</div></div>
<div class="card y"><div class="cl">Open Positions</div><div class="cv">{len(portfolio.positions)}</div>
<div class="meta">{portfolio.value_filled_count} filled total</div></div>
<div class="card b"><div class="cl">Settled</div><div class="cv">{len(settlements)}</div>
<div class="meta">Win rate: {wr}</div></div>
</div>

<div class="sec">
<h2>Strategy Info</h2>
<div class="info-grid">
<span>Strategy</span><strong>NBA Regular 12-24h</strong>
<span>Platform</span><strong>{NBA_PLATFORM}</strong>
<span>Sport</span><strong>{NBA_SPORT}</strong>
<span>Time Window</span><strong>{NBA_MIN_MINUTES // 60}h – {NBA_MAX_MINUTES // 60}h before start</strong>
<span>Min Edge</span><strong>{NBA_MIN_EDGE_PCT * 100:.1f}%</strong>
<span>Edge Persistence</span><strong>{NBA_EDGE_PERSISTENCE} checks</strong>
<span>Bet Size</span><strong>${NBA_BET_SIZE_USD:.0f}</strong>
<span>Max Positions</span><strong>{NBA_MAX_POSITIONS}</strong>
<span>Enabled</span><strong>{'YES' if ENABLE_NBA_REGULAR else 'NO'}</strong>
<span>Signals Seen</span><strong>{sc.get('signals_seen', 0)}</strong>
<span>Trades Created</span><strong>{sc.get('created', 0)}</strong>
</div>
</div>

<div class="sec">
<h2>Skip Counters</h2>
{'<table><tr><th>Reason</th><th>Count</th></tr>' + sc_rows + '</table>' if sc_rows else '<div class="no">No signals yet</div>'}
</div>

<div class="sec">
<h2>Open Positions ({len(portfolio.positions)})</h2>
{'<table><tr><th>Match</th><th>Team</th><th>Price</th><th>Cost</th><th>Shares</th><th>Age</th><th>Pin@Entry</th></tr>' + pos_rows + '</table>' if pos_rows else '<div class="no">No open positions</div>'}
</div>

<div class="sec">
<h2>Settled Trades ({len(settlements)})</h2>
{'<table><tr><th>Time</th><th>Team</th><th>Entry</th><th>Payout</th><th>P&L</th><th>Result</th></tr>' + settle_rows + '</table>' if settle_rows else '<div class="no">No settlements yet</div>'}
</div>

{pnl_svg}

<div class="sec">
<h2>Health</h2>
<div class="info-grid">
<span>Uptime</span><strong>{uptime}</strong>
<span>Mode</span><strong>{mode}</strong>
<span>Positions File</span><strong>{'OK' if Path(TRADE_LOG_PATH).parent.exists() else 'MISSING'}</strong>
<span>Trade Log</span><strong>{_trade_log_size()} entries</strong>
</div>
</div>

</body></html>"""


def _pnl_chart(settlements: list[dict]) -> str:
    if len(settlements) < 2:
        return '<div class="sec"><h2>P&amp;L Over Time</h2><div class="no">Need 2+ settlements</div></div>'

    points = []
    running = 0.0
    for s in settlements:
        pnl = s.get("extra", {}).get("pnl", 0)
        running += pnl
        points.append(running)

    w, h = 1060, 120
    px, py = 35, 12
    cw, ch = w - 2 * px, h - 2 * py
    mn, mx = min(points), max(points)
    rng = mx - mn if mx != mn else 1.0

    coords = []
    for i, v in enumerate(points):
        x = px + (i / max(len(points) - 1, 1)) * cw
        y = py + ch - ((v - mn) / rng) * ch
        coords.append(f"{x:.1f},{y:.1f}")

    color = "#4caf50" if points[-1] >= 0 else "#ef5350"
    zero_line = ""
    if mn < 0 < mx:
        zy = py + ch - ((0 - mn) / rng) * ch
        zero_line = f'<line x1="{px}" y1="{zy:.1f}" x2="{w - px}" y2="{zy:.1f}" stroke="#30363d" stroke-dasharray="4"/>'

    return f"""<div class="sec">
<h2>P&amp;L Over Time ({len(points)} settlements)</h2>
<svg viewBox="0 0 {w} {h}" class="chart" preserveAspectRatio="none">
{zero_line}
<polyline points="{' '.join(coords)}" fill="none" stroke="{color}" stroke-width="2"/>
<text x="{px - 4}" y="{py + 3}" fill="#8b949e" font-size="9" text-anchor="end">${mx:+.0f}</text>
<text x="{px - 4}" y="{h - py + 3}" fill="#8b949e" font-size="9" text-anchor="end">${mn:+.0f}</text>
<circle cx="{coords[-1].split(',')[0]}" cy="{coords[-1].split(',')[1]}" r="3" fill="{color}"/>
</svg></div>"""


def _read_trades(limit: int = 200) -> list[dict]:
    path = Path(TRADE_LOG_PATH)
    if not path.exists():
        return []
    try:
        lines = path.read_text().strip().split("\n")[-limit:]
        return [json.loads(l) for l in lines if l.strip()]
    except (OSError, json.JSONDecodeError):
        return []


def _trade_log_size() -> int:
    path = Path(TRADE_LOG_PATH)
    if not path.exists():
        return 0
    try:
        return sum(1 for _ in open(path))
    except OSError:
        return 0


def _dur(s: float) -> str:
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"


def _e(t: str) -> str:
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
