"""Live bot dashboard — lightweight HTTP server serving a results page."""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from live_bot.config import DASHBOARD_PORT, TRADE_LOG_PATH, SIMULATION_MODE
from live_bot.feeds.pinnacle_poll import pinnacle_health

logger = logging.getLogger(__name__)

CET = timezone(timedelta(hours=1))

# Track when the bot started (set on first request or import)
_start_time = time.time()


async def dashboard_server(portfolio, shutdown_event: asyncio.Event) -> None:
    """Run a tiny HTTP server that serves the dashboard page."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # Read request line
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            # Drain headers
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if line == b"\r\n" or line == b"\n" or not line:
                    break

            html = _render_html(portfolio)
            body = html.encode("utf-8")
            header = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            )
            writer.write(header.encode("utf-8") + body)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", DASHBOARD_PORT)
    logger.info("Dashboard server listening on http://0.0.0.0:%d", DASHBOARD_PORT)

    try:
        await shutdown_event.wait()
    finally:
        server.close()
        await server.wait_closed()
        logger.info("Dashboard server stopped")


def _render_html(portfolio) -> str:
    """Generate the full dashboard HTML from current portfolio state."""
    now = datetime.now(CET)
    uptime_s = time.time() - _start_time
    uptime_str = _fmt_duration(uptime_s)
    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    mode_color = "#4caf50" if SIMULATION_MODE else "#ef5350"

    # Read recent trades from JSONL
    trades = _read_recent_trades(200)
    # Filter to actual trades (not events or settlements)
    trade_entries = [
        t for t in trades
        if t.get("strategy") in ("ARB", "VALUE")
        and t.get("type") not in ("SETTLEMENT", "ENGINE_START", "SETTLEMENT_START", "SETTLEMENT_BATCH")
    ]
    settlement_entries = [t for t in trades if t.get("type") == "SETTLEMENT"]

    # Build P&L chart data from settlements
    pnl_svg = _build_pnl_chart(settlement_entries)

    # Open positions
    positions_html = _build_positions_table(portfolio)

    # Recent trades table
    trades_html = _build_trades_table(trade_entries[-50:])

    # Strategy breakdown
    strategy_html = _build_strategy_table(portfolio)

    # Pinnacle health
    pin_status = pinnacle_health["status"]
    pin_last_ok = pinnacle_health["last_success"]
    pin_ago = _fmt_duration(time.time() - pin_last_ok) if pin_last_ok > 0 else "never"
    pin_errors = pinnacle_health["consecutive_errors"]
    pin_outcomes = pinnacle_health["last_outcome_count"]

    if pin_status == "ok":
        pin_color = "#4caf50"
        pin_icon = "OK"
        pin_detail = f"{pin_outcomes} outcomes &middot; {pin_ago} ago"
    elif pin_status == "rate_limited":
        pin_color = "#ffd54f"
        pin_icon = "RATE LIMITED"
        pin_detail = f"{pin_errors} consecutive errors"
    elif pin_status == "blocked":
        pin_color = "#ef5350"
        pin_icon = "BLOCKED"
        pin_detail = f"{pin_errors} consecutive errors &middot; last ok {pin_ago}"
    elif pin_status == "error":
        pin_color = "#ff9800"
        pin_icon = "ERROR"
        pin_detail = f"{pin_errors} errors &middot; last ok {pin_ago}"
    else:
        pin_color = "#8b949e"
        pin_icon = "STARTING"
        pin_detail = "Waiting for first poll..."

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>Arb Bot Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    background: #0d1117;
    color: #e0e0e0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 20px;
    max-width: 1200px;
    margin: 0 auto;
}}
.header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px;
    background: #161b22;
    border-radius: 8px;
    margin-bottom: 20px;
    border: 1px solid #30363d;
}}
.header h1 {{ font-size: 20px; color: #58a6ff; }}
.mode {{ font-size: 14px; font-weight: bold; color: {mode_color}; }}
.meta {{ font-size: 12px; color: #8b949e; }}
.cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
    margin-bottom: 20px;
}}
.card {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 16px;
    border-left: 3px solid #30363d;
}}
.card.green {{ border-left-color: #4caf50; }}
.card.red {{ border-left-color: #ef5350; }}
.card.blue {{ border-left-color: #58a6ff; }}
.card.yellow {{ border-left-color: #ffd54f; }}
.card-label {{ font-size: 12px; color: #8b949e; text-transform: uppercase; margin-bottom: 4px; }}
.card-value {{ font-size: 24px; font-weight: bold; font-family: 'SF Mono', Consolas, monospace; }}
.positive {{ color: #4caf50; }}
.negative {{ color: #ef5350; }}
.section {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 20px;
}}
.section h2 {{
    font-size: 14px;
    color: #8b949e;
    text-transform: uppercase;
    margin-bottom: 12px;
    letter-spacing: 0.5px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    font-family: 'SF Mono', Consolas, monospace;
    font-size: 13px;
}}
th {{
    text-align: left;
    padding: 8px 10px;
    border-bottom: 1px solid #30363d;
    color: #8b949e;
    font-size: 11px;
    text-transform: uppercase;
}}
td {{
    padding: 6px 10px;
    border-bottom: 1px solid #21262d;
}}
tr:hover {{ background: rgba(88, 166, 255, 0.04); }}
@media (max-width: 768px) {{
    .cards {{ grid-template-columns: 1fr 1fr; }}
}}
.chart-container {{
    width: 100%;
    height: 150px;
    margin-top: 8px;
}}
.no-data {{ color: #8b949e; font-style: italic; padding: 20px; text-align: center; }}
.fill-yes {{ color: #4caf50; }}
.fill-no {{ color: #ef5350; }}
</style>
</head>
<body>

<div class="header">
    <div>
        <h1>Arb Bot Dashboard</h1>
        <span class="meta">Updated {now.strftime("%Y-%m-%d %H:%M:%S")} CET &middot; Uptime {uptime_str}</span>
    </div>
    <span class="mode">{mode}</span>
</div>

<div class="cards">
    <div class="card blue">
        <div class="card-label">Balance</div>
        <div class="card-value">${portfolio.current_balance:.2f}</div>
        <div class="meta">Started ${portfolio.starting_balance:.2f}</div>
    </div>
    <div class="card {'green' if portfolio.total_pnl >= 0 else 'red'}">
        <div class="card-label">Total P&amp;L</div>
        <div class="card-value {'positive' if portfolio.total_pnl >= 0 else 'negative'}">${portfolio.total_pnl:+.2f}</div>
        <div class="meta">{portfolio.total_pnl / portfolio.starting_balance * 100:+.1f}% return</div>
    </div>
    <div class="card {'green' if portfolio.daily_pnl >= 0 else 'red'}">
        <div class="card-label">Daily P&amp;L</div>
        <div class="card-value {'positive' if portfolio.daily_pnl >= 0 else 'negative'}">${portfolio.daily_pnl:+.2f}</div>
    </div>
    <div class="card yellow">
        <div class="card-label">Open Positions</div>
        <div class="card-value">{len(portfolio.positions)}</div>
        <div class="meta">{portfolio.value_filled_count} filled trades</div>
    </div>
    <div class="card" style="border-left-color: {pin_color}">
        <div class="card-label">Pinnacle API</div>
        <div class="card-value" style="color: {pin_color}; font-size: 18px">{pin_icon}</div>
        <div class="meta">{pin_detail}</div>
    </div>
</div>

{pnl_svg}

{strategy_html}

{positions_html}

{trades_html}

{_build_48h_shadow_section(portfolio)}

</body>
</html>"""


def _build_strategy_table(portfolio) -> str:
    """Strategy breakdown section — value bets only."""
    rows = [
        ("Pregame Value", portfolio.pregame_value_count, portfolio.pregame_value_pnl),
        ("Midgame Value", portfolio.midgame_value_count, portfolio.midgame_value_pnl),
    ]
    total_count = sum(r[1] for r in rows)
    total_pnl = sum(r[2] for r in rows)

    row_html = ""
    for name, count, pnl in rows:
        pnl_class = "positive" if pnl >= 0 else "negative"
        row_html += f"<tr><td>{name}</td><td>{count}</td><td class='{pnl_class}'>${pnl:+.2f}</td></tr>\n"

    total_class = "positive" if total_pnl >= 0 else "negative"
    row_html += (
        f"<tr style='border-top:2px solid #30363d;font-weight:bold'>"
        f"<td>Total</td><td>{total_count}</td>"
        f"<td class='{total_class}'>${total_pnl:+.2f}</td></tr>\n"
    )

    return f"""<div class="section">
<h2>Strategy Breakdown</h2>
<table>
<tr><th>Strategy</th><th>Trades</th><th>P&amp;L</th></tr>
{row_html}
</table>
</div>"""



def _build_positions_table(portfolio) -> str:
    """Open positions table."""
    if not portfolio.positions:
        return """<div class="section">
<h2>Open Positions</h2>
<div class="no-data">No open positions</div>
</div>"""

    rows = ""
    now = time.time()
    for pos in portfolio.positions:
        age = _fmt_duration(now - pos.opened_at)
        rows += (
            f"<tr>"
            f"<td>{_esc(pos.match_id[:30])}</td>"
            f"<td>{_esc(pos.team)}</td>"
            f"<td>{pos.platform}</td>"
            f"<td>{pos.strategy}</td>"
            f"<td>{pos.timing}</td>"
            f"<td>{pos.price * 100:.0f}\u00a2</td>"
            f"<td>${pos.cost_usd:.2f}</td>"
            f"<td>{age}</td>"
            f"</tr>\n"
        )

    return f"""<div class="section">
<h2>Open Positions ({len(portfolio.positions)})</h2>
<table>
<tr><th>Match</th><th>Team</th><th>Platform</th><th>Strategy</th><th>Timing</th><th>Price</th><th>Cost</th><th>Age</th></tr>
{rows}
</table>
</div>"""


def _build_trades_table(trades: list[dict]) -> str:
    """Recent trades table from JSONL entries."""
    if not trades:
        return """<div class="section">
<h2>Recent Trades</h2>
<div class="no-data">No trades yet</div>
</div>"""

    rows = ""
    for t in reversed(trades):  # newest first
        ts = t.get("timestamp", "")[:19]
        strategy = t.get("strategy", "")
        trade_type = t.get("type", "")
        match = _esc(t.get("match", "")[:35])
        team_a = _esc(t.get("team_a", ""))
        platform_a = t.get("platform_a", "")
        price_a = t.get("price_a", 0)
        size = t.get("size_usd", 0)
        profit = t.get("profit_pct", 0)
        edge = t.get("edge_pct", 0)
        would_fill = t.get("would_fill", False)
        fill_class = "fill-yes" if would_fill else "fill-no"
        fill_text = "YES" if would_fill else "NO"

        metric = f"{profit:.1f}%" if strategy == "ARB" else f"{edge:.1f}% edge"

        rows += (
            f"<tr>"
            f"<td>{ts}</td>"
            f"<td>{strategy}</td>"
            f"<td>{match}</td>"
            f"<td>{team_a} @ {platform_a}</td>"
            f"<td>{price_a * 100:.0f}\u00a2</td>"
            f"<td>${size:.2f}</td>"
            f"<td>{metric}</td>"
            f"<td class='{fill_class}'>{fill_text}</td>"
            f"</tr>\n"
        )

    return f"""<div class="section">
<h2>Recent Trades (last {min(len(trades), 50)})</h2>
<table>
<tr><th>Time</th><th>Strategy</th><th>Match</th><th>Leg</th><th>Price</th><th>Size</th><th>Edge/Profit</th><th>Fill</th></tr>
{rows}
</table>
</div>"""


def _build_pnl_chart(settlements: list[dict]) -> str:
    """Build an inline SVG P&L chart from settlement entries."""
    if len(settlements) < 2:
        return """<div class="section">
<h2>P&amp;L Over Time</h2>
<div class="no-data">Need at least 2 settlements for chart</div>
</div>"""

    # Accumulate running P&L
    points = []
    running = 0.0
    for s in settlements:
        pnl = s.get("extra", {}).get("pnl", 0) if isinstance(s.get("extra"), dict) else s.get("pnl", 0)
        running += pnl
        points.append(running)

    if not points:
        return ""

    # SVG dimensions
    w, h = 1140, 130
    pad_x, pad_y = 40, 15
    chart_w = w - 2 * pad_x
    chart_h = h - 2 * pad_y

    min_pnl = min(points)
    max_pnl = max(points)
    pnl_range = max_pnl - min_pnl if max_pnl != min_pnl else 1.0

    # Build polyline
    coords = []
    for i, val in enumerate(points):
        x = pad_x + (i / max(len(points) - 1, 1)) * chart_w
        y = pad_y + chart_h - ((val - min_pnl) / pnl_range) * chart_h
        coords.append(f"{x:.1f},{y:.1f}")

    polyline = " ".join(coords)
    line_color = "#4caf50" if points[-1] >= 0 else "#ef5350"

    # Zero line
    zero_y = pad_y + chart_h - ((0 - min_pnl) / pnl_range) * chart_h
    zero_line = ""
    if min_pnl < 0 < max_pnl:
        zero_line = f'<line x1="{pad_x}" y1="{zero_y:.1f}" x2="{w - pad_x}" y2="{zero_y:.1f}" stroke="#30363d" stroke-dasharray="4"/>'

    return f"""<div class="section">
<h2>P&amp;L Over Time ({len(points)} settlements)</h2>
<svg viewBox="0 0 {w} {h}" class="chart-container" preserveAspectRatio="none">
{zero_line}
<polyline points="{polyline}" fill="none" stroke="{line_color}" stroke-width="2"/>
<text x="{pad_x - 5}" y="{pad_y + 4}" fill="#8b949e" font-size="10" text-anchor="end">${max_pnl:+.0f}</text>
<text x="{pad_x - 5}" y="{h - pad_y + 4}" fill="#8b949e" font-size="10" text-anchor="end">${min_pnl:+.0f}</text>
<circle cx="{coords[-1].split(',')[0]}" cy="{coords[-1].split(',')[1]}" r="3" fill="{line_color}"/>
</svg>
</div>"""


def _read_recent_trades(limit: int = 200) -> list[dict]:
    """Read the last N lines from the trade log JSONL file."""
    path = Path(TRADE_LOG_PATH)
    if not path.exists():
        return []

    try:
        lines = path.read_text().strip().split("\n")
        lines = lines[-limit:]
        entries = []
        for line in lines:
            if line.strip():
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return entries
    except OSError:
        return []


def _fmt_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m}m"


def _build_48h_shadow_section(portfolio) -> str:
    """Show 48h early bet research — entry edge vs 1h-before-start edge."""
    bets = getattr(portfolio, "early_48h_bets", [])
    if not bets:
        return ""

    rows = ""
    for b in bets[-20:]:  # last 20
        team = b.get("team", "?")[:25]
        sport = b.get("sport", "?")
        platform = b.get("platform", "?")
        entry_edge = b.get("edge_pct", 0)
        hours = b.get("hours_until", 0)
        pre_edge = b.get("pre_match_edge_pct")
        pre_ask = b.get("pre_match_ask")

        if pre_edge is not None:
            edge_diff = pre_edge - entry_edge
            diff_class = "positive" if edge_diff >= 0 else "negative"
            pre_col = f"<td class='{diff_class}'>{pre_edge:+.1f}%</td><td class='{diff_class}'>{edge_diff:+.1f}pp</td>"
        else:
            pre_col = "<td style='color:#8b949e'>pending</td><td>—</td>"

        rows += f"<tr><td>{team}</td><td>{sport}</td><td>{platform}</td><td>{entry_edge:.1f}%</td><td>{hours:.0f}h</td>{pre_col}</tr>\n"

    return f"""
<div class="card" style="margin-top:16px">
<h2 style="margin-bottom:12px">48h Research (edges &gt;8%, logged 24-48h before start)</h2>
<table>
<tr><th>Team</th><th>Sport</th><th>Platform</th><th>Entry Edge</th><th>Hours Out</th><th>Edge @1h</th><th>Change</th></tr>
{rows}
</table>
<p style="color:#8b949e;margin-top:8px;font-size:12px">{len(bets)} total entries &middot; "Edge @1h" = edge 1 hour before match start</p>
</div>"""


def _esc(text: str) -> str:
    """HTML-escape a string."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
