"""Live bot dashboard — lightweight HTTP server serving a results page."""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from live_bot.config import DASHBOARD_PORT, TRADE_LOG_PATH, SIMULATION_MODE, WALLET_CAP
from live_bot.feeds.pinnacle_poll import pinnacle_health

logger = logging.getLogger(__name__)

CET = timezone(timedelta(hours=1))

# Track when the bot started (set on first request or import)
_start_time = time.time()


DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")


async def dashboard_server(portfolio, shutdown_event: asyncio.Event) -> None:
    """Run a tiny HTTP server that serves the dashboard page."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # Read request line
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            # Read headers
            headers_raw = {}
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if line == b"\r\n" or line == b"\n" or not line:
                    break
                decoded = line.decode("utf-8", errors="ignore").strip()
                if ":" in decoded:
                    k, v = decoded.split(":", 1)
                    headers_raw[k.strip().lower()] = v.strip()

            # Basic auth check (if DASHBOARD_PASSWORD is set)
            if DASHBOARD_PASSWORD:
                import base64
                auth = headers_raw.get("authorization", "")
                authorized = False
                if auth.startswith("Basic "):
                    try:
                        decoded_auth = base64.b64decode(auth[6:]).decode("utf-8")
                        # Accept any username, just check password
                        if ":" in decoded_auth:
                            _, pwd = decoded_auth.split(":", 1)
                            authorized = (pwd == DASHBOARD_PASSWORD)
                    except Exception:
                        pass
                if not authorized:
                    resp = (
                        "HTTP/1.1 401 Unauthorized\r\n"
                        'WWW-Authenticate: Basic realm="Dashboard"\r\n'
                        "Content-Length: 0\r\n"
                        "Connection: close\r\n"
                        "\r\n"
                    )
                    writer.write(resp.encode("utf-8"))
                    await writer.drain()
                    return

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

    # CLV stats from settlements
    # Trade CLV: (pinnacle_close - market_entry) / market_entry
    # Measures whether we bought cheaper than the sharp closing line.
    # Pinnacle drift: (pinnacle_close - pinnacle_entry) / pinnacle_entry
    # Measures whether the sharp line moved in our favor (signal quality).
    clv_values = []
    drift_values = []
    for s in settlement_entries:
        extra = s.get("extra", s)
        # Trade CLV
        pct = extra.get("clv_pct", 0)
        if pct and pct != 0:
            clv_values.append(pct)
        else:
            # Retroactively compute from pinnacle_close and market entry price
            pin_close = extra.get("pinnacle_prob_at_close", 0)
            entry_price = s.get("price_a", 0)
            if pin_close > 0 and entry_price > 0:
                clv_values.append((pin_close - entry_price) / entry_price)
        # Pinnacle drift
        drift_pct = extra.get("pin_drift_pct", 0)
        if drift_pct and drift_pct != 0:
            drift_values.append(drift_pct)
        else:
            pin_entry = extra.get("pinnacle_prob_at_entry", 0)
            pin_close = extra.get("pinnacle_prob_at_close", 0)
            if pin_entry > 0 and pin_close > 0:
                drift_values.append((pin_close - pin_entry) / pin_entry)
    avg_clv = sum(clv_values) / len(clv_values) if clv_values else 0.0
    avg_drift = sum(drift_values) / len(drift_values) if drift_values else 0.0
    clv_count = len(clv_values)

    # Shadow exit summary stats
    shadow_stats = _build_shadow_summary(settlement_entries)

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
        <div class="meta">Portfolio ${portfolio.total_portfolio_value:.0f} / Cap ${WALLET_CAP:.0f}</div>
    </div>
    <div class="card {'green' if portfolio.total_pnl >= 0 else 'red'}">
        <div class="card-label">Total P&amp;L</div>
        <div class="card-value {'positive' if portfolio.total_pnl >= 0 else 'negative'}">${portfolio.total_pnl:+.2f}</div>
        <div class="meta">{portfolio.total_pnl / portfolio.total_portfolio_value * 100 if portfolio.total_portfolio_value > 0 else 0:+.1f}% return</div>
    </div>
    <div class="card {'green' if avg_clv >= 0 else 'red'}">
        <div class="card-label">Trade CLV</div>
        <div class="card-value {'positive' if avg_clv >= 0 else 'negative'}">{avg_clv * 100:+.1f}%</div>
        <div class="meta">{clv_count} settled | drift {avg_drift * 100:+.1f}%</div>
    </div>
    <div class="card yellow">
        <div class="card-label">Open Positions</div>
        <div class="card-value">{len(portfolio.positions)}</div>
        <div class="meta">{portfolio.value_filled_count} trades | {sum(p.cost_usd for p in portfolio.positions) / portfolio.total_portfolio_value * 100 if portfolio.total_portfolio_value > 0 else 0:.0f}% deployed</div>
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

{shadow_stats}

</body>
</html>"""


def _build_strategy_table(portfolio) -> str:
    """Strategy breakdown section — filled value bets only."""
    avg_edge = portfolio.value_edge_sum / portfolio.value_filled_count if portfolio.value_filled_count else 0
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
<tr><th>Strategy</th><th>Filled Trades</th><th>P&amp;L</th></tr>
{row_html}
</table>
<p style="color:#8b949e;margin-top:8px;font-size:12px">Avg edge on filled: {avg_edge:.1f}% &middot; {portfolio.value_count - portfolio.value_filled_count} unfilled attempts filtered</p>
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


def _build_shadow_summary(settlement_entries: list) -> str:
    """Build shadow early-exit comparison table from settled trades."""
    checkpoints = ["30m", "10m", "5m", "1m"]
    # Collect data per checkpoint
    cp_data = {cp: [] for cp in checkpoints}
    hold_pnls = []

    for s in settlement_entries:
        extra = s.get("extra", s)
        actual_pnl = extra.get("pnl", 0)
        shadow = extra.get("shadow_exits", {})
        if not shadow:
            continue

        hold_pnls.append(actual_pnl)
        cost = s.get("size_usd", 0)

        for cp in checkpoints:
            data = shadow.get(cp, {})
            if data.get("skipped") or not data.get("shadow_pnl") is not None:
                continue
            if "shadow_pnl" not in data:
                continue
            cp_data[cp].append({
                "shadow_pnl": data["shadow_pnl"],
                "shadow_roi": data.get("shadow_roi", 0),
                "actual_pnl": actual_pnl,
                "cost": cost,
                "fully_exec": data.get("fully_executable", False),
            })

    if not hold_pnls:
        return """
    <h2>Shadow Early-Exit Model</h2>
    <p style="color: #888; font-size: 0.85rem;">Analytics only — collecting data. Stats appear after positions with shadow checkpoints settle.</p>
    <table>
        <tr><th>Exit</th><th>Trades</th><th>Avg P&L</th><th>Avg ROI</th><th>Win Rate</th><th>Std Dev</th><th>vs Hold</th></tr>
        <tr><td>30m</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>
        <tr><td>10m</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>
        <tr><td>5m</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>
        <tr><td>1m</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>
        <tr style="border-top: 2px solid #555; font-weight: bold;"><td>HOLD</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>
    </table>"""

    import statistics

    avg_hold = sum(hold_pnls) / len(hold_pnls)
    hold_wins = sum(1 for p in hold_pnls if p > 0)
    hold_wr = hold_wins / len(hold_pnls) * 100 if hold_pnls else 0

    rows = ""
    for cp in checkpoints:
        entries = cp_data[cp]
        if not entries:
            rows += f"""<tr>
                <td>{cp}</td><td>0</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td>
            </tr>"""
            continue

        pnls = [e["shadow_pnl"] for e in entries]
        rois = [e["shadow_roi"] for e in entries]
        wins = sum(1 for p in pnls if p > 0)
        avg_pnl = sum(pnls) / len(pnls)
        avg_roi = sum(rois) / len(rois) * 100
        win_rate = wins / len(pnls) * 100
        std_dev = statistics.stdev(pnls) if len(pnls) > 1 else 0
        vs_hold = avg_pnl - avg_hold
        fully_exec_pct = sum(1 for e in entries if e["fully_exec"]) / len(entries) * 100

        color = "positive" if avg_pnl >= 0 else "negative"
        vs_color = "positive" if vs_hold >= 0 else "negative"

        rows += f"""<tr>
            <td><strong>{cp}</strong></td>
            <td>{len(entries)}</td>
            <td class="{color}">${avg_pnl:+.2f}</td>
            <td>{avg_roi:+.1f}%</td>
            <td>{win_rate:.0f}%</td>
            <td>${std_dev:.2f}</td>
            <td class="{vs_color}">${vs_hold:+.2f}</td>
        </tr>"""

    # Hold row
    hold_std = statistics.stdev(hold_pnls) if len(hold_pnls) > 1 else 0
    rows += f"""<tr style="border-top: 2px solid #555; font-weight: bold;">
        <td>HOLD</td>
        <td>{len(hold_pnls)}</td>
        <td class="{'positive' if avg_hold >= 0 else 'negative'}">${avg_hold:+.2f}</td>
        <td>—</td>
        <td>{hold_wr:.0f}%</td>
        <td>${hold_std:.2f}</td>
        <td>—</td>
    </tr>"""

    return f"""
    <h2>Shadow Early-Exit Model</h2>
    <p style="color: #888; font-size: 0.85rem;">Analytics only — compares hypothetical pre-start exits vs holding to settlement</p>
    <table>
        <tr>
            <th>Exit</th><th>Trades</th><th>Avg P&L</th><th>Avg ROI</th>
            <th>Win Rate</th><th>Std Dev</th><th>vs Hold</th>
        </tr>
        {rows}
    </table>"""


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



def _esc(text: str) -> str:
    """HTML-escape a string."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
