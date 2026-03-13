"""Rich terminal dashboard + HTML chart for true arb signals."""

import logging
import os
from datetime import datetime, timezone

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from arb_scanner.calculator import TrueArb

logger = logging.getLogger(__name__)
console = Console()


def _truncate(text: str, max_len: int = 40) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _hours_until(commence_time: str) -> float | None:
    """Return hours until match start, or None if unknown."""
    if not commence_time:
        return None
    try:
        start = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        return (start - datetime.now(timezone.utc)).total_seconds() / 3600
    except (ValueError, TypeError):
        return None


def _plat_short(platform: str) -> str:
    """Short platform name for display."""
    return {"polymarket": "POLY", "kalshi": "KALSHI", "pinnacle": "PIN"}.get(platform, platform.upper())


def render_dashboard(arbs: list[TrueArb]) -> None:
    """Render terminal dashboard with true arb signals."""
    console.clear()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = Text()
    header.append("Esports Arb Scanner", style="bold cyan")
    header.append(f"  |  {now}", style="dim")
    header.append(f"  |  ", style="dim")
    header.append(f"{len(arbs)} ARB", style="bold green" if arbs else "dim")

    console.print(Panel(header, expand=True))

    # Filter: pre-game (≥5min to start) or unknown start time
    filtered = [
        a for a in arbs
        if (h := _hours_until(a.commence_time)) is None  # unknown = show it
        or h >= 5 / 60                                     # known & ≥5min away
    ]

    if not filtered:
        console.print("[dim]No pre-game arbs found. Waiting for next scan...[/dim]")
        return

    table = Table(
        title=f"[bold green]TRUE ARBS — Risk-Free Profit ({len(filtered)})[/bold green]",
        show_header=True,
        header_style="bold green",
        expand=True,
        title_style="bold green",
        show_lines=True,
    )
    table.add_column("Match", style="dim", max_width=24)
    table.add_column("Buy Side A", style="white")
    table.add_column("Buy Side B", style="white")
    table.add_column("Result", justify="right")

    for arb in filtered:
        h = _hours_until(arb.commence_time)
        time_str = f"{h:.1f}h" if h is not None else "?"
        depth_str = f"${arb.max_deploy:.0f}" if arb.max_deploy > 0 else "—"

        leg_a = f"{arb.leg_a_team}\n[blue]{arb.leg_a_price*100:.0f}¢[/blue] @{_plat_short(arb.leg_a_platform)}"
        leg_b = f"{arb.leg_b_team}\n[blue]{arb.leg_b_price*100:.0f}¢[/blue] @{_plat_short(arb.leg_b_platform)}"

        result = (
            f"[yellow]{arb.total_cost*100:.1f}¢[/yellow] cost\n"
            f"[green]{arb.profit_pct:.2f}%[/green] ROI\n"
            f"[yellow]{depth_str}[/yellow] depth\n"
            f"[dim]{time_str}[/dim]"
        )

        table.add_row(
            _truncate(arb.market_name, 24),
            leg_a,
            leg_b,
            result,
        )

    console.print(table)
    console.print()


def save_chart(arbs: list[TrueArb], output_path: str = "pages/index.html") -> None:
    """Generate an HTML dashboard with arb cards."""
    if not arbs:
        logger.info("No arbs to chart")
        return

    # Filter: pre-game (≥5min to start) or unknown start time
    filtered = [
        a for a in arbs
        if (h := _hours_until(a.commence_time)) is None
        or h >= 5 / 60
    ]

    if not filtered:
        logger.info("No pre-game arbs to chart")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _arb_card(a: TrueArb) -> str:
        h = _hours_until(a.commence_time)
        time_str = f"{h:.1f}h" if h is not None else "?"
        depth_html = ""
        if a.max_deploy > 0:
            vwap_profit = (1.0 - a.vwap_cost) / a.vwap_cost * 100 if a.vwap_cost > 0 else 0
            depth_html = (
                f'<span style="color:#ffd54f;">${a.max_deploy:.0f} deployable</span>'
                f' <span style="color:#78909c;">· VWAP {a.vwap_cost*100:.1f}¢ ({vwap_profit:.2f}%)</span>'
            )
        else:
            depth_html = '<span style="color:#78909c;">no book data</span>'

        return f"""
        <div style="background:rgba(76,175,80,0.08); border:1px solid rgba(76,175,80,0.3);
                    border-left:4px solid #4caf50; border-radius:6px; padding:14px 18px;
                    margin:6px 0; font-family:'SF Mono',Consolas,monospace;">
            <div style="display:flex; align-items:center; gap:16px;">
                <div style="font-size:24px; color:#4caf50; min-width:28px; text-align:center;">⚡</div>
                <div style="flex:1; min-width:0;">
                    <div style="font-size:14px; color:#90a4ae; margin-bottom:6px;">
                        {_truncate(a.market_name, 50)} &nbsp;·&nbsp; {time_str} to start
                    </div>
                    <div style="display:flex; gap:24px; align-items:baseline; flex-wrap:wrap;">
                        <div>
                            <div style="font-size:11px; color:#78909c; text-transform:uppercase;">Leg A</div>
                            <div style="font-size:15px; color:#e0e0e0; font-weight:600;">
                                {a.leg_a_team}
                                <span style="color:#78909c; font-weight:400;">@{_plat_short(a.leg_a_platform)}</span>
                            </div>
                            <div style="font-size:16px; color:#90caf9;">{a.leg_a_price*100:.0f}¢</div>
                        </div>
                        <div style="font-size:20px; color:#546e7a;">+</div>
                        <div>
                            <div style="font-size:11px; color:#78909c; text-transform:uppercase;">Leg B</div>
                            <div style="font-size:15px; color:#e0e0e0; font-weight:600;">
                                {a.leg_b_team}
                                <span style="color:#78909c; font-weight:400;">@{_plat_short(a.leg_b_platform)}</span>
                            </div>
                            <div style="font-size:16px; color:#90caf9;">{a.leg_b_price*100:.0f}¢</div>
                        </div>
                        <div style="font-size:20px; color:#546e7a;">=</div>
                        <div>
                            <div style="font-size:11px; color:#78909c; text-transform:uppercase;">Total Cost</div>
                            <div style="font-size:16px; color:#ffd54f; font-weight:500;">{a.total_cost*100:.1f}¢</div>
                        </div>
                        <div>
                            <div style="font-size:11px; color:#78909c; text-transform:uppercase;">Profit</div>
                            <div style="font-size:18px; color:#4caf50; font-weight:700;">{a.profit_pct:.2f}%</div>
                        </div>
                    </div>
                    <div style="font-size:12px; margin-top:6px;">{depth_html}</div>
                </div>
            </div>
        </div>"""

    cards = "\n".join(_arb_card(a) for a in filtered)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="1800">
    <title>Esports Arb Scanner</title>
    <style>
        body {{
            background: #0d1117;
            color: #e0e0e0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 24px 32px;
        }}
    </style>
</head>
<body>
    <div style="max-width:900px; margin:0 auto;">
        <div style="margin-bottom:24px;">
            <h1 style="color:#e0e0e0; font-size:22px; font-weight:600; margin:0 0 6px 0;">
                Esports Arb Scanner
            </h1>
            <div style="font-size:13px; color:#78909c; font-family:'SF Mono',Consolas,monospace;">
                {now} &nbsp;·&nbsp; Polymarket × Pinnacle × Kalshi &nbsp;·&nbsp;
                <span style="color:#4caf50;">{len(filtered)} ARB{'S' if len(filtered) != 1 else ''}</span>
            </div>
        </div>

        <div style="margin-bottom:28px;">
            <h2 style="color:#4caf50; font-size:16px; font-weight:600; margin:0 0 10px 0;
                        font-family:'SF Mono',Consolas,monospace; letter-spacing:0.5px;
                        border-bottom:1px solid rgba(76,175,80,0.3); padding-bottom:8px;">
                TRUE ARBS — Risk-Free Profit ({len(filtered)})
            </h2>
            {cards}
        </div>

        <div style="font-size:11px; color:#455a64; margin-top:20px; text-align:center;
                    font-family:'SF Mono',Consolas,monospace;">
            Arb = buy both sides across platforms for &lt;$1 total &nbsp;·&nbsp;
            Profit = guaranteed regardless of outcome &nbsp;·&nbsp;
            Depth = max $ deployable before arb closes &nbsp;·&nbsp;
            Pre-game only (≥5min to start)
        </div>
    </div>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)
    logger.info("Chart saved to %s", output_path)
    console.print(f"[dim]Chart saved to {output_path}[/dim]")
