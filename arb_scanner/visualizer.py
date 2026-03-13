"""Rich terminal dashboard + Plotly spread chart."""

import logging
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
import plotly.graph_objects as go

from arb_scanner.calculator import ArbOpportunity
from arb_scanner.config import EDGE_THRESHOLD

logger = logging.getLogger(__name__)
console = Console()


def _edge_color(opp: ArbOpportunity) -> str:
    """Return rich color based on edge magnitude."""
    if opp.is_arb:
        return "green"
    if opp.abs_edge > EDGE_THRESHOLD * 0.5:
        return "yellow"
    return "white"


def _truncate(text: str, max_len: int = 45) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def render_dashboard(opportunities: list[ArbOpportunity]) -> None:
    """Render a live terminal table with matched markets and edges."""
    console.clear()

    arb_count = sum(1 for o in opportunities if o.is_arb)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = Text()
    header.append("Polymarket vs Pinnacle Arb Scanner", style="bold cyan")
    header.append(f"  |  {now}", style="dim")
    header.append(f"  |  {len(opportunities)} pairs", style="dim")
    header.append(f"  |  ", style="dim")
    header.append(f"{arb_count} arbs", style="bold green" if arb_count else "dim")
    header.append(f"  |  threshold: {EDGE_THRESHOLD*100:.1f}%", style="dim")

    console.print(Panel(header, expand=True))

    if not opportunities:
        console.print("[dim]No matched markets found. Waiting for next scan...[/dim]")
        return

    table = Table(
        show_header=True,
        header_style="bold magenta",
        expand=True,
        title_style="bold",
    )
    table.add_column("Market", style="white", max_width=45, no_wrap=True)
    table.add_column("Sport", style="cyan", max_width=12)
    table.add_column("Poly YES", justify="right", style="blue")
    table.add_column("Pin NoVig", justify="right", style="blue")
    table.add_column("Edge %", justify="right")
    table.add_column("Signal", justify="center")
    table.add_column("Match %", justify="right", style="dim")

    for opp in opportunities:
        color = _edge_color(opp)
        edge_str = f"[{color}]{opp.edge * 100:+.2f}%[/{color}]"

        signal_styles = {
            "BUY_POLY_YES": "[bold green]BUY YES[/bold green]",
            "BUY_POLY_NO": "[bold red]BUY NO[/bold red]",
            "NO_EDGE": "[dim]—[/dim]",
        }
        signal_str = signal_styles.get(opp.signal, opp.signal)

        sport_short = opp.sport.split("_")[-1].upper() if "_" in opp.sport else opp.sport.upper()

        table.add_row(
            _truncate(opp.poly_question),
            sport_short,
            f"{opp.poly_yes_price:.3f}",
            f"{opp.pinnacle_no_vig_prob:.3f}",
            edge_str,
            signal_str,
            f"{opp.match_confidence:.0f}%",
        )

    console.print(table)

    if arb_count:
        console.print(
            f"\n[bold green]>>> {arb_count} arbitrage opportunit{'y' if arb_count == 1 else 'ies'} "
            f"detected (>{EDGE_THRESHOLD*100:.1f}% edge) <<<[/bold green]"
        )


def save_chart(opportunities: list[ArbOpportunity], output_path: str = "spreads.html") -> None:
    """Generate a Plotly bar chart of edges by market, saved as HTML."""
    if not opportunities:
        logger.info("No opportunities to chart")
        return

    # Take top 25 by absolute edge for readability
    top = opportunities[:25]

    labels = [_truncate(o.poly_question, 35) for o in top]
    edges = [o.edge * 100 for o in top]
    colors = [
        "#00c853" if o.is_arb and o.edge > 0
        else "#ff1744" if o.is_arb and o.edge < 0
        else "#ffc107" if o.abs_edge > EDGE_THRESHOLD * 0.5
        else "#90a4ae"
        for o in top
    ]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=labels,
        x=edges,
        orientation="h",
        marker_color=colors,
        text=[f"{e:+.2f}%" for e in edges],
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Edge: %{x:.2f}%<br>"
            "<extra></extra>"
        ),
    ))

    fig.add_vline(x=EDGE_THRESHOLD * 100, line_dash="dash", line_color="green",
                  annotation_text=f"+{EDGE_THRESHOLD*100:.1f}% threshold")
    fig.add_vline(x=-EDGE_THRESHOLD * 100, line_dash="dash", line_color="red",
                  annotation_text=f"-{EDGE_THRESHOLD*100:.1f}% threshold")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fig.update_layout(
        title=f"Polymarket vs Pinnacle — Edge by Market ({now})",
        xaxis_title="Edge % (Poly YES - Pinnacle NoVig)",
        yaxis_title="",
        template="plotly_dark",
        height=max(400, len(top) * 35),
        margin=dict(l=250),
        yaxis=dict(autorange="reversed"),
    )

    fig.write_html(output_path)
    logger.info("Chart saved to %s", output_path)
    console.print(f"[dim]Chart saved to {output_path}[/dim]")
