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

PAIR_COLORS = {
    "Poly↔Pin": "cyan",
    "Poly↔Kalshi": "magenta",
    "Kalshi↔Pin": "yellow",
}


def _edge_color(opp: ArbOpportunity) -> str:
    if opp.is_arb:
        return "green"
    if opp.abs_edge > EDGE_THRESHOLD * 0.5:
        return "yellow"
    return "white"


def _truncate(text: str, max_len: int = 40) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def render_dashboard(opportunities: list[ArbOpportunity]) -> None:
    """Render a live terminal table with matched markets and edges."""
    console.clear()

    arb_count = sum(1 for o in opportunities if o.is_arb)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = Text()
    header.append("Esports Arb Scanner", style="bold cyan")
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
    )
    table.add_column("Market", style="white", max_width=40, no_wrap=True)
    table.add_column("Pair", style="cyan", max_width=14)
    table.add_column("Sport", style="cyan", max_width=5)
    table.add_column("Pr. A", justify="right", style="blue")
    table.add_column("Pr. B", justify="right", style="blue")
    table.add_column("Edge %", justify="right")
    table.add_column("Signal", justify="center")
    table.add_column("Conf", justify="right", style="dim")

    for opp in opportunities:
        color = _edge_color(opp)
        edge_str = f"[{color}]{opp.edge * 100:+.2f}%[/{color}]"

        if opp.signal == "NO_EDGE":
            signal_str = "[dim]—[/dim]"
        else:
            signal_str = f"[bold green]{opp.signal}[/bold green]"

        pair_color = PAIR_COLORS.get(opp.pair_label, "white")

        table.add_row(
            _truncate(opp.market_name),
            f"[{pair_color}]{opp.pair_label}[/{pair_color}]",
            opp.sport.upper(),
            f"{opp.price_a:.3f}",
            f"{opp.price_b:.3f}",
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

    top = opportunities[:25]

    labels = [f"[{o.pair_label}] {_truncate(o.market_name, 30)}" for o in top]
    edges = [o.edge * 100 for o in top]

    pair_chart_colors = {
        "Poly↔Pin": ("#00bcd4", "#006064"),
        "Poly↔Kalshi": ("#e040fb", "#6a1b9a"),
        "Kalshi↔Pin": ("#ffeb3b", "#f57f17"),
    }

    colors = []
    for o in top:
        arb_c, normal_c = pair_chart_colors.get(o.pair_label, ("#90a4ae", "#546e7a"))
        colors.append(arb_c if o.is_arb else normal_c)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=labels,
        x=edges,
        orientation="h",
        marker_color=colors,
        text=[f"{e:+.2f}%" for e in edges],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Edge: %{x:.2f}%<extra></extra>",
    ))

    fig.add_vline(x=EDGE_THRESHOLD * 100, line_dash="dash", line_color="green",
                  annotation_text=f"+{EDGE_THRESHOLD*100:.1f}%")
    fig.add_vline(x=-EDGE_THRESHOLD * 100, line_dash="dash", line_color="red",
                  annotation_text=f"-{EDGE_THRESHOLD*100:.1f}%")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fig.update_layout(
        title=f"Esports Arb Scanner — Edge by Market ({now})",
        xaxis_title="Edge % (Platform A - Platform B)",
        yaxis_title="",
        template="plotly_dark",
        height=max(400, len(top) * 35),
        margin=dict(l=300),
        yaxis=dict(autorange="reversed"),
    )

    fig.write_html(output_path)
    logger.info("Chart saved to %s", output_path)
    console.print(f"[dim]Chart saved to {output_path}[/dim]")
