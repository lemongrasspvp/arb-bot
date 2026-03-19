"""Paper portfolio and position tracking for simulation mode."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """A single executed (or simulated) trade."""
    timestamp: float
    strategy: str          # "ARB" or "VALUE"
    match_id: str
    match_name: str
    # Leg details
    platform_a: str
    team_a: str
    price_a: float
    platform_b: str        # empty for value bets
    team_b: str
    price_b: float         # 0 for value bets
    # Metrics
    size_usd: float
    profit_pct: float
    edge_pct: float        # 0 for arbs, Pinnacle edge for value bets
    pinnacle_prob: float   # Pinnacle reference price (for value bets)
    # Timing
    timing: str = ""       # "pregame" or "midgame"
    # Status
    simulated: bool = True
    would_fill: bool = False  # In simulation: was the price still there?
    filled_a: bool = False
    filled_b: bool = False
    latency_ms: float = 0.0


@dataclass
class Position:
    """An open position on a platform."""
    match_id: str
    platform: str
    market_id: str         # token_id or ticker
    team: str
    side: str              # "buy"
    price: float
    size: float            # number of contracts
    cost_usd: float
    opened_at: float       # timestamp
    strategy: str          # "ARB" or "VALUE"
    timing: str = ""       # "pregame" or "midgame"
    condition_id: str = ""  # Polymarket condition_id (for resolution lookups)


@dataclass
class PaperPortfolio:
    """Tracks simulated trading performance."""
    starting_balance: float = 1000.0
    current_balance: float = 1000.0
    positions: list[Position] = field(default_factory=list)
    trades: list[Trade] = field(default_factory=list)

    # Aggregate stats
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    _daily_reset_date: str = ""

    # Strategy-specific stats
    arb_count: int = 0
    arb_pnl: float = 0.0
    value_count: int = 0        # all attempts (filled + unfilled)
    value_filled_count: int = 0  # only filled trades
    value_pnl: float = 0.0
    value_edge_sum: float = 0.0  # sum of edge_pct on filled trades (for avg)

    # 4-bucket stats: strategy × timing (filled only)
    pregame_value_count: int = 0
    pregame_value_pnl: float = 0.0
    midgame_value_count: int = 0
    midgame_value_pnl: float = 0.0

    # Timing
    last_trade_time: float = 0.0

    @property
    def open_positions(self) -> list[Position]:
        return self.positions

    @property
    def open_market_ids(self) -> set[str]:
        return {p.market_id for p in self.positions}

    def _check_daily_reset(self) -> None:
        """Reset daily P&L counter at midnight."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._daily_reset_date:
            if self._daily_reset_date:
                logger.info(
                    "Daily P&L reset (was $%.2f) — new day: %s",
                    self.daily_pnl, today,
                )
            self.daily_pnl = 0.0
            self._daily_reset_date = today

    def record_arb_trade(
        self,
        trade: Trade,
        market_id_a: str = "",
        market_id_b: str = "",
        condition_id_a: str = "",
        condition_id_b: str = "",
    ) -> None:
        """Record an arb trade (both legs).

        Capital is locked until the match settles. You buy opposite sides
        on two platforms: one will pay $1/share, the other $0. The spread
        is your guaranteed profit, but the money stays locked until then.
        """
        self._check_daily_reset()
        self.trades.append(trade)
        self.arb_count += 1
        timing = trade.timing or "pregame"
        if timing == "pregame":
            self.pregame_arb_count += 1
        else:
            self.midgame_arb_count += 1
        self.last_trade_time = trade.timestamp

        if not trade.would_fill:
            return

        # Calculate shares and cost per leg
        combined_cost = trade.price_a + trade.price_b
        shares = int(trade.size_usd / combined_cost) if combined_cost > 0 else 0
        if shares < 1:
            return
        cost_a = shares * trade.price_a
        cost_b = shares * trade.price_b
        total_cost = cost_a + cost_b

        # Deduct capital — money is locked until settlement
        self.current_balance -= total_cost

        # Create two open positions (one on each platform)
        # When match resolves, one wins ($1 * shares) and one loses ($0)
        self.positions.append(Position(
            match_id=trade.match_id,
            platform=trade.platform_a,
            market_id=market_id_a,
            team=trade.team_a,
            side="buy",
            price=trade.price_a,
            size=shares,
            cost_usd=cost_a,
            opened_at=trade.timestamp,
            strategy="ARB",
            timing=timing,
            condition_id=condition_id_a,
        ))
        self.positions.append(Position(
            match_id=trade.match_id,
            platform=trade.platform_b,
            market_id=market_id_b,
            team=trade.team_b,
            side="buy",
            price=trade.price_b,
            size=shares,
            cost_usd=cost_b,
            opened_at=trade.timestamp,
            strategy="ARB",
            timing=timing,
            condition_id=condition_id_b,
        ))

        locked_profit = shares * 1.0 - total_cost  # guaranteed profit at settlement
        logger.info(
            "ARB LOCKED: %s | cost=$%.2f (%d shares) | locked profit=$%.2f (%.2f%%) | balance=$%.2f",
            trade.match_name, total_cost, shares, locked_profit,
            trade.profit_pct, self.current_balance,
        )

    def record_value_trade(
        self, trade: Trade, market_id: str = "", condition_id: str = ""
    ) -> None:
        """Record a value bet (single leg)."""
        self._check_daily_reset()
        self.trades.append(trade)
        self.value_count += 1
        self.last_trade_time = trade.timestamp

        if trade.simulated and trade.would_fill:
            self.value_filled_count += 1
            self.value_edge_sum += trade.edge_pct
            timing = trade.timing or "pregame"
            if timing == "pregame":
                self.pregame_value_count += 1
            else:
                self.midgame_value_count += 1
            # In simulation, don't resolve P&L immediately — track as position
            self.positions.append(Position(
                match_id=trade.match_id,
                platform=trade.platform_a,
                market_id=market_id,
                team=trade.team_a,
                side="buy",
                price=trade.price_a,
                size=trade.size_usd / trade.price_a,
                cost_usd=trade.size_usd,
                opened_at=trade.timestamp,
                strategy="VALUE",
                timing=timing,
                condition_id=condition_id,
            ))
            self.current_balance -= trade.size_usd
            logger.info(
                "SIM VALUE: %s %s@%s at %.0f¢ (edge=%.1f%%, pinnacle=%.0f¢) | size=$%.2f | balance=$%.2f",
                trade.team_a, trade.platform_a, trade.match_name,
                trade.price_a * 100, trade.edge_pct, trade.pinnacle_prob * 100,
                trade.size_usd, self.current_balance,
            )

    def settle_position(self, market_id: str, won: bool) -> float:
        """Settle a position when the event resolves.

        Returns P&L for this position.
        """
        to_remove = []
        pnl = 0.0
        for i, pos in enumerate(self.positions):
            if pos.market_id == market_id:
                if won:
                    # Payout is $1 per contract
                    payout = pos.size * 1.0
                    pnl = payout - pos.cost_usd
                else:
                    pnl = -pos.cost_usd
                to_remove.append(i)

                if pos.strategy == "VALUE":
                    self.value_pnl += pnl
                    if pos.timing == "midgame":
                        self.midgame_value_pnl += pnl
                    else:
                        self.pregame_value_pnl += pnl
                else:
                    self.arb_pnl += pnl
                    if pos.timing == "midgame":
                        self.midgame_arb_pnl += pnl
                    else:
                        self.pregame_arb_pnl += pnl

                self.total_pnl += pnl
                self.daily_pnl += pnl
                self.current_balance += pos.cost_usd + pnl  # return cost + profit/loss

                logger.info(
                    "SETTLED %s: %s %s — %s | P&L=$%.2f | balance=$%.2f",
                    pos.strategy, pos.team, pos.platform,
                    "WON" if won else "LOST", pnl, self.current_balance,
                )

        for i in reversed(to_remove):
            self.positions.pop(i)

        return pnl

    def summary(self) -> str:
        """Return a human-readable summary of portfolio state."""
        self._check_daily_reset()
        avg_edge = self.value_edge_sum / self.value_filled_count if self.value_filled_count else 0
        lines = [
            f"Balance: ${self.current_balance:.2f} (started ${self.starting_balance:.2f}) | "
            f"Total P&L: ${self.total_pnl:.2f} | Daily P&L: ${self.daily_pnl:.2f} | "
            f"Open positions: {len(self.positions)}",
            f"  Pregame Value: {self.pregame_value_count} filled (${self.pregame_value_pnl:.2f})",
            f"  Midgame Value: {self.midgame_value_count} filled (${self.midgame_value_pnl:.2f})",
            f"  Avg Edge: {avg_edge:.1f}%",
        ]
        return "\n".join(lines)
