"""Tests for numeric payout settlement logic."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from live_bot.portfolio import PaperPortfolio as Portfolio, Position


# ── Portfolio.settle_position tests ─────────────────────────────────


def _make_portfolio(balance=1000.0):
    p = Portfolio()
    p.starting_balance = balance
    p.current_balance = balance
    return p


def _make_position(price=0.42, size=100, cost_usd=42.0, strategy="VALUE",
                   timing="pregame", platform="polymarket"):
    return Position(
        match_id="test_match",
        platform=platform,
        market_id="token_123",
        team="Team A",
        side="buy",
        price=price,
        size=size,
        cost_usd=cost_usd,
        opened_at=1000000.0,
        strategy=strategy,
        timing=timing,
    )


class TestSettlePositionPayout:
    """Test settle_position with numeric payout_per_share."""

    def test_full_win(self):
        p = _make_portfolio(balance=958.0)  # 1000 - 42 cost
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=1.0)
        assert pnl == pytest.approx(58.0)  # 100 * 1.0 - 42
        # balance = 958 + cost(42) + pnl(58) = 1058
        assert p.current_balance == pytest.approx(1058.0)
        assert len(p.positions) == 0

    def test_full_loss(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.0)
        assert pnl == pytest.approx(-42.0)  # 100 * 0 - 42
        # balance = 958 + cost(42) + pnl(-42) = 958
        assert p.current_balance == pytest.approx(958.0)
        assert len(p.positions) == 0

    def test_split_50_50(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.5)
        # 100 shares * 0.50 = $50 payout, cost was $42
        assert pnl == pytest.approx(8.0)
        # balance = 958 + cost(42) + pnl(8) = 1008
        assert p.current_balance == pytest.approx(1008.0)
        assert len(p.positions) == 0

    def test_split_bought_above_50(self):
        """Bought at 58¢, split at 50¢ → loss."""
        p = _make_portfolio(balance=942.0)
        pos = _make_position(price=0.58, size=100, cost_usd=58.0)
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.5)
        # 100 * 0.50 = $50, cost $58 → -$8
        assert pnl == pytest.approx(-8.0)
        # balance = 942 + cost(58) + pnl(-8) = 992
        assert p.current_balance == pytest.approx(992.0)

    def test_partial_payout_025(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.25)
        # 100 * 0.25 = $25, cost $42 → -$17
        assert pnl == pytest.approx(-17.0)

    def test_partial_payout_075(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.75)
        # 100 * 0.75 = $75, cost $42 → +$33
        assert pnl == pytest.approx(33.0)

    def test_legacy_won_true(self):
        """Legacy bool interface still works."""
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", won=True)
        assert pnl == pytest.approx(58.0)

    def test_legacy_won_false(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        pnl = p.settle_position("token_123", won=False)
        assert pnl == pytest.approx(-42.0)

    def test_payout_overrides_won(self):
        """payout_per_share takes precedence over won bool."""
        p = _make_portfolio(balance=958.0)
        pos = _make_position()
        p.positions.append(pos)
        # won=True but payout=0.5 → payout wins
        pnl = p.settle_position("token_123", won=True, payout_per_share=0.5)
        assert pnl == pytest.approx(8.0)

    def test_void_refund_at_entry_price(self):
        """Kalshi void: payout = entry price → P&L = 0."""
        p = _make_portfolio(balance=958.0)
        pos = _make_position(price=0.42, size=100, cost_usd=42.0)
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.42)
        assert pnl == pytest.approx(0.0)

    def test_strategy_stats_updated(self):
        p = _make_portfolio(balance=958.0)
        pos = _make_position(strategy="VALUE", timing="pregame")
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=0.5)
        assert p.value_pnl == pytest.approx(8.0)
        assert p.pregame_value_pnl == pytest.approx(8.0)
        assert p.total_pnl == pytest.approx(8.0)


# ── PnL formula correctness ────────────────────────────────────────


class TestPnlFormula:
    """Verify pnl = shares * payout_per_share - total_cost."""

    @pytest.mark.parametrize("shares,cost,pps,expected_pnl", [
        (100, 42.0, 0.50, 8.0),
        (100, 58.0, 0.50, -8.0),
        (100, 42.0, 1.0, 58.0),
        (100, 42.0, 0.0, -42.0),
        (200, 80.0, 0.25, -30.0),   # 200*0.25=50, 50-80=-30
        (50, 35.0, 0.75, 2.5),      # 50*0.75=37.5, 37.5-35=2.5
        (100, 42.0, 0.42, 0.0),     # refund at entry price
    ])
    def test_pnl_formula(self, shares, cost, pps, expected_pnl):
        p = _make_portfolio(balance=1000.0 - cost)
        pos = _make_position(price=cost/shares, size=shares, cost_usd=cost)
        p.positions.append(pos)
        pnl = p.settle_position("token_123", payout_per_share=pps)
        assert pnl == pytest.approx(expected_pnl, abs=0.01)
