"""Live bot configuration — API keys, risk limits, strategy toggles."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from broker_tracker on Desktop, fall back to local .env
_broker_env = Path.home() / "Desktop" / "broker_tracker" / ".env"
load_dotenv(_broker_env)
load_dotenv()

# ── Mode ──────────────────────────────────────────────────────────────
SIMULATION_MODE = os.getenv("LIVE_BOT_SIMULATION", "true").lower() == "true"

# ── Strategy toggles ─────────────────────────────────────────────────
ENABLE_ARB = os.getenv("ENABLE_ARB", "false").lower() == "true"
ENABLE_VALUE = os.getenv("ENABLE_VALUE", "true").lower() == "true"

# ── Polymarket credentials ───────────────────────────────────────────
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
POLYMARKET_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))  # Polygon mainnet

# ── Kalshi credentials ───────────────────────────────────────────────
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
# For cloud: paste the RSA key content directly (avoids needing a file)
KALSHI_PRIVATE_KEY_CONTENT = os.getenv("KALSHI_PRIVATE_KEY_CONTENT", "")

# ── Arb settings (Strategy 1) ────────────────────────────────────────
MIN_ARB_PROFIT_PCT = float(os.getenv("MIN_ARB_PROFIT_PCT", "1.5"))

# ── Value settings (Strategy 2) ──────────────────────────────────────
MIN_VALUE_EDGE_PCT = float(os.getenv("MIN_VALUE_EDGE_PCT", "4.0"))
# Midgame (live) value bets require a larger edge buffer due to stale refs + execution delay
MIDGAME_VALUE_EDGE_PCT = float(os.getenv("MIDGAME_VALUE_EDGE_PCT", "8.0"))
PINNACLE_POLL_INTERVAL = int(os.getenv("PINNACLE_POLL_INTERVAL", "8"))  # pregame
PINNACLE_LIVE_POLL_INTERVAL = int(os.getenv("PINNACLE_LIVE_POLL_INTERVAL", "4"))  # live
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.5"))  # half-Kelly
# Edge persistence: require edge to be seen on N consecutive checks (30s apart) before betting
VALUE_EDGE_PERSISTENCE = int(os.getenv("VALUE_EDGE_PERSISTENCE", "2"))

# ── Risk limits ──────────────────────────────────────────────────────
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "50"))
MAX_DAILY_LOSS_USD = float(os.getenv("MAX_DAILY_LOSS_USD", "25"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "5"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "5"))

# ── Win probability tiers (value bets only) ──────────────────────────
# Bets below MIN_WIN_PROB are skipped entirely.
# Between tiers, max position is capped regardless of Kelly output.
MIN_WIN_PROB = float(os.getenv("MIN_WIN_PROB", "0.30"))          # skip below 30%
TIER_MID_PROB = float(os.getenv("TIER_MID_PROB", "0.50"))        # boundary between mid and high
TIER_LOW_MAX_USD = float(os.getenv("TIER_LOW_MAX_USD", "25"))    # 30-50% win prob: max $25
TIER_HIGH_MAX_USD = float(os.getenv("TIER_HIGH_MAX_USD", "50"))  # >50% win prob: max $50

# ── Fill simulation ────────────────────────────────────────────────
# Simulate as if we have Kalshi WS access (removes the +7.5s REST penalty).
# Set to "true" to model realistic fills with proper API keys.
SIMULATE_KALSHI_WS = os.getenv("SIMULATE_KALSHI_WS", "true").lower() == "true"

# Minimum depth on EACH arb leg before we'll take the trade (USD).
# Below this, the fill simulator says the depth is too likely to be sniped.
MIN_ARB_DEPTH_USD = float(os.getenv("MIN_ARB_DEPTH_USD", "200"))

# ── Timing / staleness guards ──────────────────────────────────────
ALLOW_MIDGAME_VALUE = os.getenv("ALLOW_MIDGAME_VALUE", "true").lower() == "true"
# Max divergence between Pinnacle ref and market price (percentage points).
# If |pin_prob - market_price| > this, the Pinnacle reference is likely stale → skip.
# Tightened from 15 → 10: a 14pp gap (e.g. pin=43% vs market=28¢) is almost always stale
MAX_PRICE_DIVERGENCE_PCT = float(os.getenv("MAX_PRICE_DIVERGENCE_PCT", "10"))
# Max believable edge — anything above this is almost certainly stale/mismatched data
MAX_VALUE_EDGE_PCT = float(os.getenv("MAX_VALUE_EDGE_PCT", "20"))
# Max seconds since last Pinnacle data before we consider it stale (default 120s = 2 min)
MAX_PINNACLE_AGE_SECONDS = float(os.getenv("MAX_PINNACLE_AGE_SECONDS", "120"))
# Max hours before match start to accept pregame bets (skip if match is >24h away)
MAX_PREGAME_HOURS = float(os.getenv("MAX_PREGAME_HOURS", "24"))
# Max hours after commence_time before we assume the match is over (skip stale matches)
MAX_MATCH_DURATION_HOURS = float(os.getenv("MAX_MATCH_DURATION_HOURS", "8"))

# ── Maker order simulation ────────────────────────────────────────────
# Polymarket maker rebate: ~0.5% of notional (you get paid instead of paying)
POLY_MAKER_REBATE = float(os.getenv("POLY_MAKER_REBATE", "0.005"))
# Expected fill rate for limit orders (lower than market orders)
MAKER_FILL_RATE = float(os.getenv("MAKER_FILL_RATE", "0.40"))  # 40% of limit orders fill
# How much better the maker price is vs taker (post slightly below the ask)
MAKER_PRICE_IMPROVEMENT = float(os.getenv("MAKER_PRICE_IMPROVEMENT", "0.01"))  # 1 cent better

# ── Early exit simulation ────────────────────────────────────────────
EARLY_EXIT_CHECK_INTERVAL = int(os.getenv("EARLY_EXIT_CHECK_INTERVAL", "120"))
# Spread cost when selling back (taker into the bid)
EARLY_EXIT_SPREAD_COST = float(os.getenv("EARLY_EXIT_SPREAD_COST", "0.02"))
# Multiple TP/SL tiers to find the optimal exit strategy
# Each tier tracks independently: (take_profit_cents, stop_loss_cents)
EARLY_EXIT_TIERS = [
    (0.03, 0.05),   # Tight:  TP 3¢ / SL 5¢
    (0.05, 0.08),   # Medium: TP 5¢ / SL 8¢
    (0.08, 0.12),   # Wide:   TP 8¢ / SL 12¢
    (0.12, 0.18),   # Extra:  TP 12¢ / SL 18¢
    (0.03, 0.12),   # Asym A: TP 3¢ / SL 12¢ (tight profit, wide stop)
    (0.08, 0.05),   # Asym B: TP 8¢ / SL 5¢ (wide profit, tight stop)
]

# ── Kalshi fee model ─────────────────────────────────────────────────
KALSHI_FEE_RATE = 0.07  # 0.07 * p * (1-p) per contract

# ── WebSocket URLs ───────────────────────────────────────────────────
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# ── Data persistence ─────────────────────────────────────────────────
# Set DATA_DIR to a Railway Volume mount (e.g. "/data") so files survive deploys
DATA_DIR = os.getenv("DATA_DIR", "")
if DATA_DIR:
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def _data_path(filename: str) -> str:
    """Resolve a data filename to DATA_DIR if set, otherwise current dir."""
    if DATA_DIR:
        return str(Path(DATA_DIR) / filename)
    return filename

# ── Logging ──────────────────────────────────────────────────────────
TRADE_LOG_PATH = os.getenv("TRADE_LOG_PATH", _data_path("live_bot_trades.jsonl"))
BOT_LOG_PATH = os.getenv("BOT_LOG_PATH", _data_path("live_bot.log"))
POSITIONS_FILE_PATH = _data_path("live_bot_positions.json")

# ── Settlement ──────────────────────────────────────────────────────
SETTLEMENT_CHECK_INTERVAL = int(os.getenv("SETTLEMENT_CHECK_INTERVAL", "60"))  # seconds

# ── Registry refresh ─────────────────────────────────────────────────
REGISTRY_REFRESH_INTERVAL = int(os.getenv("REGISTRY_REFRESH_INTERVAL", "1800"))  # 30 min

# ── Dashboard ───────────────────────────────────────────────────────
DASHBOARD_PORT = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8080")))
