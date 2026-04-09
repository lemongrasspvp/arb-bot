"""NBA Regular 12–24h strategy — configuration.

All values are env-var driven with safe defaults.
SIMULATION_MODE and ENABLE are both false/off by default.
"""

import os
from pathlib import Path


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ── Strategy toggle ───────────────────────────────────────────────────

ENABLE_NBA_REGULAR = _env("ENABLE_NBA_REGULAR_STRATEGY", "false").lower() == "true"

# ── Strategy filters ──────────────────────────────────────────────────

NBA_PLATFORM = _env("NBA_REGULAR_PLATFORM", "polymarket")
NBA_SPORT = _env("NBA_REGULAR_SPORT", "nba")
NBA_MIN_MINUTES = int(_env("NBA_REGULAR_MIN_MINUTES_TO_START", "720"))
NBA_MAX_MINUTES = int(_env("NBA_REGULAR_MAX_MINUTES_TO_START", "1440"))

# ── Edge / quality filters ────────────────────────────────────────────

NBA_MIN_EDGE_PCT = float(_env("NBA_MIN_EDGE_PCT", "0.02"))
NBA_EDGE_PERSISTENCE = int(_env("NBA_EDGE_PERSISTENCE", "2"))
NBA_MIN_WIN_PROB = float(_env("NBA_MIN_WIN_PROB", "0.15"))
NBA_MAX_WIN_PROB = float(_env("NBA_MAX_WIN_PROB", "0.85"))
NBA_MAX_PINNACLE_AGE_S = int(_env("NBA_MAX_PINNACLE_AGE_S", "120"))

# ── Execution ─────────────────────────────────────────────────────────

SIMULATION_MODE = _env("SIMULATION_MODE", "true").lower() == "true"
NBA_BET_SIZE_USD = float(_env("NBA_BET_SIZE_USD", "50"))
NBA_MAX_POSITIONS = int(_env("NBA_MAX_POSITIONS", "10"))
NBA_MAX_PER_EVENT = int(_env("NBA_MAX_PER_EVENT", "1"))
WALLET_CAP = float(_env("WALLET_CAP", "1000"))
STARTING_BALANCE = float(_env("STARTING_BALANCE", "1000"))
KELLY_FRACTION = float(_env("KELLY_FRACTION", "0.5"))
MAX_BET_PCT = float(_env("MAX_BET_PCT", "15"))
MAX_EXPOSURE_PCT = float(_env("MAX_EXPOSURE_PCT", "80"))
MAX_DAILY_LOSS_PCT = float(_env("MAX_DAILY_LOSS_PCT", "15"))
COOLDOWN_SECONDS = float(_env("COOLDOWN_SECONDS", "30"))
FILL_RECHECK_DELAY_S = float(_env("FILL_RECHECK_DELAY_S", "2.0"))
SLIPPAGE_TOLERANCE = float(_env("SLIPPAGE_TOLERANCE", "0.02"))

# ── Persistence ───────────────────────────────────────────────────────

DATA_DIR = _env("DATA_DIR", "")
if DATA_DIR:
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def _data_path(filename: str) -> str:
    if DATA_DIR:
        return str(Path(DATA_DIR) / filename)
    return filename


POSITIONS_FILE_PATH = _data_path("nba_bot_positions.json")
TRADE_LOG_PATH = _data_path("nba_bot_trades.jsonl")

# ── Dashboard ─────────────────────────────────────────────────────────

DASHBOARD_PORT = int(_env("DASHBOARD_PORT", "8080"))
DASHBOARD_PASSWORD = _env("DASHBOARD_PASSWORD", "")

# ── Timing / polling ─────────────────────────────────────────────────

SETTLEMENT_CHECK_INTERVAL = int(_env("SETTLEMENT_CHECK_INTERVAL", "120"))
REGISTRY_REFRESH_INTERVAL = int(_env("REGISTRY_REFRESH_INTERVAL", "300"))
PERSIST_SAVE_INTERVAL = int(_env("PERSIST_SAVE_INTERVAL", "60"))
PINNACLE_POLL_INTERVAL = int(_env("PINNACLE_POLL_INTERVAL", "15"))

# ── Polymarket credentials (shared) ──────────────────────────────────

POLYMARKET_PRIVATE_KEY = _env("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_FUNDER_ADDRESS = _env("POLYMARKET_FUNDER_ADDRESS", "")
POLYMARKET_CHAIN_ID = int(_env("POLYMARKET_CHAIN_ID", "137"))
POLYMARKET_API_KEY = _env("POLY_API_KEY", "")
POLYMARKET_API_SECRET = _env("POLY_API_SECRET", "")
POLYMARKET_PASSPHRASE = _env("POLY_PASSPHRASE", "")
POLYMARKET_WS_URL = _env(
    "POLYMARKET_WS_URL",
    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
)

# ── Pinnacle credentials (shared) ────────────────────────────────────

PINNACLE_API_KEY = _env("PINNACLE_API_KEY", "")
