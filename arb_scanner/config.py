import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from broker_tracker on Desktop, fall back to local .env
_broker_env = Path.home() / "Desktop" / "broker_tracker" / ".env"
load_dotenv(_broker_env)
load_dotenv()  # local .env can still override if present

# API keys
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

# Polymarket CLOB API
POLYMARKET_BASE_URL = "https://clob.polymarket.com"

# The Odds API
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"

# Matching
MATCH_CONFIDENCE_THRESHOLD = 75  # minimum rapidfuzz score (lowered for UFC/tennis name variants)

# Arbitrage
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.03"))  # 3% default

# Polymarket filters
MIN_24H_VOLUME = float(os.getenv("MIN_24H_VOLUME", "1000"))  # minimum 24h volume in USD

# Sports to query (comma-separated in .env, defaults to LoL + CS2)
ODDS_SPORTS = [
    s.strip()
    for s in os.getenv("ODDS_SPORTS", "esports_league_of_legends,esports_csgo").split(",")
    if s.strip()
]

# Betfair Exchange
BETFAIR_USERNAME = os.getenv("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD = os.getenv("BETFAIR_PASSWORD", "")
BETFAIR_APP_KEY = os.getenv("BETFAIR_APP_KEY", "")
BETFAIR_COMMISSION = float(os.getenv("BETFAIR_COMMISSION", "0.05"))  # 5% default

# Refresh interval (seconds)
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "60"))

# Rate limiting / retries
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # seconds, multiplied by attempt number
RATE_LIMIT_DELAY = 0.5  # seconds between API calls
