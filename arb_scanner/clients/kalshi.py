"""Kalshi prediction market client — public REST API, no auth for market data."""

import logging
import re
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from arb_scanner.config import MAX_RETRIES, RETRY_BACKOFF

logger = logging.getLogger(__name__)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Series tickers for esports match-winner markets
ESPORT_SERIES = ["KXLOLGAME", "KXCS2GAME"]


@dataclass
class KalshiMarket:
    """A match-winner market from Kalshi."""
    ticker: str
    event_ticker: str
    question: str         # e.g. "Team A vs Team B"
    team_name: str        # which team this YES outcome is for
    yes_price: float      # midpoint implied probability (0–1)
    yes_ask: float
    yes_bid: float
    sport: str            # "lol" or "cs2"


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _extract_teams(title: str) -> tuple[str, str] | None:
    """Extract 'Team A vs Team B' from Kalshi event title.

    Kalshi titles look like: 'Team Secret Whales vs. G2 Esports'
    """
    m = re.search(r"(.+?)\s+vs\.?\s+(.+?)$", title.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _dollar_to_prob(dollar_str: str | None) -> float:
    """Convert Kalshi dollar string (e.g. '0.6300') to float probability."""
    if not dollar_str:
        return 0.0
    try:
        return float(dollar_str)
    except (ValueError, TypeError):
        return 0.0


def fetch_markets() -> list[KalshiMarket]:
    """Fetch active esports match-winner markets from Kalshi.

    Uses /events endpoint with series_ticker filter and nested markets
    to get prices in a single call per series.
    """
    session = _build_session()
    markets: list[KalshiMarket] = []

    for series in ESPORT_SERIES:
        sport = "lol" if "LOL" in series else "cs2"
        cursor = None

        while True:
            params: dict = {
                "limit": 200,
                "status": "open",
                "with_nested_markets": "true",
                "series_ticker": series,
            }
            if cursor:
                params["cursor"] = cursor

            try:
                resp = session.get(f"{KALSHI_BASE}/events", params=params, timeout=15)
                if resp.status_code != 200:
                    logger.warning("Kalshi events returned %d for %s", resp.status_code, series)
                    break

                data = resp.json()
                events = data.get("events", [])
            except requests.RequestException:
                logger.exception("Failed to fetch Kalshi events for %s", series)
                break

            for event in events:
                event_title = event.get("title", "")
                event_ticker = event.get("event_ticker", "")
                teams = _extract_teams(event_title)

                for mkt in event.get("markets", []):
                    if mkt.get("status") != "active":
                        continue

                    ticker = mkt.get("ticker", "")
                    title = mkt.get("title", "")
                    yes_ask = _dollar_to_prob(mkt.get("yes_ask_dollars"))
                    yes_bid = _dollar_to_prob(mkt.get("yes_bid_dollars"))

                    # Midpoint as best estimate of implied probability
                    if yes_ask > 0 and yes_bid > 0:
                        mid = (yes_ask + yes_bid) / 2
                    elif yes_ask > 0:
                        mid = yes_ask
                    else:
                        mid = _dollar_to_prob(mkt.get("last_price_dollars"))

                    if mid <= 0.01 or mid >= 0.99:
                        continue

                    # Determine which team this YES is for.
                    # yes_sub_title is the cleanest source (e.g. "G2 Esports")
                    team_name = mkt.get("yes_sub_title", "")
                    if not team_name:
                        # Fallback: extract from market title "Will X win the..."
                        win_match = re.match(r"Will (.+?) win ", title, re.IGNORECASE)
                        if win_match:
                            team_name = win_match.group(1).strip()

                    markets.append(KalshiMarket(
                        ticker=ticker,
                        event_ticker=event_ticker,
                        question=event_title,
                        team_name=team_name,
                        yes_price=mid,
                        yes_ask=yes_ask,
                        yes_bid=yes_bid,
                        sport=sport,
                    ))

            cursor = data.get("cursor", "")
            if not events or not cursor:
                break

    logger.info("Fetched %d Kalshi esports markets", len(markets))
    return markets
