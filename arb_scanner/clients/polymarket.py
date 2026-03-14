"""Polymarket client — discovers esports markets via gamma API, fetches prices from CLOB."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from arb_scanner.config import (
    POLYMARKET_BASE_URL,
    MAX_RETRIES,
    RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Gamma tag IDs for esports (discovered from /sports endpoint)
# Tag 65 = LoL, Tag 100780 = CS2, Tag 102366 = Dota 2, Tag 101672 = Valorant
ESPORT_TAGS = {
    "lol": 65,
    "cs2": 100780,
    "dota2": 102366,
    "valorant": 101672,
}

# Concurrent CLOB fetches — Polymarket CLOB is generous with rate limits
CLOB_WORKERS = 15


@dataclass
class PolymarketEvent:
    """A binary market from Polymarket."""
    condition_id: str
    question: str
    yes_price: float      # 0–1 implied probability (mid/last)
    no_price: float       # 0–1 implied probability (mid/last)
    active: bool
    slug: str
    yes_token_id: str = ""
    no_token_id: str = ""
    yes_ask: float = 0.0  # best ask for YES (what you'd actually pay)
    no_ask: float = 0.0   # best ask for NO (what you'd actually pay)
    yes_depth_dollars: float = 0.0  # $ available near best ask for YES
    no_depth_dollars: float = 0.0   # $ available near best ask for NO


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=CLOB_WORKERS, pool_maxsize=CLOB_WORKERS)
    session.mount("https://", adapter)
    return session


def _is_match_winner_market(market_question: str) -> bool:
    """Check if a market is a H2H match-winner (Team A vs Team B)."""
    q = market_question.lower()
    # Must be a head-to-head: require "vs" in the question
    if " vs " not in q and " vs." not in q:
        return False
    # Exclude per-map/per-game props
    prop_keywords = [
        "game 1", "game 2", "game 3", "game 4", "game 5",
        "map 1", "map 2", "map 3", "map 4", "map 5",
        "total", "handicap", "o/u", "odd/even",
        "kill", "dragon", "baron", "tower", "inhibitor", "rift herald",
        "first blood", "penta", "quadra", "triple",
    ]
    return not any(kw in q for kw in prop_keywords)


def _get_best_ask(session: requests.Session, token_id: str) -> float:
    """Get the best (lowest) ask price from the CLOB order book.

    This is what you'd actually pay to buy 1 share.
    Returns 0.0 if unavailable.
    """
    try:
        resp = session.get(f"{POLYMARKET_BASE_URL}/book?token_id={token_id}", timeout=8)
        if resp.status_code != 200:
            return 0.0
        book = resp.json()
        asks = book.get("asks", [])
        if not asks:
            return 0.0
        best = min(asks, key=lambda x: float(x.get("price", 999)))
        return float(best["price"])
    except (requests.RequestException, ValueError, KeyError):
        return 0.0


def _fetch_book_depth(session: requests.Session, token_id: str, price: float, side: str = "asks") -> float:
    """Fetch $ depth available near the quoted price from the CLOB order book.

    For buying YES at `price`, we check the asks at or below `price`.
    Returns total $ of liquidity within 2¢ of the quoted price.
    """
    try:
        resp = session.get(f"{POLYMARKET_BASE_URL}/book?token_id={token_id}", timeout=8)
        if resp.status_code != 200:
            return 0.0
        book = resp.json()
        levels = book.get(side, [])
        total = 0.0
        for level in levels:
            lvl_price = float(level.get("price", 0))
            lvl_size = float(level.get("size", 0))
            # For asks: levels at or below our target price (we can buy at these)
            # For bids: levels at or above our target price (we can sell at these)
            if side == "asks" and lvl_price <= price + 0.02:
                total += lvl_size * lvl_price  # $ cost to buy these shares
            elif side == "bids" and lvl_price >= price - 0.02:
                total += lvl_size * lvl_price
        return total
    except (requests.RequestException, ValueError):
        return 0.0


def _fetch_book_levels(session: requests.Session, token_id: str, side: str = "asks") -> list[tuple[float, float]]:
    """Fetch raw order book levels as [(price, size_in_shares), ...] sorted by price.

    For asks: sorted ascending (cheapest first).
    For bids: sorted descending (best bid first).
    """
    try:
        resp = session.get(f"{POLYMARKET_BASE_URL}/book?token_id={token_id}", timeout=8)
        if resp.status_code != 200:
            return []
        book = resp.json()
        levels = book.get(side, [])
        parsed = []
        for level in levels:
            lvl_price = float(level.get("price", 0))
            lvl_size = float(level.get("size", 0))
            if lvl_price > 0 and lvl_size > 0:
                parsed.append((lvl_price, lvl_size))
        if side == "asks":
            parsed.sort(key=lambda x: x[0])  # cheapest first
        else:
            parsed.sort(key=lambda x: x[0], reverse=True)  # best bid first
        return parsed
    except (requests.RequestException, ValueError):
        return []


def _fetch_clob_price(session: requests.Session, gm: dict) -> PolymarketEvent | None:
    """Fetch a single market's price from the CLOB. Used by thread pool."""
    cid = gm["condition_id"]
    try:
        resp = session.get(f"{POLYMARKET_BASE_URL}/markets/{cid}", timeout=10)
        if resp.status_code != 200:
            return None

        clob_data = resp.json()
        tokens = clob_data.get("tokens", [])
        if len(tokens) != 2:
            return None

        yes_price = float(tokens[0].get("price", 0))
        no_price = float(tokens[1].get("price", 0))

        if yes_price <= 0.01 or yes_price >= 0.99:
            return None

        active = clob_data.get("active", True)
        if not active:
            return None

        yes_token_id = tokens[0].get("token_id", "")
        no_token_id = tokens[1].get("token_id", "")

        # Fetch best ask (real buy price) from order books
        yes_ask = _get_best_ask(session, yes_token_id) if yes_token_id else yes_price
        no_ask = _get_best_ask(session, no_token_id) if no_token_id else no_price

        return PolymarketEvent(
            condition_id=cid,
            question=gm["question"],
            yes_price=yes_price,
            no_price=no_price,
            active=active,
            slug=gm["slug"],
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_ask=yes_ask,
            no_ask=no_ask,
        )
    except requests.RequestException:
        return None


def fetch_markets() -> list[PolymarketEvent]:
    """Fetch active esports match-winner markets from Polymarket.

    Pipeline:
      1. Gamma API (/events?tag_id=...) → discover events + condition IDs
      2. CLOB API (/markets/{conditionId}) → fetch live prices (concurrent)
    """
    session = _build_session()
    events: list[PolymarketEvent] = []

    # Step 1: Discover esports events from gamma API
    gamma_markets: list[dict] = []

    for sport, tag_id in ESPORT_TAGS.items():
        try:
            time.sleep(0.5)
            resp = session.get(
                f"{GAMMA_BASE}/events",
                params={
                    "tag_id": tag_id,
                    "limit": 200,
                    "active": "true",
                    "closed": "false",
                },
                timeout=15,
            )
            resp.raise_for_status()
            event_list = resp.json()
        except requests.RequestException:
            logger.exception("Failed to fetch gamma events for %s (tag %d)", sport, tag_id)
            continue

        for event in event_list:
            if not isinstance(event, dict):
                continue
            event_title = event.get("title", "")
            event_slug = event.get("slug", "")

            for market in event.get("markets", []):
                question = market.get("question", "")
                cid = market.get("conditionId", "")
                if not cid:
                    continue
                if _is_match_winner_market(question):
                    gamma_markets.append({
                        "condition_id": cid,
                        "question": question or event_title,
                        "slug": event_slug,
                    })

        logger.info("Gamma %s (tag %d): %d events found", sport, tag_id, len(event_list))

    logger.info("Discovered %d match-winner markets from gamma", len(gamma_markets))

    # Step 2: Fetch live prices from CLOB concurrently
    with ThreadPoolExecutor(max_workers=CLOB_WORKERS) as pool:
        futures = {pool.submit(_fetch_clob_price, session, gm): gm for gm in gamma_markets}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                events.append(result)

    logger.info("Fetched %d tradeable Polymarket esports markets", len(events))
    return events
