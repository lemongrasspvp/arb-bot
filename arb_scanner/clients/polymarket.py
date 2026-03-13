"""Polymarket client — discovers esports markets via gamma API, fetches prices from CLOB."""

import time
import logging
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from arb_scanner.config import (
    POLYMARKET_BASE_URL,
    MAX_RETRIES,
    RETRY_BACKOFF,
    RATE_LIMIT_DELAY,
)

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Gamma tag IDs for esports (discovered from /sports endpoint)
# Tag 65 = LoL, Tag 100780 = CS2
ESPORT_TAGS = {
    "lol": 65,
    "cs2": 100780,
}


@dataclass
class PolymarketEvent:
    """A binary market from Polymarket."""
    condition_id: str
    question: str
    yes_price: float      # 0–1 implied probability
    no_price: float       # 0–1 implied probability
    active: bool
    slug: str


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _is_match_winner_market(event_title: str, market_question: str) -> bool:
    """Check if a market is the main match-winner (not map/prop/total)."""
    q = market_question.lower()
    # The main match winner market's question matches the event title
    if market_question == event_title:
        return True
    # Exclude prop/map/handicap/total markets
    prop_keywords = [
        "game 1", "game 2", "game 3", "map 1", "map 2", "map 3",
        "total", "handicap", "o/u", "odd/even",
        "kill", "dragon", "baron", "tower", "inhibitor", "rift herald",
        "first blood", "penta", "quadra", "triple",
    ]
    return not any(kw in q for kw in prop_keywords)


def fetch_markets() -> list[PolymarketEvent]:
    """Fetch active esports match-winner markets from Polymarket.

    Pipeline:
      1. Gamma API (/events?tag_id=...) → discover events + condition IDs
      2. CLOB API (/markets/{conditionId}) → fetch live prices
    """
    session = _build_session()
    events: list[PolymarketEvent] = []

    # Step 1: Discover esports events from gamma API
    gamma_markets: list[dict] = []  # (condition_id, question, event_title, slug)

    for sport, tag_id in ESPORT_TAGS.items():
        try:
            time.sleep(RATE_LIMIT_DELAY)
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
                # Only keep match-winner markets
                if _is_match_winner_market(event_title, question):
                    gamma_markets.append({
                        "condition_id": cid,
                        "question": question or event_title,
                        "slug": event_slug,
                    })

        logger.info("Gamma %s (tag %d): %d events found", sport, tag_id, len(event_list))

    logger.info("Discovered %d match-winner markets from gamma", len(gamma_markets))

    # Step 2: Fetch live prices from CLOB for each condition ID
    for gm in gamma_markets:
        cid = gm["condition_id"]
        try:
            time.sleep(RATE_LIMIT_DELAY)
            resp = session.get(
                f"{POLYMARKET_BASE_URL}/markets/{cid}",
                timeout=10,
            )
            if resp.status_code != 200:
                logger.debug("CLOB returned %d for %s", resp.status_code, cid[:20])
                continue

            clob_data = resp.json()
            tokens = clob_data.get("tokens", [])
            if len(tokens) != 2:
                continue

            yes_price = float(tokens[0].get("price", 0))
            no_price = float(tokens[1].get("price", 0))

            # Skip settled / dead markets
            if yes_price <= 0.01 or yes_price >= 0.99:
                continue

            active = clob_data.get("active", True)
            if not active:
                continue

            events.append(PolymarketEvent(
                condition_id=cid,
                question=gm["question"],
                yes_price=yes_price,
                no_price=no_price,
                active=active,
                slug=gm["slug"],
            ))

        except requests.RequestException:
            logger.debug("Failed to fetch CLOB price for %s", cid[:20])
            continue

    logger.info("Fetched %d tradeable Polymarket esports markets", len(events))
    return events
