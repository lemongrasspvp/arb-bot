"""Betfair Exchange client — fetch esports, UFC, and tennis odds via API-NG.

Uses betfairlightweight to interact with the Betfair Exchange API.
Betfair is an exchange (like Polymarket) — it never limits accounts,
making it a permanent arb source.

Requires BETFAIR_USERNAME, BETFAIR_PASSWORD, BETFAIR_APP_KEY in env.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import betfairlightweight
from betfairlightweight import filters

from arb_scanner.config import (
    BETFAIR_USERNAME,
    BETFAIR_PASSWORD,
    BETFAIR_APP_KEY,
)

logger = logging.getLogger(__name__)

# Event type IDs on Betfair (discovered via list_event_types)
# These are well-known stable IDs
EVENT_TYPE_IDS = {
    "esports": "27454571",
    "mma": "26420387",
    "tennis": "2",
}

# Sport label mapping from Betfair competition/event names
SPORT_KEYWORDS = {
    "league of legends": "lol",
    "lol": "lol",
    "cs2": "cs2",
    "counter-strike": "cs2",
    "csgo": "cs2",
    "cs:go": "cs2",
    "valorant": "valorant",
    "dota": "dota2",
    "call of duty": "cod",
    "ufc": "ufc",
    "mma": "ufc",
    "atp": "tennis",
    "wta": "tennis",
    "tennis": "tennis",
}


@dataclass
class BetfairOutcome:
    """A single outcome from a Betfair Exchange market."""
    event_name: str        # "Team A vs Team B"
    team_name: str         # runner name
    back_price: float      # best back decimal odds
    back_size: float       # £ available at best back
    actual_price: float    # 1/back_price (0-1 cost)
    sport: str             # "lol", "cs2", "ufc", "tennis"
    market_id: str         # Betfair market ID
    selection_id: int      # Betfair selection/runner ID
    commence_time: str     # ISO timestamp


def _build_client() -> betfairlightweight.APIClient | None:
    """Create and login a Betfair API client.

    Returns None if credentials are missing or login fails.
    """
    if not BETFAIR_USERNAME or not BETFAIR_PASSWORD or not BETFAIR_APP_KEY:
        logger.info("Betfair credentials not configured — skipping")
        return None

    try:
        client = betfairlightweight.APIClient(
            username=BETFAIR_USERNAME,
            password=BETFAIR_PASSWORD,
            app_key=BETFAIR_APP_KEY,
            lightweight=True,  # return dicts instead of objects (faster)
        )
        client.login()
        logger.info("Betfair login successful")
        return client
    except Exception:
        logger.exception("Betfair login failed")
        return None


def _detect_sport(event_name: str, competition_name: str = "") -> str:
    """Detect sport label from event/competition name."""
    combined = f"{competition_name} {event_name}".lower()
    for keyword, sport in SPORT_KEYWORDS.items():
        if keyword in combined:
            return sport
    return "other"


def _fetch_book_levels(
    client: betfairlightweight.APIClient,
    market_id: str,
    selection_id: int,
) -> list[tuple[float, float]]:
    """Fetch back book levels for a selection on Betfair.

    Returns list of (price_0_to_1, size_in_shares) sorted cheapest first.
    Back levels are what you can buy at — equivalent to ask levels on Poly/Kalshi.
    """
    try:
        books = client.betting.list_market_book(
            market_ids=[market_id],
            price_projection=filters.price_projection(
                price_data=["EX_ALL_OFFERS"],
            ),
        )
        if not books:
            return []

        book = books[0] if isinstance(books, list) else books
        runners = book.get("runners", []) if isinstance(book, dict) else getattr(book, "runners", [])

        for runner in runners:
            r_id = runner.get("selectionId") if isinstance(runner, dict) else getattr(runner, "selection_id", None)
            if r_id != selection_id:
                continue

            ex = runner.get("ex", {}) if isinstance(runner, dict) else getattr(runner, "ex", {})
            available_to_back = ex.get("availableToBack", []) if isinstance(ex, dict) else getattr(ex, "available_to_back", [])

            levels = []
            for level in available_to_back:
                if isinstance(level, dict):
                    dec_odds = level.get("price", 0)
                    size = level.get("size", 0)
                else:
                    dec_odds = getattr(level, "price", 0)
                    size = getattr(level, "size", 0)

                if dec_odds > 1.0 and size > 0:
                    price_01 = 1.0 / dec_odds
                    # Convert £ size to number of shares
                    # If you back at decimal odds D with stake S,
                    # you get S * D if you win = S * D shares at $1 each
                    # But for our book walker we need "contracts" at price_01
                    # Stake S buys S/price_01 contracts of value price_01 each
                    num_contracts = size / price_01
                    levels.append((price_01, num_contracts))

            # Sort cheapest first (lowest price = best back odds = highest decimal odds)
            levels.sort(key=lambda x: x[0])
            return levels

    except Exception:
        logger.debug("Failed to fetch Betfair book for market %s", market_id)

    return []


def fetch_markets() -> list[BetfairOutcome]:
    """Fetch active match-winner markets from Betfair Exchange.

    Covers esports (LoL, CS2, Valorant), UFC/MMA, and Tennis.
    Returns empty list if credentials are missing or login fails.
    """
    client = _build_client()
    if not client:
        return []

    outcomes: list[BetfairOutcome] = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(minutes=5)
    max_time = now + timedelta(days=14)  # don't look more than 2 weeks ahead

    for sport_label, event_type_id in EVENT_TYPE_IDS.items():
        try:
            # Get market catalogue — MATCH_ODDS markets for this sport
            catalogues = client.betting.list_market_catalogue(
                filter=filters.market_filter(
                    event_type_ids=[event_type_id],
                    market_type_codes=["MATCH_ODDS"],
                    market_start_time={
                        "from": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to": max_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    in_play_only=False,
                ),
                market_projection=[
                    "RUNNER_DESCRIPTION",
                    "EVENT",
                    "COMPETITION",
                    "MARKET_START_TIME",
                ],
                max_results=200,
                sort="FIRST_TO_START",
            )

            if not catalogues:
                logger.debug("No Betfair catalogues for %s", sport_label)
                continue

            # Collect market IDs for batch price fetch
            market_ids = []
            catalogue_map = {}
            for cat in catalogues:
                if isinstance(cat, dict):
                    mid = cat.get("marketId", "")
                else:
                    mid = getattr(cat, "market_id", "")
                if mid:
                    market_ids.append(mid)
                    catalogue_map[mid] = cat

            if not market_ids:
                continue

            # Batch fetch prices (up to 40 markets per call)
            for i in range(0, len(market_ids), 40):
                batch = market_ids[i:i + 40]
                try:
                    books = client.betting.list_market_book(
                        market_ids=batch,
                        price_projection=filters.price_projection(
                            price_data=["EX_BEST_OFFERS"],
                        ),
                    )
                except Exception:
                    logger.debug("Failed to fetch Betfair books batch %d", i)
                    continue

                if not books:
                    continue

                for book in books:
                    if isinstance(book, dict):
                        mid = book.get("marketId", "")
                        runners = book.get("runners", [])
                        status = book.get("status", "")
                    else:
                        mid = getattr(book, "market_id", "")
                        runners = getattr(book, "runners", [])
                        status = getattr(book, "status", "")

                    if status not in ("OPEN", ""):
                        continue

                    cat = catalogue_map.get(mid)
                    if not cat:
                        continue

                    # Extract event info from catalogue
                    if isinstance(cat, dict):
                        event = cat.get("event", {})
                        competition = cat.get("competition", {})
                        cat_runners = cat.get("runners", [])
                        start_time = cat.get("marketStartTime", "")
                    else:
                        event = getattr(cat, "event", {})
                        competition = getattr(cat, "competition", {})
                        cat_runners = getattr(cat, "runners", [])
                        start_time = getattr(cat, "market_start_time", "")

                    if isinstance(event, dict):
                        event_name = event.get("name", "")
                    else:
                        event_name = getattr(event, "name", "")

                    if isinstance(competition, dict):
                        comp_name = competition.get("name", "")
                    else:
                        comp_name = getattr(competition, "name", "")

                    # Convert start_time to ISO string
                    if isinstance(start_time, datetime):
                        commence = start_time.isoformat()
                    elif isinstance(start_time, str):
                        commence = start_time
                    else:
                        commence = str(start_time) if start_time else ""

                    sport = _detect_sport(event_name, comp_name)
                    if sport == "other" and sport_label == "esports":
                        sport = "cs2"  # default esports to cs2

                    # Build runner name lookup from catalogue
                    runner_names: dict[int, str] = {}
                    for cr in cat_runners:
                        if isinstance(cr, dict):
                            rid = cr.get("selectionId", 0)
                            rname = cr.get("runnerName", "")
                        else:
                            rid = getattr(cr, "selection_id", 0)
                            rname = getattr(cr, "runner_name", "")
                        if rid and rname:
                            runner_names[rid] = rname

                    # Only process markets with exactly 2 runners (H2H)
                    if len(runners) != 2:
                        continue

                    for runner in runners:
                        if isinstance(runner, dict):
                            sel_id = runner.get("selectionId", 0)
                            r_status = runner.get("status", "ACTIVE")
                            ex = runner.get("ex", {})
                        else:
                            sel_id = getattr(runner, "selection_id", 0)
                            r_status = getattr(runner, "status", "ACTIVE")
                            ex = getattr(runner, "ex", {})

                        if r_status != "ACTIVE":
                            continue

                        # Get best back price
                        if isinstance(ex, dict):
                            backs = ex.get("availableToBack", [])
                        else:
                            backs = getattr(ex, "available_to_back", [])

                        if not backs:
                            continue

                        best_back = backs[0]
                        if isinstance(best_back, dict):
                            back_odds = best_back.get("price", 0)
                            back_size = best_back.get("size", 0)
                        else:
                            back_odds = getattr(best_back, "price", 0)
                            back_size = getattr(best_back, "size", 0)

                        if back_odds <= 1.01 or back_odds >= 100:
                            continue

                        team_name = runner_names.get(sel_id, f"Selection {sel_id}")
                        actual_price = 1.0 / back_odds

                        outcomes.append(BetfairOutcome(
                            event_name=event_name,
                            team_name=team_name,
                            back_price=back_odds,
                            back_size=back_size,
                            actual_price=actual_price,
                            sport=sport,
                            market_id=mid,
                            selection_id=sel_id,
                            commence_time=commence,
                        ))

        except Exception:
            logger.exception("Failed to fetch Betfair markets for %s", sport_label)
            continue

    logger.info("Fetched %d Betfair outcomes", len(outcomes))
    return outcomes
