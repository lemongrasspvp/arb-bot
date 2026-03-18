"""Pinnacle guest API client — fetches esports odds directly."""

import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from arb_scanner.config import (
    MAX_RETRIES,
    RETRY_BACKOFF,
    RATE_LIMIT_DELAY,
)

logger = logging.getLogger(__name__)

PINNACLE_BASE = "https://guest.api.arcadia.pinnacle.com/0.1"
PINNACLE_API_KEY = "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R"

# Sport IDs and league name filters for each sport category
SPORT_CONFIGS = [
    # (sport_id, league_prefix_filters)
    (12, ["CS2", "League of Legends", "Dota 2", "Valorant", "Call of Duty"]),  # Esports
    (22, ["UFC"]),                                                               # MMA
    (33, ["ATP", "WTA"]),                                                        # Tennis
    (4, ["NCAA", "Europe - Euroleague"]),                                          # Basketball (Australian NBL not on Pinnacle)
]

HEADERS = {
    "X-API-Key": PINNACLE_API_KEY,
    "Referer": "https://www.pinnacle.com/",
    "Accept": "application/json",
}


@dataclass
class PinnacleOutcome:
    """A single outcome from a Pinnacle market."""
    event_name: str
    sport: str
    outcome_name: str         # team name (moneyline) or "Over 2.5" / "Under 2.5" (totals)
    raw_price: float          # decimal odds
    implied_prob: float       # before vig removal
    no_vig_prob: float        # after vig removal
    commence_time: str
    market_type: str = "moneyline"  # "moneyline" or "totals"
    handicap: float = 0.0           # the line (e.g. 2.5 for over/under 2.5 maps)


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _american_to_decimal(american: float) -> float:
    """Convert American odds to decimal odds."""
    if american > 0:
        return (american / 100) + 1
    return (100 / abs(american)) + 1


def _decimal_to_implied(decimal_odds: float) -> float:
    """Convert decimal odds to implied probability."""
    if decimal_odds <= 0:
        return 0.0
    return 1.0 / decimal_odds


def _remove_vig(probabilities: list[float]) -> list[float]:
    """Remove vig by normalizing probabilities so they sum to 1.0."""
    total = sum(probabilities)
    if total == 0:
        return probabilities
    return [p / total for p in probabilities]


def _league_to_sport(league_name: str) -> str:
    """Map a Pinnacle league name to our internal sport label."""
    ln = league_name
    if ln.startswith("CS2"):
        return "cs2"
    if ln.startswith("Dota"):
        return "dota2"
    if ln.startswith("Valorant"):
        return "valorant"
    if ln.startswith("Call of Duty"):
        return "cod"
    if ln.startswith("League of Legends"):
        return "lol"
    if ln.startswith("UFC"):
        return "ufc"
    if ln.startswith("ATP"):
        return "tennis"
    if ln.startswith("WTA"):
        return "tennis"
    if ln.startswith("NCAA"):
        return "ncaab"
    if "Euroleague" in ln:
        return "euroleague"
    return "other"


def fetch_odds() -> list[PinnacleOutcome]:
    """Fetch odds from Pinnacle's guest API across all configured sports."""
    session = _build_session()
    outcomes: list[PinnacleOutcome] = []

    target_leagues: list[tuple[int, str]] = []

    for sport_id, filters in SPORT_CONFIGS:
        try:
            time.sleep(RATE_LIMIT_DELAY)
            resp = session.get(
                f"{PINNACLE_BASE}/sports/{sport_id}/leagues",
                params={"all": "false"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("Failed to fetch leagues for sport %d (status %d)", sport_id, resp.status_code)
                continue

            all_leagues = resp.json()
        except requests.RequestException:
            logger.exception("Failed to fetch leagues for sport %d", sport_id)
            continue

        for lg in all_leagues:
            if not isinstance(lg, dict):
                continue
            name = lg.get("name", "")
            lg_id = lg.get("id")
            if lg_id and any(name.startswith(f) for f in filters):
                target_leagues.append((lg_id, name))

        logger.info("Pinnacle sport %d: %d target leagues (of %d total)", sport_id, len(target_leagues), len(all_leagues))

    for league_id, league_name in target_leagues:
        sport_label = _league_to_sport(league_name)
        try:
            # Step 2: Get matchups for this league
            time.sleep(RATE_LIMIT_DELAY)
            matchups_resp = session.get(
                f"{PINNACLE_BASE}/leagues/{league_id}/matchups",
                timeout=15,
            )
            if matchups_resp.status_code != 200:
                continue
            matchups = matchups_resp.json()

            # Build matchup lookup: id -> {name, startTime}
            matchup_info: dict[int, dict] = {}
            now = datetime.now(timezone.utc)
            cutoff = now + timedelta(minutes=5)

            for mu in matchups:
                if not isinstance(mu, dict):
                    continue
                mu_id = mu.get("id")
                if not mu_id:
                    continue

                # Skip live games
                if mu.get("isLive"):
                    continue

                # Skip games starting within 5 minutes
                start_str = mu.get("startTime", "")
                if start_str:
                    try:
                        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                        if start_dt <= cutoff:
                            continue
                    except ValueError:
                        pass

                participants = mu.get("participants", [])
                if len(participants) < 2:
                    continue
                home = next((p.get("name", "") for p in participants if p.get("alignment") == "home"), "")
                away = next((p.get("name", "") for p in participants if p.get("alignment") == "away"), "")
                if not home or not away:
                    home = participants[0].get("name", "")
                    away = participants[1].get("name", "")
                matchup_info[mu_id] = {
                    "name": f"{home} vs {away}",
                    "home": home,
                    "away": away,
                    "start": mu.get("startTime", ""),
                }

            # Step 3: Get straight markets (moneyline odds)
            time.sleep(RATE_LIMIT_DELAY)
            markets_resp = session.get(
                f"{PINNACLE_BASE}/leagues/{league_id}/markets/straight",
                timeout=15,
            )
            if markets_resp.status_code != 200:
                continue
            markets = markets_resp.json()

            # Group moneyline prices by matchup
            ml_by_matchup: dict[int, list[dict]] = {}
            for mkt in markets:
                if not isinstance(mkt, dict):
                    continue
                if mkt.get("type") != "moneyline":
                    continue
                if mkt.get("period") != 0:  # full match only
                    continue
                mu_id = mkt.get("matchupId")
                if mu_id and mu_id in matchup_info:
                    for price_obj in mkt.get("prices", []):
                        ml_by_matchup.setdefault(mu_id, []).append(price_obj)

            # Convert moneyline to outcomes
            for mu_id, prices in ml_by_matchup.items():
                if len(prices) < 2:
                    continue

                info = matchup_info[mu_id]
                team_prices = []
                for p in prices:
                    designation = p.get("designation", "")
                    if designation == "home":
                        name = info["home"]
                    elif designation == "away":
                        name = info["away"]
                    else:
                        name = designation
                    american_odds = p.get("price", 0)
                    dec = _american_to_decimal(american_odds)
                    team_prices.append((name, dec, _decimal_to_implied(dec)))

                implied_probs = [tp[2] for tp in team_prices]
                no_vig = _remove_vig(implied_probs)

                for i, (name, dec, imp) in enumerate(team_prices):
                    outcomes.append(PinnacleOutcome(
                        event_name=info["name"],
                        sport=sport_label,
                        outcome_name=name,
                        raw_price=dec,
                        implied_prob=imp,
                        no_vig_prob=no_vig[i] if i < len(no_vig) else imp,
                        commence_time=info["start"],
                    ))

            # ── Totals (over/under) ──
            # Pinnacle totals: type="total", period=0 (full match).
            # The line (handicap) is in prices[].points, not a top-level field.
            # Only take non-alternate, period 0 (full match) totals.
            totals_by_key: dict[tuple[int, float], list[dict]] = {}
            for mkt in markets:
                if not isinstance(mkt, dict):
                    continue
                if mkt.get("type") != "total":
                    continue
                if mkt.get("period") != 0:  # full match only
                    continue
                if mkt.get("isAlternate", False):
                    continue
                mu_id = mkt.get("matchupId")
                if not mu_id or mu_id not in matchup_info:
                    continue
                # Extract handicap from prices[0].points
                prices = mkt.get("prices", [])
                if not prices:
                    continue
                handicap = prices[0].get("points")
                if handicap is None:
                    continue
                handicap = float(handicap)
                # Skip whole-number handicaps (push risk makes de-vig unreliable)
                if handicap == int(handicap):
                    continue
                for price_obj in prices:
                    totals_by_key.setdefault((mu_id, handicap), []).append(price_obj)

            for (mu_id, handicap), prices in totals_by_key.items():
                # Need exactly over + under
                over_prices = [p for p in prices if p.get("designation") == "over"]
                under_prices = [p for p in prices if p.get("designation") == "under"]
                if not over_prices or not under_prices:
                    continue

                info = matchup_info[mu_id]
                handicap_str = f"{handicap:g}"  # "2.5", not "2.50"

                ou_prices = []
                for label, p_list in [("Over", over_prices), ("Under", under_prices)]:
                    p = p_list[0]
                    american_odds = p.get("price", 0)
                    dec = _american_to_decimal(american_odds)
                    ou_prices.append((f"{label} {handicap_str}", dec, _decimal_to_implied(dec)))

                implied_probs = [tp[2] for tp in ou_prices]
                no_vig = _remove_vig(implied_probs)

                for i, (name, dec, imp) in enumerate(ou_prices):
                    outcomes.append(PinnacleOutcome(
                        event_name=info["name"],
                        sport=sport_label,
                        outcome_name=name,
                        raw_price=dec,
                        implied_prob=imp,
                        no_vig_prob=no_vig[i] if i < len(no_vig) else imp,
                        commence_time=info["start"],
                        market_type="totals",
                        handicap=handicap,
                    ))

        except requests.RequestException:
            logger.debug("Failed to fetch matchups/markets for league %d", league_id)
            continue

    totals_count = sum(1 for o in outcomes if o.market_type == "totals")
    logger.info("Fetched %d Pinnacle outcomes (%d moneyline, %d totals)",
                len(outcomes), len(outcomes) - totals_count, totals_count)
    return outcomes


def fetch_live_odds() -> list[PinnacleOutcome]:
    """Fetch LIVE in-game odds from Pinnacle's guest API.

    Same API as fetch_odds() but inverted filter: only returns matchups
    where isLive=True. These odds update in real-time as matches progress
    and serve as a fresh truth oracle for midgame value betting.
    """
    session = _build_session()
    outcomes: list[PinnacleOutcome] = []

    target_leagues: list[tuple[int, str]] = []

    for sport_id, filters in SPORT_CONFIGS:
        try:
            time.sleep(RATE_LIMIT_DELAY)
            resp = session.get(
                f"{PINNACLE_BASE}/sports/{sport_id}/leagues",
                params={"all": "false"},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            all_leagues = resp.json()
        except requests.RequestException:
            continue

        for lg in all_leagues:
            if not isinstance(lg, dict):
                continue
            name = lg.get("name", "")
            lg_id = lg.get("id")
            if lg_id and any(name.startswith(f) for f in filters):
                target_leagues.append((lg_id, name))

    for league_id, league_name in target_leagues:
        sport_label = _league_to_sport(league_name)
        try:
            time.sleep(RATE_LIMIT_DELAY)
            matchups_resp = session.get(
                f"{PINNACLE_BASE}/leagues/{league_id}/matchups",
                timeout=15,
            )
            if matchups_resp.status_code != 200:
                continue
            matchups = matchups_resp.json()

            # Only keep LIVE matchups (opposite of fetch_odds)
            matchup_info: dict[int, dict] = {}
            for mu in matchups:
                if not isinstance(mu, dict):
                    continue
                mu_id = mu.get("id")
                if not mu_id:
                    continue

                # Only live games
                if not mu.get("isLive"):
                    continue

                participants = mu.get("participants", [])
                if len(participants) < 2:
                    continue
                home = next((p.get("name", "") for p in participants if p.get("alignment") == "home"), "")
                away = next((p.get("name", "") for p in participants if p.get("alignment") == "away"), "")
                if not home or not away:
                    home = participants[0].get("name", "")
                    away = participants[1].get("name", "")
                matchup_info[mu_id] = {
                    "name": f"{home} vs {away}",
                    "home": home,
                    "away": away,
                    "start": mu.get("startTime", ""),
                }

            if not matchup_info:
                continue

            # Get straight markets (moneyline odds)
            time.sleep(RATE_LIMIT_DELAY)
            markets_resp = session.get(
                f"{PINNACLE_BASE}/leagues/{league_id}/markets/straight",
                timeout=15,
            )
            if markets_resp.status_code != 200:
                continue
            markets = markets_resp.json()

            # Group moneyline prices by matchup
            ml_by_matchup: dict[int, list[dict]] = {}
            for mkt in markets:
                if not isinstance(mkt, dict):
                    continue
                if mkt.get("type") != "moneyline":
                    continue
                if mkt.get("period") != 0:  # full match only
                    continue
                mu_id = mkt.get("matchupId")
                if mu_id and mu_id in matchup_info:
                    for price_obj in mkt.get("prices", []):
                        ml_by_matchup.setdefault(mu_id, []).append(price_obj)

            # Convert to outcomes
            for mu_id, prices in ml_by_matchup.items():
                if len(prices) < 2:
                    continue

                info = matchup_info[mu_id]
                team_prices = []
                for p in prices:
                    designation = p.get("designation", "")
                    if designation == "home":
                        name = info["home"]
                    elif designation == "away":
                        name = info["away"]
                    else:
                        name = designation
                    american_odds = p.get("price", 0)
                    dec = _american_to_decimal(american_odds)
                    team_prices.append((name, dec, _decimal_to_implied(dec)))

                implied_probs = [tp[2] for tp in team_prices]
                no_vig = _remove_vig(implied_probs)

                for i, (name, dec, imp) in enumerate(team_prices):
                    outcomes.append(PinnacleOutcome(
                        event_name=info["name"],
                        sport=sport_label,
                        outcome_name=name,
                        raw_price=dec,
                        implied_prob=imp,
                        no_vig_prob=no_vig[i] if i < len(no_vig) else imp,
                        commence_time=info["start"],
                    ))

        except requests.RequestException:
            continue

    logger.info("Fetched %d Pinnacle LIVE outcomes", len(outcomes))
    return outcomes
