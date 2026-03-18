"""Kalshi WebSocket feed — real-time ticker updates."""

import asyncio
import base64
import json
import logging
import ssl
import time

import certifi
import websockets

from live_bot.config import KALSHI_WS_URL, KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH

# Build SSL context using certifi certificates
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

logger = logging.getLogger(__name__)


def _load_private_key():
    """Load RSA private key from PEM file for WebSocket auth."""
    if not KALSHI_PRIVATE_KEY_PATH:
        return None
    try:
        from cryptography.hazmat.primitives import serialization
        with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)
    except Exception:
        logger.exception("Failed to load Kalshi private key from %s", KALSHI_PRIVATE_KEY_PATH)
        return None


def _sign_ws_request() -> dict[str, str]:
    """Generate auth headers for Kalshi WebSocket connection."""
    if not KALSHI_API_KEY_ID or not KALSHI_PRIVATE_KEY_PATH:
        return {}

    private_key = _load_private_key()
    if not private_key:
        return {}

    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}GET/trade-api/ws/v2"
        signature = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        encoded = base64.b64encode(signature).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": encoded,
        }
    except Exception:
        logger.exception("Failed to sign Kalshi WS request")
        return {}


async def kalshi_feed(
    tickers: list[str],
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
    new_tickers_queue: asyncio.Queue | None = None,
) -> None:
    """Stream Kalshi price updates.

    Uses WebSocket if API credentials are available, otherwise falls back
    to REST polling (Kalshi WS requires auth for all channels).

    If new_tickers_queue is provided, periodically checks it for new tickers
    to subscribe to (from registry refresh).
    """
    if not tickers and not new_tickers_queue:
        logger.warning("No Kalshi tickers to subscribe — feed idle")
        return

    has_creds = bool(KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH)

    if has_creds:
        await _kalshi_ws_feed(tickers, price_queue, shutdown_event, new_tickers_queue)
    else:
        logger.info("No Kalshi API keys — falling back to REST polling (every 15s)")
        await _kalshi_rest_feed(tickers, price_queue, shutdown_event, new_tickers_queue)


async def _kalshi_ws_feed(
    tickers: list[str],
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
    new_tickers_queue: asyncio.Queue | None = None,
) -> None:
    """WebSocket-based Kalshi feed (requires API credentials)."""
    subscribed_tickers = set(tickers)
    backoff = 1
    while not (shutdown_event and shutdown_event.is_set()):
        try:
            extra_headers = _sign_ws_request()

            logger.info("Connecting to Kalshi WebSocket (%d tickers)...", len(subscribed_tickers))
            async with websockets.connect(
                KALSHI_WS_URL,
                additional_headers=extra_headers,
                ping_interval=20,
                ssl=_ssl_ctx,
            ) as ws:
                if subscribed_tickers:
                    sub_msg = json.dumps({
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": list(subscribed_tickers),
                        },
                    })
                    await ws.send(sub_msg)
                logger.info("Kalshi WS subscribed to %d tickers", len(subscribed_tickers))
                backoff = 1

                async for raw_msg in ws:
                    if shutdown_event and shutdown_event.is_set():
                        break

                    # Check for new tickers to subscribe to
                    if new_tickers_queue:
                        new_ids = []
                        while not new_tickers_queue.empty():
                            try:
                                new_id = new_tickers_queue.get_nowait()
                                if new_id not in subscribed_tickers:
                                    new_ids.append(new_id)
                                    subscribed_tickers.add(new_id)
                            except asyncio.QueueEmpty:
                                break
                        if new_ids:
                            add_sub = json.dumps({
                                "id": 2,
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["ticker"],
                                    "market_tickers": new_ids,
                                },
                            })
                            await ws.send(add_sub)
                            logger.info("Kalshi WS subscribed to %d NEW tickers (total: %d)", len(new_ids), len(subscribed_tickers))

                    try:
                        data = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    update = _parse_ticker_update(data)
                    if update:
                        await price_queue.put(update)

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            logger.warning("Kalshi WS disconnected: %s — reconnecting in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except asyncio.CancelledError:
            logger.info("Kalshi feed cancelled")
            return
        except Exception:
            logger.exception("Unexpected error in Kalshi feed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def _kalshi_rest_feed(
    tickers: list[str],
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
    new_tickers_queue: asyncio.Queue | None = None,
) -> None:
    """REST polling fallback for Kalshi (no auth needed for public orderbook)."""
    import requests as req

    KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
    session = req.Session()
    session.headers["Accept"] = "application/json"
    poll_interval = 15  # seconds
    active_tickers = list(tickers)

    while not (shutdown_event and shutdown_event.is_set()):
        # Check for new tickers from registry refresh
        if new_tickers_queue:
            while not new_tickers_queue.empty():
                try:
                    new_id = new_tickers_queue.get_nowait()
                    if new_id not in active_tickers:
                        active_tickers.append(new_id)
                        logger.info("Kalshi REST added new ticker: %s (total: %d)", new_id, len(active_tickers))
                except asyncio.QueueEmpty:
                    break

        try:
            for ticker in active_tickers:
                if shutdown_event and shutdown_event.is_set():
                    break
                try:
                    resp = session.get(f"{KALSHI_BASE}/markets/{ticker}", timeout=8)
                    if resp.status_code != 200:
                        continue
                    market = resp.json().get("market", {})
                    yes_ask = market.get("yes_ask_dollars") or market.get("yes_ask", 0)
                    yes_bid = market.get("yes_bid_dollars") or market.get("yes_bid", 0)

                    if isinstance(yes_ask, int) and yes_ask > 1:
                        yes_ask = yes_ask / 100.0
                    if isinstance(yes_bid, int) and yes_bid > 1:
                        yes_bid = yes_bid / 100.0

                    yes_ask = float(yes_ask) if yes_ask else 0.0
                    yes_bid = float(yes_bid) if yes_bid else 0.0

                    # Get available size at best ask/bid
                    ask_size = float(market.get("yes_ask_size_fp", 0) or 0)
                    bid_size = float(market.get("yes_bid_size_fp", 0) or 0)

                    if yes_ask > 0 or yes_bid > 0:
                        await price_queue.put({
                            "platform": "kalshi",
                            "market_id": ticker,
                            "best_ask": yes_ask,
                            "best_bid": yes_bid,
                            "ask_size": ask_size,
                            "bid_size": bid_size,
                            "no_vig_prob": 0.0,
                            "timestamp": time.time(),
                        })

                    # Small delay between individual ticker requests
                    await asyncio.sleep(0.3)
                except (req.RequestException, ValueError, KeyError):
                    continue

            logger.debug("Kalshi REST poll complete: %d tickers", len(tickers))

        except asyncio.CancelledError:
            logger.info("Kalshi REST feed cancelled")
            return
        except Exception:
            logger.exception("Error in Kalshi REST poll")

        # Wait for next poll cycle
        try:
            await asyncio.wait_for(
                shutdown_event.wait() if shutdown_event else asyncio.sleep(poll_interval),
                timeout=poll_interval,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass


def _parse_ticker_update(data: dict) -> dict | None:
    """Parse a Kalshi WS ticker message into a PriceUpdate dict.

    Ticker messages have format:
    {
        "type": "ticker",
        "msg": {
            "market_ticker": "...",
            "yes_bid_dollars": 0.45,
            "yes_ask_dollars": 0.55,
            ...
        }
    }
    """
    msg_type = data.get("type", "")

    if msg_type != "ticker":
        return None

    msg = data.get("msg", {})
    ticker = msg.get("market_ticker", "")
    if not ticker:
        return None

    # Try dollar-denominated fields first, then cents
    yes_ask = msg.get("yes_ask_dollars") or msg.get("yes_ask", 0)
    yes_bid = msg.get("yes_bid_dollars") or msg.get("yes_bid", 0)

    # Convert cents to dollars if needed (cents are integers 1-99)
    if isinstance(yes_ask, int) and yes_ask > 1:
        yes_ask = yes_ask / 100.0
    if isinstance(yes_bid, int) and yes_bid > 1:
        yes_bid = yes_bid / 100.0

    yes_ask = float(yes_ask) if yes_ask else 0.0
    yes_bid = float(yes_bid) if yes_bid else 0.0

    if yes_ask <= 0 and yes_bid <= 0:
        return None

    return {
        "platform": "kalshi",
        "market_id": ticker,
        "best_ask": yes_ask,
        "best_bid": yes_bid,
        "no_vig_prob": 0.0,
        "timestamp": time.time(),
    }
