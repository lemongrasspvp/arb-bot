"""Polymarket CLOB WebSocket feed — real-time orderbook updates."""

import asyncio
import json
import logging
import ssl
import time

import certifi
import websockets

from live_bot.config import POLYMARKET_WS_URL

# Build SSL context using certifi certificates
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

logger = logging.getLogger(__name__)

# Heartbeat interval (seconds)
PING_INTERVAL = 10


async def polymarket_feed(
    token_ids: list[str],
    price_queue: asyncio.Queue,
    shutdown_event: asyncio.Event | None = None,
    new_tokens_queue: asyncio.Queue | None = None,
) -> None:
    """Connect to Polymarket CLOB WebSocket and stream price updates.

    Subscribes to the given token IDs and pushes PriceUpdate dicts
    into the shared price_queue whenever best ask/bid changes.

    If new_tokens_queue is provided, periodically checks it for new token IDs
    to subscribe to (from registry refresh).
    """
    if not token_ids:
        logger.warning("No Polymarket token IDs to subscribe — feed idle")
        # Even with no initial tokens, wait for new ones
        if not new_tokens_queue:
            return

    subscribed_ids = set(token_ids)
    backoff = 1
    while not (shutdown_event and shutdown_event.is_set()):
        try:
            logger.info("Connecting to Polymarket WebSocket (%d tokens)...", len(subscribed_ids))
            async with websockets.connect(POLYMARKET_WS_URL, ping_interval=PING_INTERVAL, ssl=_ssl_ctx) as ws:
                # Subscribe to market channel for all token IDs
                if subscribed_ids:
                    sub_msg = json.dumps({
                        "assets_ids": list(subscribed_ids),
                        "type": "market",
                    })
                    await ws.send(sub_msg)
                logger.info("Polymarket WS subscribed to %d tokens", len(subscribed_ids))
                backoff = 1  # reset on successful connection

                async for raw_msg in ws:
                    if shutdown_event and shutdown_event.is_set():
                        break

                    # Check for new tokens to subscribe to
                    if new_tokens_queue:
                        new_ids = []
                        while not new_tokens_queue.empty():
                            try:
                                new_id = new_tokens_queue.get_nowait()
                                if new_id not in subscribed_ids:
                                    new_ids.append(new_id)
                                    subscribed_ids.add(new_id)
                            except asyncio.QueueEmpty:
                                break
                        if new_ids:
                            add_sub = json.dumps({
                                "assets_ids": new_ids,
                                "type": "market",
                            })
                            await ws.send(add_sub)
                            logger.info("Polymarket WS subscribed to %d NEW tokens (total: %d)", len(new_ids), len(subscribed_ids))

                    try:
                        data = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    # Process book updates
                    updates = _parse_book_update(data)
                    for update in updates:
                        await price_queue.put(update)

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            logger.warning("Polymarket WS disconnected: %s — reconnecting in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except asyncio.CancelledError:
            logger.info("Polymarket feed cancelled")
            return
        except Exception:
            logger.exception("Unexpected error in Polymarket feed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def _parse_book_update(data: dict) -> list[dict]:
    """Parse a Polymarket WS message into PriceUpdate dicts.

    The CLOB WebSocket sends various event types. We care about:
    - 'book' events: contain bids/asks arrays
    - 'price_change' events: contain price/side info
    """
    updates = []
    # Handle array-format messages (batch updates)
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                updates.extend(_parse_book_update(item))
        return updates

    event_type = data.get("event_type", "")

    if event_type == "book":
        # Full or delta book update
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return []

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        best_ask, ask_top_size, ask_depth_usd = _best_price_and_size(asks, side="ask")
        best_bid, bid_top_size, bid_depth_usd = _best_price_and_size(bids, side="bid")

        if best_ask > 0 or best_bid > 0:
            # Parse raw levels for VWAP computation at trade time
            ask_levels = _parse_levels(asks)
            bid_levels = _parse_levels(bids)
            updates.append({
                "platform": "polymarket",
                "market_id": asset_id,
                "best_ask": best_ask,
                "best_bid": best_bid,
                "ask_size": ask_top_size,       # shares at best ask
                "bid_size": bid_top_size,        # shares at best bid
                "ask_depth_usd": ask_depth_usd,  # total USD across all ask levels
                "bid_depth_usd": bid_depth_usd,  # total USD across all bid levels
                "ask_levels": ask_levels,        # [(price, size), ...] sorted best→worst
                "bid_levels": bid_levels,        # [(price, size), ...] sorted best→worst
                "no_vig_prob": 0.0,
                "timestamp": time.time(),
            })

    elif event_type == "price_change":
        # Simplified price update
        asset_id = data.get("asset_id", "")
        price = float(data.get("price", 0))
        side = data.get("side", "")

        if asset_id and price > 0:
            update = {
                "platform": "polymarket",
                "market_id": asset_id,
                "best_ask": price if side == "sell" else 0.0,
                "best_bid": price if side == "buy" else 0.0,
                "no_vig_prob": 0.0,
                "timestamp": time.time(),
            }
            updates.append(update)

    elif event_type == "last_trade_price":
        # Not directly useful for arb detection but can log
        pass

    return updates


def _parse_levels(levels: list) -> list[tuple[float, float]]:
    """Parse raw order book levels into [(price, size), ...] list."""
    parsed = []
    for level in levels:
        try:
            if isinstance(level, dict):
                p = float(level.get("price", 0))
                s = float(level.get("size", 0))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                p = float(level[0])
                s = float(level[1])
            else:
                continue
            if p > 0 and s > 0:
                parsed.append((p, s))
        except (ValueError, TypeError):
            continue
    return parsed


def _best_price_and_size(levels: list, side: str) -> tuple[float, float, float]:
    """Extract best price, size at best, and total USD depth across all levels.

    For asks: cheapest (lowest price)
    For bids: most expensive (highest price)

    Returns (best_price, size_at_best, total_depth_usd).
    - size_at_best: shares available at the best price level only
    - total_depth_usd: sum of (price × size) across ALL levels — true USD liquidity
    """
    if not levels:
        return 0.0, 0.0, 0.0

    try:
        parsed = []
        for level in levels:
            if isinstance(level, dict):
                p = float(level.get("price", 0))
                s = float(level.get("size", 0))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                p = float(level[0])
                s = float(level[1])
            elif isinstance(level, (list, tuple)) and len(level) == 1:
                p = float(level[0])
                s = 0.0
            else:
                continue
            if p > 0:
                parsed.append((p, s))

        if not parsed:
            return 0.0, 0.0, 0.0

        if side == "ask":
            best = min(parsed, key=lambda x: x[0])
        else:
            best = max(parsed, key=lambda x: x[0])

        # Total USD depth across ALL levels (price × size per level)
        total_depth_usd = sum(p * s for p, s in parsed)

        return best[0], best[1], total_depth_usd
    except (ValueError, TypeError):
        return 0.0, 0.0, 0.0
