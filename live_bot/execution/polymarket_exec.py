"""Polymarket order execution via py-clob-client."""

import asyncio
import logging
import time

from live_bot.config import (
    POLYMARKET_PRIVATE_KEY,
    POLYMARKET_FUNDER_ADDRESS,
    POLYMARKET_CHAIN_ID,
    SIMULATION_MODE,
)

logger = logging.getLogger(__name__)


class PolymarketExecutor:
    """Handles order placement on Polymarket's CLOB."""

    def __init__(self, simulation: bool = True):
        self.simulation = simulation
        self._client = None
        self._initialized = False

    def _init_client(self) -> bool:
        """Initialize the CLOB client (lazy, only when first needed)."""
        if self._initialized:
            return self._client is not None

        self._initialized = True

        if self.simulation:
            logger.info("Polymarket executor in SIMULATION mode — no client needed")
            return True

        if not POLYMARKET_PRIVATE_KEY or not POLYMARKET_FUNDER_ADDRESS:
            logger.error(
                "Polymarket credentials missing — set POLYMARKET_PRIVATE_KEY and "
                "POLYMARKET_FUNDER_ADDRESS in .env"
            )
            return False

        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                "https://clob.polymarket.com",
                key=POLYMARKET_PRIVATE_KEY,
                chain_id=POLYMARKET_CHAIN_ID,
                signature_type=1,  # POLY_PROXY
                funder=POLYMARKET_FUNDER_ADDRESS,
            )
            # Derive API credentials
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            logger.info("Polymarket CLOB client initialized (LIVE mode)")
            return True

        except Exception:
            logger.exception("Failed to initialize Polymarket client")
            return False

    async def place_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str = "BUY",
    ) -> tuple[bool, dict]:
        """Place a Fill-or-Kill order on Polymarket.

        Args:
            token_id: CTF ERC1155 token ID
            price: Limit price (0-1)
            size: Number of shares
            side: "BUY" or "SELL"

        Returns:
            (filled, details) — filled is True if order executed
        """
        start = time.time()

        if not self._init_client():
            return False, {"error": "Client not initialized"}

        if self.simulation:
            # Simulate: assume fill if price is reasonable
            latency = (time.time() - start) * 1000
            logger.info(
                "SIM Polymarket %s %s: %.0f shares @ %.0f¢ (%.0fms)",
                side, token_id[:16], size, price * 100, latency,
            )
            return True, {
                "simulated": True,
                "token_id": token_id,
                "price": price,
                "size": size,
                "side": side,
                "latency_ms": latency,
            }

        # Live execution
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType

            side_const = None
            try:
                from py_clob_client.order_builder.constants import BUY, SELL
                side_const = BUY if side.upper() == "BUY" else SELL
            except ImportError:
                side_const = side.upper()

            result = await asyncio.to_thread(
                self._client.create_and_post_order,
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=side_const,
                ),
                OrderType.FOK,
            )

            latency = (time.time() - start) * 1000

            # Check fill status
            filled = False
            if isinstance(result, dict):
                status = result.get("status", "").lower()
                filled = status in ("matched", "filled", "executed")
            elif hasattr(result, "status"):
                filled = result.status in ("matched", "filled", "executed")

            logger.info(
                "LIVE Polymarket %s %s: %.0f shares @ %.0f¢ — %s (%.0fms)",
                side, token_id[:16], size, price * 100,
                "FILLED" if filled else "REJECTED", latency,
            )

            return filled, {
                "simulated": False,
                "result": str(result),
                "latency_ms": latency,
            }

        except Exception as e:
            latency = (time.time() - start) * 1000
            logger.error("Polymarket order failed: %s (%.0fms)", e, latency)
            return False, {"error": str(e), "latency_ms": latency}
