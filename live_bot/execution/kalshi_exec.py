"""Kalshi order execution via REST API with RSA authentication."""

import asyncio
import base64
import logging
import time
import uuid

import requests

from live_bot.config import (
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_PRIVATE_KEY_CONTENT,
    KALSHI_REST_BASE,
    SIMULATION_MODE,
)

logger = logging.getLogger(__name__)


class KalshiExecutor:
    """Handles order placement on Kalshi's exchange."""

    def __init__(self, simulation: bool = True):
        self.simulation = simulation
        self._private_key = None
        self._session = None
        self._initialized = False

    def _init_client(self) -> bool:
        """Load RSA key and prepare session (lazy init)."""
        if self._initialized:
            return self._private_key is not None or self.simulation

        self._initialized = True

        if self.simulation:
            logger.info("Kalshi executor in SIMULATION mode")
            return True

        if not KALSHI_API_KEY_ID or (not KALSHI_PRIVATE_KEY_PATH and not KALSHI_PRIVATE_KEY_CONTENT):
            logger.error(
                "Kalshi credentials missing — set KALSHI_API_KEY_ID and "
                "KALSHI_PRIVATE_KEY_PATH (or KALSHI_PRIVATE_KEY_CONTENT) in .env"
            )
            return False

        try:
            from cryptography.hazmat.primitives import serialization

            if KALSHI_PRIVATE_KEY_CONTENT:
                # Load key from env var content (for cloud deployment)
                key_bytes = KALSHI_PRIVATE_KEY_CONTENT.encode("utf-8")
                self._private_key = serialization.load_pem_private_key(
                    key_bytes, password=None
                )
            else:
                with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
                    self._private_key = serialization.load_pem_private_key(
                        f.read(), password=None
                    )

            self._session = requests.Session()
            self._session.headers["Content-Type"] = "application/json"
            logger.info("Kalshi REST client initialized (LIVE mode)")
            return True

        except Exception:
            logger.exception("Failed to initialize Kalshi client")
            return False

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Generate RSA-PSS signed auth headers for a Kalshi API request."""
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method}{path}"

        signature = self._private_key.sign(
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

    async def place_order(
        self,
        ticker: str,
        side: str,
        price: float,
        count: int,
        action: str = "buy",
    ) -> tuple[bool, dict]:
        """Place a Fill-or-Kill order on Kalshi.

        Args:
            ticker: Market ticker (e.g. "KXUFCFIGHT-26MAR14-EVLOEV")
            side: "yes" or "no"
            price: Price in dollars (0-1)
            count: Number of contracts (integer)
            action: "buy" or "sell"

        Returns:
            (filled, details) — filled is True if order executed
        """
        # Defense-in-depth: hard block when trading is globally disabled
        from live_bot.config import DISABLE_LIVE_TRADING
        if DISABLE_LIVE_TRADING:
            return False, {"error": "DISABLE_LIVE_TRADING is active"}

        start = time.time()

        if not self._init_client():
            return False, {"error": "Client not initialized"}

        if self.simulation:
            latency = (time.time() - start) * 1000
            logger.info(
                "SIM Kalshi %s %s %s: %d contracts @ %.0f¢ (%.0fms)",
                action, side, ticker, count, price * 100, latency,
            )
            return True, {
                "simulated": True,
                "ticker": ticker,
                "side": side,
                "price": price,
                "count": count,
                "latency_ms": latency,
            }

        # Live execution
        try:
            path = "/trade-api/v2/portfolio/orders"
            auth_headers = self._sign_request("POST", path)

            # Build order payload
            order = {
                "ticker": ticker,
                "side": side,
                "action": action,
                "count": count,
                "type": "limit",
                "time_in_force": "fill_or_kill",
                "client_order_id": str(uuid.uuid4()),
            }

            # Use dollar-denominated price fields
            if side == "yes":
                order["yes_price_dollars"] = f"{price:.6f}"
            else:
                order["no_price_dollars"] = f"{price:.6f}"

            result = await asyncio.to_thread(
                self._session.post,
                f"{KALSHI_REST_BASE}/portfolio/orders",
                json=order,
                headers=auth_headers,
                timeout=10,
            )

            latency = (time.time() - start) * 1000

            if result.status_code in (200, 201):
                data = result.json()
                order_data = data.get("order", data)
                status = order_data.get("status", "").lower()
                filled = status in ("executed", "filled")

                logger.info(
                    "LIVE Kalshi %s %s %s: %d contracts @ %.0f¢ — %s (%.0fms)",
                    action, side, ticker, count, price * 100,
                    status.upper(), latency,
                )

                return filled, {
                    "simulated": False,
                    "status": status,
                    "order_id": order_data.get("order_id", ""),
                    "latency_ms": latency,
                }
            else:
                logger.error(
                    "Kalshi order rejected: %d %s (%.0fms)",
                    result.status_code, result.text[:200], latency,
                )
                return False, {
                    "error": f"HTTP {result.status_code}",
                    "body": result.text[:500],
                    "latency_ms": latency,
                }

        except Exception as e:
            latency = (time.time() - start) * 1000
            logger.error("Kalshi order failed: %s (%.0fms)", e, latency)
            return False, {"error": str(e), "latency_ms": latency}

    async def get_balance(self) -> float:
        """Get current account balance in USD."""
        if self.simulation:
            return 0.0

        if not self._init_client():
            return 0.0

        try:
            path = "/trade-api/v2/portfolio/balance"
            auth_headers = self._sign_request("GET", path)
            result = await asyncio.to_thread(
                self._session.get,
                f"{KALSHI_REST_BASE}/portfolio/balance",
                headers=auth_headers,
                timeout=10,
            )
            if result.status_code == 200:
                data = result.json()
                return float(data.get("balance", 0)) / 100  # cents → dollars
        except Exception:
            logger.exception("Failed to get Kalshi balance")

        return 0.0
