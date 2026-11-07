import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class AscendexWSClient:
    """WebSocket client for AscendEX orderbook streaming.

    API Documentation: https://ascendex.github.io/ascendex-pro-api/

    WebSocket URL (unified for spot and futures):
    - wss://ascendex.com:443/api/pro/v2/stream

    Protocol uses {"op": "sub", "ch": "depth:{symbol}"} for subscription.
    Ping/pong: Server sends {"m": "ping"}, client responds {"op": "pong"}.
    """

    WS_URL = "wss://ascendex.com:443/api/pro/v2/stream"

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType = MarketType.SPOT,
            proxy_url: str | None = None  # Added for compatibility
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None

    async def start(self):
        retry_delay = 0.5
        while True:
            try:
                conn = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.ws_connect(self.WS_URL) as ws:
                        await self._subscribe_all(ws)

                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_message(ws, msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error("AscendEX WebSocket error, closing")
                                break

            except Exception as e:
                logger.error(f"AscendEX WS error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 5.0)
            finally:
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    @staticmethod
    async def _enforce_lifetime(ws):
        # Reconnect after 23 hours to avoid connection timeout
        await asyncio.sleep(23 * 60 * 60)
        logger.info("AscendEX WS lifetime exceeded, closing connection to force reconnect")
        await ws.close()

    async def _subscribe_all(self, ws):
        """Subscribe to bbo (best bid/offer) streams for all symbols.

        bbo always sends fresh best bid/ask — never empty or incremental.
        Format: {"op": "sub", "ch": "bbo:{symbol}"}
        Examples:
        - Spot: "bbo:BTC/USDT"
        - Futures: "bbo:BTC-PERP"
        """
        batch_size = 50
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i:i + batch_size]
            for sym in batch:
                payload = {
                    "op": "sub",
                    "ch": f"bbo:{sym}"
                }
                await ws.send_str(orjson.dumps(payload).decode("utf-8"))
                await asyncio.sleep(0.05)

    async def _handle_message(self, ws, raw: str):
        """Handle incoming WebSocket messages."""
        try:
            msg = orjson.loads(raw)

            # Handle ping - respond with pong
            if msg.get("m") == "ping":
                pong = {"op": "pong"}
                await ws.send_str(orjson.dumps(pong).decode("utf-8"))
                return

            # Handle bbo updates (best bid/offer — always fresh, never incremental)
            msg_type = msg.get("m")
            if msg_type == "bbo":
                parsed = self._parse_bbo_message(msg)
                if parsed:
                    await self._handle_orderbook(parsed)
            elif msg_type in ("depth-snapshot", "depth"):
                parsed = self._parse_depth_message(msg)
                if parsed:
                    await self._handle_orderbook(parsed)

        except Exception as e:
            logger.debug(f"AscendEX: Failed to handle message: {e}")

    @staticmethod
    def _parse_depth_message(msg: dict) -> dict[str, Any] | None:
        """Parse orderbook depth message from AscendEX.

        Expected format:
        {
            "m": "depth-snapshot" or "depth",
            "symbol": "BTC/USDT" or "BTC-PERP",
            "data": {
                "ts": 1234567890123,
                "seqnum": 123,
                "bids": [["price", "qty"], ...],
                "asks": [["price", "qty"], ...]
            }
        }
        """
        try:
            symbol = msg.get("symbol")
            data = msg.get("data")

            if not symbol or not data:
                return None

            bids = data.get("bids", [])
            asks = data.get("asks", [])
            timestamp = data.get("ts", int(time.time() * 1000))

            return {
                "symbol": symbol,
                "data": {
                    "bids": bids,
                    "asks": asks,
                },
                "ts": timestamp
            }
        except Exception:
            return None

    @staticmethod
    def _parse_bbo_message(msg: dict) -> dict | None:
        """Parse bbo (best bid/offer) message from AscendEX.

        Format:
        {
            "m": "bbo",
            "symbol": "BTC-PERP",
            "data": {
                "ts": 1234567890123,
                "bid": ["price", "qty"],
                "ask": ["price", "qty"]
            }
        }
        """
        try:
            symbol = msg.get("symbol")
            data = msg.get("data")
            if not symbol or not data:
                return None
            bid = data.get("bid")
            ask = data.get("ask")
            timestamp = data.get("ts", int(time.time() * 1000))
            bids = [bid] if bid and float(bid[0]) > 0 else []
            asks = [ask] if ask and float(ask[0]) > 0 else []
            if not bids and not asks:
                return None
            return {
                "symbol": symbol,
                "data": {"bids": bids, "asks": asks},
                "ts": timestamp,
            }
        except Exception:
            return None

    async def _handle_orderbook(self, msg: dict):
        """Process orderbook update and call callback."""
        data = msg.get("data", {})
        symbol = msg.get("symbol")
        if not symbol or not data:
            return

        timestamp = msg.get("ts")
        now = time.time()
        last = self.last_update_time.get(symbol, 0)

        # Rate limit updates to 1 per second per symbol
        if now - last < 1.0:
            return

        self.last_update_time[symbol] = now

        # Format orderbook for callback
        # Limit to top 20 levels
        bids = data.get("bids", [])[:20]
        asks = data.get("asks", [])[:20]

        ob = {"asks": asks, "bids": bids}
        asyncio.create_task(self.update_callback(symbol, ob, timestamp))
