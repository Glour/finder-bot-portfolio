import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class HyperliquidWSClient:
    """WebSocket client for Hyperliquid orderbook streaming.

    API Documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket

    WebSocket URL: wss://api.hyperliquid.xyz/ws

    Subscription format:
    {
        "method": "subscribe",
        "subscription": {"type": "l2Book", "coin": "BTC"}
    }

    Response format:
    {
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": 1234567890123,
            "levels": [
                [{"px": "77500.0", "sz": "1.5", "n": 3}, ...],  # bids
                [{"px": "77501.0", "sz": "2.0", "n": 5}, ...]   # asks
            ]
        }
    }

    Note: Hyperliquid uses same WS for both perps and spot.
    - Perps: use coin name like "BTC", "ETH"
    - Spot: use pair name like "PURR/USDC" or index like "@0"
    """

    WS_URL = "wss://api.hyperliquid.xyz/ws"
    PING_INTERVAL = 30  # seconds
    RECONNECT_DELAY = 1.0
    MAX_RECONNECT_DELAY = 30.0

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType = MarketType.FUTURES,
            proxy_url: str | None = None,  # For compatibility with other clients
            spot_symbol_map: dict[str, str] | None = None  # Maps our symbol to HL coin format
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None
        self._reconnect_delay = self.RECONNECT_DELAY
        # For spot: map our symbol (HFUNUSDC) -> HL format (@1)
        self.spot_symbol_map = spot_symbol_map or {}
        # Reverse map: HL format (@1) -> our symbol (HFUNUSDC)
        self._coin_to_symbol_map = {v: k for k, v in self.spot_symbol_map.items()}

    async def start(self):
        """Main connection loop with auto-reconnect."""
        while True:
            try:
                conn = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.ws_connect(self.WS_URL, heartbeat=30) as ws:
                        logger.info(f"Hyperliquid WS connected, subscribing to {len(self.symbols)} symbols")
                        await self._subscribe_all(ws)

                        self._ping_task = asyncio.create_task(self._ping_loop(ws))
                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        # Reset reconnect delay on successful connection
                        self._reconnect_delay = self.RECONNECT_DELAY

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_message(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"Hyperliquid WS error: {ws.exception()}")
                                break
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                logger.warning("Hyperliquid WS connection closed")
                                break

            except asyncio.CancelledError:
                logger.info("Hyperliquid WS client cancelled")
                break
            except Exception as e:
                logger.error(f"Hyperliquid WS error, reconnecting in {self._reconnect_delay}s: {e}")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self.MAX_RECONNECT_DELAY)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                if self._lifetime_task:
                    self._lifetime_task.cancel()
                    try:
                        await self._lifetime_task
                    except asyncio.CancelledError:
                        pass

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        """Subscribe to l2Book for all symbols.

        Hyperliquid doesn't support batch subscriptions, so we subscribe one by one.
        """
        for symbol in self.symbols:
            # Convert symbol format
            # For perps: use coin name directly (e.g., "BTC", "ETH")
            # For spot: use mapping or reconstruct pair name
            coin = self._symbol_to_coin(symbol)

            payload = {
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book",
                    "coin": coin
                }
            }
            await ws.send_str(orjson.dumps(payload).decode("utf-8"))
            await asyncio.sleep(0.05)  # Small delay to avoid rate limits

        logger.info(f"Hyperliquid: subscribed to {len(self.symbols)} l2Book streams ({self.market_type.value})")

    def _symbol_to_coin(self, symbol: str) -> str:
        """Convert our symbol format to Hyperliquid coin format.

        Examples:
        - "BTC" -> "BTC" (perp)
        - "PURRUSDC" -> "PURR/USDC" (spot canonical)
        - "HFUNUSDC" -> "@1" (spot non-canonical, via mapping)
        """
        # Check spot symbol map first (for non-canonical pairs like @1, @2...)
        if symbol in self.spot_symbol_map:
            return self.spot_symbol_map[symbol]

        # If already has slash, use as-is
        if "/" in symbol:
            return symbol

        # For spot, try to add slash before USDC/USDT
        for quote in ("USDC", "USDT"):
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return f"{symbol[:-len(quote)]}/{quote}"

        # For perps, return as-is (just coin name)
        return symbol

    async def _handle_message(self, data: str):
        """Parse and process incoming WebSocket message."""
        try:
            msg = orjson.loads(data)

            channel = msg.get("channel", "")

            # Handle subscription confirmations
            if channel == "subscriptionResponse":
                return

            # Handle l2Book updates
            if channel == "l2Book":
                await self._handle_l2book(msg.get("data", {}))

        except Exception as e:
            logger.debug(f"Hyperliquid: failed to parse message: {e}")

    async def _handle_l2book(self, data: dict):
        """Process l2Book update.

        Data format:
        {
            "coin": "BTC",
            "time": 1234567890123,
            "levels": [
                [{"px": "77500.0", "sz": "1.5", "n": 3}, ...],  # bids
                [{"px": "77501.0", "sz": "2.0", "n": 5}, ...]   # asks
            ]
        }
        """
        coin = data.get("coin", "")
        if not coin:
            return

        levels = data.get("levels", [])
        if len(levels) != 2:
            return

        timestamp = data.get("time", int(time.time() * 1000))

        # Convert coin back to our symbol format
        symbol = self._coin_to_symbol(coin)

        # Rate limit updates
        now = time.time()
        last = self.last_update_time.get(symbol, 0)
        if now - last < 1.0:
            return
        self.last_update_time[symbol] = now

        # Parse bids and asks
        # levels[0] = bids (descending), levels[1] = asks (ascending)
        bids_raw, asks_raw = levels

        # Convert to standard format [[price, size], ...]
        bids = [[level.get("px", "0"), level.get("sz", "0")] for level in bids_raw[:20]]
        asks = [[level.get("px", "0"), level.get("sz", "0")] for level in asks_raw[:20]]

        orderbook = {"bids": bids, "asks": asks}
        asyncio.create_task(self.update_callback(symbol, orderbook, timestamp))

    def _coin_to_symbol(self, coin: str) -> str:
        """Convert Hyperliquid coin format back to our symbol format.

        Examples:
        - "BTC" -> "BTC" (perp)
        - "PURR/USDC" -> "PURRUSDC" (spot canonical)
        - "@1" -> "HFUNUSDC" (spot non-canonical, via reverse mapping)
        """
        # Check reverse mapping for non-canonical pairs
        if coin in self._coin_to_symbol_map:
            return self._coin_to_symbol_map[coin]

        # Remove slash for spot pairs
        return coin.replace("/", "")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Send periodic pings to keep connection alive."""
        while True:
            try:
                await asyncio.sleep(self.PING_INTERVAL)
                # Hyperliquid uses standard WebSocket ping/pong
                await ws.ping()
            except (RuntimeError, ConnectionResetError, asyncio.CancelledError):
                break

    async def _enforce_lifetime(self, ws: aiohttp.ClientWebSocketResponse):
        """Force reconnect after 23 hours to avoid stale connections."""
        await asyncio.sleep(23 * 60 * 60)  # 23 hours
        logger.info("Hyperliquid WS lifetime exceeded, closing for reconnect")
        await ws.close()
