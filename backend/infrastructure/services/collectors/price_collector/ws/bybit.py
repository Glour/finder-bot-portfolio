import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class BybitWSClient:
    """WebSocket client for Bybit orderbook streaming.

    API docs: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
    URLs (split by market type):
      Spot:   wss://stream.bybit.com/v5/public/spot
      Linear: wss://stream.bybit.com/v5/public/linear

    Subscription: {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}
    Channel "orderbook.1" — BBO (best bid/offer), always full snapshot, no incremental.

    Message format:
      {"topic": "orderbook.1.BTCUSDT", "type": "snapshot",
       "data": {"s": "BTCUSDT", "b": [["50000","1.5"]], "a": [["50001","2.0"]], "u": 1},
       "ts": 1234567890000}

    Heartbeat: {"op": "ping"} every 20s, server responds {"op": "pong", ...}
    """

    SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
    LINEAR_WS_URL = "wss://stream.bybit.com/v5/public/linear"
    PING_INTERVAL = 20
    RECONNECT_DELAY_INIT = 0.5
    RECONNECT_DELAY_MAX = 5.0
    BATCH_SIZE = 10  # Bybit recommends ≤10 args per subscribe

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType = MarketType.SPOT,
            proxy_url: str | None = None,
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.proxy_url = proxy_url
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None

        self.ws_url = (
            self.SPOT_WS_URL if market_type == MarketType.SPOT
            else self.LINEAR_WS_URL
        )

    async def start(self):
        retry_delay = self.RECONNECT_DELAY_INIT
        while True:
            try:
                await self._run_connection()
                retry_delay = self.RECONNECT_DELAY_INIT
            except Exception as e:
                logger.error(f"Bybit WS ({self.market_type.value}) error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_DELAY_MAX)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _run_connection(self):
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.ws_connect(self.ws_url, proxy=self.proxy_url) as ws:
                await self._subscribe_all(ws)
                self._ping_task = asyncio.create_task(self._ping_loop(ws))
                self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"Bybit WS error: {ws.exception()}")
                        break

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        args = [f"orderbook.1.{sym}" for sym in self.symbols]
        for i in range(0, len(args), self.BATCH_SIZE):
            batch = args[i:i + self.BATCH_SIZE]
            payload = {"op": "subscribe", "args": batch}
            await ws.send_str(orjson.dumps(payload).decode())
            await asyncio.sleep(0.1)
        logger.info(f"Bybit WS ({self.market_type.value}): subscribed to {len(self.symbols)} symbols")

    async def _handle_message(self, raw: str):
        try:
            msg = orjson.loads(raw)

            # Skip pong / subscription confirmations
            op = msg.get("op")
            if op in ("pong", "subscribe"):
                return
            if "success" in msg:
                return

            topic = msg.get("topic", "")
            if not topic.startswith("orderbook.1."):
                return

            symbol = topic[len("orderbook.1."):]
            if symbol not in self.last_update_time:
                return

            data = msg.get("data", {})
            ts = int(msg.get("ts", int(time.time() * 1000)))

            bids_raw = data.get("b", [])
            asks_raw = data.get("a", [])

            orderbook = {
                "bids": [[float(b[0]), float(b[1])] for b in bids_raw if len(b) >= 2],
                "asks": [[float(a[0]), float(a[1])] for a in asks_raw if len(a) >= 2],
            }

            if not orderbook["bids"] or not orderbook["asks"]:
                return

            now = time.time()
            if now - self.last_update_time[symbol] >= 1.0:
                self.last_update_time[symbol] = now
                asyncio.create_task(self.update_callback(symbol, orderbook, ts))

        except Exception as e:
            logger.debug(f"Bybit WS parse error: {e}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            try:
                await ws.send_str(orjson.dumps({"op": "ping"}).decode())
            except Exception:
                break

    @staticmethod
    async def _enforce_lifetime(ws: aiohttp.ClientWebSocketResponse):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("Bybit WS: lifetime exceeded, forcing reconnect")
        await ws.close()
