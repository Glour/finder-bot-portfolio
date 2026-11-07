import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class BinanceWSClient:
    """WebSocket client for Binance bookTicker streaming.

    API docs: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

    URLs:
      Spot:    wss://stream.binance.com:9443/ws
      Futures: wss://fstream.binance.com/ws

    Subscribe: {"method": "SUBSCRIBE", "params": ["btcusdt@bookTicker"], "id": 1}

    Message format (no wrapper):
      {"u":..., "s":"BTCUSDT", "b":"86000.0", "B":"1.5", "a":"86001.0", "A":"2.0"}
    Futures also includes "T" and "E" timestamps.

    Ping: Binance server sends WS ping frames every ~3 min;
          aiohttp auto-responds with pong (no explicit ping_loop needed).
    """

    SPOT_WS_URL = "wss://stream.binance.com:9443/ws"
    FUTURES_WS_URL = "wss://fstream.binance.com/ws"
    RECONNECT_DELAY_INIT = 0.5
    RECONNECT_DELAY_MAX = 5.0
    BATCH_SIZE = 200  # Binance supports up to 1024 per connection

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
        self._lifetime_task: asyncio.Task | None = None

        self.ws_url = (
            self.SPOT_WS_URL if market_type == MarketType.SPOT
            else self.FUTURES_WS_URL
        )

    async def start(self):
        retry_delay = self.RECONNECT_DELAY_INIT
        while True:
            try:
                await self._run_connection()
                retry_delay = self.RECONNECT_DELAY_INIT
            except Exception as e:
                logger.error(f"Binance WS ({self.market_type.value}) error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_DELAY_MAX)
            finally:
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _run_connection(self):
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.ws_connect(self.ws_url, proxy=self.proxy_url, heartbeat=30) as ws:
                await self._subscribe_all(ws)
                self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"Binance WS error: {ws.exception()}")
                        break

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        streams = [f"{sym.lower()}@bookTicker" for sym in self.symbols]
        for i in range(0, len(streams), self.BATCH_SIZE):
            batch = streams[i:i + self.BATCH_SIZE]
            payload = {"method": "SUBSCRIBE", "params": batch, "id": i + 1}
            await ws.send_str(orjson.dumps(payload).decode())
            await asyncio.sleep(0.05)
        logger.info(
            f"Binance WS ({self.market_type.value}): subscribed to {len(self.symbols)} symbols"
        )

    async def _handle_message(self, raw: str):
        try:
            msg = orjson.loads(raw)

            # Skip subscription acks: {"result": null, "id": 1}
            if "result" in msg or "id" in msg and "s" not in msg:
                return

            symbol = msg.get("s")
            if not symbol or symbol not in self.last_update_time:
                return

            bid_price = msg.get("b")
            bid_qty = msg.get("B")
            ask_price = msg.get("a")
            ask_qty = msg.get("A")

            if not bid_price or not ask_price:
                return

            ts = int(msg.get("T", msg.get("u", int(time.time() * 1000))))

            orderbook = {
                "bids": [[float(bid_price), float(bid_qty or 0)]],
                "asks": [[float(ask_price), float(ask_qty or 0)]],
            }

            now = time.time()
            if now - self.last_update_time[symbol] >= 1.0:
                self.last_update_time[symbol] = now
                asyncio.create_task(self.update_callback(symbol, orderbook, ts))

        except Exception as e:
            logger.debug(f"Binance WS parse error: {e}")

    @staticmethod
    async def _enforce_lifetime(ws: aiohttp.ClientWebSocketResponse):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("Binance WS: lifetime exceeded, forcing reconnect")
        await ws.close()
