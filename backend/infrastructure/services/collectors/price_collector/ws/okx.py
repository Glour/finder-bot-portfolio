import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class OkxWSClient:
    """WebSocket client for OKX orderbook streaming.

    API docs: https://www.okx.com/docs-v5/en/#websocket-api-public-channel
    URL: wss://ws.okx.com:8443/ws/v5/public

    Custom protocol (NOT Binance-compatible):
      Subscribe: {"op": "subscribe", "args": [{"channel": "books5", "instId": "BTC-USDT"}]}
      Response:  {"arg": {"channel": "books5", "instId": "BTC-USDT"}, "data": [{...}]}

    Heartbeat: client sends "ping" (plain text) every 25s, server replies "pong".

    channel "books5" sends top-5 levels as full snapshot every time (no incremental state).
    For spot: instId = "BTC-USDT"
    For futures (SWAP): instId = "BTC-USDT-SWAP"
    """

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
    PING_INTERVAL = 25  # seconds — OKX disconnects after 30s without activity
    RECONNECT_DELAY_INIT = 0.5
    RECONNECT_DELAY_MAX = 5.0
    BATCH_SIZE = 50  # max args per subscribe message

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
        self._ping_task: asyncio.Task | None = None

    async def start(self):
        retry_delay = self.RECONNECT_DELAY_INIT
        while True:
            try:
                await self._run_connection()
                retry_delay = self.RECONNECT_DELAY_INIT  # reset on clean disconnect
            except Exception as e:
                logger.error(f"OKX WS ({self.market_type.value}) error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_DELAY_MAX)
            finally:
                if self._lifetime_task:
                    self._lifetime_task.cancel()
                if self._ping_task:
                    self._ping_task.cancel()

    async def _run_connection(self):
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.ws_connect(
                self.WS_URL,
                proxy=self.proxy_url,
            ) as ws:
                await self._subscribe_all(ws)

                self._ping_task = asyncio.create_task(self._ping_loop(ws))
                self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == "pong":
                            continue  # heartbeat response, skip
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"OKX WS error: {ws.exception()}")
                        break

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        """Subscribe to books5 channel for all symbols in batches."""
        args = [{"channel": "books5", "instId": sym} for sym in self.symbols]

        # Send in batches to avoid oversized messages
        for i in range(0, len(args), self.BATCH_SIZE):
            batch = args[i:i + self.BATCH_SIZE]
            payload = {"op": "subscribe", "args": batch}
            await ws.send_str(orjson.dumps(payload).decode())
            await asyncio.sleep(0.1)  # small delay between batches

        logger.info(
            f"OKX WS ({self.market_type.value}): subscribed to {len(self.symbols)} symbols"
        )

    async def _handle_message(self, raw: str):
        try:
            msg = orjson.loads(raw)

            # Subscription confirmation / error
            if "event" in msg:
                if msg.get("event") == "error":
                    logger.error(f"OKX WS subscription error: {msg}")
                return

            arg = msg.get("arg", {})
            channel = arg.get("channel", "")
            if channel != "books5":
                return

            inst_id = arg.get("instId", "")
            if not inst_id or inst_id not in self.last_update_time:
                return

            data = msg.get("data", [])
            if not data:
                return

            book = data[0]
            ts_str = book.get("ts", "0")
            ts = int(ts_str) if ts_str else int(time.time() * 1000)

            bids_raw = book.get("bids", [])
            asks_raw = book.get("asks", [])

            # OKX books5 format: [price, qty, deprecated, num_orders]
            orderbook = {
                "bids": [[float(b[0]), float(b[1])] for b in bids_raw if len(b) >= 2],
                "asks": [[float(a[0]), float(a[1])] for a in asks_raw if len(a) >= 2],
            }

            if not orderbook["bids"] or not orderbook["asks"]:
                return

            # Throttle to 1 update/sec per symbol
            now = time.time()
            if now - self.last_update_time[inst_id] >= 1.0:
                self.last_update_time[inst_id] = now
                asyncio.create_task(self.update_callback(inst_id, orderbook, ts))

        except Exception as e:
            logger.debug(f"OKX WS parse error: {e}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Send ping every PING_INTERVAL seconds to keep connection alive."""
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            try:
                await ws.send_str("ping")
            except Exception:
                break

    @staticmethod
    async def _enforce_lifetime(ws: aiohttp.ClientWebSocketResponse):
        """Force reconnect after 23h to avoid connection staleness."""
        await asyncio.sleep(23 * 60 * 60)
        logger.info("OKX WS: lifetime exceeded, forcing reconnect")
        await ws.close()
