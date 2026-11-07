import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class BitgetWSClient:
    """WebSocket client for Bitget orderbook streaming.

    API docs: https://www.bitget.com/api-doc/common/websocket-introduction
    URL: wss://ws.bitget.com/v2/ws/public

    Subscription format:
      {"op": "subscribe", "args": [{"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"}]}
    For futures: instType = "USDT-FUTURES"

    Message format:
      {"action": "snapshot", "arg": {"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"},
       "data": [{"bids": [["50000","1.5"]], "asks": [["50001","2.0"]], "ts": "1234567890"}]}

    Heartbeat: send {"op": "ping"} every 25s, server replies {"event": "pong"}
    books5 sends full 5-level snapshots (no incremental state needed).
    """

    WS_URL = "wss://ws.bitget.com/v2/ws/public"
    PING_INTERVAL = 25  # Bitget disconnects after 30s without activity
    RECONNECT_DELAY_INIT = 0.5
    RECONNECT_DELAY_MAX = 5.0
    BATCH_SIZE = 100  # args per subscribe message

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

        # instType differs for spot vs futures
        self._inst_type = "SPOT" if market_type == MarketType.SPOT else "USDT-FUTURES"

    async def start(self):
        retry_delay = self.RECONNECT_DELAY_INIT
        while True:
            try:
                await self._run_connection()
                retry_delay = self.RECONNECT_DELAY_INIT
            except Exception as e:
                logger.error(f"Bitget WS ({self.market_type.value}) error, reconnecting: {e}")
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
            async with session.ws_connect(self.WS_URL, proxy=self.proxy_url) as ws:
                await self._subscribe_all(ws)
                self._ping_task = asyncio.create_task(self._ping_loop(ws))
                self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == "pong":
                            continue  # heartbeat reply
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"Bitget WS error: {ws.exception()}")
                        break

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        args = [
            {"instType": self._inst_type, "channel": "books5", "instId": sym}
            for sym in self.symbols
        ]
        for i in range(0, len(args), self.BATCH_SIZE):
            batch = args[i:i + self.BATCH_SIZE]
            payload = {"op": "subscribe", "args": batch}
            await ws.send_str(orjson.dumps(payload).decode())
            await asyncio.sleep(0.05)
        logger.info(f"Bitget WS ({self.market_type.value}): subscribed to {len(self.symbols)} symbols")

    async def _handle_message(self, raw: str):
        try:
            msg = orjson.loads(raw)

            # Handle pong / event messages
            event = msg.get("event")
            if event:
                if event == "error":
                    logger.error(f"Bitget WS error event: {msg}")
                return

            action = msg.get("action")
            if action not in ("snapshot", "update"):
                return

            arg = msg.get("arg", {})
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

            # Bitget books5 format: [price, qty] (2 fields)
            orderbook = {
                "bids": [[float(b[0]), float(b[1])] for b in bids_raw if len(b) >= 2],
                "asks": [[float(a[0]), float(a[1])] for a in asks_raw if len(a) >= 2],
            }

            if not orderbook["bids"] or not orderbook["asks"]:
                return

            now = time.time()
            if now - self.last_update_time[inst_id] >= 1.0:
                self.last_update_time[inst_id] = now
                asyncio.create_task(self.update_callback(inst_id, orderbook, ts))

        except Exception as e:
            logger.debug(f"Bitget WS parse error: {e}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            try:
                await ws.send_str("ping")
            except Exception:
                break

    @staticmethod
    async def _enforce_lifetime(ws: aiohttp.ClientWebSocketResponse):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("Bitget WS: lifetime exceeded, forcing reconnect")
        await ws.close()
