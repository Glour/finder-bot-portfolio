import asyncio
import time
from datetime import datetime, UTC
from typing import Callable, Any, Coroutine

import aiohttp
from orjson import orjson

from infrastructure.database.models.base import MarketType
from infrastructure.services.collectors.price_collector.ws.websocket_proto.PushDataV3ApiWrapper_pb2 import \
    PushDataV3ApiWrapper  # type: ignore
from settings import logger


class XtWSClient:
    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType,
            proxy_url: str | None = None  # Added for compatibility with base collector
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.proxy_url = proxy_url  # XT doesn't need proxy, but parameter required
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None

        if market_type == MarketType.SPOT:
            self.ws_url = "wss://stream.xt.com/public"
        elif market_type == MarketType.FUTURES:
            self.ws_url = "wss://fstream.xt.com/ws/market"

    async def start(self):
        retry_delay = 0.5
        while True:
            try:
                conn = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.ws_connect(
                            url=self.ws_url,
                            headers={"Sec-WebSocket-Extensions": "permessage-deflate"}
                    ) as ws:
                        await self._subscribe_all(ws)

                        self._ping_task = asyncio.create_task(self._ping(ws))
                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                parsed = self._parse_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"WebSocket error, closing: {msg.data}")
                                break

            except Exception as e:
                logger.error(f"WS error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 5.0)

            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    @staticmethod
    async def _ping(ws):
        while True:
            try:
                await ws.ping()
            except (RuntimeError, ConnectionResetError, asyncio.CancelledError):
                break
            await asyncio.sleep(25)

    @staticmethod
    async def _enforce_lifetime(ws):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("WS lifetime exceeded, closing connection to force reconnect")
        await ws.close()

    async def _subscribe_all(self, ws):
        tasks = []
        for symbol in self.symbols:
            tasks.append(self._subscribe(ws, symbol))
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def _subscribe(ws, symbol: str):
        payload = {
            "method": "subscribe",
            "params": [f"depth@{symbol.lower()},20"],
            "id": str(id(symbol))
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    @staticmethod
    def _parse_message(raw: str) -> dict[str, Any] | None:
        try:
            msg = orjson.loads(raw)
            if msg.get("topic") != "depth":
                return None

            data = msg.get("data", {})
            # если это спот, то ставим текущий таймстемп
            if data.get("i"):
                timestamp = int(datetime.now(UTC).replace(tzinfo=None).timestamp() * 1000)
            else:
                timestamp = data.get("t")

            return {
                "symbol": data.get("s").upper(),
                "timestamp": timestamp,
                "asks": data.get("a", []),
                "bids": data.get("b", []),
            }

        except Exception as e:
            logger.error(f"Failed to parse depth message: {e}")
            return None

    async def _handle_orderbook(self, msg: dict[str, Any]):
        if not msg:
            return

        symbol = msg.get("symbol")
        if not symbol:
            return

        now = time.time()
        last = self.last_update_time.get(symbol, 0)
        if now - last < 1.0:
            return

        timestamp = msg.get("timestamp")
        ob = {"asks": msg.get("asks", []), "bids": msg.get("bids", [])}
        self.last_update_time[symbol] = now
        asyncio.create_task(self.update_callback(symbol, ob, timestamp))
