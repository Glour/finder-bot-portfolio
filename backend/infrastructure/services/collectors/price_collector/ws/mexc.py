import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
from orjson import orjson

from infrastructure.database.models.base import MarketType
from infrastructure.services.collectors.price_collector.ws.websocket_proto.PushDataV3ApiWrapper_pb2 import \
    PushDataV3ApiWrapper  # type: ignore
from settings import logger


class MexcWSClient:
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

        if market_type == MarketType.SPOT:
            self.ws_url = "wss://wbs-api.mexc.com/ws"
        elif market_type == MarketType.FUTURES:
            self.ws_url = "wss://contract.mexc.com/edge"

    async def start(self):
        retry_delay = 0.5
        while True:
            try:
                conn = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.ws_connect(self.ws_url) as ws:
                        await self._subscribe_all(ws)

                        self._ping_task = asyncio.create_task(self._ping(ws))
                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.BINARY and self.market_type == MarketType.SPOT:
                                parsed = self._parse_spot_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.TEXT and self.market_type == MarketType.FUTURES:
                                parsed = self._parse_futures_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error("WebSocket error, closing")
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
            await asyncio.sleep(30)

    @staticmethod
    async def _enforce_lifetime(ws):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("WS lifetime exceeded, closing connection to force reconnect")
        await ws.close()

    async def _subscribe_all(self, ws):
        tasks = []
        for symbol in self.symbols:
            if self.market_type == MarketType.SPOT:
                tasks.append(self._subscribe_spot(ws, symbol))
            elif self.market_type == MarketType.FUTURES:
                tasks.append(self._subscribe_futures(ws, symbol))
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def _subscribe_spot(ws, symbol: str):
        payload = {
            "method": "SUBSCRIPTION",
            "params": [f"spot@public.limit.depth.v3.api.pb@{symbol.upper()}@20"]
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    @staticmethod
    async def _subscribe_futures(ws, symbol: str):
        payload = {
            "method": "sub.depth.full",
            "param": {"symbol": symbol.upper(), "limit": 20}
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    @staticmethod
    def _parse_spot_message(raw: bytes) -> dict[str, Any] | None:
        wrapper = PushDataV3ApiWrapper()
        wrapper.ParseFromString(raw)

        if not wrapper.HasField("publicLimitDepths"):
            return None

        depth = wrapper.publicLimitDepths
        result = {
            "symbol": wrapper.symbol,
            "data": {
                "asks": [(ask.price, ask.quantity) for ask in depth.asks],
                "bids": [(bid.price, bid.quantity) for bid in depth.bids],
            },
            "ts": wrapper.sendTime
        }
        return result

    @staticmethod
    def _parse_futures_message(raw: str) -> dict[str, Any] | None:
        try:
            msg = orjson.loads(raw)
            if msg.get("channel") != "push.depth.full":
                return None
            return msg
        except Exception as e:
            logger.error(f"Failed to parse futures depth message: {e}")
            return None

    async def _handle_orderbook(self, msg: dict):
        data = msg.get("data", {})
        symbol = msg.get("symbol")
        if not symbol or not data:
            return

        timestamp = msg.get("ts")
        now = time.time()
        last = self.last_update_time.get(symbol, 0)
        if now - last < 1.0:
            return

        self.last_update_time[symbol] = now
        ob = {"asks": data.get("asks", []), "bids": data.get("bids", [])}
        asyncio.create_task(self.update_callback(symbol, ob, timestamp))
