import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson
from sortedcontainers import SortedDict

from infrastructure.database.models.base import MarketType
from settings import logger


class GateWSClient:
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
            self.ws_url = "wss://api.gateio.ws/ws/v4/"
        elif market_type == MarketType.FUTURES:
            self.ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

        self.orderbooks: dict = {
            sym: {"bids": SortedDict(lambda x: -float(x)),
                  "asks": SortedDict(lambda x: float(x)),
                  "last_id": None,
                  "pending_update": None}
            for sym in symbols
        }

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
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                parsed = self._parse_ob_message(msg.data)
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
            "time": int(time.time()),
            "channel": "spot.obu",
            "event": "subscribe",
            "payload": [f"ob.{symbol.upper()}.50"]
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    @staticmethod
    async def _subscribe_futures(ws, symbol: str):
        payload = {
            "time": int(time.time()),
            "channel": "futures.obu",
            "event": "subscribe",
            "payload": [f"ob.{symbol.upper()}.50"]
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    @staticmethod
    def _parse_ob_message(raw: str) -> dict | None:
        try:
            msg = orjson.loads(raw)
            if msg.get("channel") not in ("spot.obu", "futures.obu"):
                return None
            return msg
        except Exception as e:
            logger.error(f"Failed to parse depth message: {e}")
            return None

    @staticmethod
    def _trim_orderbook(orderbook: dict[str, SortedDict], depth: int = 20):
        while len(orderbook["bids"]) > depth:
            orderbook["bids"].popitem(-1)

        while len(orderbook["asks"]) > depth:
            orderbook["asks"].popitem(-1)

    async def _handle_orderbook(self, msg: dict):
        data = msg.get("result", {})
        sym = data.get("s")
        if not sym or not data:
            return

        timestamp = data.get("t")
        symbol = sym.split(".")[1]
        orderbook = self.orderbooks[symbol]

        if data.get("full", False):
            orderbook["bids"].clear()
            orderbook["asks"].clear()
            for p, q in data.get("b", []):
                if q != "0":
                    orderbook["bids"][p] = q
            for p, q in data.get("a", []):
                if q != "0":
                    orderbook["asks"][p] = q
            orderbook["last_id"] = data["u"]
            return

        else:
            start_update, end_update = data["U"], data["u"]
            if orderbook["last_id"] is None or start_update != orderbook["last_id"] + 1:
                logger.warning(
                    f"{symbol}: out of sync (expected {orderbook['last_id'] + 1 if orderbook['last_id'] else None}, got {start_update}), forcing reconnect")
                raise ConnectionResetError("Orderbook out of sync")

            for p, q in data.get("b", []):
                if q == "0":
                    orderbook["bids"].pop(p, None)
                else:
                    orderbook["bids"][p] = q

            for p, q in data.get("a", []):
                if q == "0":
                    orderbook["asks"].pop(p, None)
                else:
                    orderbook["asks"][p] = q

            orderbook["last_id"] = end_update

        self._trim_orderbook(orderbook)
        ob = {"bids": list(orderbook["bids"].items()), "asks": list(orderbook["asks"].items())}
        if ob.get("bids") == [] or ob.get("asks") == []:
            return

        now = time.time()
        if now - self.last_update_time[symbol] >= 1.0:
            self.last_update_time[symbol] = now
            if orderbook["pending_update"]:
                orderbook["pending_update"].cancel()
                orderbook["pending_update"] = None
            asyncio.create_task(self.update_callback(symbol, ob, timestamp))
        else:
            delay = 1.0 - (now - self.last_update_time[symbol])
            if orderbook["pending_update"] is None:
                orderbook["pending_update"] = asyncio.create_task(
                    self._delayed_update(symbol, ob, timestamp, delay))

    async def _delayed_update(self, symbol, ob, timestamp, delay):
        try:
            await asyncio.sleep(delay)
            asyncio.create_task(self.update_callback(symbol, ob, timestamp))
            self.last_update_time[symbol] = time.time()
        finally:
            self.orderbooks[symbol]["pending_update"] = None
