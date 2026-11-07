import asyncio
import time
import uuid
from typing import Callable, Any, Coroutine

import aiohttp
import httpx
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class KucoinWSClient:
    """WebSocket client for KuCoin orderbook streaming.

    API docs: https://docs.kucoin.com/#websocket-feed

    KuCoin WS requires a dynamic token and endpoint from bullet-public:
      POST https://api.kucoin.com/api/v1/bullet-public   (spot)
      POST https://api-futures.kucoin.com/api/v1/bullet-public (futures)

    Subscribe:
      Spot:    /spotMarket/level2Depth5:{symbol},{symbol2}
      Futures: /contractMarket/level2Depth5:{symbol}

    Message format (spot depth5):
      {"topic": "/spotMarket/level2Depth5:BTC-USDT", "type": "message",
       "data": {"bids": [["price","qty"],...], "asks": [...], "timestamp": 1234567890000}}

    Futures depth5:
      {"topic": "/contractMarket/level2Depth5:XBTUSDTM", "type": "message",
       "data": {"sequence": ..., "bids": [[price, qty],...], "asks": [...], "ts": ...}}

    Ping: {"id": "<uuid>", "type": "ping"} every pingInterval (18000ms = 18s)
    Server responds: {"id": "...", "type": "pong"}
    """

    SPOT_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"
    FUTURES_BULLET_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"
    RECONNECT_DELAY_INIT = 0.5
    RECONNECT_DELAY_MAX = 5.0
    SPOT_BATCH_SIZE = 50   # KuCoin allows comma-separated symbols on one subscription
    FUTURES_BATCH_SIZE = 1  # Futures: one subscription per symbol

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

        self.bullet_url = (
            self.SPOT_BULLET_URL if market_type == MarketType.SPOT
            else self.FUTURES_BULLET_URL
        )

    async def start(self):
        retry_delay = self.RECONNECT_DELAY_INIT
        while True:
            try:
                await self._run_connection()
                retry_delay = self.RECONNECT_DELAY_INIT
            except Exception as e:
                logger.error(f"KuCoin WS ({self.market_type.value}) error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_DELAY_MAX)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _get_ws_endpoint(self) -> tuple[str, int]:
        """Fetch dynamic WS endpoint and token from bullet-public."""
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(self.bullet_url)
            resp.raise_for_status()
            data = resp.json()
        if data.get("code") != "200000":
            raise ValueError(f"KuCoin bullet-public error: {data}")
        server = data["data"]["instanceServers"][0]
        token = data["data"]["token"]
        connect_id = uuid.uuid4().hex
        ws_url = f"{server['endpoint']}?token={token}&connectId={connect_id}"
        ping_interval = server.get("pingInterval", 18000) / 1000  # ms → s
        return ws_url, int(ping_interval)

    async def _run_connection(self):
        ws_url, ping_interval = await self._get_ws_endpoint()
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.ws_connect(ws_url, proxy=self.proxy_url) as ws:
                # Wait for welcome message
                welcome = await ws.receive_str()
                welcome_msg = orjson.loads(welcome)
                if welcome_msg.get("type") != "welcome":
                    logger.warning(f"KuCoin WS: unexpected first message: {welcome_msg}")

                await self._subscribe_all(ws)
                self._ping_task = asyncio.create_task(self._ping_loop(ws, ping_interval))
                self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"KuCoin WS error: {ws.exception()}")
                        break

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        if self.market_type == MarketType.SPOT:
            # Spot: comma-separated symbols, e.g. "/spotMarket/level2Depth5:BTC-USDT,ETH-USDT"
            for i in range(0, len(self.symbols), self.SPOT_BATCH_SIZE):
                batch = self.symbols[i:i + self.SPOT_BATCH_SIZE]
                topic = f"/spotMarket/level2Depth5:{','.join(batch)}"
                payload = {
                    "id": uuid.uuid4().hex,
                    "type": "subscribe",
                    "topic": topic,
                    "privateChannel": False,
                    "response": True,
                }
                await ws.send_str(orjson.dumps(payload).decode())
                await asyncio.sleep(0.2)
        else:
            # Futures: one symbol per subscription
            for sym in self.symbols:
                topic = f"/contractMarket/level2Depth5:{sym}"
                payload = {
                    "id": uuid.uuid4().hex,
                    "type": "subscribe",
                    "topic": topic,
                    "privateChannel": False,
                    "response": True,
                }
                await ws.send_str(orjson.dumps(payload).decode())
                await asyncio.sleep(0.05)

        logger.info(f"KuCoin WS ({self.market_type.value}): subscribed to {len(self.symbols)} symbols")

    async def _handle_message(self, raw: str):
        try:
            msg = orjson.loads(raw)
            msg_type = msg.get("type")

            # Skip ack, pong, welcome
            if msg_type in ("ack", "pong", "welcome"):
                return

            if msg_type != "message":
                return

            topic = msg.get("topic", "")
            data = msg.get("data", {})

            if self.market_type == MarketType.SPOT:
                # topic: /spotMarket/level2Depth5:BTC-USDT
                if "level2Depth5:" not in topic:
                    return
                symbol = topic.split("level2Depth5:")[-1]
                bids_raw = data.get("bids", [])
                asks_raw = data.get("asks", [])
                ts = int(data.get("timestamp", int(time.time() * 1000)))
            else:
                # topic: /contractMarket/level2Depth5:XBTUSDTM
                if "level2Depth5:" not in topic:
                    return
                symbol = topic.split("level2Depth5:")[-1]
                bids_raw = data.get("bids", [])
                asks_raw = data.get("asks", [])
                ts = int(data.get("ts", int(time.time() * 1000)))

            if symbol not in self.last_update_time:
                return

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
            logger.debug(f"KuCoin WS parse error: {e}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse, interval: float):
        while True:
            await asyncio.sleep(interval)
            try:
                payload = {"id": uuid.uuid4().hex, "type": "ping"}
                await ws.send_str(orjson.dumps(payload).decode())
            except Exception:
                break

    @staticmethod
    async def _enforce_lifetime(ws: aiohttp.ClientWebSocketResponse):
        await asyncio.sleep(23 * 60 * 60)
        logger.info("KuCoin WS: lifetime exceeded, forcing reconnect")
        await ws.close()
