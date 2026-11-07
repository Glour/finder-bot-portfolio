import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Callable, Any, Coroutine, Optional
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from aiohttp import ClientWebSocketResponse
from orjson import orjson

from infrastructure.database.models.base import MarketType
from infrastructure.services.collectors.price_collector.supports import lbank_price_tick_info
from settings import logger


class LbankWSClient:
    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType,
            proxy_url: Optional[str] = None
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.proxy_url = proxy_url  # Format: http://user:pass@ip:port
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self.lbank_price_tick = {token.get("symbol"): token.get("priceTick") for token in lbank_price_tick_info}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None

        if market_type == MarketType.SPOT:
            self.ws_url = "wss://www.lbkex.net/ws/V2/"
        elif market_type == MarketType.FUTURES:
            self.ws_url = "wss://uuws.rerrkvifj.com/ws/v3"

    async def start(self):
        retry_delay = 1
        consecutive_failures = 0
        while True:
            try:
                if self.market_type == MarketType.SPOT:
                    # Use default SSL settings (ssl=True by default)
                    # Add proxy support for LBank SPOT to bypass IP blocking
                    async with aiohttp.ClientSession() as session:
                        connect_kwargs = {
                            "url": self.ws_url,
                            "timeout": aiohttp.ClientTimeout(total=10, connect=5)  # Reduced timeouts for faster proxy failover
                        }

                        # Add proxy if provided
                        if self.proxy_url:
                            connect_kwargs["proxy"] = self.proxy_url
                            # Only log proxy IP, not credentials
                            proxy_display = self.proxy_url.split('@')[1] if '@' in self.proxy_url else self.proxy_url

                        async with session.ws_connect(**connect_kwargs) as ws:
                            # Reset failure counter on successful connection
                            consecutive_failures = 0
                            msg_count = 0

                            await self._subscribe_all(ws)
                            self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                            async for msg in ws:
                                msg_count += 1
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    parsed = await self._handle_spot_message(ws, msg.data)
                                    if parsed:
                                        await self._handle_orderbook(parsed)
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    logger.error(f"WebSocket error, closing: {msg.data}")
                                    break

                            # Log when loop exits (connection closed)
                            if msg_count > 0:
                                logger.info(f"WS loop ended after {msg_count} messages")

                elif self.market_type == MarketType.FUTURES:
                    # FUTURES uses different library and endpoint
                    async with websockets.connect(
                            self.ws_url,
                            additional_headers={
                                "Origin": "https://www.lbank.com",
                                "User-Agent": "price-collector/1.0"
                            },
                            ping_interval=5
                    ) as ws:
                        await self._subscribe_all(ws)
                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        async for msg in ws:
                            parsed = self._parse_message(msg)
                            if parsed:
                                await self._handle_orderbook(parsed)

                retry_delay = 1
                consecutive_failures = 0

            except (aiohttp.ClientProxyConnectionError, asyncio.TimeoutError) as e:
                # Proxy failed - increment counter and try different proxy faster
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    # After 3 consecutive failures, wait longer
                    logger.warning(f"Proxy failed {consecutive_failures} times, waiting {min(retry_delay * 2, 30)}s")
                    await asyncio.sleep(min(retry_delay * 2, 30))
                    retry_delay = min(retry_delay * 2, 30)
                else:
                    # Quick retry with different proxy
                    await asyncio.sleep(1)
            except aiohttp.WSServerHandshakeError as e:
                # 405 or other handshake error - likely bad proxy
                consecutive_failures += 1
                logger.warning(f"Handshake failed (bad proxy?), trying different one: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                # Other errors - standard retry logic
                consecutive_failures += 1
                logger.error(f"WS error, reconnecting in {retry_delay}s: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)  # Max 30s instead of 60s

            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _subscribe_all(self, ws):
        if self.market_type == MarketType.SPOT:
            for symbol in self.symbols:
                await self._subscribe_spot(ws, symbol)
                await asyncio.sleep(0.05)  # Reduced from 0.2s to speed up subscription
        elif self.market_type == MarketType.FUTURES:
            for symbol in self.symbols:
                await self._subscribe_futures(ws, symbol)
                await asyncio.sleep(0.05)  # Reduced from 0.2s to speed up subscription

    @staticmethod
    async def _ping_futures(ws):
        while True:
            try:
                await ws.send(orjson.dumps({"ping": int(time.time() * 1000)}).decode("utf-8"))
            except Exception as e:
                logger.error(f"Ping failed: {e}")
                break
            await asyncio.sleep(10)

    @staticmethod
    async def _enforce_lifetime(ws):
        # Randomize lifetime between 20-30 minutes to prevent synchronized reconnects
        base_lifetime = 60 * 20  # 20 minutes
        random_offset = random.randint(0, 60 * 10)  # 0-10 minutes
        lifetime = base_lifetime + random_offset
        await asyncio.sleep(lifetime)
        logger.info(f"WS lifetime exceeded ({lifetime/60:.1f}min), closing connection to force reconnect")
        await ws.close()

    @staticmethod
    async def _subscribe_spot(ws: ClientWebSocketResponse, symbol: str):
        payload = {
            "action": "subscribe",
            "subscribe": "depth",
            "depth": "50",
            "pair": symbol.lower()
        }
        await ws.send_str(orjson.dumps(payload).decode("utf-8"))

    async def _subscribe_futures(self, ws, symbol: str):
        price_tick = self.lbank_price_tick.get(symbol)
        if price_tick is None:
            price_tick_str = "0.0001"
        else:
            price_tick_str = format(price_tick, ".10f").rstrip("0").rstrip(".")
        payload = {
            "x": 3,
            "y": symbol,
            "a": {"i": f"{symbol}_{price_tick_str}_25"},
            "z": 1,
        }
        await ws.send(orjson.dumps(payload).decode("utf-8"))

    async def _handle_spot_message(self, ws, raw: str):
        msg = orjson.loads(raw)

        if msg.get("action") == "ping" and "ping" in msg:
            pong_msg = {"action": "pong", "pong": msg["ping"]}
            await ws.send_json(pong_msg)
            return None

        if msg.get("type") == "depth":
            return self._parse_message(raw)

        return None

    def _parse_message(self, raw: str) -> dict[str, Any] | None:
        try:
            msg = orjson.loads(raw)
            if self.market_type == MarketType.SPOT:
                if msg.get("type") != "depth":
                    return None

                dt = datetime.fromisoformat(msg.get("TS"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("Asia/Shanghai"))

                dt_utc = dt.astimezone(timezone.utc)
                timestamp = int(dt_utc.timestamp() * 1000)

                return {
                    "symbol": msg.get("pair", "").upper(),
                    "timestamp": timestamp,
                    "asks": msg.get("depth", {}).get("asks", []),
                    "bids": msg.get("depth", {}).get("bids", []),
                }

            elif self.market_type == MarketType.FUTURES:
                if "b" not in msg or "s" not in msg:
                    return None

                return {
                    "symbol": msg.get("y", ""),
                    "timestamp": int(msg.get("w")),
                    "asks": msg.get("s", []),
                    "bids": msg.get("b", []),
                }

            return None

        except Exception as e:
            logger.error(f"Failed to parse message: {e} | raw={raw[:200]}")
            return None

    async def _handle_orderbook(self, data: dict[str, Any]):
        if not data:
            return

        symbol = data.get("symbol")
        if not symbol:
            return

        now = time.time()
        last = self.last_update_time.get(symbol, 0)
        if now - last < 1.0:
            return

        timestamp = int(data.get("timestamp"))
        ob = {"asks": data.get("asks", []), "bids": data.get("bids", [])}
        self.last_update_time[symbol] = now
        asyncio.create_task(self.update_callback(symbol, ob, timestamp))
