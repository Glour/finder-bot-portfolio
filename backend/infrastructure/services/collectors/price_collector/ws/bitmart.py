import asyncio
import random
import time
import zlib
from typing import Callable, Any, Coroutine

import aiohttp
from orjson import orjson

from infrastructure.database.models.base import MarketType
from settings import logger


class BitmartWSClient:
    """WebSocket client for Bitmart orderbook data.

    WebSocket URLs:
    - Spot: wss://ws-manager-compress.bitmart.com/api?protocol=1.1
    - Futures: wss://openapi-ws-v2.bitmart.com/api?protocol=1.1

    Subscription format:
    - Spot: {"op": "subscribe", "args": ["spot/depth5:BTC_USDT"]}
    - Futures: {"action": "subscribe", "args": ["futures/depth20:BTCUSDT"]}

    Message format (after decompression for spot):
    {
        "table": "spot/depth5",
        "data": [{
            "asks": [["161.96", "7.37567"]],
            "bids": [["161.94", "4.552355"]],
            "symbol": "ETH_USDT",
            "ms_t": 1542337219120
        }]
    }
    """

    SPOT_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
    FUTURES_WS_URL = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"

    PING_INTERVAL = 15  # seconds - Bitmart requires ping every 15-20 seconds
    MAX_LIFETIME = 23 * 60 * 60  # 23 hours before forced reconnect

    def __init__(
            self,
            symbols: list[str],
            update_callback: Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]],
            market_type: MarketType = MarketType.SPOT,
            proxy_url: str | None = None
    ):
        self.symbols = symbols
        self.update_callback = update_callback
        self.market_type = market_type
        self.proxy_url = proxy_url
        self.last_update_time: dict[str, float] = {sym: 0.0 for sym in symbols}
        self._ping_task: asyncio.Task | None = None
        self._lifetime_task: asyncio.Task | None = None

        # Futures: collect both way=1 (bids) and way=2 (asks) before sending
        self._futures_partial_books: dict[str, dict] = {}

        if market_type == MarketType.SPOT:
            self.ws_url = self.SPOT_WS_URL
        else:
            self.ws_url = self.FUTURES_WS_URL

    async def start(self):
        """Main connection loop with auto-reconnect."""
        retry_delay = 1.0
        consecutive_429 = 0

        while True:
            try:
                conn = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.ws_connect(self.ws_url, proxy=self.proxy_url) as ws:
                        logger.info(f"Bitmart WS connected: {self.market_type.value}, {len(self.symbols)} symbols")
                        await self._subscribe_all(ws)

                        self._ping_task = asyncio.create_task(self._ping_loop(ws))
                        self._lifetime_task = asyncio.create_task(self._enforce_lifetime(ws))

                        # Reset on successful connection
                        retry_delay = 1.0
                        consecutive_429 = 0

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                # Spot messages are compressed with zlib
                                parsed = self._parse_compressed_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                parsed = self._parse_text_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"Bitmart WS error: {msg.data}")
                                break

            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    consecutive_429 += 1
                    # Aggressive backoff for rate limiting: 10s, 30s, 60s, 120s...
                    retry_delay = min(10 * (2 ** consecutive_429), 300)
                    jitter = random.uniform(0, retry_delay * 0.3)
                    logger.warning(f"Bitmart 429 rate limited, waiting {retry_delay + jitter:.1f}s (attempt {consecutive_429})")
                    await asyncio.sleep(retry_delay + jitter)
                else:
                    logger.error(f"Bitmart WS HTTP error {e.status}, reconnecting: {e}")
                    await asyncio.sleep(retry_delay + random.uniform(0, 2))
                    retry_delay = min(retry_delay * 2, 60.0)

            except Exception as e:
                error_str = str(e)
                if "429" in error_str:
                    consecutive_429 += 1
                    retry_delay = min(10 * (2 ** consecutive_429), 300)
                    jitter = random.uniform(0, retry_delay * 0.3)
                    logger.warning(f"Bitmart 429 rate limited, waiting {retry_delay + jitter:.1f}s (attempt {consecutive_429})")
                    await asyncio.sleep(retry_delay + jitter)
                else:
                    logger.error(f"Bitmart WS error, reconnecting: {e}")
                    await asyncio.sleep(retry_delay + random.uniform(0, 2))
                    retry_delay = min(retry_delay * 2, 60.0)

            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Send periodic pings to keep connection alive."""
        while True:
            try:
                # Bitmart expects text 'ping' message
                await ws.send_str("ping")
            except (RuntimeError, ConnectionResetError, asyncio.CancelledError):
                break
            await asyncio.sleep(self.PING_INTERVAL)

    async def _enforce_lifetime(self, ws: aiohttp.ClientWebSocketResponse):
        """Force reconnect after max lifetime to ensure fresh connection."""
        await asyncio.sleep(self.MAX_LIFETIME)
        logger.info("Bitmart WS lifetime exceeded, closing for reconnect")
        await ws.close()

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse):
        """Subscribe to orderbook depth for all symbols.

        Bitmart limits subscriptions to 20 topics per message (error 90005).
        Also max 100 messages per 10 seconds (error 90007).
        """
        batch_size = 20  # Bitmart limit per subscription message

        if self.market_type == MarketType.SPOT:
            # Spot: subscribe in batches to avoid rate limit
            # Format: spot/depth5:BTC_USDT or spot/depth20:BTC_USDT
            all_args = [f"spot/depth20:{sym.upper()}" for sym in self.symbols]

            for i in range(0, len(all_args), batch_size):
                if ws.closed:
                    logger.warning(f"Bitmart: WS closed during subscription at batch {i // batch_size}")
                    return
                batch = all_args[i:i + batch_size]
                payload = {"op": "subscribe", "args": batch}
                try:
                    await ws.send_str(orjson.dumps(payload).decode("utf-8"))
                except (RuntimeError, ConnectionResetError) as e:
                    logger.warning(f"Bitmart: failed to send subscription batch: {e}")
                    return
                # Small delay to avoid hitting 100 messages per 10s limit
                if i + batch_size < len(all_args):
                    await asyncio.sleep(0.12)
        else:
            # Futures: different format and URL
            # Futures symbols don't have underscore: BTCUSDT
            all_args = [f"futures/depth20:{sym.upper().replace('_', '')}" for sym in self.symbols]

            for i in range(0, len(all_args), batch_size):
                if ws.closed:
                    logger.warning(f"Bitmart: WS closed during subscription at batch {i // batch_size}")
                    return
                batch = all_args[i:i + batch_size]
                payload = {"action": "subscribe", "args": batch}
                try:
                    await ws.send_str(orjson.dumps(payload).decode("utf-8"))
                except (RuntimeError, ConnectionResetError) as e:
                    logger.warning(f"Bitmart: failed to send subscription batch: {e}")
                    return
                if i + batch_size < len(all_args):
                    await asyncio.sleep(0.12)

        logger.info(f"Bitmart: subscribed to {len(self.symbols)} depth channels")

    def _parse_compressed_message(self, raw: bytes) -> dict[str, Any] | None:
        """Parse zlib-compressed message (spot channel)."""
        try:
            # Decompress using zlib
            decompressed = zlib.decompress(raw, zlib.MAX_WBITS | 16)
            return self._parse_json_message(decompressed.decode("utf-8"))
        except zlib.error:
            # Try raw deflate
            try:
                decompressed = zlib.decompress(raw, -zlib.MAX_WBITS)
                return self._parse_json_message(decompressed.decode("utf-8"))
            except Exception as e:
                logger.debug(f"Bitmart: failed to decompress message: {e}")
                return None
        except Exception as e:
            logger.debug(f"Bitmart: failed to parse compressed message: {e}")
            return None

    def _parse_text_message(self, raw: str) -> dict[str, Any] | None:
        """Parse text message (futures channel or pong)."""
        if raw == "pong":
            return None
        return self._parse_json_message(raw)

    def _parse_json_message(self, raw: str) -> dict[str, Any] | None:
        """Parse JSON message and extract orderbook data.

        Spot format: {"table":"spot/depth5","data":[{"symbol":"BTC_USDT","asks":[...],"bids":[...]}]}
        Futures format: {"group":"futures/depth20:BTCUSDT","data":{"symbol":"BTCUSDT","way":1,"depths":[...]}}
        """
        try:
            msg = orjson.loads(raw)

            # Check if it's a depth message
            table = msg.get("table", "")
            group = msg.get("group", "")

            if "depth" not in table and "depth" not in group:
                return None

            data = msg.get("data")
            if not data:
                return None

            # SPOT format: data is a list
            if isinstance(data, list):
                if not data:
                    return None
                data = data[0]

                symbol = data.get("symbol", "").upper()
                if not symbol:
                    return None

                # Parse asks and bids: [["price", "qty"], ...]
                asks = data.get("asks", [])
                bids = data.get("bids", [])

                parsed_asks = []
                for item in asks[:20]:
                    if len(item) >= 2:
                        try:
                            parsed_asks.append((float(item[0]), float(item[1])))
                        except (ValueError, TypeError):
                            continue

                parsed_bids = []
                for item in bids[:20]:
                    if len(item) >= 2:
                        try:
                            parsed_bids.append((float(item[0]), float(item[1])))
                        except (ValueError, TypeError):
                            continue

                return {
                    "symbol": symbol,
                    "timestamp": data.get("ms_t", int(time.time() * 1000)),
                    "asks": parsed_asks,
                    "bids": parsed_bids,
                }

            # FUTURES format: data is a dict with "way" field
            elif isinstance(data, dict):
                symbol = data.get("symbol", "").upper()
                way = data.get("way")  # 1 = bids, 2 = asks
                depths = data.get("depths", [])

                if not symbol or way not in (1, 2) or not depths:
                    return None

                # Parse depths: [{"price": "123.45", "vol": "100"}, ...]
                parsed_depths = []
                for item in depths[:20]:
                    try:
                        parsed_depths.append((float(item["price"]), float(item["vol"])))
                    except (KeyError, ValueError, TypeError):
                        continue

                # Store partial orderbook and combine later
                # We need both way=1 (bids) and way=2 (asks) messages
                return {
                    "symbol": symbol,
                    "timestamp": data.get("ms_t", int(time.time() * 1000)),
                    "way": way,
                    "depths": parsed_depths,
                }

        except Exception as e:
            logger.debug(f"Bitmart: failed to parse message: {e}")
            return None

    async def _handle_orderbook(self, msg: dict[str, Any]):
        """Process orderbook update and call callback."""
        symbol = msg.get("symbol")
        if not symbol:
            return

        # Futures: collect both way=1 (bids) and way=2 (asks) messages
        if "way" in msg:
            way = msg["way"]
            depths = msg.get("depths", [])
            timestamp = msg.get("timestamp")

            # Initialize or update partial book
            if symbol not in self._futures_partial_books:
                self._futures_partial_books[symbol] = {"timestamp": timestamp}

            if way == 1:
                self._futures_partial_books[symbol]["bids"] = depths
            elif way == 2:
                self._futures_partial_books[symbol]["asks"] = depths

            # Check if we have both sides
            partial = self._futures_partial_books[symbol]
            if "bids" not in partial or "asks" not in partial:
                return  # Wait for both sides

            # Rate limit: 1 update per second per symbol
            now = time.time()
            last = self.last_update_time.get(symbol, 0)
            if now - last < 1.0:
                return

            self.last_update_time[symbol] = now

            ob = {
                "asks": partial["asks"],
                "bids": partial["bids"]
            }
            asyncio.create_task(self.update_callback(symbol, ob, timestamp))

        # SPOT: direct processing
        else:
            # Rate limit: 1 update per second per symbol
            now = time.time()
            last = self.last_update_time.get(symbol, 0)
            if now - last < 1.0:
                return

            self.last_update_time[symbol] = now

            timestamp = msg.get("timestamp")
            ob = {
                "asks": msg.get("asks", []),
                "bids": msg.get("bids", [])
            }

            asyncio.create_task(self.update_callback(symbol, ob, timestamp))
