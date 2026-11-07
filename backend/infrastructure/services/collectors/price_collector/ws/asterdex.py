import asyncio
import time
from typing import Callable, Any, Coroutine

import aiohttp
import orjson

from infrastructure.database.models.base import MarketType
from settings import logger

# REST polling interval for stale symbols (seconds)
REST_POLL_INTERVAL = 30
# If WS hasn't updated a symbol in this many seconds, use REST fallback
REST_FALLBACK_THRESHOLD = 120


class AsterdexWSClient:
    """WebSocket client for Asterdex orderbook streaming.

    API Documentation: https://github.com/asterdex/api-docs

    WebSocket URLs:
    - Spot: wss://sstream.asterdex.com
    - Futures: wss://fstream.asterdex.com

    Protocol is Binance-compatible with SUBSCRIBE/UNSUBSCRIBE methods.

    REST fallback: for symbols that don't receive WS updates (low-liquidity pairs),
    periodically polls bookTicker REST endpoint every 30s.
    """

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
        self._rest_poll_task: asyncio.Task | None = None

        if market_type == MarketType.SPOT:
            self.ws_url = "wss://sstream.asterdex.com/ws"
            self.rest_ticker_url = "https://sapi.asterdex.com/api/v1/ticker/bookTicker"
        elif market_type == MarketType.FUTURES:
            self.ws_url = "wss://fstream.asterdex.com/ws"
            self.rest_ticker_url = "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker"

    async def start(self):
        # Start REST polling as a background task (runs independently of WS)
        self._rest_poll_task = asyncio.create_task(self._rest_poll_loop())

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
                                parsed = self._parse_depth_message(msg.data)
                                if parsed:
                                    await self._handle_orderbook(parsed)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error("Asterdex WebSocket error, closing")
                                break

            except Exception as e:
                logger.error(f"Asterdex WS error, reconnecting: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 5.0)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                if self._lifetime_task:
                    self._lifetime_task.cancel()

    async def _rest_poll_loop(self):
        """Periodically fetch prices via REST for symbols that haven't received WS updates."""
        # Wait a bit before first poll to let WS connect and receive initial data
        await asyncio.sleep(REST_POLL_INTERVAL)

        while True:
            try:
                await self._fetch_and_update_stale()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Asterdex REST poll error: {e}")
            await asyncio.sleep(REST_POLL_INTERVAL)

    async def _fetch_and_update_stale(self):
        """Fetch REST bookTicker for all symbols and update stale ones."""
        now = time.time()
        stale_symbols = {
            sym for sym, ts in self.last_update_time.items()
            if now - ts > REST_FALLBACK_THRESHOLD
        }

        if not stale_symbols:
            return

        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(self.rest_ticker_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"Asterdex REST ticker returned {resp.status}")
                    return
                data = await resp.json()

        if not isinstance(data, list):
            return

        updated = 0
        ts_ms = int(time.time() * 1000)
        for ticker in data:
            sym = ticker.get("symbol", "")
            if sym not in stale_symbols:
                continue

            bid_price = ticker.get("bidPrice", "0")
            bid_qty = ticker.get("bidQty", "0")
            ask_price = ticker.get("askPrice", "0")
            ask_qty = ticker.get("askQty", "0")

            try:
                bid_f = float(bid_price)
                ask_f = float(ask_price)
            except (ValueError, TypeError):
                continue

            # Skip symbols with no valid order book
            if bid_f <= 0 and ask_f <= 0:
                continue

            bids = [[bid_price, bid_qty]] if bid_f > 0 else []
            asks = [[ask_price, ask_qty]] if ask_f > 0 else []

            if not bids and not asks:
                continue

            ob = {"bids": bids, "asks": asks}
            self.last_update_time[sym] = time.time()
            asyncio.create_task(self.update_callback(sym, ob, ts_ms))
            updated += 1

        if updated > 0:
            logger.info(f"Asterdex REST fallback: updated {updated}/{len(stale_symbols)} stale symbols")

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
        # Reconnect after 23 hours to avoid connection timeout
        await asyncio.sleep(23 * 60 * 60)
        logger.info("Asterdex WS lifetime exceeded, closing connection to force reconnect")
        await ws.close()

    async def _subscribe_all(self, ws):
        """Subscribe to bookTicker streams for all symbols.

        bookTicker always sends the best bid/ask — never empty.
        Unlike @depth (incremental), bookTicker is a full snapshot on every change.
        Format: <symbol>@bookTicker
        """
        # Subscribe in batches to avoid hitting message limits
        batch_size = 50  # Max streams per subscribe message
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i:i + batch_size]
            streams = [f"{sym.lower()}@bookTicker" for sym in batch]

            payload = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": i + 1
            }
            await ws.send_str(orjson.dumps(payload).decode("utf-8"))
            await asyncio.sleep(0.1)  # Small delay between batches

    @staticmethod
    def _parse_depth_message(raw: str) -> dict[str, Any] | None:
        """Parse bookTicker message from Asterdex.

        bookTicker always contains the current best bid/ask — never empty.
        Expected format (Binance-compatible):
        {
            "e": "bookTicker",
            "s": "BTCUSDT",   # Symbol
            "b": "50000.00",  # Best bid price
            "B": "1.5",       # Best bid qty
            "a": "50001.00",  # Best ask price
            "A": "0.8",       # Best ask qty
            "T": 1234567890123
        }
        """
        try:
            msg = orjson.loads(raw)

            # Skip subscription confirmations and other non-data messages
            if "result" in msg or "id" in msg:
                return None

            # Accept bookTicker events
            event_type = msg.get("e")
            if event_type != "bookTicker":
                return None

            symbol = msg.get("s")
            if not symbol:
                return None

            bid_price = msg.get("b")
            bid_qty   = msg.get("B")
            ask_price = msg.get("a")
            ask_qty   = msg.get("A")
            timestamp = msg.get("T") or msg.get("E") or int(time.time() * 1000)

            bids = [[bid_price, bid_qty]] if bid_price and float(bid_price) > 0 else []
            asks = [[ask_price, ask_qty]] if ask_price and float(ask_price) > 0 else []

            if not bids and not asks:
                return None

            return {
                "symbol": symbol,
                "data": {
                    "bids": bids,
                    "asks": asks,
                },
                "ts": timestamp
            }
        except Exception as e:
            logger.error(f"Asterdex: Failed to parse bookTicker message: {e}, raw: {raw[:500]}")
            return None

    async def _handle_orderbook(self, msg: dict):
        data = msg.get("data", {})
        symbol = msg.get("symbol")
        if not symbol or not data:
            return

        timestamp = msg.get("ts")
        now = time.time()
        last = self.last_update_time.get(symbol, 0)

        # Rate limit updates to 1 per second per symbol
        if now - last < 1.0:
            return

        self.last_update_time[symbol] = now

        # Format orderbook for callback
        # Limit to top 20 levels
        bids = data.get("bids", [])[:20]
        asks = data.get("asks", [])[:20]

        if not bids and not asks:
            return

        ob = {"asks": asks, "bids": bids}
        asyncio.create_task(self.update_callback(symbol, ob, timestamp))
