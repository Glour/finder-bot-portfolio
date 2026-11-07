import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, List

import msgpack
import orjson

from infrastructure.database.models.base import SourceType, MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.base import BaseParser
from infrastructure.services.collectors.structures.interfaces import SupportsPriceParsing
from infrastructure.services.collectors.structures.utils import make_orderbook_key
from infrastructure.services.common.cache import RedisCache
from settings import logger


class BasePriceParser(BaseParser, ABC):
    def __init__(
            self,
            cache: RedisCache,
            repo: RequestsRepo,
            *,
            source_name: str,
            source_type: SourceType,
            stream_key: str = "prices:spot",
    ):
        super().__init__(cache, repo, source_name=source_name, source_type=source_type)
        self.stream_key = stream_key

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    async def send_to_redis_batch(self, batch: List[dict]):
        if not batch:
            return

        try:
            pipe = self.cache.client.pipeline(transaction=False)
            for data in batch:
                redis_key = make_orderbook_key(
                    str(data["global_token_id"]),
                    str(data["global_source_id"]),
                    str(data["token_id"]),
                    str(data["market_type"])
                )

                payload = {
                    "symbol": data["symbol"],
                    "market_type": data["market_type"],
                    "token_id": data["token_id"],
                    "global_token_id": data["global_token_id"],
                    "global_source_id": data["global_source_id"],
                    "get_at": data["get_at"],
                    "orderbook": data["orderbook"],
                }

                packed = msgpack.packb(payload, use_bin_type=True)
                pipe.set(redis_key, packed)
                pipe.expire(redis_key, 120)
                
                # CRITICAL FIX (2026-02-19): Send to Redis Stream for price-saver/spread-collector
                pipe.xadd(self.stream_key, {"data": packed}, maxlen=100000, approximate=True)

            await pipe.execute()

        except Exception as e:
            logger.exception(f"Failed to push batch to Redis stream {self.stream_key}: {e}")


class BaseWSCexPriceParser(BasePriceParser, SupportsPriceParsing, ABC):
    def __init__(
            self,
            repo: RequestsRepo,
            cache: RedisCache,
            *,
            source_name: str,
            ws_client_cls: type,
            symbols_per_ws: int,
            stream_key: str,
            market_type: MarketType = MarketType.SPOT,
            use_proxies: bool = False,
    ):
        super().__init__(
            cache=cache,
            repo=repo,
            source_name=source_name,
            source_type=SourceType.CEX,
            stream_key=stream_key
        )
        self.market_type = market_type
        self.token_map: dict[str, dict] = {}
        self._clients: dict[int, Any] = {}
        self._ws_tasks: dict[int, asyncio.Task] = {}
        self._client_symbols: dict[int, set[str]] = {}
        self._symbol_to_client: dict[str, int] = {}
        self._next_client_id: int = 0
        self.symbols_per_ws = symbols_per_ws
        self._ws_client_cls = ws_client_cls
        self._known_symbols: set[str] = set()
        self.use_proxies = use_proxies
        self._proxies: list[str] = []
        self._proxy_weights: dict[str, float] = {}  # Proxy reliability scores

        self._latest_updates: dict[str, dict] = {}
        self._latest_updates_lock = asyncio.Lock()

    def _create_ws_client(self, batch: list[str], callback, proxy_url: str | None = None):
        """Create WebSocket client. Override in subclass for custom behavior."""
        return self._ws_client_cls(batch, callback, market_type=self.market_type, proxy_url=proxy_url)

    async def setup(self):
        """Setup method to load proxies if needed"""
        await super().setup()
        if self.use_proxies:
            await self._load_proxies()

    async def _load_proxies(self):
        """Load proxies from file for WebSocket connections"""
        import os
        proxy_file = "/app/data/proxies.txt"
        if not os.path.exists(proxy_file):
            logger.warning(f"Proxy file not found: {proxy_file}, WS connections may fail if IP is blocked")
            return

        try:
            with open(proxy_file, "r") as f:
                proxies = [line.strip() for line in f if line.strip()]

            # Format: user:pass@ip:port -> http://user:pass@ip:port
            self._proxies = [f"http://{p}" if not p.startswith("http") else p for p in proxies]
            # Initialize all proxies with equal weight
            self._proxy_weights = {p: 1.0 for p in self._proxies}
            logger.info(f"Loaded {len(self._proxies)} proxies for WebSocket connections")
        except Exception as e:
            logger.error(f"Failed to load proxies: {e}")

    def _get_random_proxy(self) -> str | None:
        """Get a weighted random proxy URL for WebSocket connection.
        Proxies with higher weights (more reliable) are chosen more often."""
        if not self._proxies:
            return None

        import random
        # Filter out proxies with very low weight (< 0.1)
        valid_proxies = [(p, w) for p, w in self._proxy_weights.items() if w >= 0.1]
        if not valid_proxies:
            # All proxies are bad, reset weights
            logger.warning("All proxies marked as unreliable, resetting weights")
            self._proxy_weights = {p: 1.0 for p in self._proxies}
            valid_proxies = list(self._proxy_weights.items())

        # Weighted random selection
        proxies, weights = zip(*valid_proxies)
        return random.choices(proxies, weights=weights, k=1)[0]

    async def collect_tokens(self) -> list[dict]:
        redis_key = f"available_tokens:{self.source_name}:{self.source.id}:{self.market_type.value}"
        tokens = []

        token_jsons = await self.cache.client.smembers(redis_key)

        for token_json in token_jsons:
            try:
                token = orjson.loads(token_json)
            except Exception as e:
                logger.error(f"Failed to parse token JSON: {e}")
                continue

            symbol = token["symbol"]
            if symbol in self._known_symbols or symbol in self._symbol_to_client:
                continue

            lock_key = f"token_lock:{self.source.id}:{self.market_type.value}:{symbol}"
            locked = await self.cache.client.set(lock_key, 1, ex=60, nx=True)
            if not locked:
                continue

            await self.cache.client.expire(lock_key, 60)
            await self.cache.client.srem(redis_key, token_json)
            tokens.append(token)
        return tokens

    async def redis_writer(self, flush_interval: float = 0.02):
        while True:
            await asyncio.sleep(flush_interval)

            async with self._latest_updates_lock:
                batch = list(self._latest_updates.values())
                self._latest_updates.clear()

            if not batch:
                continue

            asyncio.create_task(self.send_to_redis_batch(batch))

    async def update_callback(self, sym: str, ob: dict, get_at_time: int):
        # CRITICAL FIX (2026-02-19): Try unique key first (symbol:market_type)
        # to avoid mixing SPOT and FUTURES tokens with similar symbols
        unique_key = f"{sym}:{self.market_type.value}"
        tok = self.token_map.get(unique_key)
        
        # DEBUG: Log VIOLET lookups
        if "VIOLET" in sym.upper():
            logger.info(f"DEBUG VIOLET: sym={sym}, unique_key={unique_key}, found={bool(tok)}")
            if tok:
                logger.info(f"DEBUG VIOLET tok: gt_id={tok.get('global_token_id')}, symbol={tok.get('symbol')}")
        
        if not tok:
            # Try lowercase unique key
            unique_key_lower = f"{sym.lower()}:{self.market_type.value}"
            tok = self.token_map.get(unique_key_lower)
            
            if "VIOLET" in sym.upper() and tok:
                logger.info(f"DEBUG VIOLET lower: found via {unique_key_lower}, gt_id={tok.get('global_token_id')}")
        
        if not tok:
            # Fallback: try symbol only (backward compatibility)
            tok = self.token_map.get(sym)
            if not tok:
                tok = self.token_map.get(sym.lower())
                if not tok:
                    return
            
            if "VIOLET" in sym.upper():
                logger.info(f"DEBUG VIOLET fallback: found via symbol, gt_id={tok.get('global_token_id')}")
        
        if not tok.get("global_source_id"):
            return

        data = {
            "symbol": sym,
            "orderbook": ob,
            "market_type": tok["market_type"],
            "token_id": tok["id"],
            "global_token_id": tok["global_token_id"],
            "global_source_id": tok["global_source_id"],
            "get_at": get_at_time,
        }

        # TEMPORARY FIX: TQQQ stock split 1:2 on Gate (2026-02-19)
        # SPOT price needs to be multiplied by 2 to match FUTURES
        if (self.source_name.lower() == "gate" and 
            sym.upper() == "TQQQ" and 
            tok["market_type"] == "SPOT"):
            if "asks" in ob and "bids" in ob:
                ob["asks"] = [[price * 2, volume] for price, volume in ob["asks"]]
                ob["bids"] = [[price * 2, volume] for price, volume in ob["bids"]]
                logger.info(f"Applied TQQQ SPOT price multiplier x2 for {self.source_name}")

        async with self._latest_updates_lock:
            self._latest_updates[sym] = data.copy()

    async def start(self):
        asyncio.create_task(self._monitor_ws_updates())
        asyncio.create_task(self.redis_writer())

        # First collect immediately, then every 180 seconds (3 minutes)
        first_run = True
        while True:
            if not first_run:
                # Wait 180s between collections to prevent rapid client recreation
                await asyncio.sleep(180)
            first_run = False

            logger.info(f"{self.source_name}: attempting to collect tokens...")
            new_tokens = await self.collect_tokens()
            logger.info(f"{self.source_name}: collected {len(new_tokens)} new tokens")
            new_symbols = [t["symbol"] for t in new_tokens]
            for t in new_tokens:
                # CRITICAL FIX (2026-02-19): Index by unique key (symbol + market_type)
                # to avoid collisions when SPOT and FUTURES have similar symbols
                # Example: CAKE/USDT (SPOT) and CAKE-PERP (FUTURES) both match "CAKE"
                unique_key = f"{t['symbol']}:{t['market_type']}"
                unique_key_lower = f"{t['symbol'].lower()}:{t['market_type']}"
                
                self.token_map[unique_key] = t
                self.token_map[unique_key_lower] = t
                
                # Also store by symbol only for backward compatibility
                # WARNING: This may cause collisions if SPOT and FUTURES coexist
                self.token_map[t["symbol"]] = t
                self.token_map[t["symbol"].lower()] = t

            for i in range(0, len(new_symbols), self.symbols_per_ws):
                batch = new_symbols[i:i + self.symbols_per_ws]
                client_id = self._next_client_id
                self._next_client_id += 1

                # Pass proxy URL if proxies are enabled
                proxy_url = self._get_random_proxy() if self.use_proxies else None
                client = self._create_ws_client(batch, self.update_callback, proxy_url)
                self._clients[client_id] = client
                self._client_symbols[client_id] = set(batch)
                for sym in batch:
                    self._symbol_to_client[sym] = client_id
                    self._known_symbols.add(sym)

                self._ws_tasks[client_id] = asyncio.create_task(self._start_client(client_id, client))

                # Delay between WS connections to avoid rate limiting (429)
                # 3 seconds is conservative for Bitmart which has strict rate limits
                await asyncio.sleep(3.0)

            for cid, t in list(self._ws_tasks.items()):
                if t.done():
                    await self._cleanup_client(cid)

    async def _start_client(self, client_id: int, client: Any):
        try:
            await client.start()
        except Exception as e:
            logger.error(f"Client {client_id} crashed: {e}")
        finally:
            await self._cleanup_client(client_id)

    async def _cleanup_client(self, client_id: int):
        symbols = self._client_symbols.pop(client_id, set())

        for sym in symbols:
            self._symbol_to_client.pop(sym, None)
            self._known_symbols.discard(sym)

            lock_key = f"token_lock:{self.source.id}:{self.market_type.value}:{sym}"
            try:
                await self.cache.client.delete(lock_key)
            except Exception as e:
                logger.error(f"Failed to delete lock {lock_key}: {e}")

        task = self._ws_tasks.pop(client_id, None)
        if task and not task.done():
            task.cancel()

        logger.info(f"Cleaned up client {client_id}, freed {len(symbols)} symbols")

    async def _monitor_ws_updates(self, interval_seconds: int = 60, stale_threshold: int = 600):
        while True:
            await asyncio.sleep(interval_seconds)
            now = time.time()
            total_symbols, stale_symbols = 0, 0
            clients_to_kill = []

            for client_id, client in self._clients.items():
                last_updates: dict[str, float] = getattr(client, "last_update_time", {})
                client_total = len(last_updates)
                client_stale = 0

                for sym, ts in last_updates.items():
                    total_symbols += 1
                    if now - ts > stale_threshold:
                        stale_symbols += 1
                        client_stale += 1

                # Kill only completely broken clients (>90% stale) to avoid cleanup loop
                # Increased threshold from 50% to 90% to be more conservative
                if client_total > 0 and (client_stale / client_total) > 0.9:
                    clients_to_kill.append(client_id)
                    logger.warning(
                        f"Client {client_id} has {client_stale}/{client_total} "
                        f"({client_stale/client_total*100:.1f}%) stale symbols, "
                        f"scheduling for cleanup"
                    )

            logger.info(f"WS monitor: total={total_symbols}, stale={stale_symbols}, threshold={stale_threshold}s")

            # Auto-recovery disabled to prevent cleanup loop
            # LBank WebSocket connections are unstable and auto-recovery causes infinite client recreation
            # for client_id in clients_to_kill:
            #     await self._cleanup_client(client_id)
