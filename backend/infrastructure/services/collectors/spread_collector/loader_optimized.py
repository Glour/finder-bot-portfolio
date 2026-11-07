"""
Optimized Token Loader with Redis Streams support.

Performance improvements:
1. Cache token metadata in Redis (5-minute TTL)
2. Only fetch orderbooks (MGET) on each iteration
3. Redis Streams: publish only tokens with changed orderbooks (Phase 2)
4. msgpack-only serialization

Expected latency reduction with Streams: 75-90% (from ~6s to <1s)
"""
import ast
import asyncio
import hashlib
import json
import time
from datetime import datetime, UTC
from typing import Optional
from uuid import UUID

import msgpack
from redis import asyncio as aioredis
from redis.exceptions import BusyLoadingError, ConnectionError as RedisConnectionError

from infrastructure.database import setup as database
from infrastructure.database.models import TokenBlacklist
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.optimization_config import ENV_CONFIG
from infrastructure.services.collectors.structures.normalizer import normalize_pair, msgpack_default, parse_multiplier
from infrastructure.services.collectors.structures.utils import make_orderbook_key
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.collectors.utils.profiler import ComponentTimer
from settings import logger, settings


class OptimizedTokenLoader:
    """
    Optimized token loader with aggressive caching.

    Key optimizations:
    - Token metadata cached in Redis (TTL: 5 min)
    - Only MGET orderbooks each iteration
    - No SQL queries in hot path
    - msgpack-only serialization
    """

    # Cache keys
    TOKEN_METADATA_CACHE_KEY = "token_loader:metadata:v1"
    TOKEN_BLACKLIST_CACHE_KEY = "token_blacklist"
    EXCHANGE_BLACKLIST_CACHE_KEY = "exchange_blacklist"

    # Cache TTLs
    METADATA_CACHE_TTL = 300  # 5 minutes
    BLACKLIST_CACHE_TTL = 5    # 5 seconds — муты вступают в силу немедленно
    EXCHANGE_BLACKLIST_CACHE_TTL = 30  # 30 seconds

    def __init__(
        self,
        repo: RequestsRepo,
        cache: RedisCache,
        tokens_load_limit: int = 10000,
        enable_profiling: bool = True,
        use_streams: bool = False
    ):
        self.repo = repo
        self.cache = cache
        self.tokens_load_limit = tokens_load_limit
        self.enable_profiling = enable_profiling
        self.use_streams = use_streams

        # Redis Streams configuration
        self.stream_key = "orderbooks:updates"
        self.stream_maxlen = ENV_CONFIG.stream_max_len

        # Performance tracking
        self.total_cycles = 0
        self.total_changes = 0

        # In-memory cache for faster access
        self._metadata_cache: Optional[list[dict]] = None
        self._metadata_cache_timestamp: float = 0
        self._blacklist_cache: Optional[set[tuple[str, str]]] = None
        self._blacklisted_gt_ids_cache: set[str] = set()
        self._blacklisted_symbols_cache: set[str] = set()
        self._blacklist_cache_timestamp: float = 0
        self._exchange_blacklist_cache: Optional[set[tuple[UUID, str | None]]] = None
        self._exchange_blacklist_cache_timestamp: float = 0

        # DEX token index cache (rebuilt when metadata changes)
        self._dex_token_index: Optional[dict] = None
        self._dex_token_index_metadata_ts: float = 0

    async def _retry_on_redis_loading(self, operation_name: str, coro_func, max_retries: int = 10):
        """
        Retry callable that returns coroutine on BusyLoadingError.

        Args:
            operation_name: Name for logging
            coro_func: Callable that returns a coroutine (e.g., lambda: self.cache.get(...))
            max_retries: Maximum retry attempts

        Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s, 30s...
        """
        for attempt in range(max_retries):
            try:
                return await coro_func()  # Call function to get NEW coroutine
            except BusyLoadingError as e:
                delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                logger.warning(
                    f"Redis is loading dataset ({operation_name}), "
                    f"retry {attempt + 1}/{max_retries} after {delay}s"
                )
                await asyncio.sleep(delay)
            except (RedisConnectionError, ConnectionRefusedError) as e:
                delay = min(2 ** attempt, 10)  # Cap at 10 seconds for connection errors
                logger.warning(
                    f"Redis connection error ({operation_name}): {e}, "
                    f"retry {attempt + 1}/{max_retries} after {delay}s"
                )
                await asyncio.sleep(delay)

        # Final attempt without catching
        return await coro_func()

    async def start(self):
        """Main loop."""
        mode = "STREAMS" if self.use_streams else "LIST"
        logger.info(f"Starting optimized token loader | mode={mode} | stream={self.stream_key if self.use_streams else 'N/A'}")

        # Pre-warm caches with retry (handles Redis loading dataset on startup)
        await self._retry_on_redis_loading(
            "refresh_metadata_cache",
            lambda: self._refresh_metadata_cache()
        )
        await self._retry_on_redis_loading(
            "refresh_blacklist_cache",
            lambda: self._refresh_blacklist_cache()
        )
        await self._retry_on_redis_loading(
            "refresh_exchange_blacklist_cache",
            lambda: self._refresh_exchange_blacklist_cache()
        )

        # Ensure Redis Stream exists on startup (create it if missing)
        if self.use_streams:
            try:
                await self._retry_on_redis_loading(
                    "ensure_stream_exists",
                    lambda: self.cache.client.xinfo_stream(self.stream_key)
                )
                logger.info(f"Redis Stream '{self.stream_key}' already exists")
            except Exception:
                # Stream doesn't exist - create it with a dummy entry
                logger.info(f"Creating Redis Stream '{self.stream_key}'...")
                await self._retry_on_redis_loading(
                    "create_stream",
                    lambda: self.cache.client.xadd(
                        self.stream_key,
                        {"init": "stream_created"},
                        id="0-1"  # Use minimal ID for initialization
                    )
                )
                logger.info(f"Redis Stream '{self.stream_key}' created successfully")

        while True:
            start = time.perf_counter()
            try:
                # Wrap in retry to handle BusyLoadingError during connection pool health checks
                count = await self._retry_on_redis_loading(
                    "load_tokens_with_orderbooks",
                    lambda: self.load_tokens_with_orderbooks()
                )
                elapsed = time.perf_counter() - start

                if self.use_streams:
                    self.total_cycles += 1
                    self.total_changes += count
                    avg_changes = self.total_changes / self.total_cycles if self.total_cycles > 0 else 0
                    logger.info(
                        f"Published {count} changed tokens to stream in {elapsed:.2f}s | "
                        f"avg_changes={avg_changes:.1f}"
                    )
                else:
                    logger.info(
                        f"Pushed {count} tokens into analyzer queue in {elapsed:.2f}s "
                        f"(cache_hit={self._metadata_cache is not None})"
                    )
            except BusyLoadingError as e:
                # If retry exhausted, wait and try again
                logger.error(f"Redis still loading after retries, waiting 30s: {e}")
                await asyncio.sleep(30)
            except (RedisConnectionError, ConnectionRefusedError) as e:
                # Connection issues - wait and retry
                logger.error(f"Redis connection error, waiting 10s: {e}")
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Error while loading tokens: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def load_tokens_with_orderbooks(self) -> int:
        """Load tokens with fresh orderbooks from Redis."""
        timer = ComponentTimer("OptimizedTokenLoader")

        # Step 1: Get token metadata (cached)
        tokens = await self._get_token_metadata()
        timer.checkpoint("get_metadata")

        if not tokens:
            logger.warning("No tokens in metadata cache, refreshing...")
            await self._refresh_metadata_cache()
            return 0

        # Step 2: Get blacklist (cached)
        blacklist_set = await self._get_blacklist()
        timer.checkpoint("get_blacklist")

        # Step 3: Filter blacklisted tokens
        blacklisted_gt_ids = getattr(self, '_blacklisted_gt_ids_cache', set())
        blacklisted_symbols = getattr(self, '_blacklisted_symbols_cache', set())
        filtered_tokens = self._filter_tokens(tokens, blacklist_set, blacklisted_gt_ids, blacklisted_symbols)
        timer.checkpoint("filter_blacklist")

        if not filtered_tokens:
            logger.warning("All tokens are blacklisted")
            return 0

        # Step 4: Get exchange blacklist (cached)
        exchange_blacklist = await self._get_exchange_blacklist()
        timer.checkpoint("get_exchange_blacklist")

        # Step 5: Filter by exchange blacklist
        filtered_tokens = self._filter_by_exchange_blacklist(filtered_tokens, exchange_blacklist)
        timer.checkpoint("filter_exchange_blacklist")

        if not filtered_tokens:
            logger.warning("All tokens filtered by exchange blacklist")
            return 0

        # Step 6: Prepare orderbook keys
        keys, key_to_price = self._prepare_keys(filtered_tokens)
        timer.checkpoint("prepare_keys")

        # Step 7: Fetch orderbooks from Redis
        await self._fill_orderbooks(keys, key_to_price)
        timer.checkpoint("fill_orderbooks")

        # Step 8: Add DEX orderbooks from streams (GMGN, etc.)
        await self._fill_dex_orderbooks(filtered_tokens)
        timer.checkpoint("fill_dex_orderbooks")

        # Phase 2: Redis Streams mode - only publish changed tokens
        if self.use_streams:
            # Detect changes by comparing orderbook hashes
            changed_tokens = await self._detect_changes(filtered_tokens)
            timer.checkpoint("detect_changes")

            # Publish only changed tokens to stream
            if changed_tokens:
                await self._publish_to_stream(changed_tokens)
                timer.checkpoint("publish_stream")

            if self.enable_profiling and self.total_cycles % 10 == 0:
                timer.log(min_duration_ms=50)

            return len(changed_tokens)

        # Legacy: LIST mode - publish all tokens
        else:
            # Step 8: Serialize with msgpack
            token_packed = [
                msgpack.packb(token, default=msgpack_default, use_bin_type=True)
                for token in filtered_tokens
            ]
            timer.checkpoint("serialize_msgpack")

            if not token_packed:
                return 0

            # Step 9: Update Redis list (with retry on BusyLoadingError)
            pipe = self.cache.client.pipeline(transaction=False)
            pipe.delete("tokens:analyzer")
            pipe.rpush("tokens:analyzer", *token_packed)
            await self._retry_on_redis_loading(
                "publish_to_list",
                lambda: pipe.execute()
            )
            timer.checkpoint("redis_rpush")

            if self.enable_profiling:
                timer.log(min_duration_ms=50)

            return len(filtered_tokens)

    async def _get_token_metadata(self) -> list[dict]:
        """
        Get token metadata from cache or database.

        Returns cached data if fresh, otherwise refreshes from DB.
        """
        now = time.time()

        # Check in-memory cache first
        if (
            self._metadata_cache is not None
            and (now - self._metadata_cache_timestamp) < self.METADATA_CACHE_TTL
        ):
            return self._metadata_cache

        # Check Redis cache (with retry on BusyLoadingError)
        cached = await self._retry_on_redis_loading(
            "get_token_metadata",
            lambda: self.cache.client.get(self.TOKEN_METADATA_CACHE_KEY)
        )
        if cached:
            try:
                tokens = msgpack.unpackb(cached, raw=False)
                self._metadata_cache = tokens
                self._metadata_cache_timestamp = now
                logger.debug(f"Loaded {len(tokens)} tokens from Redis cache")
                return tokens
            except Exception as e:
                logger.error(f"Failed to unpack cached metadata: {e}")

        # Refresh from database
        return await self._refresh_metadata_cache()

    async def _refresh_metadata_cache(self) -> list[dict]:
        """
        Refresh token metadata from database.

        This is the expensive operation (1-3s SQL query).
        Only called every 5 minutes or on cache miss.
        """
        start = time.perf_counter()
        logger.info("Refreshing token metadata cache from database...")

        tokens = await self.repo.global_tokens.get_all_with_data(
            limit=self.tokens_load_limit
        )

        elapsed = time.perf_counter() - start
        logger.info(f"Fetched {len(tokens)} tokens from DB in {elapsed:.2f}s")

        # Update in-memory cache
        now = time.time()
        self._metadata_cache = tokens
        self._metadata_cache_timestamp = now

        # Update Redis cache (with retry on BusyLoadingError)
        try:
            packed = msgpack.packb(tokens, default=msgpack_default, use_bin_type=True)
            await self._retry_on_redis_loading(
                "cache_metadata",
                lambda: self.cache.client.setex(
                    self.TOKEN_METADATA_CACHE_KEY,
                    self.METADATA_CACHE_TTL,
                    packed
                )
            )
            logger.debug(f"Cached {len(tokens)} tokens in Redis")
        except Exception as e:
            logger.error(f"Failed to cache metadata in Redis: {e}")

        return tokens

    async def _get_blacklist(self) -> set[tuple[str, str]]:
        """Get token blacklist from cache or database."""
        now = time.time()

        # Check in-memory cache
        if (
            self._blacklist_cache is not None
            and (now - self._blacklist_cache_timestamp) < self.BLACKLIST_CACHE_TTL
        ):
            return self._blacklist_cache

        # Check Redis cache (with retry on BusyLoadingError)
        cached = await self._retry_on_redis_loading(
            "get_blacklist",
            lambda: self.cache.get(self.TOKEN_BLACKLIST_CACHE_KEY)
        )
        if cached:
            try:
                # Build blacklisted global_token_id set from cache
                blacklisted_gt_ids = {
                    item["global_token_id"]
                    for item in cached
                    if item.get("global_token_id")
                }

                # Build blacklisted symbols (global mutes: no chain/address)
                blacklisted_symbols = {
                    item["symbol"]
                    for item in cached
                    if item.get("symbol") and not item.get("chain") and not item.get("address")
                }

                # Build blacklisted (chain, address) pairs from cache
                blacklist = [
                    (item["chain"], item["address"])
                    for item in cached
                ]
                blacklist_set = self._build_blacklist_set(blacklist)

                # Update caches
                self._blacklist_cache = blacklist_set
                self._blacklisted_gt_ids_cache = blacklisted_gt_ids
                self._blacklisted_symbols_cache = blacklisted_symbols
                self._blacklist_cache_timestamp = now

                logger.info(
                    f"Loaded blacklist from Redis: {len(blacklist_set)} chain+address pairs, "
                    f"{len(blacklisted_gt_ids)} global_token_ids, "
                    f"{len(blacklisted_symbols)} symbols"
                )
                return blacklist_set
            except Exception as e:
                logger.error(f"Failed to load cached blacklist: {e}")

        # Refresh from database
        return await self._refresh_blacklist_cache()

    async def _refresh_blacklist_cache(self) -> set[tuple[str, str]]:
        """Refresh blacklist from database."""
        token_blacklist: list[TokenBlacklist] = await self.repo.token_blacklist.get_all()

        blacklist = [
            {
                "chain": t.chain_name,
                "address": t.contract_address,
                "global_token_id": str(t.global_token_id) if t.global_token_id else None,
                "symbol": t.global_token.symbol if t.global_token else None,
                "expires_at": t.expires_at.isoformat() if t.expires_at else None,
            }
            for t in token_blacklist
        ]

        # Cache in Redis (with retry on BusyLoadingError)
        await self._retry_on_redis_loading(
            "cache_blacklist",
            lambda: self.cache.set(
                self.TOKEN_BLACKLIST_CACHE_KEY,
                blacklist,
                timeout_secs=self.BLACKLIST_CACHE_TTL
            )
        )

        # Build and cache normalized sets
        now = datetime.now(UTC).replace(tzinfo=None)

        # Filter expired entries
        valid_blacklist_entries = [
            item for item in blacklist
            if not item.get("expires_at")
            or datetime.fromisoformat(item["expires_at"]) > now
        ]

        # Build blacklisted global_token_id set
        blacklisted_gt_ids = {
            item["global_token_id"]
            for item in valid_blacklist_entries
            if item.get("global_token_id")
        }

        # Build blacklisted symbols set (global mutes: no chain/address specified)
        blacklisted_symbols = {
            item["symbol"]
            for item in valid_blacklist_entries
            if item.get("symbol") and not item.get("chain") and not item.get("address")
        }

        # Build blacklisted (chain, address) pairs
        valid_blacklist_pairs = [
            (item["chain"], item["address"])
            for item in valid_blacklist_entries
            if item.get("chain") or item.get("address")
        ]

        blacklist_set = self._build_blacklist_set(valid_blacklist_pairs)
        self._blacklist_cache = blacklist_set
        self._blacklisted_gt_ids_cache = blacklisted_gt_ids
        self._blacklisted_symbols_cache = blacklisted_symbols
        self._blacklist_cache_timestamp = time.time()

        logger.info(
            f"Refreshed blacklist: {len(blacklist_set)} chain+address pairs, "
            f"{len(blacklisted_gt_ids)} global_token_ids, "
            f"{len(blacklisted_symbols)} symbols"
        )
        return blacklist_set

    @staticmethod
    def _build_blacklist_set(blacklist: list[tuple[str, str]]) -> set[tuple[str, str]]:
        """Build normalized blacklist set."""
        return {normalize_pair(chain, addr) for chain, addr in blacklist}

    async def _get_exchange_blacklist(self) -> set[tuple[UUID, str | None]]:
        """
        Get exchange blacklist from cache or DB.
        Returns: set of (source_id, market_type) tuples.
        market_type is None for global mutes, or "spot"/"futures" string.
        """
        now = time.time()

        # Check in-memory cache
        if (
            self._exchange_blacklist_cache is not None
            and (now - self._exchange_blacklist_cache_timestamp) < self.EXCHANGE_BLACKLIST_CACHE_TTL
        ):
            return self._exchange_blacklist_cache

        # Check Redis cache (with retry on BusyLoadingError)
        cached = await self._retry_on_redis_loading(
            "get_exchange_blacklist",
            lambda: self.cache.get(self.EXCHANGE_BLACKLIST_CACHE_KEY)
        )
        if cached:
            try:
                blacklist_set = {
                    (UUID(item["source_id"]), item.get("market_type"))
                    for item in cached
                }
                self._exchange_blacklist_cache = blacklist_set
                self._exchange_blacklist_cache_timestamp = now
                return blacklist_set
            except Exception as e:
                logger.error(f"Failed to load cached exchange blacklist: {e}")

        # Refresh from database
        return await self._refresh_exchange_blacklist_cache()

    async def _refresh_exchange_blacklist_cache(self) -> set[tuple[UUID, str | None]]:
        """Refresh from DB, filter expired entries."""
        exchange_blacklist = await self.repo.exchange_blacklist.get_active()

        # Build set of (source_id, market_type) tuples
        blacklist_set = {
            (entry.source_id, entry.market_type.value if entry.market_type else None)
            for entry in exchange_blacklist
        }

        # Cache in Redis (as JSON-serializable list, with retry on BusyLoadingError)
        cache_data = [
            {
                "source_id": str(source_id),
                "market_type": market_type,
            }
            for source_id, market_type in blacklist_set
        ]
        await self._retry_on_redis_loading(
            "cache_exchange_blacklist",
            lambda: self.cache.set(
                self.EXCHANGE_BLACKLIST_CACHE_KEY,
                cache_data,
                timeout_secs=self.EXCHANGE_BLACKLIST_CACHE_TTL
            )
        )

        # Update in-memory cache
        self._exchange_blacklist_cache = blacklist_set
        self._exchange_blacklist_cache_timestamp = time.time()

        logger.info(f"Refreshed exchange blacklist: {len(blacklist_set)} entries")
        return blacklist_set

    @staticmethod
    def _filter_by_exchange_blacklist(
        tokens: list[dict],
        exchange_blacklist: set[tuple[UUID, str | None]]
    ) -> list[dict]:
        """
        Filter tokens whose prices come from muted exchanges.
        Check each price's source_id and market_type against blacklist.
        Also check for global mutes (source_id, None).
        """
        filtered = []

        for token in tokens:
            prices = token.get("prices") or []
            valid_prices = []

            for price in prices:
                source_id = price.get("source_id") or price.get("source", {}).get("id")
                market_type = price.get("market_type")

                if not source_id:
                    # No source_id means we can't check against blacklist
                    # Include it (blacklist only filters prices WITH known source_id)
                    valid_prices.append(price)
                    continue

                # Convert source_id to UUID if it's a string
                if isinstance(source_id, str):
                    source_id = UUID(source_id)

                # Check if this specific (source_id, market_type) is muted
                # Also check if (source_id, None) is muted (global mute for all markets)
                if (source_id, market_type) in exchange_blacklist or (source_id, None) in exchange_blacklist:
                    # Price from muted exchange/market, skip it
                    continue

                valid_prices.append(price)

            # Only include token if it has at least one valid price
            if valid_prices:
                token["prices"] = valid_prices
                filtered.append(token)

        return filtered

    @staticmethod
    def _filter_tokens(tokens: list[dict], blacklist_set: set[tuple[str, str]], blacklisted_gt_ids: set[str] = None, blacklisted_symbols: set[str] = None) -> list[dict]:
        """Filter tokens against blacklist."""
        if blacklisted_gt_ids is None:
            blacklisted_gt_ids = set()
        if blacklisted_symbols is None:
            blacklisted_symbols = set()

        filtered = []

        for gt in tokens:
            # Check if global_token_id is blacklisted
            if gt.get("id") in blacklisted_gt_ids:
                continue

            # Check if symbol is globally blacklisted (covers all global_token_ids for this symbol)
            if gt.get("symbol") in blacklisted_symbols:
                continue
            
            prices = gt.get("prices") or []
            skip_token = False

            # Check if any (chain, address) pair is blacklisted
            for price in prices:
                for chain_info in price.get("chains") or []:
                    pair = normalize_pair(
                        chain_info.get("chain_name"),
                        chain_info.get("contract_address")
                    )
                    if pair in blacklist_set:
                        skip_token = True
                        break
                if skip_token:
                    break

            if not skip_token:
                filtered.append(gt)

        return filtered

    @staticmethod
    def _prepare_keys(tokens: list[dict]) -> tuple[list[str], dict[str, dict]]:
        """Prepare Redis keys for orderbook lookup."""
        key_to_price = {}
        keys = []

        for token in tokens:
            gtid = token["id"]
            for price in token.get("prices") or []:
                gts_id = price.get("global_token_source_id")
                token_id = price["token"]["id"]
                market_type = price["market_type"]

                key = make_orderbook_key(
                    str(gtid),
                    str(gts_id),
                    str(token_id),
                    str(market_type)  # Don't lowercase - price collectors use uppercase
                )

                keys.append(key)
                key_to_price[key] = price

        return keys, key_to_price

    async def _fill_orderbooks(self, keys: list[str], key_to_price: dict[str, dict]):
        """Fetch orderbooks from Redis and attach to price objects."""
        if not keys:
            return

        # MGET all orderbook keys (with retry on BusyLoadingError)
        results = await self._retry_on_redis_loading(
            "fill_orderbooks",
            lambda: self.cache.client.mget(keys)
        )

        for key, raw in zip(keys, results):
            if not raw:
                continue

            try:
                # Deserialize msgpack
                obj = msgpack.unpackb(raw, raw=False)
                price = key_to_price[key]
                price["orderbook"] = obj.get("orderbook")
                price["get_at"] = obj.get("get_at")
            except Exception as e:
                logger.error(f"Failed to unpack orderbook for {key}: {e}")

    async def force_refresh_metadata(self):
        """Force refresh of token metadata cache (for testing/admin)."""
        logger.info("Force refreshing token metadata cache")
        return await self._refresh_metadata_cache()

    async def force_refresh_blacklist(self):
        """Force refresh of blacklist cache (for testing/admin)."""
        logger.info("Force refreshing blacklist cache")
        return await self._refresh_blacklist_cache()

    async def force_refresh_exchange_blacklist(self):
        """Force refresh of exchange blacklist cache (for testing/admin)."""
        logger.info("Force refreshing exchange blacklist cache")
        return await self._refresh_exchange_blacklist_cache()

    async def _detect_changes(self, tokens: list[dict]) -> list[dict]:
        """
        Detect which tokens have changed orderbooks by comparing content hashes.

        Only tokens with changed orderbooks are returned for publishing to stream.
        This dramatically reduces the volume of data processed by collectors.

        Returns:
            List of tokens that have changed orderbooks
        """
        changed_tokens = []
        hash_updates = []  # [(key, hash), ...]

        tokens_with_orderbooks = 0
        tokens_without_orderbooks = 0

        # Step 1: Compute current hashes for all tokens
        token_hashes = {}  # token_id -> (token, current_hash)
        hash_keys = []

        for token in tokens:
            token_id = str(token["id"])

            # Check if token has orderbooks
            has_orderbooks = False
            hash_parts = []

            for price in token.get("prices", []):
                ob = price.get("orderbook")
                if ob and isinstance(ob, dict):
                    has_orderbooks = True
                    # Hash only best bid/ask (most important for change detection)
                    # Much faster than hashing entire orderbook
                    asks = ob.get("asks", [])
                    bids = ob.get("bids", [])

                    best_ask = asks[0] if asks else None
                    best_bid = bids[0] if bids else None

                    # Create compact hash string
                    hash_parts.append(f"{best_bid}|{best_ask}")

            if not has_orderbooks:
                tokens_without_orderbooks += 1
                continue

            tokens_with_orderbooks += 1

            # Hash the compact representation (much faster than msgpack full orderbooks)
            hash_string = "||".join(hash_parts)
            current_hash = hashlib.sha256(hash_string.encode()).hexdigest()[:16]

            hash_key = f"orderbook_hash:{token_id}"
            token_hashes[hash_key] = (token, current_hash)
            hash_keys.append(hash_key)

        # Step 2: Batch fetch previous hashes from Redis (MGET instead of N GET)
        if hash_keys:
            previous_hashes = await self._retry_on_redis_loading(
                "detect_changes_mget",
                lambda: self.cache.client.mget(hash_keys)
            )
        else:
            previous_hashes = []

        # Step 3: Compare current vs previous hashes
        for hash_key, previous_hash in zip(hash_keys, previous_hashes):
            token, current_hash = token_hashes[hash_key]

            # Decode previous hash if exists
            prev_hash_str = previous_hash.decode() if previous_hash else None

            if prev_hash_str != current_hash:
                # Changed or new token
                changed_tokens.append(token)
                hash_updates.append((hash_key, current_hash))

        # Step 4: Update hashes in Redis (batch)
        if hash_updates:
            pipe = self.cache.client.pipeline(transaction=False)
            for key, hash_value in hash_updates:
                pipe.setex(key, 3600, hash_value)  # Expire after 1 hour
            await self._retry_on_redis_loading(
                "detect_changes_update_hashes",
                lambda: pipe.execute()
            )

        # Debug logging
        if self.enable_profiling or self.total_cycles % 10 == 0:
            logger.info(
                f"Change detection: tokens_with_ob={tokens_with_orderbooks} | "
                f"tokens_without_ob={tokens_without_orderbooks} | "
                f"changed={len(changed_tokens)} | "
                f"hashes_updated={len(hash_updates)}"
            )

        return changed_tokens

    @staticmethod
    def _strip_token_for_stream(token: dict) -> dict:
        """
        Strip token down to only fields needed by analyzers.

        Reduces msgpack payload size by ~70-80%.
        Keeps only: id, symbol, prices with minimal fields, orderbooks truncated to top 5 levels.
        """
        stripped = {
            "id": token["id"],
            "symbol": token.get("symbol"),
            "prices": []
        }

        for price in token.get("prices", []):
            # Truncate orderbook to top 5 levels (arbitrage rarely uses beyond top 3-5)
            orderbook = price.get("orderbook")
            if orderbook and isinstance(orderbook, dict):
                asks = orderbook.get("asks", [])[:5]  # Keep only top 5
                bids = orderbook.get("bids", [])[:5]
                orderbook = {"asks": asks, "bids": bids}

            stripped_price = {
                "orderbook": orderbook,
                "get_at": price.get("get_at"),
                "source_name": price.get("source_name"),
                "source": price.get("source"),  # Required by analyzer
                "market_type": price.get("market_type"),
                "global_token_source_id": price.get("global_token_source_id"),
                "base_asset": price.get("base_asset"),
                "quote_asset": price.get("quote_asset"),
                "contract_size": price.get("contract_size", 1.0),
                # Keep minimal chain info for fee calculation
                "chains": [
                    {
                        "chain_name": c.get("chain_name"),
                        "contract_address": c.get("contract_address"),
                        "withdraw_enabled": c.get("withdraw_enabled"),
                        "deposit_enabled": c.get("deposit_enabled"),
                        "withdrawal_fee_in_usdt": c.get("withdrawal_fee_in_usdt"),
                        "deposit_enabled_changed_at": c.get("deposit_enabled_changed_at"),
                        "withdraw_enabled_changed_at": c.get("withdraw_enabled_changed_at"),
                        "token_source_name": price.get("source", {}).get("name"),
                    }
                    for c in price.get("chains", [])
                ],
                "token": price.get("token", {}),  # Keep token metadata
                "source_id": price.get("source_id") or price.get("source", {}).get("id"),
            }
            stripped["prices"].append(stripped_price)

        return stripped

    async def _publish_to_stream(self, tokens: list[dict]):
        """
        Publish changed tokens to Redis Stream.

        Optimizations:
        1. Strip tokens to minimal payload (70-80% size reduction)
        2. Truncate orderbooks to top 5 levels
        3. Batch msgpack serialization in single executor call
        4. Chunked pipeline execution to avoid large buffer
        """
        if not tokens:
            return

        # Strip + serialize in one executor call (avoid 2000+ executor task overhead)
        loop = asyncio.get_event_loop()
        serialized_tokens = await loop.run_in_executor(
            None,
            self._serialize_tokens_batch, tokens
        )

        # Publish in chunks of 500 to avoid huge pipeline buffers
        chunk_size = 500
        for i in range(0, len(serialized_tokens), chunk_size):
            chunk = serialized_tokens[i:i + chunk_size]
            pipe = self.cache.client.pipeline(transaction=False)
            for token_data in chunk:
                pipe.xadd(
                    self.stream_key,
                    {"token_data": token_data},
                    maxlen=self.stream_maxlen,
                    approximate=True
                )
            await self._retry_on_redis_loading(
                "publish_to_stream",
                lambda p=pipe: p.execute()
            )

        # Trim stream periodically (every 50 cycles) instead of every XADD
        # Much faster than maxlen on each XADD
        if self.total_cycles % 50 == 0:
            await self._retry_on_redis_loading(
                "trim_stream",
                lambda: self.cache.client.xtrim(
                    self.stream_key,
                    maxlen=self.stream_maxlen,
                    approximate=True
                )
            )

    def _serialize_tokens_batch(self, tokens: list[dict]) -> list[bytes]:
        """Serialize tokens to msgpack in bulk (called from executor thread)."""
        result = []
        for token in tokens:
            stripped = self._strip_token_for_stream(token)
            result.append(msgpack.packb(stripped, default=msgpack_default, use_bin_type=True))
        return result

    async def _fill_dex_orderbooks(self, tokens: list[dict]):
        """
        Fill orderbooks from DEX price streams (gmgn, dexscreener, etc.).

        Reads recent entries from dex_prices:* streams and matches them to tokens
        by chain + contract_address, then ADDS a new DEX price object to the token.
        """
        # Define DEX sources to read from
        dex_sources = ["gmgn"]  # Add more DEX sources here as needed

        # First, remove any existing DEX prices from previous cycles
        # This prevents exponential growth of price objects
        for token in tokens:
            token["prices"] = [p for p in token.get("prices", [])
                               if p.get("source", {}).get("type", "").upper() != "DEX"]

        # Build or reuse cached token_index (rebuilds only when metadata cache refreshes)
        # Token references remain valid because _get_token_metadata() returns same dict objects
        if (self._dex_token_index is None
                or self._dex_token_index_metadata_ts != self._metadata_cache_timestamp):
            token_index = {}
            chains_checked = 0
            for token in tokens:
                for price in token.get("prices", []):
                    if price.get("source", {}).get("type", "").upper() == "DEX":
                        continue
                    chains_list = price.get("chains") or []
                    chains_checked += len(chains_list)
                    for chain_info in chains_list:
                        chain = chain_info.get("chain_name", "").lower()
                        address = chain_info.get("contract_address", "")
                        if chain and address:
                            key = (chain, address.lower())
                            if key not in token_index:
                                token_index[key] = []
                            token_index[key].append((token, price))

            self._dex_token_index = token_index
            self._dex_token_index_metadata_ts = self._metadata_cache_timestamp
            logger.info(f"DEX: Rebuilt index: {chains_checked} chain entries, {len(token_index)} unique addresses")
        else:
            token_index = self._dex_token_index

        if not token_index:
            logger.info("DEX: No DEX tokens with chain addresses found in token list - skipping DEX fill")
            return  # No DEX tokens to process

        logger.debug(f"DEX: Found {len(token_index)} unique DEX token addresses to check")

        # Read from each DEX source stream
        for source in dex_sources:
            stream_key = f"dex_prices:{source}"

            try:
                # Read last 1000 entries from stream (covers ~10 minutes at 10s interval)
                # Using XREVRANGE to get most recent entries first
                entries = await self._retry_on_redis_loading(
                    f"read_{source}_stream",
                    lambda: self.cache.client.xrevrange(
                        stream_key,
                        count=1000
                    )
                )

                if not entries:
                    logger.debug(f"DEX: No entries found in stream {stream_key}")
                    continue

                logger.info(f"DEX: Read {len(entries)} entries from {stream_key}")

                # Process entries and update matching tokens
                processed = set()  # Track (chain, address) to avoid duplicates
                matches_found = 0

                # Import chain mapping for GMGN
                from infrastructure.services.collectors.structures.maps import GMGN_CHAIN_REVERSE

                for entry_id, data in entries:
                    # Extract data - preserve original case for display/links
                    gmgn_chain = data.get(b"chain", b"").decode("utf-8").lower()
                    address_original = data.get(b"contract_address", b"").decode("utf-8")

                    if not gmgn_chain or not address_original:
                        continue

                    # Map GMGN chain name to DB chain name (e.g., "sol" -> "solana")
                    chain = GMGN_CHAIN_REVERSE.get(gmgn_chain, gmgn_chain)

                    # Use lowercase key for matching (same as token_index)
                    key = (chain, address_original.lower())

                    # Skip if already processed (use most recent entry only)
                    if key in processed:
                        continue

                    # Find matching tokens (tuples of token, sample_price)
                    matching_items = token_index.get(key)
                    if not matching_items:
                        continue
                    
                    # Select ONE sample_price per contract_address to avoid:
                    # 1. Phantom spreads from duplicate DEX objects
                    # 2. Wrong price normalization (e.g. 1000000BABYDOGE divides DEX price by 1M)
                    # Prefer sample_price where multiplier == 1 (exact token price match for DEX).
                    # DEX always returns price per 1 token, so we must avoid multiplier samples.
                    if len(matching_items) > 1:
                        best = None
                        for tok, pr in matching_items:
                            base = (pr.get("token") or {}).get("base_asset", "")
                            mult, _ = parse_multiplier(base)
                            if mult == 1:
                                best = (tok, pr)
                                break
                        if best is None:
                            best = matching_items[0]
                        matching_items = [best]

                    matching_prices = matching_items  # Rename for clarity in processing

                    # Parse orderbook from stream data
                    try:
                        orderbook_str = data.get(b"orderbook", b"").decode("utf-8")
                        # Convert string representation to dict
                        # Format: "{'asks': [[price, volume]], 'bids': [[price, volume]]}"
                        # Use JSON parsing instead of ast.literal_eval for security
                        try:
                            orderbook = json.loads(orderbook_str.replace("'", '"'))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid orderbook JSON format: {e}")
                            continue

                        # Get timestamp and symbol
                        timestamp = data.get(b"timestamp", b"").decode("utf-8")
                        symbol = data.get(b"symbol", b"").decode("utf-8")

                        # Create NEW DEX price objects (don't overwrite CEX prices!)
                        for token, sample_price in matching_prices:
                            # Build DEX-specific chains with contract_address and token_source_name
                            dex_chains = []
                            for c in sample_price.get("chains", []):
                                # Only include the chain matching this DEX price
                                if c.get("chain_name", "").lower() == chain.lower():
                                    dex_chains.append({
                                        "chain_name": c.get("chain_name"),
                                        "contract_address": c.get("contract_address") or address_original,
                                        "withdraw_enabled": True,  # DEX always allows
                                        "deposit_enabled": True,   # DEX always allows
                                        "withdrawal_fee_in_usdt": 0,
                                        "token_source_name": source,  # Critical: DEX source name!
                                    })

                            # DEX prices are always in real per-token terms, so we must
                            # strip multiplier prefixes (e.g. "1000BONK" -> "BONK") from
                            # base_asset to prevent normalize_orderbook from incorrectly
                            # dividing the DEX price by the multiplier.
                            sample_token = sample_price.get("token", {})
                            sample_base_asset = sample_token.get("base_asset", "")
                            _, clean_base_asset = parse_multiplier(sample_base_asset)

                            dex_token = {**sample_token, "base_asset": clean_base_asset}

                            # Create a new price object for DEX source
                            dex_price = {
                                "orderbook": orderbook,
                                "get_at": int(timestamp) if timestamp else None,
                                "market_type": "SPOT",  # DEX is always spot
                                "token": dex_token,
                                "source": {
                                    "id": None,  # DEX source doesn't have DB id
                                    "name": source,
                                    "type": "DEX"  # Critical: mark as DEX!
                                },
                                "chains": dex_chains,
                                "created_at": None,
                                "updated_at": None,
                            }
                            # Add DEX price to token's prices list
                            token["prices"].append(dex_price)
                            matches_found += 1

                        processed.add(key)

                    except Exception as e:
                        logger.debug(f"Failed to parse DEX orderbook for {chain}/{address}: {e}")
                        continue

                if processed:
                    logger.info(f"DEX: Added {len(processed)} DEX orderbooks from {source} ({matches_found} price objects updated)")
                else:
                    logger.debug(f"DEX: No matching tokens found for {source} stream entries")

            except Exception as e:
                logger.error(f"Error reading DEX stream {stream_key}: {e}")
                continue


async def main():
    """Standalone entry point."""
    database.redis = aioredis.from_url(
        settings.redis.dsn,
        decode_responses=False  # Binary mode for msgpack
    )
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    cache = RedisCache(database.redis)

    # Use ENV_CONFIG to determine mode
    use_streams = ENV_CONFIG.use_redis_streams

    loader = OptimizedTokenLoader(
        repo=repo,
        cache=cache,
        tokens_load_limit=10000,
        enable_profiling=ENV_CONFIG.enable_profiling,
        use_streams=use_streams
    )

    await loader.start()


if __name__ == "__main__":
    asyncio.run(main())
