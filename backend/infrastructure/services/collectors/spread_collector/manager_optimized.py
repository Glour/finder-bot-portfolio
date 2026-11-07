"""
Optimized Spread Collector Manager with Redis Streams support.

Performance improvements:
1. Redis Streams for incremental processing (only changed orderbooks)
2. msgpack-only deserialization (no JSON)
3. Consumer groups for parallel workers
4. Async broadcast without blocking analysis

Expected latency reduction: 2-3 seconds
"""
import asyncio
import os
import time
from typing import Optional

import msgpack
import orjson
import redis.asyncio as redis
from redis.exceptions import BusyLoadingError, ConnectionError as RedisConnectionError

from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.spread_collector.analyzer import ArbitrageAnalyzer
from infrastructure.services.collectors.spread_collector.broadcaster import ArbitrageMessenger
from infrastructure.services.collectors.spread_collector.filters import ArbitrageFilter
from infrastructure.services.collectors.spread_collector.config import (
    SignalLevelType,
    DEFAULT_PERFORMANCE_CONFIG,
    DEFAULT_SPOT_CONFIG,
    PerformanceConfig,
    PRICE_CORRECTION_MAP,
)

# Exchange pair blacklist for known mismatched tokens
# Format: (symbol, buy_from, sell_to)
# Use None for sell_to to block all pairs from this exchange
EXCHANGE_PAIR_BLACKLIST = {
    ("ZKP", "spot_bitmart", "futures_bitmart"),
    ("ZKP", "futures_bitmart", "spot_bitmart"),
    ("GAS", "spot_mexc", "futures_mexc"),
    ("GAS", "futures_mexc", "spot_mexc"),
    # AscendEX contaminated tokens (wrong tokens with same symbol)
    ("EIGEN", "spot_ascendex", None),  # Shows 1INCH prices instead of EIGEN
    ("POWER", "spot_ascendex", None),  # Shows wrong POWER token
}
from infrastructure.services.collectors.spread_collector.service import ArbitrageAnalyzerService
from infrastructure.services.collectors.structures.normalizer import normalize_orderbook, normalize_chains, batched
from infrastructure.services.collectors.token_collector.supports import chain_normalizer_instance
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.collectors.utils.profiler import ComponentTimer
from settings import logger


class OptimizedArbitrageAnalyzerManager:
    """
    Optimized spread collector with Redis Streams support.

    Supports both legacy LIST mode and new STREAM mode via feature flag.
    """

    def __init__(
        self,
        repo: RequestsRepo,
        cache: RedisCache,
        tokens_load_limit: int,
        interval: float = None,
        performance_config: Optional[PerformanceConfig] = None,
        use_streams: bool = False,
        consumer_group: str = "spread_collectors",
        consumer_name: Optional[str] = None,
        signal_writer=None,
    ):
        self.repo = repo
        self.cache = cache
        self.tokens_load_limit = tokens_load_limit
        self.signal_writer = signal_writer

        # Create separate Redis client for binary data (msgpack)
        # Used for both streams and LIST mode when data is in msgpack format
        redis_host = os.getenv("REDIS__REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS__REDIS_PORT", 6379))
        redis_db = int(os.getenv("REDIS__REDIS_DB", 0))

        self.binary_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=False  # Keep binary data as bytes for msgpack
        )

        # Create separate Redis client for streams (decode_responses=False for binary msgpack)
        self.stream_client = None
        if use_streams:
            redis_host = os.getenv("REDIS__REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS__REDIS_PORT", 6379))
            redis_db = int(os.getenv("REDIS__REDIS_DB", 0))
            self.stream_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=False  # Keep binary data as bytes for msgpack
            )

        # Performance config
        self.perf_config = performance_config or DEFAULT_PERFORMANCE_CONFIG
        self.interval = interval if interval is not None else self.perf_config.analysis_interval

        # Streams config
        self.use_streams = use_streams
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name or f"worker-{os.getpid()}"

        # Services
        self.analyzer_service = ArbitrageAnalyzerService(analyzer=ArbitrageAnalyzer())
        self.messenger = ArbitrageMessenger(chain_normalizer=chain_normalizer_instance, signal_writer=self.signal_writer)
        self.filters = {
            "spot": ArbitrageFilter(is_futures=False, cache=cache),
            "futures": ArbitrageFilter(is_futures=True, cache=cache)
        }

        logger.info(
            f"Initialized OptimizedArbitrageAnalyzerManager | "
            f"mode={'STREAMS' if use_streams else 'LIST'} | "
            f"consumer={self.consumer_name} | "
            f"group={self.consumer_group} | "
            f"batch_size={self.perf_config.batch_size}"
        )

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

    async def run(self):
        """Main processing loop."""
        if self.use_streams:
            await self._run_streams_mode()
        else:
            await self._run_list_mode()

    async def _run_list_mode(self):
        """
        Legacy LIST mode (original implementation).

        Reads entire list from Redis, processes all tokens.
        """
        logger.info("Running in LIST mode (legacy)")

        while True:
            start = time.perf_counter()
            try:
                # Load tokens from Redis LIST (with retry on BusyLoadingError)
                # Use binary_client to get raw bytes for msgpack deserialization
                load_start = time.perf_counter()
                token_data = await self._retry_on_redis_loading(
                    "lrange_tokens",
                    lambda: self.binary_client.lrange("tokens:analyzer", 0, -1)
                )

                if not token_data:
                    logger.debug("Tokens queue is empty")
                    await asyncio.sleep(0.1)
                    continue

                redis_time = time.perf_counter() - load_start

                # Deserialize (msgpack or JSON based on data format)
                deser_start = time.perf_counter()
                tokens = []
                msgpack_count = 0
                json_count = 0
                for data in token_data:
                    try:
                        # Try msgpack first (optimized path)
                        token = msgpack.unpackb(data, raw=False)
                        tokens.append(token)
                        msgpack_count += 1
                    except Exception:
                        # Fallback to JSON (legacy)
                        import orjson
                        token = orjson.loads(data)
                        tokens.append(token)
                        json_count += 1

                deser_time = time.perf_counter() - deser_start
                load_time = time.perf_counter() - load_start

                # Analyze tokens
                analyze_start = time.perf_counter()
                results = []
                for batch in batched(tokens, self.perf_config.batch_size):
                    batch_results = await asyncio.gather(
                        *[self._analyze_token_group(t) for t in batch]
                    )
                    for r in batch_results:
                        if r:
                            results.extend(r)
                analyze_time = time.perf_counter() - analyze_start

                # Send results (grouped by token_id, non-blocking)
                send_start = time.perf_counter()
                if results:
                    asyncio.create_task(self.messenger.send_results(results))
                send_time = time.perf_counter() - send_start

                total_time = time.perf_counter() - start
                format_info = f"msgpack={msgpack_count}" if msgpack_count > 0 else f"json={json_count}"
                logger.info(
                    f"LIST mode cycle | tokens={len(tokens)} | "
                    f"redis={redis_time:.2f}s | deser={deser_time:.2f}s ({format_info}) | "
                    f"analyze={analyze_time:.2f}s | send={send_time:.2f}s | "
                    f"total={total_time:.2f}s | results={len(results)}"
                )

                # Wait for next cycle
                elapsed = time.perf_counter() - start
                if self.interval > elapsed:
                    await asyncio.sleep(self.interval - elapsed)

            except BusyLoadingError as e:
                # If retry exhausted, wait and try again
                logger.error(f"Redis still loading after retries, waiting 30s: {e}")
                await asyncio.sleep(30)
            except (RedisConnectionError, ConnectionRefusedError) as e:
                # Connection issues - wait and retry
                logger.error(f"Redis connection error, waiting 10s: {e}")
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Failed to analyze arbitrage results: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _run_streams_mode(self):
        """
        Optimized STREAMS mode.

        Only processes changed orderbooks via Redis Streams.
        Uses consumer groups for parallel processing.
        """
        logger.info(f"Running in STREAMS mode | consumer={self.consumer_name}")

        # Wait for stream to exist (loader might be starting up)
        stream_key = "orderbooks:updates"
        max_wait_attempts = 30  # Wait up to 30 seconds for stream to appear
        wait_interval = 1  # Check every second

        for attempt in range(max_wait_attempts):
            try:
                stream_info = await self._retry_on_redis_loading(
                    "xinfo_stream",
                    lambda: self.stream_client.xinfo_stream(stream_key.encode())
                )
                stream_length = stream_info.get('length', 0)
                logger.info(f"Redis Stream found: {stream_key} | length={stream_length}")
                # Stream exists - continue in STREAMS mode
                break
            except Exception as e:
                if attempt < max_wait_attempts - 1:
                    logger.info(f"Waiting for Redis Stream '{stream_key}' (attempt {attempt + 1}/{max_wait_attempts})")
                    await asyncio.sleep(wait_interval)
                else:
                    # After 30 attempts, fall back to LIST mode
                    logger.warning(f"Redis Stream '{stream_key}' not found after {max_wait_attempts}s: {e}")
                    logger.warning("This means loader is NOT using STREAMS mode")
                    logger.warning("Falling back to LIST mode (legacy compatibility)")
                    self.use_streams = False
                    return await self._run_list_mode()

        # Ensure consumer group exists
        await self._ensure_consumer_group()

        # Stream read params
        block_ms = 1000  # Block for 1 second waiting for new messages
        count = 100  # Read up to 100 messages per batch

        last_id = ">"  # Read only new messages

        while True:
            try:
                # Read from stream (with retry on BusyLoadingError)
                # Use stream_client with decode_responses=False to preserve binary msgpack data
                read_start = time.perf_counter()
                messages = await self._retry_on_redis_loading(
                    "xreadgroup",
                    lambda: self.stream_client.xreadgroup(
                        self.consumer_group,
                        self.consumer_name,
                        {stream_key.encode(): last_id.encode()},  # Encode keys when decode_responses=False
                        count=count,
                        block=block_ms
                    )
                )

                if not messages:
                    # No new messages, continue
                    continue

                read_time = time.perf_counter() - read_start

                # Parse messages
                parse_start = time.perf_counter()
                tokens_to_analyze = []
                message_ids = []

                for stream, entries in messages:
                    for message_id, data in entries:
                        message_ids.append(message_id)

                        try:
                            # Deserialize token data from stream
                            token_data = data.get(b"token_data") or data.get("token_data")
                            if token_data:
                                token = msgpack.unpackb(token_data, raw=False)
                                tokens_to_analyze.append(token)
                        except Exception as e:
                            logger.error(f"Failed to parse stream message {message_id}: {e}")

                parse_time = time.perf_counter() - parse_start

                # Analyze tokens
                analyze_start = time.perf_counter()
                results = []
                for batch in batched(tokens_to_analyze, self.perf_config.batch_size):
                    batch_results = await asyncio.gather(
                        *[self._analyze_token_group(t) for t in batch]
                    )
                    for r in batch_results:
                        if r:
                            results.extend(r)
                analyze_time = time.perf_counter() - analyze_start

                # Send results (grouped by token_id, non-blocking)
                if results:
                    asyncio.create_task(self.messenger.send_results(results))

                # Acknowledge processed messages (with retry on BusyLoadingError)
                ack_start = time.perf_counter()
                if message_ids:
                    await self._retry_on_redis_loading(
                        "xack",
                        lambda: self.stream_client.xack(stream_key.encode(), self.consumer_group.encode(), *message_ids)
                    )
                ack_time = time.perf_counter() - ack_start

                total_time = time.perf_counter() - read_start
                logger.info(
                    f"STREAMS cycle | messages={len(message_ids)} | "
                    f"tokens={len(tokens_to_analyze)} | "
                    f"read={read_time:.3f}s | parse={parse_time:.3f}s | "
                    f"analyze={analyze_time:.3f}s | ack={ack_time:.3f}s | "
                    f"total={total_time:.3f}s | results={len(results)}"
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
                error_str = str(e)
                # Check if it's a NOGROUP error - consumer group was deleted
                if "NOGROUP" in error_str:
                    logger.warning(f"Consumer group was deleted, recreating: {e}")
                    await self._ensure_consumer_group()
                    await asyncio.sleep(0.1)  # Brief pause before retry
                else:
                    logger.error(f"Failed in streams mode: {e}", exc_info=True)
                    await asyncio.sleep(1)

    async def _ensure_consumer_group(self):
        """Ensure Redis Stream consumer group exists."""
        stream_key = "orderbooks:updates"
        try:
            await self._retry_on_redis_loading(
                "xgroup_create",
                lambda: self.stream_client.xgroup_create(
                    stream_key.encode(),
                    self.consumer_group.encode(),
                    id=b"0",
                    mkstream=True
                )
            )
            logger.info(f"Created consumer group '{self.consumer_group}' on stream '{stream_key}'")
        except Exception as e:
            # Group already exists or other error
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Failed to create consumer group: {e}")

    async def _analyze_token_group(self, token: dict):
        """
        Analyze a single token for arbitrage opportunities.

        This is the core analysis logic (unchanged from original).
        """
        timer = ComponentTimer(f"analyze_token_{token.get('symbol', 'unknown')}")

        # Parse orderbooks
        spot_sources, futures_sources, dex_sources = {}, {}, {}
        tickers_data = {"spot": {}, "futures": {}, "dex": {}}

        for price in token.get("prices") or []:
            orderbook = price.get("orderbook")
            if not orderbook:
                continue

            price = normalize_orderbook(price["token"]["base_asset"], price)

            source = price["source"]
            source_name = source["name"]
            source_type = source["type"].lower()
            market_type = price["market_type"].lower()

            item = {
                "orderbook": price["orderbook"],
                "contract_size": price["token"].get("contract_size") or 1,
                "source_name": source_name,
                "base_asset": price["token"]["base_asset"],
                "quote_asset": price["token"]["quote_asset"],
                "symbol": price["token"]["symbol"],
                "updated_at": price.get("updated_at") or price.get("created_at"),
                "get_at": price.get("get_at"),
                "chains": price.get("chains", [])
            }

            # Apply price correction for tokens with known artificial price offsets
            # (e.g. GATE prices TQQQX spot at 2x real value)
            base_symbol = price["token"]["base_asset"].upper()
            token_corrections = PRICE_CORRECTION_MAP.get(base_symbol) or PRICE_CORRECTION_MAP.get(token.get("symbol", "").upper())
            if token_corrections:
                correction_key = f"{source_name.lower()}_{market_type.lower()}"
                factor = token_corrections.get(correction_key)
                if factor and factor != 1.0:
                    ob = item["orderbook"]
                    ob["asks"] = [[p * factor, v] for p, v in ob["asks"]]
                    ob["bids"] = [[p * factor, v] for p, v in ob["bids"]]

            if source_type == "dex":
                # Skip stale DEX prices to avoid phantom delays
                get_at = item.get("get_at")
                if get_at:
                    age_s = (time.time() * 1000 - get_at) / 1000
                    if age_s > DEFAULT_SPOT_CONFIG.max_dex_price_age:
                        continue
                dex_sources[source_name] = item
                tickers_data["dex"][source_name] = item
            elif source_type == "cex":
                if market_type == "spot":
                    spot_sources[source_name] = item
                    tickers_data["spot"][source_name] = item
                elif market_type == "futures":
                    futures_sources[source_name] = item
                    tickers_data["futures"][source_name] = item

        timer.checkpoint("parse_orderbooks")

        # Check if we have sources
        has_sources = (len(spot_sources) + len(futures_sources) + len(dex_sources)) > 0

        if not has_sources:
            return []

        # Analyze arbitrage
        results = []
        results += self.analyzer_service.analyze_same_type("spot", spot_sources, depth_analyze=True)
        results += self.analyzer_service.analyze_same_type("futures", futures_sources)
        results += self.analyzer_service.analyze_cross_type("spot", spot_sources, "futures", futures_sources)

        results += self.analyzer_service.analyze_cross_type(
            "spot", spot_sources, "dex", dex_sources, depth_analyze=True
        )
        results += self.analyzer_service.analyze_cross_type("futures", futures_sources, "dex", dex_sources)
        timer.checkpoint("analyze")

        # Process and filter
        # NEW LOGIC: Group all pairs by token, filter by max spread/profit
        filtered_results = []
        processed = []

        for r in results:
            r["tickers_data"] = tickers_data
            pr = await self.process_result(r, tickers_data, token, self.filters)
            
            # Debug: log ALL ZKP results (даже None)
            token_symbol = token.get("symbol", "")
            if "ZKP" in token_symbol:
                logger.warning(f"ZKP ({token_symbol}) process_result: pr={pr is not None} | buy={r.get('buy_from')} | sell={r.get('sell_to')}")
            
            if pr:
                # Check exchange pair blacklist (use clean_symbol from pr)
                symbol = pr.get("symbol", "")
                buy_from = pr.get("buy_from", "")
                sell_to = pr.get("sell_to", "")
                
                # Check blacklist (exact match or wildcard with None)
                is_blacklisted = (
                    (symbol, buy_from, sell_to) in EXCHANGE_PAIR_BLACKLIST or 
                    (symbol, buy_from, None) in EXCHANGE_PAIR_BLACKLIST or
                    (symbol, sell_to, None) in EXCHANGE_PAIR_BLACKLIST
                )
                
                # Debug logging for ALL Bitmart/AscendEX pairs
                if "bitmart" in buy_from.lower() or "bitmart" in sell_to.lower() or "ascendex" in buy_from.lower() or "ascendex" in sell_to.lower():
                    logger.warning(f"Filtered pair: symbol={symbol} | buy_from={buy_from} | sell_to={sell_to} | blacklisted={is_blacklisted}")
                
                if is_blacklisted:
                    logger.warning(f"BLOCKED: {symbol} | {buy_from} -> {sell_to}")
                    continue  # Skip blacklisted pair
                
                processed.append(pr)

        timer.checkpoint("process")

        # Group by spot/futures and filter at PAIR level
        # CRITICAL FIX (2026-02-20): Use pair-level filtering to avoid dropping valid pairs
        # Token-level filtering was dropping pairs with lower spreads when max spread was cached
        processed_by_type = {"spot": [], "futures": []}
        for pr in processed:
            result_type = "futures" if "futures" in pr["buy_from"] or "futures" in pr["sell_to"] else "spot"
            processed_by_type[result_type].append(pr)

        # Filter each pair individually (not token-level)
        for result_type, batch in processed_by_type.items():
            if not batch:
                continue
            
            # Build cache updates (pair-level keys)
            updates = []
            for pr in batch:
                # Cache key: "arbitrage:{global_token_id}:{buy_from}:{sell_to}"
                cache_key = f"arbitrage:{pr['global_token_id']}:{pr['buy_from']}:{pr['sell_to']}"
                updates.append((cache_key, pr))
            
            # Batch update cache and get broadcast flags
            should_broadcast_flags = await self.filters[result_type].update_cache_batch(updates)
            
            # Collect pairs that should be broadcast
            for pr, should_broadcast in zip(batch, should_broadcast_flags):
                if should_broadcast:
                    filtered_results.append(pr)

        timer.checkpoint("cache_update")

        # When any pair of a token triggered broadcast — include ALL profitable
        # pairs from the current analysis cycle in the same signal.
        # All data is fresh (same cycle), no Redis cache lookups needed.
        if filtered_results:
            filtered_results = [pr for pr in processed if (pr.get("spread_percent") or 0) > 0]

        timer.checkpoint("group_pairs")

        if self.perf_config.enable_profiling and results:
            timer.log(min_duration_ms=100)

        return filtered_results

    @staticmethod
    def prepare_result_chains(seller_chains, buyer_chains):
        """Prepare chains for result with deduplication by (chain_name, contract_address, token_source_name)."""
        seen = set()
        all_chains = []
        for c in seller_chains + buyer_chains:
            chain_name = (c.get("chain_name") or "").lower()
            contract = (c.get("contract_address") or "").lower()
            source = (c.get("token_source_name") or "").lower()
            key = (chain_name, contract, source)
            if key in seen:
                continue
            seen.add(key)
            all_chains.append({
                "chain_name": c.get("chain_name"),
                "contract_address": c.get("contract_address"),
                "withdrawal_fee": c.get("withdrawal_fee"),
                "withdrawal_fee_in_usdt": c.get("withdrawal_fee_in_usdt"),
                "cex_withdrawal_fee": c.get("cex_withdrawal_fee"),
                "cex_deposit_fee": c.get("cex_deposit_fee"),
                "total_tax_fee_in_token": c.get("total_tax_fee_in_token"),
                "deposit_enabled": c.get("deposit_enabled"),
                "withdraw_enabled": c.get("withdraw_enabled"),
                "deposit_enabled_changed_at": c.get("deposit_enabled_changed_at"),
                "withdraw_enabled_changed_at": c.get("withdraw_enabled_changed_at"),
                "token_source_name": c.get("token_source_name"),
            })
        return all_chains

    @staticmethod
    def compute_realizable_fee(seller_chains, buyer_chains):
        """
        Compute realizable withdrawal fee in USDT.

        Uses seller's withdrawal_fee_in_usdt (not total_tax_fee) because:
        - Seller pays withdrawal fee when sending from their exchange
        - Use withdrawal_fee_in_usdt (already converted to USDT)
        - Choose minimum fee among compatible chains
        """
        fees = []

        # Find compatible chains (same chain, both enabled)
        for s in seller_chains:
            for b in buyer_chains:
                if s["chain_name"].lower() == b["chain_name"].lower():
                    if s.get("withdraw_enabled") and b.get("deposit_enabled"):
                        # Use seller's withdrawal fee in USDT
                        fee_usdt = s.get("withdrawal_fee_in_usdt")
                        if fee_usdt is not None:
                            fees.append(fee_usdt)

        # Fallback: if no compatible chains with both enabled, check any common chain
        if not fees:
            seller_set = {s["chain_name"].lower(): s for s in seller_chains}
            buyer_set = {b["chain_name"].lower(): b for b in buyer_chains}
            intersect = set(seller_set.keys()) & set(buyer_set.keys())

            for ch in intersect:
                fee_usdt = seller_set[ch].get("withdrawal_fee_in_usdt")
                if fee_usdt is not None:
                    fees.append(fee_usdt)

        # Return minimum fee (best chain option)
        return min(fees) if fees else 0

    async def process_result(self, r, tickers_data, token, filters):
        """Process and enrich arbitrage result (unchanged)."""
        seller_info = tickers_data.get(r["buy_from"].split("_", 1)[0], {}).get(r["buy_from"].split("_", 1)[1], {})
        buyer_info = tickers_data.get(r["sell_to"].split("_", 1)[0], {}).get(r["sell_to"].split("_", 1)[1], {})

        seller_chains = normalize_chains(seller_info.get("chains", []), r["sell_price"], r["volume"])
        buyer_chains = normalize_chains(buyer_info.get("chains", []), r["sell_price"], r["volume"])

        # CRITICAL FIX (2026-02-15): Detect and skip phantom cross-chain spreads
        # Problem: GT with tokens from different networks (e.g., VON BSC + VON ETH) reports false arbitrage
        # Solution: If buyer and seller have DIFFERENT contracts on DIFFERENT chains - skip (different tokens!)
        seller_contracts = {
            (c.get("chain_name", "").lower(), c.get("contract_address", "").lower())
            for c in seller_chains
            if c.get("contract_address") and c.get("contract_address").lower() not in ["", "none", "null"]
        }
        buyer_contracts = {
            (c.get("chain_name", "").lower(), c.get("contract_address", "").lower())
            for c in buyer_chains
            if c.get("contract_address") and c.get("contract_address").lower() not in ["", "none", "null"]
        }

        # If both have contracts but NO common (chain, contract) pair - phantom spread!
        if seller_contracts and buyer_contracts:
            common_contracts = seller_contracts & buyer_contracts
            if not common_contracts:
                # Different tokens merged in one GT - skip this phantom spread
                logger.debug(
                    f"Skipping phantom cross-chain spread {token.get('symbol', 'unknown')}: "
                    f"{r['buy_from']} vs {r['sell_to']} - no common contracts "
                    f"(seller: {seller_contracts}, buyer: {buyer_contracts})"
                )
                return None

        r["chains"] = self.prepare_result_chains(seller_chains, buyer_chains)
        min_fee = self.compute_realizable_fee(seller_chains, buyer_chains)

        r["profit_before_fee"] = r["profit"]
        r["applied_fee"] = min_fee
        r["profit"] -= min_fee

        result_type = "futures" if "futures" in r["buy_from"] or "futures" in r["sell_to"] else "spot"
        # Use clean base_asset as symbol (global_token.symbol may have _FUTURES/_F10 suffixes)
        prices = token.get("prices", [])
        clean_symbol = prices[0]["token"]["base_asset"] if prices else token["symbol"]
        r["level"] = filters[result_type].get_level(r["spread_percent"], r["profit"], symbol=clean_symbol).value
        r["symbol"] = clean_symbol
        r["global_token_id"] = token["id"]
        r["analyzed_at"] = int(time.time() * 1000)

        if r["level"] == SignalLevelType.NONE:
            return None

        logger.debug(
            f"Spread {clean_symbol}: {r['buy_from']} -> {r['sell_to']}, "
            f"spread={r['spread_percent']:.2f}%, profit={r['profit']:.0f}$, level={r['level']}"
        )

        return r
