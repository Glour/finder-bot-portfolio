"""
Optimized Price Collector with Redis Streams support.

Performance improvements:
1. Optional Redis Streams for incremental updates
2. Reduced Redis writes (only orderbook key + stream)
3. Configurable batch size and flush interval
4. End-to-end latency tracking

This is an enhanced version of base.py that can work in parallel with the original.
"""
import asyncio
import time
from abc import ABC
from typing import Any, List, Optional

import msgpack

from infrastructure.database.models.base import SourceType, MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.base import BasePriceParser, BaseWSCexPriceParser
from infrastructure.services.collectors.structures.utils import make_orderbook_key
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.collectors.utils.profiler import get_tracker
from settings import logger


class OptimizedBasePriceParser(BasePriceParser, ABC):
    """
    Enhanced price parser with Redis Streams support.

    Backwards compatible with original BasePriceParser.
    """

    def __init__(
        self,
        cache: RedisCache,
        repo: RequestsRepo,
        *,
        source_name: str,
        source_type: SourceType,
        stream_key: str = "prices:spot",
        use_streams: bool = False,
        track_latency: bool = False
    ):
        super().__init__(cache, repo, source_name=source_name, source_type=source_type, stream_key=stream_key)
        self.use_streams = use_streams
        self.track_latency = track_latency
        self.stream_name = "orderbooks:updates"  # Single stream for all updates

    async def send_to_redis_batch(self, batch: List[dict]):
        """
        Send batch to Redis with optional Streams support.

        Modes:
        - Legacy: Write to 2 keys (orderbook + prices)
        - Streams: Write to 1 key (orderbook) + XADD to stream
        """
        if not batch:
            return

        try:
            pipe = self.cache.client.pipeline(transaction=False)
            stream_messages = []

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

                # Write orderbook key (always)
                pipe.set(redis_key, packed)
                pipe.expire(redis_key, 120)

                if self.use_streams:
                    # Streams mode: XADD to stream instead of second key
                    stream_messages.append((redis_key, packed))
                else:
                    # Legacy mode: Write to prices:* key
                    price_saver_key = (
                        f"{self.stream_key}:{data['global_token_id']}:"
                        f"{data['global_source_id']}:{data['token_id']}:{data['market_type']}"
                    )
                    pipe.set(price_saver_key, packed)
                    pipe.expire(price_saver_key, 300)

                # Track latency if enabled
                if self.track_latency:
                    tracker = get_tracker()
                    tracker.start_tracking(redis_key, data["symbol"], data["get_at"])
                    tracker.record_redis_write(redis_key)

            # Execute pipeline
            await pipe.execute()

            # Add to stream (separate call for better error handling)
            if self.use_streams and stream_messages:
                await self._add_to_stream(stream_messages)

        except Exception as e:
            logger.exception(f"Failed to push batch to Redis: {e}")

    async def _add_to_stream(self, messages: List[tuple[str, bytes]]):
        """
        Add orderbook updates to Redis Stream.

        Stream structure:
        {
            "orderbook_key": "orderbook:123:456:789:spot",
            "token_data": <msgpack bytes>,
            "timestamp": 1234567890
        }
        """
        try:
            pipe = self.cache.client.pipeline(transaction=False)

            for orderbook_key, packed_data in messages:
                # XADD with maxlen to prevent unbounded growth
                # Keep last 10000 messages (~5-10 minutes of data)
                pipe.xadd(
                    self.stream_name,
                    {
                        "orderbook_key": orderbook_key,
                        "token_data": packed_data,
                        "timestamp": str(int(time.time() * 1000))
                    },
                    maxlen=10000,
                    approximate=True  # ~MAXLEN for better performance
                )

            await pipe.execute()

        except Exception as e:
            logger.error(f"Failed to add messages to stream: {e}")


class OptimizedBaseWSCexPriceParser(BaseWSCexPriceParser):
    """
    Enhanced WebSocket price parser with streams support.

    Drop-in replacement for BaseWSCexPriceParser with additional features.
    """

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
        use_streams: bool = False,
        track_latency: bool = False,
        flush_interval: float = 0.02
    ):
        super().__init__(
            repo=repo,
            cache=cache,
            source_name=source_name,
            ws_client_cls=ws_client_cls,
            symbols_per_ws=symbols_per_ws,
            stream_key=stream_key,
            market_type=market_type
        )
        self.use_streams = use_streams
        self.track_latency = track_latency
        self.flush_interval = flush_interval
        self.stream_name = "orderbooks:updates"

        # Override parent's parser instance to use optimized version
        # (This is a bit hacky but maintains backwards compatibility)
        self._use_optimized_batch_sender = True

    async def redis_writer(self, flush_interval: float = None):
        """
        Optimized redis writer with configurable flush interval.

        Override parent method to use optimized batch sending.
        """
        interval = flush_interval or self.flush_interval

        while True:
            await asyncio.sleep(interval)

            async with self._latest_updates_lock:
                batch = list(self._latest_updates.values())
                self._latest_updates.clear()

            if not batch:
                continue

            # Use optimized batch sending
            asyncio.create_task(self._send_to_redis_batch_optimized(batch))

    async def _send_to_redis_batch_optimized(self, batch: List[dict]):
        """
        Optimized batch sending with streams support.

        This is the method that should be called instead of parent's send_to_redis_batch.
        """
        if not batch:
            return

        try:
            pipe = self.cache.client.pipeline(transaction=False)
            stream_messages = []

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

                # Write orderbook key (always)
                pipe.set(redis_key, packed)
                pipe.expire(redis_key, 120)

                if self.use_streams:
                    # Streams mode
                    stream_messages.append((redis_key, packed, data))
                else:
                    # Legacy mode
                    price_saver_key = (
                        f"{self.stream_key}:{data['global_token_id']}:"
                        f"{data['global_source_id']}:{data['token_id']}:{data['market_type']}"
                    )
                    pipe.set(price_saver_key, packed)
                    pipe.expire(price_saver_key, 300)

                # Track latency if enabled
                if self.track_latency:
                    tracker = get_tracker()
                    tracker.start_tracking(redis_key, data["symbol"], data["get_at"])
                    tracker.record_redis_write(redis_key)

            # Execute pipeline
            await pipe.execute()

            # Add to stream
            if self.use_streams and stream_messages:
                await self._add_to_stream_batch(stream_messages)

        except Exception as e:
            logger.exception(f"Failed to push batch to Redis: {e}")

    async def _add_to_stream_batch(self, messages: List[tuple[str, bytes, dict]]):
        """Add batch of messages to Redis Stream."""
        try:
            pipe = self.cache.client.pipeline(transaction=False)

            for orderbook_key, packed_data, original_data in messages:
                pipe.xadd(
                    self.stream_name,
                    {
                        "orderbook_key": orderbook_key,
                        "token_data": packed_data,
                        "timestamp": str(int(time.time() * 1000)),
                        "symbol": original_data["symbol"]
                    },
                    maxlen=10000,
                    approximate=True
                )

            await pipe.execute()

            if self.track_latency:
                # Log batch statistics
                logger.debug(f"Added {len(messages)} messages to stream '{self.stream_name}'")

        except Exception as e:
            logger.error(f"Failed to add batch to stream: {e}")

    async def start(self):
        """
        Start WebSocket collector with optimized redis writer.

        Overrides parent to use custom flush interval.
        """
        asyncio.create_task(self._monitor_ws_updates())
        asyncio.create_task(self.redis_writer(self.flush_interval))

        while True:
            new_tokens = await self.collect_tokens()
            new_symbols = [t["symbol"] for t in new_tokens]
            for t in new_tokens:
                self.token_map[t["symbol"]] = t

            for i in range(0, len(new_symbols), self.symbols_per_ws):
                batch = new_symbols[i:i + self.symbols_per_ws]
                client_id = self._next_client_id
                self._next_client_id += 1

                client = self._ws_client_cls(batch, self.update_callback, market_type=self.market_type)
                self._clients[client_id] = client
                self._client_symbols[client_id] = set(batch)
                for sym in batch:
                    self._symbol_to_client[sym] = client_id
                    self._known_symbols.add(sym)

                self._ws_tasks[client_id] = asyncio.create_task(self._start_client(client_id, client))

            for cid, t in list(self._ws_tasks.items()):
                if t.done():
                    await self._cleanup_client(cid)

            await asyncio.sleep(60)


def create_optimized_parser(
    parser_cls: type,
    use_streams: bool = False,
    track_latency: bool = False,
    flush_interval: float = 0.02,
    **kwargs
) -> Any:
    """
    Factory function to create optimized parser instance.

    Args:
        parser_cls: Base parser class
        use_streams: Enable Redis Streams mode
        track_latency: Enable end-to-end latency tracking
        flush_interval: Redis write flush interval (seconds)
        **kwargs: Additional arguments for parser constructor

    Returns:
        Parser instance (optimized if possible, otherwise original)
    """
    if issubclass(parser_cls, BaseWSCexPriceParser):
        # Create optimized WS parser
        return OptimizedBaseWSCexPriceParser(
            use_streams=use_streams,
            track_latency=track_latency,
            flush_interval=flush_interval,
            **kwargs
        )
    else:
        # Fallback to original
        logger.warning(f"Cannot optimize {parser_cls.__name__}, using original")
        return parser_cls(**kwargs)
