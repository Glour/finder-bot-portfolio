"""
Optimized Spread Collector entry point with feature flags.

Supports gradual rollout of optimizations:
- Token metadata caching (Phase 1)
- Redis Streams (Phase 2)
- msgpack-only serialization (Phase 3)
- Multiple workers with consumer groups

Usage:
    # Baseline (no optimizations)
    python main_optimized.py

    # With token caching
    OPTIMIZE_TOKEN_CACHE=true python main_optimized.py

    # With all optimizations
    OPTIMIZE_TOKEN_CACHE=true OPTIMIZE_USE_STREAMS=true OPTIMIZE_MSGPACK_ONLY=true python main_optimized.py

    # With profiling
    OPTIMIZE_ENABLE_PROFILING=true OPTIMIZE_ENABLE_LATENCY_TRACKING=true python main_optimized.py
"""
import asyncio
import sys

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import close_clients
from infrastructure.services.collectors.optimization_config import ENV_CONFIG
from infrastructure.services.collectors.spread_collector.config import PerformanceConfig
from infrastructure.services.collectors.spread_collector.signal_writer import SignalHistoryWriter
from infrastructure.services.collectors.utils.profiler import get_tracker
from settings import settings, logger


async def main():
    """
    Main entry point with optimization support.

    Automatically selects the right implementation based on ENV_CONFIG.
    """
    # Load optimization config
    opt_config = ENV_CONFIG

    logger.info("=" * 80)
    logger.info("Starting Optimized Spread Collector")
    logger.info("=" * 80)
    logger.info(opt_config.summary())
    logger.info("=" * 80)

    # Performance configuration
    perf_config = PerformanceConfig(
        batch_size=opt_config.batch_size,
        analysis_interval=opt_config.analysis_interval,
        redis_max_connections=50,
        enable_profiling=opt_config.enable_profiling
    )

    # Initialize Redis with optimization settings
    redis_decode = not opt_config.use_msgpack_only  # Binary mode for msgpack
    database.redis = aioredis.from_url(
        settings.redis.dsn,
        decode_responses=redis_decode,
        max_connections=perf_config.redis_max_connections,
        socket_keepalive=True,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30
    )
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis_cache = RedisCache(database.redis)

    # Start signal history writer (background DB persistence)
    signal_writer = SignalHistoryWriter(
        database.async_session,
        repo=repo,
        flush_interval=2.0,
        blacklist_refresh_interval=600.0
    )
    signal_writer.start()

    # Select implementation based on optimizations
    if opt_config.use_redis_streams:
        # Phase 2: Redis Streams mode
        logger.info("Using OptimizedArbitrageAnalyzerManager with Redis Streams")
        from infrastructure.services.collectors.spread_collector.manager_optimized import (
            OptimizedArbitrageAnalyzerManager
        )

        manager = OptimizedArbitrageAnalyzerManager(
            repo=repo,
            cache=redis_cache,
            tokens_load_limit=10000,
            performance_config=perf_config,
            use_streams=True,
            consumer_group=opt_config.stream_consumer_group,
            consumer_name=opt_config.stream_consumer_name,
            signal_writer=signal_writer
        )
    elif opt_config.use_token_metadata_cache:
        # Phase 1: Token caching with LIST mode
        logger.info("Using OptimizedArbitrageAnalyzerManager with token caching")
        from infrastructure.services.collectors.spread_collector.manager_optimized import (
            OptimizedArbitrageAnalyzerManager
        )

        manager = OptimizedArbitrageAnalyzerManager(
            repo=repo,
            cache=redis_cache,
            tokens_load_limit=10000,
            performance_config=perf_config,
            use_streams=False,
            signal_writer=signal_writer
        )
    else:
        # Baseline: Original implementation
        logger.info("Using original ArbitrageAnalyzerManager (baseline)")
        from infrastructure.services.collectors.spread_collector.manager import (
            ArbitrageAnalyzerManager
        )

        manager = ArbitrageAnalyzerManager(
            repo=repo,
            cache=redis_cache,
            tokens_load_limit=10000,
            performance_config=perf_config,
            signal_writer=signal_writer
        )

    # Start latency tracking if enabled
    if opt_config.enable_latency_tracking:
        tracker = get_tracker()
        asyncio.create_task(periodic_stats_logger(tracker))

    # Run manager
    try:
        await manager.run()
    finally:
        await close_clients()
        await database.redis.close()


async def periodic_stats_logger(tracker, interval: int = 60):
    """Log latency statistics periodically."""
    while True:
        await asyncio.sleep(interval)
        tracker.log_stats()


if __name__ == "__main__":
    asyncio.run(main())
