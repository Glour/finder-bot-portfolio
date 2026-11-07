import asyncio
import sys

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # uvloop not available, use default event loop

from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import close_clients
from infrastructure.services.collectors.spread_collector.manager import ArbitrageAnalyzerManager
from infrastructure.services.collectors.spread_collector.config import PerformanceConfig
from settings import settings, logger


async def main():
    # Load performance configuration
    perf_config = PerformanceConfig(
        batch_size=200,  # Smaller batch = lower latency per batch
        analysis_interval=0.5,
        redis_max_connections=50,
        enable_profiling=True  # Enable for production monitoring
    )

    # Optimize Redis connection pool for high throughput
    database.redis = aioredis.from_url(
        settings.redis.dsn,
        decode_responses=True,
        max_connections=perf_config.redis_max_connections,
        socket_keepalive=True,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30
    )
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis_cache = RedisCache(database.redis)

    spread_collector = ArbitrageAnalyzerManager(
        repo=repo,
        cache=redis_cache,
        tokens_load_limit=10000,
        performance_config=perf_config
    )
    logger.info(
        f"Spread collector started with batch_size={perf_config.batch_size}, "
        f"interval={perf_config.analysis_interval}s, "
        f"redis_pool={perf_config.redis_max_connections}"
    )
    await spread_collector.run()

    await close_clients()
    await database.redis.close()


if __name__ == "__main__":
    asyncio.run(main())
