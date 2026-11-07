import asyncio
import os

from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.models.base import MarketType, SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.price_collector.manager import get_price_collector_manager
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import load_proxies, init_clients, close_clients
from settings import settings, logger


async def main():
    market_type_str = os.getenv("MARKET_TYPE", "").upper()
    scale_count = os.getenv("SCALE_COUNT", 1)
    source_name = os.getenv("SOURCE_NAME")

    # market_type is optional for DEX sources
    market_type = None
    if market_type_str == MarketType.SPOT.value:
        market_type = MarketType.SPOT
    elif market_type_str == MarketType.FUTURES.value:
        market_type = MarketType.FUTURES
    elif market_type_str != "":
        raise ValueError(f"Unsupported market_type {market_type_str}")

    database.redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
    proxies = load_proxies()
    init_clients(proxies)

    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis_cache = RedisCache(database.redis)

    # Check if this source exists (try both CEX and DEX)
    source = await repo.sources.get_by_name_and_type(source_name, SourceType.CEX)
    if not source:
        # Try DEX type
        source = await repo.sources.get_by_name_and_type(source_name, SourceType.DEX)
    if not source:
        logger.error(f"Source {source_name} not found in database. Exiting.")
        return

    # Wait while source is muted (check every 60 seconds)
    # Skip mute check for DEX sources (they don't have market_type)
    if market_type and source.type == SourceType.CEX:
        while True:
            is_muted = await repo.exchange_blacklist.is_muted(source.id, market_type)
            if not is_muted:
                break
            logger.info(f"Source {source_name} ({market_type.value}) is muted. Waiting 60s...")
            await asyncio.sleep(60)

    price_collector = get_price_collector_manager(
        repo=repo, cache=redis_cache, market_type=market_type, scale_count=scale_count, source_name=source_name
    )

    logger.info("Price collector started")
    await price_collector.run()

    await close_clients()
    await database.redis.close()


if __name__ == "__main__":
    asyncio.run(main())
