"""Resource lifecycle shared by the market-data collectors."""

from contextlib import AsyncExitStack, asynccontextmanager

from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import close_clients, init_clients, load_proxies
from settings import settings


async def _close_database() -> None:
    try:
        await database.db_manager.close_database()
    finally:
        database.async_session = None


@asynccontextmanager
async def collector_resources():
    """Open collector dependencies and close them on shutdown or failed startup."""
    async with AsyncExitStack() as stack:
        redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
        database.redis = redis
        stack.push_async_callback(redis.close)
        stack.callback(setattr, database, "redis", None)

        stack.push_async_callback(close_clients)
        init_clients(load_proxies())

        stack.push_async_callback(_close_database)
        database.init_database(settings)
        yield RequestsRepo(sessionmaker=database.async_session), RedisCache(redis)
