from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from fastapi_limiter import FastAPILimiter
from fastapi_pagination import add_pagination
from redis import asyncio as aioredis

from api.tokens.v1.token import router as token_router
from infrastructure.database import setup as database
from infrastructure.services.common.cors import setup_cors
from infrastructure.services.common.http_client import load_proxies, init_clients, close_clients
from settings import settings, logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
        database.redis = redis
        stack.push_async_callback(redis.close)
        stack.callback(FastAPICache.reset)
        stack.callback(setattr, database, "redis", None)
        await initialize_cache_and_limiter(redis)

        stack.push_async_callback(close_database)
        database.init_database(settings)

        stack.push_async_callback(close_clients)
        init_clients(load_proxies())
        logger.info("Application initialized")
        yield


app = FastAPI(
    lifespan=lifespan,
    title="Finder Bot API",
    description="API for market data and token information.",
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
    version=settings.app_version,
)


@app.get("/ping")
async def pong() -> dict[str, str]:
    return {"ping": "pong!"}


@app.get("/version")
@cache(expire=600)
def get_version() -> dict[str, str]:
    return {"version": settings.app_version}


app.include_router(token_router, prefix="/api/v1/tokens", tags=["Tokens"])

setup_cors(app, settings)
add_pagination(app)


async def initialize_cache_and_limiter(redis):
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    await FastAPILimiter.init(redis)


async def close_database() -> None:
    await database.db_manager.close_database()
    database.async_session = None
