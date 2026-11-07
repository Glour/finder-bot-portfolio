from contextlib import asynccontextmanager

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
async def lifespan(App: FastAPI):  # noqa
    database.redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
    await initialize_cache_and_limiter()
    await initialize_database()
    proxies = load_proxies()
    init_clients(proxies)
    logger.info("Application initialized")
    yield
    await database.redis.close()
    await FastAPILimiter.close()
    await close_clients()


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


async def initialize_cache_and_limiter():
    database.redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
    FastAPICache.init(RedisBackend(database.redis), prefix="fastapi-cache")
    await FastAPILimiter.init(database.redis)


async def initialize_database():
    database.init_database(settings)
    await startup()


async def startup() -> None:
    pass
