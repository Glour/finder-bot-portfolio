import asyncio
import gc
import psutil
import os

from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.token_collector.manager import get_token_collector_manager
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.common.http_client import load_proxies, init_clients, close_clients
from settings import settings, logger


async def memory_monitor():
    """
    MEMORY FIX: Periodic memory monitoring and cleanup task.
    
    Runs independently to track memory usage and perform garbage collection.
    """
    process = psutil.Process(os.getpid())
    
    while True:
        try:
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # Log memory status every 10 minutes
            logger.info(f"💾 Memory monitor: {memory_mb:.1f}MB RSS")
            
            # MEMORY LEAK DETECTION: Alert if memory is too high
            if memory_mb > 1000:  # More than 1GB
                logger.warning(f"⚠️ High memory usage detected: {memory_mb:.1f}MB")
                
            if memory_mb > 2000:  # More than 2GB  
                logger.error(f"🚨 CRITICAL memory usage: {memory_mb:.1f}MB - possible memory leak!")
                
            # Force aggressive garbage collection every 10 minutes
            gc.collect()
            
            # Wait 10 minutes before next check
            await asyncio.sleep(600)
            
        except Exception as e:
            logger.error(f"Error in memory monitor: {e}")
            await asyncio.sleep(60)  # Retry in 1 minute


async def main():
    database.redis = aioredis.from_url(settings.redis.dsn, encoding="utf-8")
    proxies = load_proxies()
    init_clients(proxies)

    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis_cache = RedisCache(database.redis)

    token_collector = get_token_collector_manager(repo=repo, cache=redis_cache, interval=120.0)

    # MEMORY FIX: Start memory monitoring in background
    memory_task = asyncio.create_task(memory_monitor())

    logger.info("Token collector started")
    
    try:
        await token_collector.run()
    finally:
        # Cleanup background tasks
        memory_task.cancel()
        try:
            await memory_task
        except asyncio.CancelledError:
            pass

        await close_clients()
        await database.redis.close()


if __name__ == "__main__":
    asyncio.run(main())
