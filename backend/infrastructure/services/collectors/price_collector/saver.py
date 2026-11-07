import asyncio

import msgpack
from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from settings import settings, logger

save_interval = 60
SCAN_BATCH_SIZE = 1000
UPSERT_BATCH_SIZE = 1000


class SaverService:
    def __init__(self, repo: RequestsRepo, redis: RedisCache):
        self.repo = repo
        self.redis = redis

    async def _process_batch(self, keys, rows):
        if not keys:
            return

        client = self.redis.client
        results = await client.mget(keys)

        for idx, raw in enumerate(results):
            if not raw:
                continue
            try:
                obj = msgpack.unpackb(raw, raw=False)
            except Exception as e:
                logger.exception(f"Failed to unpack msgpack: {e}")
                continue

            # DEBUG: Log VIOLET processing
            if "VIOLET" in obj.get("symbol", "").upper():
                logger.info(f"DEBUG VIOLET saver: symbol={obj.get('symbol')}, gt_id={obj.get('global_token_id')}, token_id={obj.get('token_id')}")

            rows.append({
                "symbol": obj.get("symbol", ""),
                "price_usd": 0.0,
                "orderbook": obj.get("orderbook"),
                "market_type": (obj.get("market_type") or "").upper(),
                "token_id": obj.get("token_id"),
                "global_token_id": obj.get("global_token_id"),
                "global_token_source_id": obj.get("global_source_id"),
            })

    async def save_tokens_from_redis(self, prefix_key: str):
        client = self.redis.client
        while True:
            try:
                rows = []
                batch = []

                async for key in client.scan_iter(match=f"{prefix_key}:*", count=SCAN_BATCH_SIZE):
                    key_str = key.decode() if isinstance(key, bytes) else key
                    batch.append(key_str)

                    if len(batch) >= SCAN_BATCH_SIZE:
                        await self._process_batch(batch, rows)
                        batch = []

                        if len(rows) >= UPSERT_BATCH_SIZE:
                            await self.repo.token_prices.bulk_upsert(rows)
                            logger.info(f"Saved {len(rows)} tokens from {prefix_key}")
                            rows.clear()

                # добиваем хвост
                if batch:
                    await self._process_batch(batch, rows)
                if rows:
                    # DEBUG: Check for NULL values
                    null_global_token = sum(1 for r in rows if not r.get('global_token_id'))
                    null_global_source = sum(1 for r in rows if not r.get('global_token_source_id'))
                    null_token = sum(1 for r in rows if not r.get('token_id'))
                    if null_global_token or null_global_source or null_token:
                        logger.warning(f"{prefix_key}: NULL values - global_token_id: {null_global_token}, global_source_id: {null_global_source}, token_id: {null_token}")
                    
                    await self.repo.token_prices.bulk_upsert(rows)
                    logger.info(f"Saved {len(rows)} tokens from {prefix_key}")

            except Exception as e:
                logger.exception(f"Error saving tokens from {prefix_key}: {e}")

            await asyncio.sleep(save_interval)

    async def run(self, prefixes: list[str]):
        tasks = [asyncio.create_task(self.save_tokens_from_redis(prefix)) for prefix in prefixes]
        await asyncio.gather(*tasks)


async def main():
    database.redis = aioredis.from_url(settings.redis.dsn, decode_responses=False)
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    redis = RedisCache(database.redis)

    saver_service = SaverService(repo, redis)
    logger.info("Saver service started")
    await saver_service.run(prefixes=["prices:spot", "prices:futures"])


if __name__ == "__main__":
    asyncio.run(main())
