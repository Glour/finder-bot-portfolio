import asyncio
import time
from datetime import datetime, UTC

import msgpack
import orjson
from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.models import TokenBlacklist
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.optimization_config import ENV_CONFIG
from infrastructure.services.collectors.structures.normalizer import default_serializer, msgpack_default, normalize_pair
from infrastructure.services.collectors.structures.utils import make_orderbook_key
from infrastructure.services.common.cache import RedisCache
from settings import logger
from settings import settings


class TokenLoader:
    def __init__(self, repo: RequestsRepo, cache: RedisCache, tokens_load_limit: int = 10000):
        self.repo = repo
        self.cache = cache
        self.tokens_load_limit = tokens_load_limit

    async def start(self):
        while True:
            start = time.perf_counter()
            try:
                count = await self.load_tokens_with_orderbooks()
                logger.info(f"Pushed {count} tokens into analyzer queue in {time.perf_counter() - start:.2f} sec")
            except Exception as e:
                logger.error(f"Error while loading tokens: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def load_tokens_with_orderbooks(self) -> int:
        t0 = time.perf_counter()

        blacklist = await self.cache.get("token_blacklist") or []
        if not blacklist:
            token_blacklist: list[TokenBlacklist] = await self.repo.token_blacklist.get_all()
            blacklist = [
                {
                    "chain": t.chain_name,
                    "address": t.contract_address,
                    "global_token_id": str(t.global_token_id) if t.global_token_id else None,
                    "expires_at": t.expires_at.isoformat() if t.expires_at else None,
                }
                for t in token_blacklist
            ]
            await self.cache.set("token_blacklist", blacklist)

        now = datetime.now(UTC).replace(tzinfo=None)

        # Filter expired entries
        valid_blacklist_entries = [
            t for t in blacklist
            if not t.get("expires_at") or datetime.fromisoformat(t["expires_at"]) > now
        ]

        # Build blacklisted global_token_id set
        blacklisted_gt_ids = {
            t.get("global_token_id")
            for t in valid_blacklist_entries
            if t.get("global_token_id")
        }

        # Build blacklisted (chain, address) pairs set
        blacklist_set = {
            normalize_pair(t.get("chain"), t.get("address"))
            for t in valid_blacklist_entries
            if t.get("chain") or t.get("address")
        }

        t_sql0 = time.perf_counter()
        tokens = await self.repo.global_tokens.get_all_with_data(limit=self.tokens_load_limit)

        if tokens is None:
            logger.error("Failed to load tokens from database: query returned None")
            return 0

        filtered_tokens = []
        for gt in tokens:
            # Check if global_token_id is blacklisted
            if gt.get("id") in blacklisted_gt_ids:
                continue
            
            prices = gt.get("prices") or []
            skip_token = False

            # Check if any (chain, address) pair is blacklisted
            for price in prices:
                for c in price.get("chains") or []:
                    pair = normalize_pair(c.get("chain_name"), c.get("contract_address"))
                    if pair in blacklist_set:
                        skip_token = True
                        break
                if skip_token:
                    break

            if not skip_token:
                filtered_tokens.append(gt)

        t_sql1 = time.perf_counter()
        keys, key_to_price = self._prepare_keys(filtered_tokens)

        t_mget0 = time.perf_counter()
        await self._fill_orderbooks(keys, key_to_price)
        t_mget1 = time.perf_counter()

        t_ser0 = time.perf_counter()
        if ENV_CONFIG.use_msgpack_only:
            # Phase 3 optimization: msgpack-only serialization (83% faster deserialization)
            token_data = [msgpack.packb(token, default=msgpack_default, use_bin_type=True) for token in filtered_tokens]
        else:
            # Legacy: JSON serialization
            token_data = [orjson.dumps(token, default=default_serializer) for token in filtered_tokens]

        if not token_data:
            return 0
        t_ser1 = time.perf_counter()

        t_rpush0 = time.perf_counter()
        pipe = self.cache.client.pipeline(transaction=False)
        pipe.delete("tokens:analyzer")
        pipe.rpush("tokens:analyzer", *token_data)
        await pipe.execute()

        t_rpush1 = time.perf_counter()

        total = time.perf_counter() - t0
        logger.info(
            f"\nTokens={len(filtered_tokens)} | "
            f"SQL={t_sql1 - t_sql0:.2f}s | "
            f"mget and deserialize={t_mget1 - t_mget0:.2f}s | "
            f"Serialize={t_ser1 - t_ser0:.2f}s | "
            f"rpush={t_rpush1 - t_rpush0:.2f}s | "
            f"Total={total:.2f}s"
        )

        return len(tokens)

    @staticmethod
    def _prepare_keys(tokens):
        key_to_price = {}
        keys = []
        for t in tokens:
            gtid = t["id"]
            for p in t["prices"]:
                gts_id = p.get("global_token_source_id")
                token_id = p["token"]["id"]
                market_type = p["market_type"]
                k = make_orderbook_key(str(gtid), str(gts_id), str(token_id), str(market_type))
                keys.append(k)
                key_to_price[k] = p
        return keys, key_to_price

    async def _fill_orderbooks(self, keys, key_to_price):
        if not keys:
            return

        results = await self.cache.client.mget(keys)
        for k, raw in zip(keys, results):
            if not raw:
                continue
            obj = msgpack.unpackb(raw, raw=False)
            key_to_price[k]["orderbook"] = obj.get("orderbook")
            key_to_price[k]["get_at"] = obj.get("get_at")


async def main():
    database.redis = aioredis.from_url(settings.redis.dsn)
    database.init_database(settings)
    repo = RequestsRepo(sessionmaker=database.async_session)
    cache = RedisCache(database.redis)
    loader = TokenLoader(repo=repo, cache=cache, tokens_load_limit=10000)
    await loader.start()


if __name__ == "__main__":
    asyncio.run(main())
