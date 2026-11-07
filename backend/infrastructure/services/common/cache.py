import json
from abc import ABC, abstractmethod
from typing import Any

from redis.asyncio import Redis as AsyncRedisClient

from infrastructure.database import setup as database


class AbstractCache(ABC):
    @abstractmethod
    async def exist(self, *keys) -> int:
        """Check that keys exist in cache."""
        raise NotImplementedError

    @abstractmethod
    async def get(self, key: str) -> Any:
        """Get data from cache by the key."""
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, data: Any, timeout_secs: int | None = None) -> bool:
        """Save data (key: value) in cache with the given key and timeout."""
        raise NotImplementedError

    @abstractmethod
    async def delete(self, *keys) -> int:
        """Delete keys from cache."""
        raise NotImplementedError


class RedisCache(AbstractCache):
    """Redis cache implementation."""

    def __init__(self, redis_client: AsyncRedisClient):
        self.client = redis_client

    async def exist(self, *keys) -> int:
        return await self.client.exists(*keys)

    async def get(
            self,
            key: str,
    ) -> Any | None:
        data = await self.client.get(key)
        if data is None:
            return None
        return json.loads(data)

    async def set(self, key: str, data: Any, timeout_secs: int | None = None) -> bool:
        # MEMORY FIX: Add size monitoring to prevent large cache entries
        serialized = json.dumps(data)
        size_bytes = len(serialized.encode('utf-8'))
        
        # MEMORY WARNING: Warn about large cache entries (>10MB)
        if size_bytes > 10 * 1024 * 1024:  # 10MB
            from settings import logger
            logger.warning(f"⚠️ Large cache entry: {key} ({size_bytes / 1024 / 1024:.1f}MB)")
        
        # MEMORY FIX: Set default timeout if none provided to prevent permanent accumulation
        if timeout_secs is None:
            timeout_secs = 3600  # Default 1 hour timeout
        
        return bool(
            await self.client.set(
                name=key,
                value=serialized,
                ex=timeout_secs,
            )
        )

    async def sadd(self, key: str, *values: Any) -> int:
        return await self.client.sadd(key, *values)

    async def spop(self, key: str, count: int = 1) -> list[str]:
        res = await self.client.spop(key, count)
        if res is None:
            return []
        if isinstance(res, (bytes, str)):
            return [res.decode() if isinstance(res, bytes) else res]
        return [r.decode() if isinstance(r, bytes) else r for r in res]

    async def smembers(self, key: str) -> list[str]:
        res = await self.client.smembers(key)
        return [r.decode() if isinstance(r, bytes) else r for r in res]

    async def delete_key(self, key: str) -> int:
        return await self.client.delete(key)

    async def delete(self, *keys) -> int:
        return await self.client.delete(*keys)

    async def pop_batch(self, key: str, batch_size: int = 30) -> list[dict]:
        """
        Получаем batch токенов из множества Redis, удаляя их сразу из множества.
        """
        tokens = []
        for _ in range(batch_size):
            token_json = await self.client.spop(key)
            if token_json is None:
                break
            tokens.append(json.loads(token_json))
        return tokens

    async def push_to_queue(self, key: str, items: list[dict]) -> int:
        """
        Кладем пачку цен в очередь Redis (list).
        """
        encoded = [json.dumps(item) for item in items]
        return await self.client.rpush(key, *encoded)

    async def pop_from_queue(self, key: str, batch_size: int = 30) -> list[dict]:
        """
        Забираем пачку цен из очереди Redis (list).
        """
        pipe = self.client.pipeline()
        for _ in range(batch_size):
            pipe.lpop(key)
        results = await pipe.execute()

        return [
            json.loads(r.decode())
            for r in results
            if r is not None
        ]

    async def queue_len(self, key: str) -> int:
        return await self.client.llen(key)

    async def hset_dict(self, key: str, mapping: dict[str, Any]) -> bool:
        """
        Сохраняет словарь в Redis hash.
        Все значения сериализуются в JSON.
        """
        encoded = {k: json.dumps(v) for k, v in mapping.items()}
        return bool(await self.client.hset(key, mapping=encoded))

    async def hgetall_dict(self, key: str) -> dict[str, Any] | None:
        """
        Получаем все поля из hash и десериализуем из JSON.
        """
        raw = await self.client.hgetall(key)
        if not raw:
            return None
        return {k.decode() if isinstance(k, bytes) else k: json.loads(v.decode() if isinstance(v, bytes) else v)
                for k, v in raw.items()}


def get_redis_cache() -> RedisCache:
    return RedisCache(database.redis)
