import asyncio
from infrastructure.database.requests import get_repo
from infrastructure.services.collectors.spread_collector.config import DEFAULT_MERGE_CONFIG
from infrastructure.services.collectors.token_collector.supports.merge_global_token import MergeCandidateFinder
from infrastructure.services.common.cache import RedisCache
from settings import settings

async def main():
    cache = RedisCache(settings.redis_url)
    async with get_repo() as repo:
        finder = MergeCandidateFinder(
            repo=repo,
            cache=cache,
            environment="dev",
            config=DEFAULT_MERGE_CONFIG
        )
        
        print("Running split_conflicted_global_tokens...")
        split_count = await finder.split_conflicted_global_tokens()
        print(f"Split {split_count} conflicted global_tokens")

if __name__ == "__main__":
    asyncio.run(main())
