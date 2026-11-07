import asyncio
from infrastructure.database import setup as database_setup
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.spread_collector.config import DEFAULT_MERGE_CONFIG
from infrastructure.services.collectors.token_collector.supports.merge_global_token import MergeCandidateFinder
from infrastructure.services.common.cache import RedisCache
from settings import settings

async def main():
    # Setup database
    sessionmaker = database_setup(settings.postgres_url)
    repo = RequestsRepo(sessionmaker=sessionmaker)
    
    # Setup cache
    cache = RedisCache(settings.redis_url)
    
    # Create finder
    finder = MergeCandidateFinder(
        repo=repo,
        cache=cache,
        environment="dev",
        config=DEFAULT_MERGE_CONFIG
    )
    
    print("Running split_conflicted_global_tokens...")
    split_count = await finder.split_conflicted_global_tokens()
    print(f"✅ Split {split_count} conflicted global_tokens")

if __name__ == "__main__":
    asyncio.run(main())
