"""Cleanup utilities for orderbooks when exchanges are muted."""
import asyncio
from uuid import UUID
from infrastructure.database.models.base import MarketType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from settings import logger


async def cleanup_orderbooks(
    redis: RedisCache,
    repo: RequestsRepo,
    source_id: UUID,
    market_type: MarketType | None
):
    """
    Clean orderbooks from Redis when exchange is muted.

    Redis key pattern: orderbook:{gtid}:{gts_id}:{token_id}:{market_type}

    Args:
        redis: Redis cache client
        repo: Database repository
        source_id: Source UUID to cleanup
        market_type: Market type to cleanup (None = all markets)
    """
    try:
        # Get all global_token_source_id for this source with timeout
        gts_list = await asyncio.wait_for(
            repo.global_token_sources.get_all(source_id=source_id),
            timeout=10.0
        )

        if not gts_list:
            logger.info(f"No global_token_sources found for source_id={source_id}")
            return

        keys_to_delete = []
        max_keys = 10000  # Limit to prevent memory issues

        for gts in gts_list:
            if len(keys_to_delete) >= max_keys:
                break

            if market_type:
                pattern = f"orderbook:*:{gts.id}:*:{market_type.value}"
            else:
                pattern = f"orderbook:*:{gts.id}:*"

            # Scan with limit
            async for key in redis.client.scan_iter(match=pattern, count=500):
                keys_to_delete.append(key)
                if len(keys_to_delete) >= max_keys:
                    break

        # Delete in batches
        if keys_to_delete:
            batch_size = 1000
            for i in range(0, len(keys_to_delete), batch_size):
                batch = keys_to_delete[i:i+batch_size]
                await redis.client.delete(*batch)

            logger.info(
                f"Deleted {len(keys_to_delete)} orderbook keys for source_id={source_id}, "
                f"market_type={market_type.value if market_type else 'all'}"
            )
        else:
            logger.info(f"No orderbook keys found for source_id={source_id}")

    except asyncio.TimeoutError:
        logger.error(f"Timeout during cleanup_orderbooks for source_id={source_id}")
    except Exception as e:
        logger.error(f"Error during cleanup_orderbooks: {e}")
