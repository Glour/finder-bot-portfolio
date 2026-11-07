"""
Cleanup empty/useless GlobalTokens that have no sources, chains, or prices.

These are artifacts from token_collector runs where tokens were created
but never merged or populated with data.

Run this periodically (e.g., daily via cron) to keep DB clean.
"""
import asyncio
from datetime import datetime, timedelta, UTC

from infrastructure.database.models import GlobalToken
from infrastructure.database.requests import RequestsRepo
from infrastructure.database import setup as database
from settings import settings, logger


async def cleanup_empty_global_tokens(older_than_hours: int = 24, dry_run: bool = False):
    """
    Delete GlobalTokens that have:
    - No sources (global_token_sources is empty)
    - No token_chains
    - No prices
    - Older than specified hours
    
    Args:
        older_than_hours: Only delete GT older than this many hours (default 24)
        dry_run: If True, only count and log, don't delete
    """
    # Initialize database
    database.init_database(settings)
    sessionmaker = database.async_session
    
    # DB stores timestamps as naive UTC, so we need naive datetime
    cutoff_time = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=older_than_hours)
    
    logger.info(f"🧹 Starting cleanup of empty GlobalTokens (older than {older_than_hours}h, cutoff: {cutoff_time})")
    
    async with sessionmaker() as session:
        # Find empty GT:
        # - created_at < cutoff_time
        # - is_deleted = False
        # - No sources (LEFT JOIN on global_token_sources gives NULL)
        # - No token_chains
        # - No prices
        
        from sqlalchemy import select, func
        from infrastructure.database.models.sources import GlobalTokenSource
        from infrastructure.database.models.tokens import TokenPrice
        from infrastructure.database.models import TokenOnChain
        
        # Find GT with no sources
        stmt = (
            select(GlobalToken)
            .outerjoin(GlobalTokenSource, GlobalTokenSource.global_token_id == GlobalToken.id)
            .outerjoin(TokenOnChain, TokenOnChain.global_token_id == GlobalToken.id)
            .outerjoin(TokenPrice, TokenPrice.global_token_id == GlobalToken.id)
            .where(
                GlobalToken.is_deleted == False,
                GlobalToken.created_at < cutoff_time,
                GlobalTokenSource.id == None,  # No sources
                TokenOnChain.id == None,       # No chains
                TokenPrice.id == None          # No prices
            )
            .group_by(GlobalToken.id)
        )
        
        result = await session.execute(stmt)
        empty_gts = result.scalars().all()
        
        count = len(empty_gts)
        
        if count == 0:
            logger.info("✅ No empty GlobalTokens found")
            return 0
        
        # Group by symbol for reporting
        by_symbol = {}
        for gt in empty_gts:
            by_symbol.setdefault(gt.symbol, []).append(gt.id)
        
        logger.info(f"📊 Found {count} empty GlobalTokens across {len(by_symbol)} symbols")
        
        # Show top 10 symbols by count
        top_symbols = sorted(by_symbol.items(), key=lambda x: len(x[1]), reverse=True)[:10]
        logger.info("Top 10 symbols with most empty GT:")
        for symbol, ids in top_symbols:
            logger.info(f"  {symbol}: {len(ids)} empty GT")
        
        if dry_run:
            logger.info(f"🔍 DRY RUN: Would delete {count} empty GlobalTokens")
            return count
        
        # Delete them
        from sqlalchemy import delete
        
        gt_ids = [gt.id for gt in empty_gts]
        
        delete_stmt = delete(GlobalToken).where(GlobalToken.id.in_(gt_ids))
        await session.execute(delete_stmt)
        await session.commit()
        
        logger.info(f"✅ Deleted {count} empty GlobalTokens")
        return count


async def main():
    """Run cleanup with default settings."""
    import sys
    
    dry_run = "--dry-run" in sys.argv
    older_than = 24
    
    if "--hours" in sys.argv:
        idx = sys.argv.index("--hours")
        if idx + 1 < len(sys.argv):
            older_than = int(sys.argv[idx + 1])
    
    deleted = await cleanup_empty_global_tokens(
        older_than_hours=older_than,
        dry_run=dry_run
    )
    
    logger.info(f"Cleanup complete: {deleted} empty GT {'would be' if dry_run else 'were'} deleted")


if __name__ == "__main__":
    asyncio.run(main())
