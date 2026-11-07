"""
Merge duplicate GlobalTokens with the same symbol.

This script finds all symbols that have multiple GT and merges them:
1. Pick the "best" GT (most sources, most chains, oldest)
2. Move all token_chains, prices, sources from duplicates to the main GT
3. Mark duplicates as is_deleted=true

Run this BEFORE applying the UNIQUE constraint migration.
"""
import asyncio
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, func, update, delete
from sqlalchemy.orm import selectinload

from infrastructure.database.models import GlobalToken
from infrastructure.database.models.sources import GlobalTokenSource
from infrastructure.database.models.tokens import TokenPrice
from infrastructure.database.models import TokenOnChain
from infrastructure.database import setup as database
from settings import settings, logger


async def find_duplicate_symbols(sessionmaker):
    """Find all symbols that have multiple active GT."""
    async with sessionmaker() as session:
        # Find symbols with count > 1
        stmt = (
            select(GlobalToken.symbol, func.count(GlobalToken.id).label('count'))
            .where(GlobalToken.is_deleted == False)
            .group_by(GlobalToken.symbol)
            .having(func.count(GlobalToken.id) > 1)
            .order_by(func.count(GlobalToken.id).desc())
        )
        
        result = await session.execute(stmt)
        duplicates = result.all()
        
        return [(row[0], row[1]) for row in duplicates]


async def merge_symbol_duplicates(symbol: str, sessionmaker, dry_run: bool = False):
    """
    Merge all GT with the same symbol into one.
    
    Strategy:
    1. Load all GT with this symbol
    2. Pick the "main" GT (most sources, then most chains, then oldest)
    3. Move all relationships from others to main
    4. Mark others as deleted
    """
    
    async with sessionmaker() as session:
        # Load all GT with this symbol (with relationships)
        stmt = (
            select(GlobalToken)
            .options(
                selectinload(GlobalToken.sources),
                selectinload(GlobalToken.token_on_chains),
                selectinload(GlobalToken.prices),
            )
            .where(
                GlobalToken.symbol == symbol,
                GlobalToken.is_deleted == False
            )
        )
        
        result = await session.execute(stmt)
        gts = list(result.scalars().all())
        
        if len(gts) <= 1:
            logger.info(f"  {symbol}: only 1 GT, skipping")
            return 0
        
        # Pick the "main" GT (best one to keep)
        # Priority: most sources > most chains > oldest created_at
        main_gt = max(
            gts,
            key=lambda gt: (
                len(gt.sources) if gt.sources else 0,
                len(gt.token_on_chains) if gt.token_on_chains else 0,
                -(gt.created_at.timestamp() if gt.created_at else 0)  # older is better (negative for max)
            )
        )
        
        duplicates = [gt for gt in gts if gt.id != main_gt.id]
        
        logger.info(
            f"  {symbol}: merging {len(duplicates)} duplicates into {main_gt.id} "
            f"(sources: {len(main_gt.sources) if main_gt.sources else 0}, "
            f"chains: {len(main_gt.token_on_chains) if main_gt.token_on_chains else 0})"
        )
        
        if dry_run:
            return len(duplicates)
        
        # Move relationships from duplicates to main
        moved_sources = 0
        moved_chains = 0
        moved_prices = 0
        
        for dup_gt in duplicates:
            # Move GlobalTokenSource (delete duplicates first, then move)
            # Query existing source_ids directly from DB
            existing_gts_stmt = select(GlobalTokenSource.source_id).where(
                GlobalTokenSource.global_token_id == main_gt.id
            )
            existing_result = await session.execute(existing_gts_stmt)
            existing_source_ids = set(row[0] for row in existing_result.fetchall())
            
            if existing_source_ids:
                # Delete GlobalTokenSource entries from dup that would conflict
                await session.execute(
                    delete(GlobalTokenSource)
                    .where(GlobalTokenSource.global_token_id == dup_gt.id)
                    .where(GlobalTokenSource.source_id.in_(existing_source_ids))
                )
            
            # Move remaining GlobalTokenSource entries
            result = await session.execute(
                update(GlobalTokenSource)
                .where(GlobalTokenSource.global_token_id == dup_gt.id)
                .values(global_token_id=main_gt.id)
            )
            moved_sources += result.rowcount if hasattr(result, 'rowcount') else 0
            
            # Move TokenOnChain (delete conflicts first)
            # Query existing token_ids directly from DB
            existing_tc_stmt = select(TokenOnChain.token_id).where(
                TokenOnChain.global_token_id == main_gt.id
            )
            existing_tc_result = await session.execute(existing_tc_stmt)
            existing_token_ids = set(row[0] for row in existing_tc_result.fetchall())
            
            if existing_token_ids:
                # Delete TokenOnChain entries that would conflict
                await session.execute(
                    delete(TokenOnChain)
                    .where(TokenOnChain.global_token_id == dup_gt.id)
                    .where(TokenOnChain.token_id.in_(existing_token_ids))
                )
            
            # Move remaining TokenOnChain entries
            result = await session.execute(
                update(TokenOnChain)
                .where(TokenOnChain.global_token_id == dup_gt.id)
                .values(global_token_id=main_gt.id)
            )
            moved_chains += result.rowcount if hasattr(result, 'rowcount') else 0
            
            # Move TokenPrice (delete duplicates first - prices with same token_id)
            # Query existing token_ids for prices in main_gt
            existing_price_tokens_stmt = select(TokenPrice.token_id).where(
                TokenPrice.global_token_id == main_gt.id
            )
            existing_price_tokens_result = await session.execute(existing_price_tokens_stmt)
            existing_price_token_ids = set(row[0] for row in existing_price_tokens_result.fetchall())
            
            if existing_price_token_ids:
                # Delete TokenPrice entries that would conflict
                await session.execute(
                    delete(TokenPrice)
                    .where(TokenPrice.global_token_id == dup_gt.id)
                    .where(TokenPrice.token_id.in_(existing_price_token_ids))
                )
            
            # Move remaining prices
            result = await session.execute(
                update(TokenPrice)
                .where(TokenPrice.global_token_id == dup_gt.id)
                .values(global_token_id=main_gt.id)
            )
            moved_prices += result.rowcount if hasattr(result, 'rowcount') else 0
            
            # Mark duplicate as deleted
            await session.execute(
                update(GlobalToken)
                .where(GlobalToken.id == dup_gt.id)
                .values(is_deleted=True)
            )
        
        await session.commit()
        
        logger.info(
            f"    Merged: {moved_sources} sources, {moved_chains} chains, {moved_prices} prices"
        )
        
        return len(duplicates)


async def main():
    """Find and merge all duplicate symbols."""
    import sys
    
    dry_run = "--dry-run" in sys.argv
    
    # Initialize database once at start
    database.init_database(settings)
    sessionmaker = database.async_session
    
    logger.info("🔍 Finding duplicate symbols...")
    duplicates = await find_duplicate_symbols(sessionmaker)
    
    if not duplicates:
        logger.info("✅ No duplicates found!")
        return
    
    total_dups = sum(count - 1 for _, count in duplicates)
    logger.info(f"📊 Found {len(duplicates)} symbols with duplicates ({total_dups} extra GT total)")
    
    # Show top 20
    logger.info("\nTop 20 duplicated symbols:")
    for symbol, count in duplicates[:20]:
        logger.info(f"  {symbol}: {count} GT")
    
    if dry_run:
        logger.info(f"\n🔍 DRY RUN: Would merge {total_dups} duplicate GT")
        return
    
    # Merge all
    logger.info("\n🔧 Starting merge...")
    total_merged = 0
    
    for i, (symbol, count) in enumerate(duplicates, 1):
        if i % 100 == 0:
            logger.info(f"  Progress: {i}/{len(duplicates)} symbols...")
        merged = await merge_symbol_duplicates(symbol, sessionmaker, dry_run=dry_run)
        total_merged += merged
    
    logger.info(f"\n✅ Merge complete: {total_merged} duplicate GT merged")


if __name__ == "__main__":
    asyncio.run(main())
