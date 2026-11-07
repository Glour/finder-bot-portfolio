#!/usr/bin/env python3
"""
Cleanup duplicate source-specific global_tokens and fix base_asset for FUTURES.
"""

import asyncio
import sys
from uuid import uuid4

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


async def cleanup_source_specific_duplicates():
    """Remove duplicate source-specific GTs, keeping the one with most token_chains."""
    # db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # Find all source-specific symbols with duplicates
        result = await session.execute(text("""
            SELECT 
                symbol,
                ARRAY_AGG(id ORDER BY id) as gt_ids
            FROM global_tokens
            WHERE symbol LIKE '%:%'
            GROUP BY symbol
            HAVING COUNT(*) > 1
        """))
        
        duplicates = list(result)
        print(f"Found {len(duplicates)} duplicate source-specific symbols")
        
        deleted_count = 0
        for symbol, gt_ids in duplicates:
            # Keep first GT, delete rest
            keep_id = gt_ids[0]
            delete_ids = gt_ids[1:]
            
            for del_id in delete_ids:
                # Check if it has token_chains
                check = await session.execute(text("""
                    SELECT COUNT(*) FROM token_chains WHERE global_token_id = :gt_id
                """), {'gt_id': del_id})
                count = check.scalar()
                
                if count == 0:
                    # Safe to delete
                    await session.execute(text("""
                        DELETE FROM global_tokens WHERE id = :gt_id
                    """), {'gt_id': del_id})
                    deleted_count += 1
                else:
                    print(f"  Warning: {symbol} GT {del_id} has {count} token_chains, skipping")
        
        await session.commit()
        print(f"Deleted {deleted_count} duplicate global_tokens")


async def fix_base_asset_for_futures():
    """Add _FUTURES suffix to base_asset for FUTURES tokens missing it."""
    # db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # Find FUTURES tokens without _FUTURES in base_asset
        result = await session.execute(text("""
            SELECT 
                id,
                symbol,
                base_asset,
                source_id
            FROM tokens
            WHERE market_type = 'FUTURES'
              AND base_asset NOT LIKE '%_FUTURES'
              AND base_asset NOT LIKE '%FUTURES'
        """))
        
        tokens_to_fix = list(result)
        print(f"Found {len(tokens_to_fix)} FUTURES tokens to fix")
        
        for token_id, symbol, base_asset, source_id in tokens_to_fix:
            new_base_asset = f"{base_asset}_FUTURES"
            await session.execute(text("""
                UPDATE tokens
                SET base_asset = :new_base_asset
                WHERE id = :token_id
            """), {'new_base_asset': new_base_asset, 'token_id': token_id})
        
        await session.commit()
        print(f"Updated {len(tokens_to_fix)} tokens")


async def main():
    print("=" * 80)
    print("CLEANUP: Duplicates and base_asset sync")
    print("=" * 80)
    print()
    
    print("Step 1: Cleanup duplicate source-specific GTs...")
    await cleanup_source_specific_duplicates()
    print()
    
    print("Step 2: Fix base_asset for FUTURES tokens...")
    await fix_base_asset_for_futures()
    print()
    
    print("✅ Cleanup completed!")


if __name__ == '__main__':
    asyncio.run(main())
