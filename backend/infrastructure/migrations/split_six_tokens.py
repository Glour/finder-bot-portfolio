#!/usr/bin/env python3
"""
Split contaminated SIX global_token into two separate tokens:
- MEXC SIX (BSC contract)
- Hyperliquid SIX (HYPE contract)
"""

import asyncio
import sys
from uuid import uuid4

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


CONTAMINATED_GT_ID = '50470740-120f-40d6-bc47-b8a3f5f83c5c'
MEXC_TOKEN_ID = 'b1b7bdc6-72c3-41dd-90f5-f42b540d1e9a'  # SIXUSDT on MEXC
HYPERLIQUID_TOKEN_ID = '41bec451-d409-4668-aeeb-b40fce4c065a'  # SIXUSDC on Hyperliquid


async def main():
    # Initialize database
    db_manager.init_database(settings)
    
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # 1. Create new global_token for Hyperliquid SIX
        new_gt_id = uuid4()
        print(f"Creating new global_token {new_gt_id} for Hyperliquid SIX...")
        
        await session.execute(text("""
            INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
            SELECT :new_id, symbol, NOW(), NOW(), false
            FROM global_tokens WHERE id = :old_id
        """), {'new_id': new_gt_id, 'old_id': CONTAMINATED_GT_ID})
        
        # 2. Move Hyperliquid token_chains to new GT
        print(f"Moving Hyperliquid token_chains to new GT...")
        result = await session.execute(text("""
            UPDATE token_chains
            SET global_token_id = :new_gt_id
            WHERE token_id = :token_id
        """), {'new_gt_id': new_gt_id, 'token_id': HYPERLIQUID_TOKEN_ID})
        print(f"  Updated {result.rowcount} token_chains")
        
        # 3. Move Hyperliquid global_token_sources to new GT
        print(f"Moving Hyperliquid global_token_sources to new GT...")
        result = await session.execute(text("""
            UPDATE global_token_sources
            SET global_token_id = :new_gt_id
            WHERE global_token_id = :old_gt_id
              AND source_id IN (
                  SELECT source_id FROM tokens WHERE id = :token_id
              )
        """), {
            'new_gt_id': new_gt_id,
            'old_gt_id': CONTAMINATED_GT_ID,
            'token_id': HYPERLIQUID_TOKEN_ID
        })
        print(f"  Updated {result.rowcount} global_token_sources")
        
        # 4. Move Hyperliquid token_prices to new GT
        print(f"Moving Hyperliquid token_prices to new GT...")
        result = await session.execute(text("""
            UPDATE token_prices tp
            SET global_token_id = :new_gt_id
            FROM global_token_sources gts
            WHERE tp.global_token_source_id = gts.id
              AND gts.global_token_id = :new_gt_id
        """), {'new_gt_id': new_gt_id})
        print(f"  Updated {result.rowcount} token_prices")
        
        await session.commit()
        print("\n✅ Split completed successfully!")
        print(f"  Old GT (MEXC SIX): {CONTAMINATED_GT_ID}")
        print(f"  New GT (Hyperliquid SIX): {new_gt_id}")


if __name__ == '__main__':
    asyncio.run(main())
