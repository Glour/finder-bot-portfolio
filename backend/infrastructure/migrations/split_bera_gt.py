#!/usr/bin/env python3
"""
Split BERA global_token 9554b8da into SPOT and FUTURES.
"""

import asyncio
import sys
from uuid import uuid4

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


CONTAMINATED_GT_ID = '9554b8da-408a-45a2-b1b8-588134a0d872'


async def main():
    # Initialize database
    db_manager.init_database(settings)
    
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # 1. Create new GT for FUTURES tokens
        new_futures_gt_id = uuid4()
        print(f"Creating new global_token {new_futures_gt_id} for BERA FUTURES...")
        
        await session.execute(text("""
            INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
            SELECT :new_id, symbol, NOW(), NOW(), false
            FROM global_tokens WHERE id = :old_id
        """), {'new_id': new_futures_gt_id, 'old_id': CONTAMINATED_GT_ID})
        
        # 2. Move FUTURES token_chains to new GT
        print(f"Moving FUTURES token_chains to new GT...")
        result = await session.execute(text("""
            UPDATE token_chains tc
            SET global_token_id = :new_gt_id
            FROM tokens t
            WHERE tc.token_id = t.id
              AND tc.global_token_id = :old_gt_id
              AND t.market_type = 'FUTURES'
        """), {'new_gt_id': new_futures_gt_id, 'old_gt_id': CONTAMINATED_GT_ID})
        print(f"  Updated {result.rowcount} token_chains")
        
        # 3. Get FUTURES source IDs
        futures_sources = await session.execute(text("""
            SELECT DISTINCT t.source_id
            FROM tokens t
            JOIN token_chains tc ON t.id = tc.token_id
            WHERE tc.global_token_id = :gt_id
        """), {'gt_id': new_futures_gt_id})
        
        futures_source_ids = [row[0] for row in futures_sources]
        print(f"  Found {len(futures_source_ids)} FUTURES sources")
        
        # 4. Create global_token_sources for FUTURES GT
        for source_id in futures_source_ids:
            await session.execute(text("""
                INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                VALUES (gen_random_uuid(), :gt_id, :source_id, NOW(), NOW())
                ON CONFLICT (global_token_id, source_id) DO NOTHING
            """), {'gt_id': new_futures_gt_id, 'source_id': source_id})
        
        print(f"  Created global_token_sources for FUTURES")
        
        # 5. Move FUTURES token_prices to new GT
        print(f"Moving FUTURES token_prices to new GT...")
        result = await session.execute(text("""
            UPDATE token_prices tp
            SET global_token_id = :new_gt_id
            FROM global_token_sources gts
            WHERE tp.global_token_source_id = gts.id
              AND gts.global_token_id = :new_gt_id
        """), {'new_gt_id': new_futures_gt_id})
        print(f"  Updated {result.rowcount} token_prices")
        
        # 6. Remove FUTURES sources from old GT
        print(f"Removing FUTURES sources from old GT...")
        result = await session.execute(text("""
            DELETE FROM global_token_sources
            WHERE global_token_id = :old_gt_id
              AND source_id = ANY(:source_ids)
        """), {'old_gt_id': CONTAMINATED_GT_ID, 'source_ids': futures_source_ids})
        print(f"  Deleted {result.rowcount} global_token_sources")
        
        await session.commit()
        print("\n✅ Split completed successfully!")
        print(f"  Old GT (BERA SPOT): {CONTAMINATED_GT_ID}")
        print(f"  New GT (BERA FUTURES): {new_futures_gt_id}")


if __name__ == '__main__':
    asyncio.run(main())
