#!/usr/bin/env python3
"""
Split contaminated "4" global_token into 4 separate tokens:
- 4 SPOT
- 4 FUTURES  
- 114514 SPOT
- 114514 FUTURES
"""

import asyncio
import sys
from uuid import uuid4

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


CONTAMINATED_GT_ID = '3420b14f-4ec7-4bfe-8d98-3be13b5b1997'


async def main():
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # Get all token IDs grouped by base_asset and market_type
        result = await session.execute(text("""
            SELECT 
                t.base_asset,
                t.market_type,
                ARRAY_AGG(t.id) as token_ids,
                ARRAY_AGG(DISTINCT t.source_id) as source_ids
            FROM tokens t
            JOIN token_chains tc ON t.id = tc.token_id
            WHERE tc.global_token_id = :gt_id
            GROUP BY t.base_asset, t.market_type
            ORDER BY t.base_asset, t.market_type
        """), {'gt_id': CONTAMINATED_GT_ID})
        
        groups = list(result)
        print(f"Found {len(groups)} groups to split:")
        for base_asset, market_type, token_ids, source_ids in groups:
            print(f"  {base_asset} {market_type}: {len(token_ids)} tokens, {len(source_ids)} sources")
        
        # First group keeps the old GT, rest get new GTs
        new_gts = {}
        for i, (base_asset, market_type, token_ids, source_ids) in enumerate(groups):
            if i == 0:
                # First group keeps old GT
                gt_id = CONTAMINATED_GT_ID
                print(f"\nKeeping group {base_asset} {market_type} in old GT {gt_id}")
            else:
                # Create new GT for this group
                gt_id = uuid4()
                print(f"\nCreating new GT {gt_id} for {base_asset} {market_type}...")
                
                # Get symbol from first token in group
                symbol_result = await session.execute(text("""
                    SELECT symbol FROM global_tokens WHERE id = :old_id
                """), {'old_id': CONTAMINATED_GT_ID})
                symbol = symbol_result.scalar()
                
                # Decide symbol for new GT
                if base_asset == "114514":
                    new_symbol = "114514"
                else:
                    new_symbol = symbol
                
                await session.execute(text("""
                    INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
                    VALUES (:new_id, :symbol, NOW(), NOW(), false)
                """), {'new_id': gt_id, 'symbol': new_symbol})
                
                # Move token_chains
                await session.execute(text("""
                    UPDATE token_chains
                    SET global_token_id = :new_gt_id
                    WHERE token_id = ANY(:token_ids)
                """), {'new_gt_id': gt_id, 'token_ids': token_ids})
                
                # Create global_token_sources
                for source_id in source_ids:
                    await session.execute(text("""
                        INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                        VALUES (gen_random_uuid(), :gt_id, :source_id, NOW(), NOW())
                        ON CONFLICT (global_token_id, source_id) DO NOTHING
                    """), {'gt_id': gt_id, 'source_id': source_id})
                
                # Move token_prices
                await session.execute(text("""
                    UPDATE token_prices tp
                    SET global_token_id = :new_gt_id
                    FROM global_token_sources gts
                    WHERE tp.global_token_source_id = gts.id
                      AND gts.global_token_id = :new_gt_id
                """), {'new_gt_id': gt_id})
                
                # Remove these sources from old GT
                await session.execute(text("""
                    DELETE FROM global_token_sources
                    WHERE global_token_id = :old_gt_id
                      AND source_id = ANY(:source_ids)
                """), {'old_gt_id': CONTAMINATED_GT_ID, 'source_ids': list(source_ids)})
                
                print(f"  Moved {len(token_ids)} tokens, created {len(source_ids)} sources")
            
            new_gts[f"{base_asset}_{market_type}"] = gt_id
        
        # Update old GT symbol if needed (should be "4" not "114514")
        await session.execute(text("""
            UPDATE global_tokens
            SET symbol = '4'
            WHERE id = :gt_id
        """), {'gt_id': CONTAMINATED_GT_ID})
        
        await session.commit()
        print("\n✅ Split completed successfully!")
        for key, gt_id in new_gts.items():
            print(f"  {key}: {gt_id}")


if __name__ == '__main__':
    asyncio.run(main())
