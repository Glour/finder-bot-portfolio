#!/usr/bin/env python3
"""
Final migration: split global_tokens by base_asset.

After cleanup script added _FUTURES suffix to base_asset,
many GTs now contain tokens with different base_assets.
Need to split them.
"""

import asyncio
import sys
from uuid import uuid4

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


async def main():
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # Find all GTs with multiple base_assets
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol,
                COUNT(DISTINCT t.base_asset) as base_asset_count,
                ARRAY_AGG(DISTINCT t.base_asset ORDER BY t.base_asset) as base_assets
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN tokens t ON tc.token_id = t.id
            GROUP BY gt.id, gt.symbol
            HAVING COUNT(DISTINCT t.base_asset) > 1
            ORDER BY gt.symbol
        """))
        
        contaminated_gts = list(result)
        print(f"Found {len(contaminated_gts)} GTs with multiple base_assets")
        print()
        
        total_splits = 0
        
        for i, (gt_id, symbol, base_asset_count, base_assets) in enumerate(contaminated_gts, 1):
            # Get groups by base_asset
            groups_result = await session.execute(text("""
                SELECT 
                    t.base_asset,
                    ARRAY_AGG(tc.id) as tc_ids,
                    ARRAY_AGG(DISTINCT t.source_id) as source_ids
                FROM token_chains tc
                JOIN tokens t ON tc.token_id = t.id
                WHERE tc.global_token_id = :gt_id
                GROUP BY t.base_asset
                ORDER BY t.base_asset
            """), {'gt_id': gt_id})
            
            groups = list(groups_result)
            
            # First group keeps old GT, rest get new GTs
            for j, (base_asset, tc_ids, source_ids) in enumerate(groups):
                if j == 0:
                    # Keep first group in old GT
                    continue
                
                # Create new GT for this base_asset
                new_gt_id = uuid4()
                
                # Symbol for new GT
                if base_asset.endswith('_FUTURES'):
                    new_symbol = base_asset  # Use base_asset as symbol for FUTURES
                else:
                    new_symbol = symbol
                
                # Create new GT
                await session.execute(text("""
                    INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
                    VALUES (:new_id, :symbol, NOW(), NOW(), false)
                """), {'new_id': new_gt_id, 'symbol': new_symbol})
                
                # Move token_chains
                await session.execute(text("""
                    UPDATE token_chains
                    SET global_token_id = :new_gt_id
                    WHERE id = ANY(:tc_ids)
                """), {'new_gt_id': new_gt_id, 'tc_ids': tc_ids})
                
                # Create global_token_sources
                for source_id in source_ids:
                    await session.execute(text("""
                        INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                        VALUES (gen_random_uuid(), :gt_id, :source_id, NOW(), NOW())
                        ON CONFLICT (global_token_id, source_id) DO NOTHING
                    """), {'gt_id': new_gt_id, 'source_id': source_id})
                
                # Move token_prices
                await session.execute(text("""
                    UPDATE token_prices tp
                    SET global_token_id = :new_gt_id
                    FROM global_token_sources gts
                    WHERE tp.global_token_source_id = gts.id
                      AND gts.global_token_id = :new_gt_id
                """), {'new_gt_id': new_gt_id})
                
                # Remove these sources from old GT
                await session.execute(text("""
                    DELETE FROM global_token_sources
                    WHERE global_token_id = :old_gt_id
                      AND source_id = ANY(:source_ids)
                """), {'old_gt_id': gt_id, 'source_ids': list(source_ids)})
                
                total_splits += 1
            
            if i % 50 == 0:
                print(f"Processed {i}/{len(contaminated_gts)} GTs, created {total_splits} new GTs")
                await session.commit()
        
        await session.commit()
        
        print()
        print("✅ Migration completed!")
        print(f"  Processed: {len(contaminated_gts)} contaminated GTs")
        print(f"  Created: {total_splits} new GTs")


if __name__ == '__main__':
    asyncio.run(main())
