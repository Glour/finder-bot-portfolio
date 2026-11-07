#!/usr/bin/env python3
"""
Universal migration: split ALL contaminated global_tokens into separate SPOT and FUTURES.
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
        # Find all contaminated GTs (containing both SPOT and FUTURES)
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol,
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) as spot_count,
                COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) as futures_count,
                COUNT(DISTINCT t.base_asset) as base_assets_count
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN tokens t ON tc.token_id = t.id
            GROUP BY gt.id, gt.symbol
            HAVING 
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) > 0
                AND COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) > 0
            ORDER BY gt.symbol
        """))
        
        contaminated_gts = list(result)
        print(f"Found {len(contaminated_gts)} contaminated global_tokens")
        print(f"Processing in batches...\n")
        
        total_splits = 0
        errors = []
        
        for i, (gt_id, symbol, spot_count, futures_count, base_assets_count) in enumerate(contaminated_gts, 1):
            try:
                # Get groups by (base_asset, market_type)
                groups_result = await session.execute(text("""
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
                """), {'gt_id': gt_id})
                
                groups = list(groups_result)
                
                if len(groups) <= 1:
                    # Only one group - shouldn't happen, skip
                    continue
                
                # First group keeps old GT, rest get new GTs
                for j, (base_asset, market_type, token_ids, source_ids) in enumerate(groups):
                    if j == 0:
                        # Keep first group in old GT
                        continue
                    
                    # Create new GT for this group
                    new_gt_id = uuid4()
                    
                    # Determine symbol for new GT
                    if base_asset != symbol and base_asset:
                        new_symbol = base_asset
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
                        WHERE token_id = ANY(:token_ids)
                    """), {'new_gt_id': new_gt_id, 'token_ids': token_ids})
                    
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
                
                total_splits += (len(groups) - 1)
                
                if i % 50 == 0:
                    print(f"Processed {i}/{len(contaminated_gts)} GTs, created {total_splits} new GTs")
                    await session.commit()
            
            except Exception as e:
                errors.append((symbol, gt_id, str(e)))
                print(f"ERROR processing {symbol} ({gt_id}): {e}")
                continue
        
        await session.commit()
        
        print(f"\n✅ Migration completed!")
        print(f"  Processed: {len(contaminated_gts)} contaminated GTs")
        print(f"  Created: {total_splits} new GTs")
        print(f"  Errors: {len(errors)}")
        
        if errors:
            print("\nErrors:")
            for symbol, gt_id, error in errors[:10]:
                print(f"  {symbol} ({gt_id}): {error}")


if __name__ == '__main__':
    asyncio.run(main())
