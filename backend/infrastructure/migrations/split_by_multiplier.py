#!/usr/bin/env python3
"""
Split global_tokens that contain tokens with different contract sizes (multipliers).
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
        # Find GTs with multiple contract sizes
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol,
                COUNT(DISTINCT COALESCE(t.contract_size, 1)) as multiplier_count,
                ARRAY_AGG(DISTINCT COALESCE(t.contract_size, 1) ORDER BY COALESCE(t.contract_size, 1)) as multipliers
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN tokens t ON tc.token_id = t.id
            GROUP BY gt.id, gt.symbol
            HAVING COUNT(DISTINCT COALESCE(t.contract_size, 1)) > 1
            ORDER BY gt.symbol
        """))
        
        contaminated_gts = list(result)
        print(f"Found {len(contaminated_gts)} GTs with multiple multipliers")
        print()
        
        if len(contaminated_gts) == 0:
            print("✅ No multiplier conflicts found!")
            return
        
        # Show first 10 examples
        print("Examples:")
        for gt_id, symbol, count, multipliers in contaminated_gts[:10]:
            print(f"  - {symbol}: {multipliers}")
        print()
        
        total_splits = 0
        
        for i, (gt_id, symbol, count, multipliers) in enumerate(contaminated_gts, 1):
            # Get groups by contract_size
            groups_result = await session.execute(text("""
                SELECT 
                    COALESCE(t.contract_size, 1) as multiplier,
                    ARRAY_AGG(tc.id) as tc_ids,
                    ARRAY_AGG(DISTINCT t.source_id) as source_ids,
                    COUNT(*) as token_count
                FROM token_chains tc
                JOIN tokens t ON tc.token_id = t.id
                WHERE tc.global_token_id = :gt_id
                GROUP BY COALESCE(t.contract_size, 1)
                ORDER BY multiplier
            """), {'gt_id': gt_id})
            
            groups = list(groups_result)
            
            # First group (smallest multiplier) keeps old GT
            for j, (multiplier, tc_ids, source_ids, token_count) in enumerate(groups):
                if j == 0:
                    continue  # Keep first group in old GT
                
                # Create new GT for this multiplier
                new_gt_id = uuid4()
                
                # Symbol for new GT (add multiplier suffix if > 1)
                if multiplier > 1:
                    # Try to extract clean ticker
                    if symbol.endswith('_FUTURES'):
                        base = symbol[:-8]  # Remove _FUTURES
                        new_symbol = f"{int(multiplier)}{base}_FUTURES"
                    else:
                        new_symbol = f"{int(multiplier)}{symbol}"
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
        print("✅ Multiplier migration completed!")
        print(f"  Processed: {len(contaminated_gts)} contaminated GTs")
        print(f"  Created: {total_splits} new GTs")


if __name__ == '__main__':
    asyncio.run(main())
