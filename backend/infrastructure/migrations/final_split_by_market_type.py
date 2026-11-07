#!/usr/bin/env python3
"""
FINAL MIGRATION: Split all contaminated GTs by market_type directly.

This is the definitive solution - split SPOT and FUTURES regardless of base_asset.
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
        # Find ALL contaminated GTs
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN tokens t ON tc.token_id = t.id
            GROUP BY gt.id, gt.symbol
            HAVING 
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) > 0
                AND COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) > 0
        """))
        
        contaminated = list(result)
        print(f"Found {len(contaminated)} contaminated GTs")
        print()
        
        total_splits = 0
        
        for i, (gt_id, symbol) in enumerate(contaminated, 1):
            # Create new GT for FUTURES
            new_futures_gt_id = uuid4()
            
            await session.execute(text("""
                INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
                VALUES (:new_id, :symbol, NOW(), NOW(), false)
            """), {'new_id': new_futures_gt_id, 'symbol': f"{symbol}_FUTURES"})
            
            # Move ALL FUTURES token_chains to new GT
            await session.execute(text("""
                UPDATE token_chains tc
                SET global_token_id = :new_gt_id
                FROM tokens t
                WHERE tc.token_id = t.id
                  AND tc.global_token_id = :old_gt_id
                  AND t.market_type = 'FUTURES'
            """), {'new_gt_id': new_futures_gt_id, 'old_gt_id': gt_id})
            
            # Get FUTURES source IDs
            sources_result = await session.execute(text("""
                SELECT DISTINCT t.source_id
                FROM tokens t
                JOIN token_chains tc ON t.id = tc.token_id
                WHERE tc.global_token_id = :gt_id
            """), {'gt_id': new_futures_gt_id})
            
            futures_source_ids = [row[0] for row in sources_result]
            
            if futures_source_ids:
                # Create global_token_sources for FUTURES GT
                for source_id in futures_source_ids:
                    await session.execute(text("""
                        INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                        VALUES (gen_random_uuid(), :gt_id, :source_id, NOW(), NOW())
                        ON CONFLICT (global_token_id, source_id) DO NOTHING
                    """), {'gt_id': new_futures_gt_id, 'source_id': source_id})
                
                # Move token_prices
                await session.execute(text("""
                    UPDATE token_prices tp
                    SET global_token_id = :new_gt_id
                    FROM global_token_sources gts
                    WHERE tp.global_token_source_id = gts.id
                      AND gts.global_token_id = :new_gt_id
                """), {'new_gt_id': new_futures_gt_id})
                
                # Remove FUTURES sources from old GT
                await session.execute(text("""
                    DELETE FROM global_token_sources
                    WHERE global_token_id = :old_gt_id
                      AND source_id = ANY(:source_ids)
                """), {'old_gt_id': gt_id, 'source_ids': futures_source_ids})
            
            total_splits += 1
            
            if i % 50 == 0:
                print(f"Processed {i}/{len(contaminated)} GTs")
                await session.commit()
        
        await session.commit()
        
        print()
        print("✅ FINAL migration completed!")
        print(f"  Split: {len(contaminated)} contaminated GTs")
        print(f"  Created: {total_splits} FUTURES GTs")


if __name__ == '__main__':
    asyncio.run(main())
