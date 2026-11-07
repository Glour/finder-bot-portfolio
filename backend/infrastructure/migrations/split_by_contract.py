#!/usr/bin/env python3
"""
Split global_tokens that contain tokens with different contract addresses on same chain.
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
        # Find GTs with multiple contracts on same chain
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol,
                c.name as chain_name,
                COUNT(DISTINCT tc.contract_address) as contract_count,
                ARRAY_AGG(DISTINCT tc.contract_address ORDER BY tc.contract_address) as contracts
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN chains c ON tc.chain_id = c.id
            WHERE tc.contract_address IS NOT NULL
              AND tc.contract_address != ''
            GROUP BY gt.id, gt.symbol, c.id, c.name
            HAVING COUNT(DISTINCT tc.contract_address) > 1
            ORDER BY gt.symbol, c.name
        """))
        
        conflicts = list(result)
        print(f"Found {len(conflicts)} GTs with contract conflicts")
        print()
        
        if len(conflicts) == 0:
            print("✅ No contract conflicts found!")
            return
        
        # Show first 10 examples
        print("Examples:")
        for gt_id, symbol, chain, count, contracts in conflicts[:10]:
            print(f"  - {symbol} on {chain}: {count} contracts")
        print()
        
        # Group by gt_id (one GT may have conflicts on multiple chains)
        gts_to_split = {}
        for gt_id, symbol, chain, count, contracts in conflicts:
            if gt_id not in gts_to_split:
                gts_to_split[gt_id] = {
                    'symbol': symbol,
                    'chains': {}
                }
            gts_to_split[gt_id]['chains'][chain] = contracts
        
        print(f"Total GTs to split: {len(gts_to_split)}")
        print()
        
        total_splits = 0
        
        for i, (gt_id, info) in enumerate(gts_to_split.items(), 1):
            symbol = info['symbol']
            
            # For each conflicting chain, split by contract
            for chain_name, contracts in info['chains'].items():
                # Get chain_id
                chain_result = await session.execute(text("""
                    SELECT id FROM chains WHERE name = :chain_name
                """), {'chain_name': chain_name})
                chain_id = chain_result.scalar()
                
                # First contract stays in old GT, rest get new GTs
                for j, contract in enumerate(contracts):
                    if j == 0:
                        continue  # Keep first contract in old GT
                    
                    # Create new GT for this contract
                    new_gt_id = uuid4()
                    new_symbol = f"{symbol}_{j+1}"  # Add suffix to differentiate
                    
                    await session.execute(text("""
                        INSERT INTO global_tokens (id, symbol, created_at, updated_at, is_deleted)
                        VALUES (:new_id, :symbol, NOW(), NOW(), false)
                    """), {'new_id': new_gt_id, 'symbol': new_symbol})
                    
                    # Move token_chains with this contract on this chain
                    await session.execute(text("""
                        UPDATE token_chains
                        SET global_token_id = :new_gt_id
                        WHERE global_token_id = :old_gt_id
                          AND chain_id = :chain_id
                          AND contract_address = :contract
                    """), {
                        'new_gt_id': new_gt_id,
                        'old_gt_id': gt_id,
                        'chain_id': chain_id,
                        'contract': contract
                    })
                    
                    # Get source IDs for moved tokens
                    sources_result = await session.execute(text("""
                        SELECT DISTINCT t.source_id
                        FROM tokens t
                        JOIN token_chains tc ON t.id = tc.token_id
                        WHERE tc.global_token_id = :gt_id
                    """), {'gt_id': new_gt_id})
                    
                    source_ids = [row[0] for row in sources_result]
                    
                    if source_ids:
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
                        """), {'old_gt_id': gt_id, 'source_ids': source_ids})
                    
                    total_splits += 1
            
            if i % 50 == 0:
                print(f"Processed {i}/{len(gts_to_split)} GTs, created {total_splits} new GTs")
                await session.commit()
        
        await session.commit()
        
        print()
        print("✅ Contract conflict migration completed!")
        print(f"  Processed: {len(gts_to_split)} contaminated GTs")
        print(f"  Created: {total_splits} new GTs")


if __name__ == '__main__':
    asyncio.run(main())
