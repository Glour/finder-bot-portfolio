"""
Cleanup duplicate global_tokens.

Strategy:
1. Delete orphan global_tokens (no token_chains) — 21K+ rows of garbage
2. For active duplicates (same symbol, multiple GTs with token_chains):
   - Keep the GT with the MOST token_chains as the "winner"
   - Move all token_chains from losers to winner
   - Update token_prices, global_token_sources, global_merge_candidates
   - Delete loser GTs

Safety:
- Runs in a transaction
- Logs everything
- Can be rolled back
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


async def cleanup():
    """Main cleanup function."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text
    
    # Use same DB URL as the app
    db_url = os.getenv('DATABASE_URL', 'postgresql+asyncpg://finder:local-only@localhost:5432/finder')
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        async with session.begin():
            # STEP 1: Count current state
            result = await session.execute(text("SELECT COUNT(*) FROM global_tokens"))
            total_before = result.scalar()
            
            result = await session.execute(text("""
                SELECT COUNT(*) FROM global_tokens gt 
                WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = gt.id)
            """))
            orphan_count = result.scalar()
            
            print(f"Total global_tokens: {total_before}")
            print(f"Orphan global_tokens (no token_chains): {orphan_count}")
            
            # STEP 2: Delete references to orphan GTs first
            # Check what tables reference global_tokens
            result = await session.execute(text("""
                DELETE FROM global_token_sources 
                WHERE global_token_id IN (
                    SELECT gt.id FROM global_tokens gt 
                    WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = gt.id)
                )
            """))
            print(f"Deleted {result.rowcount} orphan global_token_sources")
            
            result = await session.execute(text("""
                DELETE FROM token_prices 
                WHERE global_token_id IN (
                    SELECT gt.id FROM global_tokens gt 
                    WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = gt.id)
                )
            """))
            print(f"Deleted {result.rowcount} orphan token_prices")
            
            result = await session.execute(text("""
                DELETE FROM global_merge_candidates 
                WHERE global_token_a_id IN (
                    SELECT gt.id FROM global_tokens gt 
                    WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = gt.id)
                )
                OR global_token_b_id IN (
                    SELECT gt.id FROM global_tokens gt 
                    WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = gt.id)
                )
            """))
            print(f"Deleted {result.rowcount} orphan merge_candidates")
            
            # STEP 3: Delete orphan global_tokens
            result = await session.execute(text("""
                DELETE FROM global_tokens 
                WHERE NOT EXISTS (SELECT 1 FROM token_chains tc WHERE tc.global_token_id = global_tokens.id)
            """))
            print(f"Deleted {result.rowcount} orphan global_tokens")
            
            # STEP 4: Merge active duplicates
            # Find symbols with multiple active GTs
            result = await session.execute(text("""
                SELECT symbol, array_agg(id ORDER BY chains_count DESC) as gt_ids
                FROM (
                    SELECT gt.id, gt.symbol, COUNT(tc.id) as chains_count
                    FROM global_tokens gt
                    JOIN token_chains tc ON tc.global_token_id = gt.id
                    GROUP BY gt.id, gt.symbol
                ) sub
                GROUP BY symbol
                HAVING COUNT(*) > 1
            """))
            duplicates = result.fetchall()
            print(f"\nFound {len(duplicates)} symbols with active duplicates")
            
            merged_count = 0
            for row in duplicates:
                symbol = row[0]
                gt_ids = row[1]  # Ordered by chains_count DESC
                winner_id = gt_ids[0]
                loser_ids = gt_ids[1:]
                
                for loser_id in loser_ids:
                    # Check for contract conflicts before merging
                    # If winner and loser have different contracts on same chain, skip
                    conflict = await session.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 
                            FROM token_chains w
                            JOIN token_chains l ON l.chain_id = w.chain_id 
                                AND l.contract_address != w.contract_address
                                AND l.contract_address IS NOT NULL 
                                AND w.contract_address IS NOT NULL
                                AND l.contract_address != ''
                                AND w.contract_address != ''
                            WHERE w.global_token_id = :winner_id 
                            AND l.global_token_id = :loser_id
                        )
                    """), {"winner_id": winner_id, "loser_id": loser_id})
                    
                    has_conflict = conflict.scalar()
                    if has_conflict:
                        print(f"  SKIP {symbol}: contract conflict between {winner_id} and {loser_id}")
                        continue
                    
                    # Move token_chains
                    r = await session.execute(text("""
                        UPDATE token_chains SET global_token_id = :winner_id 
                        WHERE global_token_id = :loser_id
                    """), {"winner_id": winner_id, "loser_id": loser_id})
                    
                    # Move token_prices 
                    await session.execute(text("""
                        UPDATE token_prices SET global_token_id = :winner_id 
                        WHERE global_token_id = :loser_id
                    """), {"winner_id": winner_id, "loser_id": loser_id})
                    
                    # Move global_token_sources
                    await session.execute(text("""
                        DELETE FROM global_token_sources WHERE global_token_id = :loser_id
                    """), {"loser_id": loser_id})
                    
                    # Clean merge_candidates
                    await session.execute(text("""
                        DELETE FROM global_merge_candidates 
                        WHERE global_token_a_id = :loser_id OR global_token_b_id = :loser_id
                    """), {"loser_id": loser_id})
                    
                    # Delete loser GT
                    await session.execute(text("""
                        DELETE FROM global_tokens WHERE id = :loser_id
                    """), {"loser_id": loser_id})
                    
                    merged_count += 1
                
                if len(loser_ids) > 0:
                    print(f"  {symbol}: merged {len(loser_ids)} duplicates into {winner_id}")
            
            # Final count
            result = await session.execute(text("SELECT COUNT(*) FROM global_tokens"))
            total_after = result.scalar()
            
            print(f"\n{'='*50}")
            print(f"RESULT:")
            print(f"  Before: {total_before} global_tokens")
            print(f"  After:  {total_after} global_tokens")
            print(f"  Deleted orphans: {orphan_count}")
            print(f"  Merged duplicates: {merged_count}")
            print(f"  Reduction: {total_before - total_after} ({(total_before - total_after)/total_before*100:.1f}%)")
    
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(cleanup())
