#!/usr/bin/env python3
"""
Find and blacklist problematic pairs using SQL queries.
"""
import asyncio
import sys
sys.path.insert(0, '/app')

from sqlalchemy import text
from infrastructure.database import setup as db_setup
from infrastructure.database.requests import get_database_repo
from settings import settings


async def main():
    # Initialize database
    db_setup.init_database(settings)
    repo = get_database_repo()
    
    # Define problematic pairs
    problems = [
        # (symbol_a, source_a, market_a, symbol_b, source_b, market_b, reason)
        ("BMT", "XT", "FUTURES", "BMT", "XT", "SPOT", "XT BMT: FUTURES vs SPOT"),
        ("MSTR", "XT", "SPOT", "MSTR", "XT", "FUTURES", "XT MSTR: SPOT vs FUTURES"),
        ("GNC", "GATE", "SPOT", "GNC", "XT", "SPOT", "GNC: XT wrong network"),
        ("RYO", "ASCENDEX", "SPOT", "RYO", "XT", "SPOT", "RYO: AscendEX C0BAN vs XT ETH"),
        ("RYO", "ASCENDEX", "SPOT", "RYO", "MEXC", "SPOT", "RYO: AscendEX C0BAN vs MEXC ETH"),
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "XT", "FUTURES", "DOGE vs DRIFT"),
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "BITMART", "FUTURES", "DOGE vs DRIFT"),
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "BITMART", "SPOT", "DOGE vs DRIFT"),
        ("RVN", "ASCENDEX", "SPOT", "RVN", "MEXC", "SPOT", "MINDFAK vs RVN"),
        ("RVN", "ASCENDEX", "SPOT", "RVN", "GATE", "SPOT", "MINDFAK vs RVN"),
        ("RVN", "ASCENDEX", "SPOT", "RVN", "XT", "SPOT", "MINDFAK vs RVN"),
        ("S", "ASCENDEX", "SPOT", "S", "XT", "SPOT", "S: SUI vs SONIC"),
        ("S", "ASCENDEX", "SPOT", "S", "GATE", "SPOT", "S: SUI vs SONIC"),
        ("S", "ASCENDEX", "SPOT", "S", "MEXC", "SPOT", "S: SUI vs SONIC"),
        ("BABYBONK", "ASCENDEX", "SPOT", "BABYBONK", "MEXC", "SPOT", "BABYBONK wrong contract"),
    ]
    
    added = 0
    errors = []
    
    async with repo.sessionmaker() as session:
        for symbol_a, source_a, market_a, symbol_b, source_b, market_b, reason in problems:
            try:
                # Find GT ID for A
                query_a = text("""
                    SELECT DISTINCT gt.id
                    FROM global_tokens gt
                    JOIN token_chains tc ON tc.global_token_id = gt.id
                    JOIN tokens t ON t.id = tc.token_id
                    JOIN sources s ON s.id = t.source_id
                    WHERE gt.symbol = :symbol
                      AND s.name = :source
                      AND t.market_type = :market_type
                    LIMIT 1
                """)
                
                result_a = await session.execute(
                    query_a,
                    {"symbol": symbol_a, "source": source_a, "market_type": market_a}
                )
                gt_a_id = result_a.scalar()
                
                if not gt_a_id:
                    errors.append(f"Not found: {symbol_a} {source_a} {market_a}")
                    print(f"❌ Not found: {symbol_a} {source_a} {market_a}")
                    continue
                
                # Find GT ID for B
                result_b = await session.execute(
                    query_a,  # Same query
                    {"symbol": symbol_b, "source": source_b, "market_type": market_b}
                )
                gt_b_id = result_b.scalar()
                
                if not gt_b_id:
                    errors.append(f"Not found: {symbol_b} {source_b} {market_b}")
                    print(f"❌ Not found: {symbol_b} {source_b} {market_b}")
                    continue
                
                # Add to blacklist
                result = await repo.global_token_pair_blacklist.add_pair(
                    gt_a_id,
                    gt_b_id,
                    reason=reason,
                    created_by="fix_vlad_pairs_sql"
                )
                
                if result:
                    added += 1
                    print(f"✅ Added: {reason}")
                else:
                    print(f"⚠️  Exists: {reason}")
                    
            except Exception as e:
                errors.append(f"Error: {reason} - {e}")
                print(f"❌ Error: {reason} - {e}")
    
    print(f"\n{'='*60}")
    print(f"Summary: Added {added}/{len(problems)} pairs")
    if errors:
        print(f"Errors: {len(errors)}")
        for err in errors:
            print(f"  {err}")


if __name__ == "__main__":
    asyncio.run(main())
