#!/usr/bin/env python3
"""
Find and blacklist problematic pairs from Vlad's list.
"""
import asyncio
import sys
sys.path.insert(0, '/app')

from infrastructure.database import setup as db_setup
from infrastructure.database.requests import get_database_repo
from settings import settings


async def main():
    # Initialize database
    db_setup.init_database(settings)
    
    repo = get_database_repo()
    
    # Define problematic pairs by symbol + source + market_type
    problems = [
        # XT SPOT vs FUTURES
        ("BMT", "XT", "FUTURES", "BMT", "XT", "SPOT", "XT BMT: FUTURES vs SPOT - different tokens"),
        ("MSTR", "XT", "SPOT", "MSTR", "XT", "FUTURES", "XT MSTR: SPOT vs FUTURES - different tokens"),
        ("GNC", "GATE", "SPOT", "GNC", "XT", "SPOT", "GNC: XT wrong network/contract"),
        
        # RYO AscendEX issues
        ("RYO", "ASCENDEX", "SPOT", "RYO", "XT", "SPOT", "RYO: AscendEX wrong network (C0BAN vs ETH)"),
        ("RYO", "ASCENDEX", "SPOT", "RYO", "MEXC", "SPOT", "RYO: AscendEX wrong network (C0BAN vs ETH)"),
        
        # DOGE/DRIFT confusion
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "XT", "FUTURES", "DOGE/DRIFT: AscendEX DOGE vs XT DRIFT"),
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "BITMART", "FUTURES", "DOGE/DRIFT: AscendEX DOGE vs Bitmart DRIFT"),
        ("DOGE", "ASCENDEX", "FUTURES", "DRIFT", "BITMART", "SPOT", "DOGE/DRIFT: AscendEX DOGE vs Bitmart DRIFT"),
        
        # RVN/MINDFAK
        ("RVN", "ASCENDEX", "SPOT", "RVN", "MEXC", "SPOT", "RVN/MINDFAK: AscendEX MINDFAK vs MEXC RVN"),
        ("RVN", "ASCENDEX", "SPOT", "RVN", "GATE", "SPOT", "RVN/MINDFAK: AscendEX MINDFAK vs GATE RVN"),
        ("RVN", "ASCENDEX", "SPOT", "RVN", "XT", "SPOT", "RVN/MINDFAK: AscendEX MINDFAK vs XT RVN"),
        
        # S different networks
        ("S", "ASCENDEX", "SPOT", "S", "XT", "SPOT", "S: AscendEX SUI vs XT SONIC"),
        ("S", "ASCENDEX", "SPOT", "S", "GATE", "SPOT", "S: AscendEX SUI vs GATE SONIC"),
        ("S", "ASCENDEX", "SPOT", "S", "MEXC", "SPOT", "S: AscendEX SUI vs MEXC SONIC"),
        
        # BABYBONK
        ("BABYBONK", "ASCENDEX", "SPOT", "BABYBONK", "MEXC", "SPOT", "BABYBONK: AscendEX wrong contract"),
    ]
    
    added = []
    errors = []
    
    for symbol_a, source_a, market_a, symbol_b, source_b, market_b, reason in problems:
        try:
            # Find GT for A
            gt_a = await find_gt(repo, symbol_a, source_a, market_a)
            if not gt_a:
                errors.append(f"❌ Not found: {symbol_a} {source_a} {market_a}")
                continue
            
            # Find GT for B
            gt_b = await find_gt(repo, symbol_b, source_b, market_b)
            if not gt_b:
                errors.append(f"❌ Not found: {symbol_b} {source_b} {market_b}")
                continue
            
            # Add to blacklist
            result = await repo.global_token_pair_blacklist.add_pair(
                gt_a.id,
                gt_b.id,
                reason=reason,
                created_by="fix_vlad_pairs_script"
            )
            
            if result:
                added.append(f"✅ {symbol_a}({source_a},{market_a}) ❌ {symbol_b}({source_b},{market_b})")
                print(f"✅ Added: {reason}")
            else:
                print(f"⚠️  Already exists: {reason}")
        
        except Exception as e:
            errors.append(f"❌ Error: {reason} - {e}")
            print(f"❌ Error: {reason} - {e}")
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"Added: {len(added)}")
    print(f"Errors: {len(errors)}")
    
    if errors:
        print(f"\nErrors:")
        for err in errors:
            print(f"  {err}")


async def find_gt(repo, symbol, source_name, market_type):
    """Find global_token by symbol, source, and market_type."""
    # Get all GTs with this symbol (with metadata)
    all_gts = await repo.global_tokens.get_all_with_meta()
    global_tokens = [gt for gt in all_gts if gt.symbol.upper() == symbol.upper()]
    
    for gt in global_tokens:
        # Check if this GT has tokens from the specified source
        sources = [s.name.upper() for s in gt.sources] if gt.sources else []
        if source_name.upper() not in sources:
            continue
        
        # Check if this GT has the specified market_type
        market_types = set()
        for tc in gt.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types.add(tc.token.market_type.value)
        
        if market_type.upper() in market_types:
            return gt
    
    return None


if __name__ == "__main__":
    asyncio.run(main())
