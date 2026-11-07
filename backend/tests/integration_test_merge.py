#!/usr/bin/env python3
"""
Integration test for MergeGlobalToken - runs against real database.

This test:
1. Checks for contaminated global_tokens in DB
2. Validates that no SPOT/FUTURES mixing exists
3. Validates that chainless tokens with large price diffs are separated
4. Validates base_asset == base_coin_name for FUTURES tokens
"""

import asyncio
import sys
from typing import List, Dict, Set

# Set PYTHONPATH to the backend directory when running this script.

from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text


async def check_contaminated_global_tokens() -> List[dict]:
    """
    Check for global_tokens containing both SPOT and FUTURES tokens.
    
    Returns:
        List of contaminated GT info
    """
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        result = await session.execute(text("""
            SELECT 
                gt.id as gt_id,
                gt.symbol,
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) as spot_count,
                COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) as futures_count,
                STRING_AGG(DISTINCT s.name, ', ' ORDER BY s.name) as sources
            FROM global_tokens gt
            JOIN token_chains tc ON gt.id = tc.global_token_id
            JOIN tokens t ON tc.token_id = t.id
            JOIN sources s ON t.source_id = s.id
            GROUP BY gt.id, gt.symbol
            HAVING 
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) > 0
                AND COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) > 0
            ORDER BY gt.symbol
        """))
        
        return [dict(row._mapping) for row in result]


async def check_chainless_price_splits() -> List[dict]:
    """
    Check for chainless tokens with same symbol but different prices in same GT.
    
    Should find cases like MEXC SIX + Hyperliquid SIX if not properly split.
    """
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        result = await session.execute(text("""
            WITH chainless_prices AS (
                SELECT 
                    tc.global_token_id,
                    gt.symbol,
                    t.base_asset,
                    s.name as source_name,
                    tp.price_usd,
                    tc.contract_address,
                    c.name as chain_name
                FROM token_chains tc
                JOIN global_tokens gt ON tc.global_token_id = gt.id
                JOIN tokens t ON tc.token_id = t.id
                JOIN sources s ON t.source_id = s.id
                JOIN chains c ON tc.chain_id = c.id
                LEFT JOIN token_prices tp ON t.id = tp.token_id
                WHERE s.name IN ('mexc', 'hyperliquid', 'ascendex', 'asterdex', 'gmgn')
                  AND c.name IN ('hyperliquid', 'ascendex')  -- synthetic chains
                  AND tp.price_usd > 0
            )
            SELECT 
                global_token_id,
                symbol,
                base_asset,
                COUNT(DISTINCT source_name) as source_count,
                MAX(price_usd) as max_price,
                MIN(price_usd) as min_price,
                (MAX(price_usd) / NULLIF(MIN(price_usd), 0)) as price_ratio,
                STRING_AGG(DISTINCT source_name, ', ') as sources
            FROM chainless_prices
            GROUP BY global_token_id, symbol, base_asset
            HAVING COUNT(DISTINCT source_name) > 1
              AND (MAX(price_usd) / NULLIF(MIN(price_usd), 0)) > 10  -- 10x threshold
            ORDER BY price_ratio DESC
            LIMIT 20
        """))
        
        return [dict(row._mapping) for row in result]


async def check_base_asset_futures_sync() -> List[dict]:
    """
    Check that FUTURES tokens have _FUTURES suffix in base_asset.
    
    If base_asset doesn't match base_coin_name, it's a bug.
    """
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        result = await session.execute(text("""
            SELECT 
                t.id,
                t.symbol,
                t.base_asset,
                t.market_type,
                s.name as source_name,
                tc.global_token_id
            FROM tokens t
            JOIN sources s ON t.source_id = s.id
            LEFT JOIN token_chains tc ON t.id = tc.token_id
            WHERE t.market_type = 'FUTURES'
              AND t.base_asset NOT LIKE '%_FUTURES'
              AND t.base_asset NOT LIKE '%FUTURES'
              AND s.name NOT IN ('hyperliquid')  -- hyperliquid uses skip_futures_suffix
            LIMIT 50
        """))
        
        return [dict(row._mapping) for row in result]


async def check_source_specific_duplicates() -> List[dict]:
    """
    Check for duplicate source-specific global_tokens.
    
    Should not have both "BERA:hyperliquid" and "BERA_FUTURES:hyperliquid"
    pointing to same market_type.
    """
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        result = await session.execute(text("""
            WITH source_specific AS (
                SELECT 
                    gt.id,
                    gt.symbol,
                    CASE 
                        WHEN gt.symbol LIKE '%:%' THEN 
                            SPLIT_PART(gt.symbol, ':', 2)
                        ELSE NULL
                    END as source_suffix,
                    CASE
                        WHEN gt.symbol LIKE '%_FUTURES:%' THEN 'FUTURES'
                        WHEN gt.symbol LIKE '%:%' THEN 'SPOT'
                        ELSE 'UNKNOWN'
                    END as inferred_market_type
                FROM global_tokens gt
                WHERE gt.symbol LIKE '%:%'
            )
            SELECT 
                ss.symbol,
                ss.source_suffix,
                ss.inferred_market_type,
                COUNT(*) as count
            FROM source_specific ss
            WHERE ss.source_suffix IS NOT NULL
            GROUP BY ss.symbol, ss.source_suffix, ss.inferred_market_type
            HAVING COUNT(*) > 1
        """))
        
        return [dict(row._mapping) for row in result]


async def run_all_checks():
    """Run all integration tests and report results."""
    print("=" * 80)
    print("INTEGRATION TEST: MergeGlobalToken Validation")
    print("=" * 80)
    print()
    
    # Test 1: Contaminated global_tokens
    print("TEST 1: Checking for SPOT/FUTURES contamination...")
    contaminated = await check_contaminated_global_tokens()
    if contaminated:
        print(f"❌ FAIL: Found {len(contaminated)} contaminated global_tokens:")
        for gt in contaminated[:10]:
            print(f"  - {gt['symbol']} ({gt['gt_id'][:8]}...): "
                  f"{gt['spot_count']} SPOT + {gt['futures_count']} FUTURES "
                  f"from {gt['sources']}")
        if len(contaminated) > 10:
            print(f"  ... and {len(contaminated) - 10} more")
    else:
        print("✅ PASS: No contaminated global_tokens found")
    print()
    
    # Test 2: Chainless price splits
    print("TEST 2: Checking for chainless tokens with large price differences...")
    price_issues = await check_chainless_price_splits()
    if price_issues:
        print(f"❌ FAIL: Found {len(price_issues)} tokens with >10x price ratio:")
        for issue in price_issues[:10]:
            print(f"  - {issue['symbol']} ({issue['base_asset']}): "
                  f"${issue['min_price']:.6f} - ${issue['max_price']:.6f} "
                  f"(ratio: {issue['price_ratio']:.1f}x) "
                  f"from {issue['sources']}")
    else:
        print("✅ PASS: No chainless price issues found")
    print()
    
    # Test 3: base_asset synchronization
    print("TEST 3: Checking base_asset _FUTURES suffix synchronization...")
    sync_issues = await check_base_asset_futures_sync()
    if sync_issues:
        print(f"❌ FAIL: Found {len(sync_issues)} FUTURES tokens without _FUTURES suffix:")
        for issue in sync_issues[:10]:
            print(f"  - {issue['symbol']} ({issue['source_name']}): "
                  f"base_asset={issue['base_asset']} (should have _FUTURES)")
    else:
        print("✅ PASS: All FUTURES tokens have proper base_asset suffix")
    print()
    
    # Test 4: Source-specific duplicates
    print("TEST 4: Checking for source-specific GT duplicates...")
    duplicates = await check_source_specific_duplicates()
    if duplicates:
        print(f"❌ FAIL: Found {len(duplicates)} duplicate source-specific GTs:")
        for dup in duplicates:
            print(f"  - {dup['symbol']}: {dup['count']} duplicates")
    else:
        print("✅ PASS: No source-specific duplicates found")
    print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    total_issues = len(contaminated) + len(price_issues) + len(sync_issues) + len(duplicates)
    if total_issues == 0:
        print("✅ ALL TESTS PASSED - No merge issues found!")
    else:
        print(f"❌ TESTS FAILED - {total_issues} total issues found:")
        print(f"  - Contaminated GTs: {len(contaminated)}")
        print(f"  - Price split issues: {len(price_issues)}")
        print(f"  - Sync issues: {len(sync_issues)}")
        print(f"  - Duplicates: {len(duplicates)}")
        print()
        print("🔧 Run migration to fix these issues")
    print()


if __name__ == '__main__':
    asyncio.run(run_all_checks())
