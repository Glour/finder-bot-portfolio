#!/usr/bin/env python3
"""
Migrate contaminated global_tokens by splitting cross-chain conflicts.

This script:
1. Finds all contaminated GT (same chain with multiple contracts)
2. Splits them into separate GT per chain+contract
3. Updates all references in token_chains

CRITICAL: Run on dev first, verify results, then prod.
"""

import asyncio
import asyncpg
import sys
import os
from uuid import uuid4


async def find_contaminated_global_tokens(conn):
    """Find all GT with cross-chain contamination."""
    
    query = """
    WITH chain_groups AS (
        SELECT 
            tc.global_token_id,
            tc.chain_id,
            COUNT(DISTINCT tc.contract_address) as contract_count,
            array_agg(DISTINCT tc.contract_address) as contracts
        FROM token_chains tc
        GROUP BY tc.global_token_id, tc.chain_id
        HAVING COUNT(DISTINCT tc.contract_address) > 1
    )
    SELECT 
        gt.id as global_token_id,
        gt.symbol,
        array_agg(
            json_build_object(
                'chain_id', cg.chain_id,
                'contracts', cg.contracts
            )
        ) as conflicts
    FROM chain_groups cg
    JOIN global_tokens gt ON cg.global_token_id = gt.id
    GROUP BY gt.id, gt.symbol
    ORDER BY gt.symbol
    """
    
    return await conn.fetch(query)


async def split_contaminated_gt(
    conn,
    gt_id: str, 
    symbol: str,
    conflicts: list,
    dry_run: bool = True
):
    """
    Split one contaminated GT into separate GT per chain+contract.
    
    Strategy:
    1. Keep first (chain, contract) in original GT
    2. Create new GT for each additional (chain, contract)
    3. Update token_chains references
    """
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Processing GT {gt_id[:8]}... ({symbol})")
    
    # Get all token_chains for this GT
    query_chains = """
    SELECT 
        tc.id as tc_id,
        tc.chain_id,
        tc.contract_address,
        c.name as chain_name,
        tc.token_id
    FROM token_chains tc
    JOIN chains c ON tc.chain_id = c.id
    WHERE tc.global_token_id = $1
    ORDER BY tc.chain_id, tc.contract_address NULLS FIRST
    """
    
    all_chains = await conn.fetch(query_chains, gt_id)
    
    # Group by (chain, contract)
    groups = {}
    for row in all_chains:
        key = (str(row["chain_id"]), row["contract_address"])
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    
    print(f"  Found {len(groups)} distinct (chain, contract) groups:")
    for (chain_id, contract), rows in groups.items():
        chain_name = rows[0]["chain_name"]
        print(f"    - {chain_name}: {contract or 'NULL'} ({len(rows)} token_chains)")
    
    if len(groups) <= 1:
        print("  ⚠️ No contamination found (single group). Skipping.")
        return {"status": "no_contamination", "gt_id": gt_id}
    
    # Keep first group in original GT, create new GT for others
    groups_list = list(groups.items())
    first_group = groups_list[0]
    other_groups = groups_list[1:]
    
    print(f"  ✓ Keeping {first_group[0]} in original GT {gt_id[:8]}...")
    
    new_gts_created = []
    
    for (chain_id, contract), rows in other_groups:
        chain_name = rows[0]["chain_name"]
        
        if dry_run:
            print(f"  [DRY RUN] Would create new GT for {chain_name}: {contract or 'NULL'}")
            print(f"  [DRY RUN] Would move {len(rows)} token_chains to new GT")
            continue
        
        # Create new global_token
        insert_gt = """
        INSERT INTO global_tokens (id, symbol, created_at, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        RETURNING id
        """
        
        new_gt_id = str(uuid4())
        await conn.execute(insert_gt, new_gt_id, symbol)
        
        print(f"  ✓ Created new GT {new_gt_id[:8]}... for {chain_name}: {contract or 'NULL'}")
        
        # Update token_chains to point to new GT
        tc_ids = [str(row["tc_id"]) for row in rows]
        
        update_tc = """
        UPDATE token_chains
        SET global_token_id = $1
        WHERE id = ANY($2::uuid[])
        """
        
        await conn.execute(update_tc, new_gt_id, tc_ids)
        
        print(f"  ✓ Moved {len(tc_ids)} token_chains to new GT")
        
        new_gts_created.append({
            "new_gt_id": new_gt_id,
            "chain": chain_name,
            "contract": contract,
            "moved_count": len(tc_ids)
        })
    
    return {
        "status": "split",
        "original_gt_id": gt_id,
        "kept_group": first_group[0],
        "new_gts": new_gts_created
    }


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate contaminated global_tokens")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of GT to process (for testing)")
    args = parser.parse_args()
    
    # Connect to database
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "dex-dev")
    db_user = os.getenv("DB_USER", "dex-dev")
    db_pass = os.getenv("DB_PASS", "dex")
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_pass
    )
    
    print("=" * 70)
    print("CONTAMINATED GLOBAL_TOKENS MIGRATION")
    print("=" * 70)
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
    else:
        print("🔥 LIVE MODE - Changes will be written to database")
    print("=" * 70)
    
    try:
        # Find all contaminated GT
        contaminated = await find_contaminated_global_tokens(conn)
        
        total = len(contaminated)
        print(f"\nFound {total} contaminated global_tokens")
        
        if total == 0:
            print("✅ No contaminations found!")
            return 0
        
        if args.limit:
            contaminated = contaminated[:args.limit]
            print(f"⚠️ Limited to first {args.limit} GT for testing")
        
        # Process each contaminated GT
        results = {
            "split": 0,
            "no_contamination": 0,
            "errors": 0
        }
    
        for row in contaminated:
            try:
                result = await split_contaminated_gt(
                    conn=conn,
                    gt_id=str(row["global_token_id"]),
                    symbol=row["symbol"],
                    conflicts=row["conflicts"],
                    dry_run=args.dry_run
                )
                results[result["status"]] += 1
            except Exception as e:
                import traceback
                print(f"  ❌ ERROR for GT {str(row['global_token_id'])[:8]}: {e}")
                if args.dry_run and args.limit and args.limit <= 5:
                    traceback.print_exc()
                results["errors"] += 1
        
        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"Total contaminated GT: {total}")
        print(f"Successfully split: {results['split']}")
        print(f"No contamination (skipped): {results['no_contamination']}")
        print(f"Errors: {results['errors']}")
        
        if args.dry_run:
            print("\n⚠️  This was a DRY RUN. Run without --dry-run to apply changes.")
        else:
            print("\n✅ Migration complete!")
    
    finally:
        await conn.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
