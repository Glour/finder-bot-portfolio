#!/usr/bin/env python3
"""
Migration: Split cross-chain contract conflicts into separate global_tokens.

Root cause: assign_global_ids used chainless search even when adding chained tokens,
causing tokens from different networks (e.g., BSC vs ETH) to merge into one GT.

This creates phantom spreads like:
- VON: BSC $0.0000162 vs ETH $314 (1943858156%)
- ANT: BSC $0.000321 vs BASE $3.06 (951108%)
- JEFF: ETH $0.000340 vs HYPE $2.25 (660463%)

Affected: 549 GTs created 2026-02-02 (last token-collector run)
"""

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple
from uuid import UUID

from sqlalchemy import and_, func, select
from sqlalchemy.orm import joinedload

from infrastructure.database.repo.requests import RequestsRepo
from infrastructure.db.database import SessionLocal
from infrastructure.db.models import Chain, GlobalToken, Token, TokenOnChain


async def find_cross_chain_conflicts() -> List[Dict]:
    """
    Find all GTs with cross-chain contract conflicts.
    
    Returns list of dicts with GT info and conflicting chains.
    """
    db = SessionLocal()
    conflicts = []
    
    try:
        # Find GTs with multiple chains and multiple contracts
        stmt = select(GlobalToken).where(
            GlobalToken.is_deleted == False
        )
        
        result = await db.execute(stmt)
        all_gts = result.scalars().all()
        
        print(f"Checking {len(all_gts)} global_tokens...")
        
        for gt in all_gts:
            # Get all token_chains for this GT
            tc_stmt = select(TokenOnChain).where(
                TokenOnChain.global_token_id == gt.id,
                TokenOnChain.contract_address.isnot(None),
                TokenOnChain.contract_address != ""
            ).options(
                joinedload(TokenOnChain.chain),
                joinedload(TokenOnChain.token)
            )
            
            tc_result = await db.execute(tc_stmt)
            token_chains = tc_result.scalars().all()
            
            if not token_chains:
                continue
            
            # Group by chain_id
            contracts_by_chain: Dict[UUID, Set[str]] = defaultdict(set)
            
            for tc in token_chains:
                chain_id = tc.chain_id
                contract = tc.contract_address.lower() if tc.contract_address else None
                if contract:
                    contracts_by_chain[chain_id].add(contract)
            
            # Check for conflicts
            chain_count = len(contracts_by_chain)
            total_contracts = sum(len(contracts) for contracts in contracts_by_chain.values())
            
            # Conflict if: multiple chains AND multiple unique contracts
            if chain_count > 1 and total_contracts > chain_count:
                # Get chain names
                chain_names = []
                for tc in token_chains:
                    if tc.chain and tc.chain.name:
                        chain_names.append(tc.chain.name)
                
                conflicts.append({
                    "gt_id": gt.id,
                    "symbol": gt.symbol,
                    "chain_count": chain_count,
                    "contract_count": total_contracts,
                    "chains": list(set(chain_names))
                })
        
        print(f"\nFound {len(conflicts)} GTs with cross-chain conflicts")
        
    finally:
        await db.close()
    
    return conflicts


async def split_global_token(
    gt_id: UUID,
    symbol: str,
    repo: RequestsRepo
) -> Dict[str, int]:
    """
    Split one GT with cross-chain conflicts into separate GTs per (chain, contract).
    
    Returns stats about created GTs.
    """
    db = SessionLocal()
    stats = {
        "new_gts_created": 0,
        "token_chains_updated": 0
    }
    
    try:
        # Get all token_chains for this GT
        tc_stmt = select(TokenOnChain).where(
            TokenOnChain.global_token_id == gt_id,
            TokenOnChain.contract_address.isnot(None),
            TokenOnChain.contract_address != ""
        ).options(
            joinedload(TokenOnChain.chain),
            joinedload(TokenOnChain.token).joinedload(Token.source)
        )
        
        tc_result = await db.execute(tc_stmt)
        token_chains = list(tc_result.scalars().all())
        
        if not token_chains:
            return stats
        
        # Group by (chain_id, contract_address)
        groups: Dict[Tuple[UUID, str], List[TokenOnChain]] = defaultdict(list)
        
        for tc in token_chains:
            key = (tc.chain_id, tc.contract_address.lower())
            groups[key].append(tc)
        
        # If only one group, no split needed
        if len(groups) <= 1:
            return stats
        
        print(f"\n{symbol}: splitting into {len(groups)} groups")
        
        # Keep first group in original GT, create new GTs for others
        first_group = True
        
        for (chain_id, contract), tcs in groups.items():
            if first_group:
                # Keep in original GT
                first_group = False
                chain_name = tcs[0].chain.name if tcs[0].chain else "unknown"
                print(f"  Group 1: {len(tcs)} token_chains stay in GT {gt_id} ({chain_name})")
                continue
            
            # Create new GT
            new_gt = GlobalToken(
                symbol=symbol,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(new_gt)
            await db.flush()  # Get new GT id
            
            stats["new_gts_created"] += 1
            
            # Update token_chains to point to new GT
            for tc in tcs:
                tc.global_token_id = new_gt.id
                stats["token_chains_updated"] += 1
            
            chain_name = tcs[0].chain.name if tcs[0].chain else "unknown"
            print(f"  Group {stats['new_gts_created'] + 1}: {len(tcs)} token_chains → new GT {new_gt.id} ({chain_name}, {contract[:8]}...)")
        
        await db.commit()
        
    except Exception as e:
        await db.rollback()
        print(f"ERROR splitting {symbol}: {e}")
        raise
    finally:
        await db.close()
    
    return stats


async def main():
    """Run migration."""
    print("=" * 80)
    print("MIGRATION: Split cross-chain contract conflicts")
    print("=" * 80)
    print()
    
    # Step 1: Find conflicts
    print("Step 1: Finding cross-chain conflicts...")
    conflicts = await find_cross_chain_conflicts()
    
    if not conflicts:
        print("\n✅ No cross-chain conflicts found!")
        return
    
    # Show top conflicts
    print("\nTop 20 conflicts:")
    for c in sorted(conflicts, key=lambda x: x["contract_count"], reverse=True)[:20]:
        print(f"  {c['symbol']:20s} {c['chain_count']} chains, {c['contract_count']} contracts: {', '.join(c['chains'][:5])}")
    
    # Ask confirmation
    print(f"\n⚠️  Found {len(conflicts)} GTs to split")
    response = input("Continue? [y/N]: ")
    if response.lower() != 'y':
        print("Aborted.")
        return
    
    # Step 2: Split conflicts
    print("\nStep 2: Splitting conflicts...")
    repo = RequestsRepo()
    
    total_stats = {
        "new_gts_created": 0,
        "token_chains_updated": 0,
        "errors": 0
    }
    
    for i, conflict in enumerate(conflicts, 1):
        try:
            print(f"\n[{i}/{len(conflicts)}] Processing {conflict['symbol']}...")
            stats = await split_global_token(
                conflict["gt_id"],
                conflict["symbol"],
                repo
            )
            total_stats["new_gts_created"] += stats["new_gts_created"]
            total_stats["token_chains_updated"] += stats["token_chains_updated"]
        except Exception as e:
            print(f"  ❌ Error: {e}")
            total_stats["errors"] += 1
    
    # Step 3: Verify
    print("\n" + "=" * 80)
    print("Step 3: Verifying...")
    remaining = await find_cross_chain_conflicts()
    
    print("\n" + "=" * 80)
    print("MIGRATION COMPLETE")
    print("=" * 80)
    print(f"GTs split: {len(conflicts)}")
    print(f"New GTs created: {total_stats['new_gts_created']}")
    print(f"Token chains updated: {total_stats['token_chains_updated']}")
    print(f"Errors: {total_stats['errors']}")
    print(f"Remaining conflicts: {len(remaining)}")
    print()
    
    if len(remaining) == 0:
        print("✅ SUCCESS: 0 cross-chain conflicts remaining!")
    else:
        print(f"⚠️  WARNING: {len(remaining)} conflicts remaining")
        print("\nRemaining conflicts:")
        for c in remaining[:10]:
            print(f"  {c['symbol']}: {c['chains']}")


if __name__ == "__main__":
    asyncio.run(main())
