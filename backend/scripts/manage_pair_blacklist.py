#!/usr/bin/env python3
"""
Manage global_token pair blacklist.

Usage:
    # Add pair to blacklist
    python scripts/manage_pair_blacklist.py add <gt_a_id> <gt_b_id> --reason "XT KEKE: FUTURES vs SPOT"
    
    # Remove pair from blacklist
    python scripts/manage_pair_blacklist.py remove <gt_a_id> <gt_b_id>
    
    # List all blacklisted pairs
    python scripts/manage_pair_blacklist.py list
    
    # Check if pair is blacklisted
    python scripts/manage_pair_blacklist.py check <gt_a_id> <gt_b_id>
"""

import asyncio
import sys
from uuid import UUID

from infrastructure.database.requests import get_database_repo


async def add_pair(gt_a_id: str, gt_b_id: str, reason: str | None = None):
    """Add pair to blacklist."""
    repo = get_database_repo()
    
    try:
        a_uuid = UUID(gt_a_id)
        b_uuid = UUID(gt_b_id)
    except ValueError as e:
        print(f"❌ Invalid UUID: {e}")
        return
    
    # Get global token info
    gt_a = await repo.global_tokens.get_by_id_with_meta(a_uuid)
    gt_b = await repo.global_tokens.get_by_id_with_meta(b_uuid)
    
    if not gt_a or not gt_b:
        print(f"❌ Global token not found: {gt_a_id if not gt_a else gt_b_id}")
        return
    
    # Get sources info
    sources_a = [s.name for s in gt_a.sources] if gt_a.sources else []
    sources_b = [s.name for s in gt_b.sources] if gt_b.sources else []
    
    # Get market types
    market_types_a = set()
    market_types_b = set()
    
    for tc in gt_a.token_on_chains or []:
        if tc.token and tc.token.market_type:
            market_types_a.add(tc.token.market_type.value)
    
    for tc in gt_b.token_on_chains or []:
        if tc.token and tc.token.market_type:
            market_types_b.add(tc.token.market_type.value)
    
    print(f"\n📋 Blacklisting pair:")
    print(f"  A: {gt_a.symbol} ({gt_a_id})")
    print(f"     Sources: {', '.join(sources_a)}")
    print(f"     Market types: {', '.join(market_types_a)}")
    print(f"  B: {gt_b.symbol} ({gt_b_id})")
    print(f"     Sources: {', '.join(sources_b)}")
    print(f"     Market types: {', '.join(market_types_b)}")
    if reason:
        print(f"  Reason: {reason}")
    
    result = await repo.global_token_pair_blacklist.add_pair(
        a_uuid,
        b_uuid,
        reason=reason,
        created_by="admin_script"
    )
    
    if result:
        print(f"\n✅ Pair added to blacklist successfully!")
    else:
        print(f"\n⚠️  Pair already in blacklist")


async def remove_pair(gt_a_id: str, gt_b_id: str):
    """Remove pair from blacklist."""
    repo = get_database_repo()
    
    try:
        a_uuid = UUID(gt_a_id)
        b_uuid = UUID(gt_b_id)
    except ValueError as e:
        print(f"❌ Invalid UUID: {e}")
        return
    
    removed = await repo.global_token_pair_blacklist.remove_pair(a_uuid, b_uuid)
    
    if removed:
        print(f"✅ Pair removed from blacklist")
    else:
        print(f"❌ Pair not found in blacklist")


async def list_blacklisted():
    """List all blacklisted pairs."""
    repo = get_database_repo()
    
    pairs = await repo.global_token_pair_blacklist.get_all_blacklisted_pairs()
    
    if not pairs:
        print("No blacklisted pairs found")
        return
    
    print(f"\n📋 Blacklisted pairs ({len(pairs)}):\n")
    
    for pair in pairs:
        gt_a = pair.global_token_a
        gt_b = pair.global_token_b
        
        # Get sources
        sources_a = [s.name for s in gt_a.sources] if gt_a.sources else []
        sources_b = [s.name for s in gt_b.sources] if gt_b.sources else []
        
        # Get market types
        market_types_a = set()
        market_types_b = set()
        
        for tc in gt_a.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_a.add(tc.token.market_type.value)
        
        for tc in gt_b.token_on_chains or []:
            if tc.token and tc.token.market_type:
                market_types_b.add(tc.token.market_type.value)
        
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"A: {gt_a.symbol} ({pair.global_token_a_id})")
        print(f"   Sources: {', '.join(sources_a)}")
        print(f"   Market types: {', '.join(market_types_a)}")
        print(f"B: {gt_b.symbol} ({pair.global_token_b_id})")
        print(f"   Sources: {', '.join(sources_b)}")
        print(f"   Market types: {', '.join(market_types_b)}")
        if pair.reason:
            print(f"Reason: {pair.reason}")
        print(f"Created: {pair.created_at}")
        print()


async def check_pair(gt_a_id: str, gt_b_id: str):
    """Check if pair is blacklisted."""
    repo = get_database_repo()
    
    try:
        a_uuid = UUID(gt_a_id)
        b_uuid = UUID(gt_b_id)
    except ValueError as e:
        print(f"❌ Invalid UUID: {e}")
        return
    
    is_blacklisted = await repo.global_token_pair_blacklist.is_pair_blacklisted(a_uuid, b_uuid)
    
    if is_blacklisted:
        print(f"🚫 Pair IS blacklisted")
        
        # Get details
        pairs = await repo.global_token_pair_blacklist.get_all_blacklisted_pairs()
        for pair in pairs:
            if (pair.global_token_a_id in [a_uuid, b_uuid] and 
                pair.global_token_b_id in [a_uuid, b_uuid]):
                if pair.reason:
                    print(f"   Reason: {pair.reason}")
                print(f"   Created: {pair.created_at}")
                break
    else:
        print(f"✅ Pair is NOT blacklisted")


def print_usage():
    """Print usage information."""
    print(__doc__)


async def main():
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1]
    
    if command == "add":
        if len(sys.argv) < 4:
            print("Usage: manage_pair_blacklist.py add <gt_a_id> <gt_b_id> [--reason <reason>]")
            return
        
        gt_a_id = sys.argv[2]
        gt_b_id = sys.argv[3]
        
        reason = None
        if len(sys.argv) >= 6 and sys.argv[4] == "--reason":
            reason = sys.argv[5]
        
        await add_pair(gt_a_id, gt_b_id, reason)
    
    elif command == "remove":
        if len(sys.argv) < 4:
            print("Usage: manage_pair_blacklist.py remove <gt_a_id> <gt_b_id>")
            return
        
        gt_a_id = sys.argv[2]
        gt_b_id = sys.argv[3]
        
        await remove_pair(gt_a_id, gt_b_id)
    
    elif command == "list":
        await list_blacklisted()
    
    elif command == "check":
        if len(sys.argv) < 4:
            print("Usage: manage_pair_blacklist.py check <gt_a_id> <gt_b_id>")
            return
        
        gt_a_id = sys.argv[2]
        gt_b_id = sys.argv[3]
        
        await check_pair(gt_a_id, gt_b_id)
    
    else:
        print(f"Unknown command: {command}")
        print_usage()


if __name__ == "__main__":
    asyncio.run(main())
