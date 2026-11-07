#!/usr/bin/env python3
"""
Monitor cross-chain contamination in global_tokens.
Checks for GT with multiple chains having different contracts.
Used to validate FIX-003 effectiveness.
"""

import os
import sys
import psycopg2
from datetime import datetime
from collections import defaultdict

# Database config
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "dex_bot"),
    "user": os.getenv("POSTGRES_USER", "dex_bot"),
    "password": os.getenv("POSTGRES_PASSWORD", "dex_bot_password"),
}


def check_contamination(conn):
    """
    Find global_tokens with cross-chain contract conflicts.
    Returns: list of contaminated GT ids with details.
    """
    query = """
        SELECT 
            gt.id as gt_id,
            gt.symbol,
            COUNT(DISTINCT tc.chain_id) as chain_count,
            COUNT(DISTINCT tc.contract_address) FILTER (WHERE tc.contract_address IS NOT NULL) as contract_count
        FROM global_tokens gt
        JOIN token_chains tc ON tc.global_token_id = gt.id
        GROUP BY gt.id, gt.symbol
        HAVING COUNT(DISTINCT tc.chain_id) > 1 
           AND COUNT(DISTINCT tc.contract_address) FILTER (WHERE tc.contract_address IS NOT NULL) > 1
        ORDER BY gt.id;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        contaminated = cur.fetchall()

    return [
        {
            "gt_id": row[0],
            "symbol": row[1],
            "chain_count": row[2],
            "contract_count": row[3],
        }
        for row in contaminated
    ]


def get_contamination_details(conn, gt_id):
    """Get detailed info about chains and contracts for a GT."""
    query = """
        SELECT 
            c.name as chain,
            tc.contract_address,
            COUNT(DISTINCT tc.token_id) as token_count
        FROM token_chains tc
        JOIN chains c ON c.id = tc.chain_id
        WHERE tc.global_token_id = %s
        GROUP BY c.name, tc.contract_address
        ORDER BY c.name, tc.contract_address;
    """

    with conn.cursor() as cur:
        cur.execute(query, (gt_id,))
        details = cur.fetchall()

    return [
        {"chain": row[0], "contract": row[1], "token_count": row[2]} for row in details
    ]


def get_new_contaminations_24h(conn):
    """
    Find GT that became contaminated in last 24h.
    (Tokens added to existing GT with conflicting chains)
    """
    query = """
        WITH recent_chains AS (
            SELECT 
                tc.global_token_id,
                tc.chain_id,
                tc.contract_address,
                tc.created_at
            FROM token_chains tc
            WHERE tc.created_at > NOW() - INTERVAL '24 hours'
        ),
        gt_status AS (
            SELECT 
                gt.id as gt_id,
                COUNT(DISTINCT tc.chain_id) as chain_count,
                COUNT(DISTINCT tc.contract_address) FILTER (WHERE tc.contract_address IS NOT NULL) as contract_count,
                MAX(CASE WHEN rc.global_token_id IS NOT NULL THEN 1 ELSE 0 END) as has_recent
            FROM global_tokens gt
            JOIN token_chains tc ON tc.global_token_id = gt.id
            LEFT JOIN recent_chains rc ON rc.global_token_id = gt.id
            GROUP BY gt.id
        )
        SELECT 
            gt_id,
            chain_count,
            contract_count
        FROM gt_status
        WHERE chain_count > 1 
          AND contract_count > 1
          AND has_recent = 1
        ORDER BY gt_id;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        new_contaminations = cur.fetchall()

    return [{"gt_id": row[0], "chain_count": row[1], "contract_count": row[2]} for row in new_contaminations]


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print(f"🔍 Contamination Monitor — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Total contaminated GT
    contaminated = check_contamination(conn)
    print(f"\n📊 Total contaminated GT: {len(contaminated)}")

    # New contaminations in last 24h
    new_24h = get_new_contaminations_24h(conn)
    print(f"🆕 New contaminations (24h): {len(new_24h)}")

    if new_24h:
        print("\n⚠️  NEW CONTAMINATIONS DETECTED!")
        for item in new_24h[:10]:  # Show first 10
            gt_id = item["gt_id"]
            details = get_contamination_details(conn, gt_id)
            print(f"\n  GT #{gt_id}:")
            for d in details:
                print(f"    - {d['chain']}: {d['contract'] or 'NULL'} ({d['token_count']} tokens)")
    else:
        print("\n✅ No new contaminations in last 24h — FIX-003 working!")

    # Sample of old contaminations
    if contaminated:
        print(f"\n📋 Sample contaminated GT (showing 5/{len(contaminated)}):")
        for item in contaminated[:5]:
            gt_id = item["gt_id"]
            symbol = item["symbol"]
            details = get_contamination_details(conn, gt_id)

            print(f"\n  GT #{str(gt_id)[:8]}... ({symbol}):")
            for d in details:
                print(f"    - {d['chain']}: {d['contract'] or 'NULL'} ({d['token_count']} tokens)")

    conn.close()

    # Exit code: 0 if no new contaminations, 1 otherwise
    sys.exit(1 if new_24h else 0)


if __name__ == "__main__":
    main()
