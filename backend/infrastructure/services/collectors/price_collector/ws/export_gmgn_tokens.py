"""Export GMGN tokens from DB to JSON file.

Запуск:
    python backend/infrastructure/services/collectors/price_collector/ws/export_gmgn_tokens.py
"""

import asyncio
import json
from sqlalchemy import text

from infrastructure.database.setup import DatabaseManager
from infrastructure.services.collectors.structures.maps import GMGN_CHAIN_MAP
from settings import settings


async def export_tokens():
    """Export all GMGN tokens to JSON file."""
    db_manager = DatabaseManager()
    db_manager.init_database(settings)

    query_text = text("""
        SELECT DISTINCT c.name as chain_name,
                        tc.contract_address,
                        t.symbol,
                        t.id as token_id
        FROM token_chains tc
        JOIN chains c ON tc.chain_id = c.id
        JOIN tokens t ON tc.token_id = t.id
        WHERE c.name = ANY(:chains)
          AND tc.contract_address IS NOT NULL
          AND LENGTH(tc.contract_address) > 0
        ORDER BY c.name, t.symbol
    """)

    supported_chains = list(GMGN_CHAIN_MAP.keys())

    async with db_manager.async_session() as session:
        result = await session.execute(query_text, {"chains": supported_chains})
        rows = result.fetchall()

    # Convert to GMGN subscription format
    subscription_list = []
    tokens_by_chain = {}

    for chain_name, contract_address, symbol, token_id in rows:
        gmgn_chain = GMGN_CHAIN_MAP.get(chain_name)
        if not gmgn_chain:
            continue

        if chain_name not in tokens_by_chain:
            tokens_by_chain[chain_name] = 0
        tokens_by_chain[chain_name] += 1

        subscription_list.append({"chain": gmgn_chain, "addresses": contract_address})

    # Save to file
    output_file = "/app/data/gmgn_tokens.json"
    with open(output_file, "w") as f:
        json.dump(subscription_list, f, indent=2)

    print(f"✅ Exported {len(subscription_list)} tokens to {output_file}")
    print(f"\nTokens per chain:")
    for chain, count in sorted(tokens_by_chain.items()):
        gmgn_chain = GMGN_CHAIN_MAP[chain]
        print(f"  {chain} -> {gmgn_chain}: {count} tokens")


if __name__ == "__main__":
    asyncio.run(export_tokens())
