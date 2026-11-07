"""fix_contaminated_global_tokens

Split global_tokens that contain multiple different base_assets.
Each unique base_asset gets its own global_token.

Revision ID: fix_contaminated_gt
Revises: a1b2c3d4e5f6
Create Date: 2026-02-13 18:00:00.000000
"""
from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa


revision: str = 'fix_contaminated_gt'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Find all global_tokens with multiple base_assets
    contaminated = conn.execute(sa.text("""
        SELECT tc.global_token_id, gt.symbol as gt_symbol,
               array_agg(DISTINCT t.base_asset) as base_assets
        FROM token_chains tc
        JOIN tokens t ON t.id = tc.token_id
        JOIN global_tokens gt ON gt.id = tc.global_token_id
        WHERE gt.is_deleted = false
        GROUP BY tc.global_token_id, gt.symbol
        HAVING count(DISTINCT t.base_asset) > 1
    """)).fetchall()

    print(f"Found {len(contaminated)} contaminated global_tokens to fix")

    total_splits = 0

    for row in contaminated:
        gt_id = row[0]
        gt_symbol = row[1]
        base_assets = row[2]

        # Determine which base_asset is the "original" (matches gt symbol)
        original_base = None
        for ba in base_assets:
            if ba.upper() == gt_symbol.upper() or gt_symbol.upper().startswith(ba.upper()):
                original_base = ba
                break

        if original_base is None:
            # If no match, keep the first one
            original_base = base_assets[0]

        # For each non-original base_asset, create new global_token and reassign
        for ba in base_assets:
            if ba == original_base:
                continue

            new_gt_id = str(uuid4())

            # Create new global_token
            conn.execute(sa.text("""
                INSERT INTO global_tokens (id, symbol, is_deleted, created_at, updated_at)
                VALUES (:id, :symbol, false, now(), now())
            """), {"id": new_gt_id, "symbol": ba})

            # Reassign token_chains for this base_asset to new global_token
            result = conn.execute(sa.text("""
                UPDATE token_chains tc
                SET global_token_id = :new_gt_id, updated_at = now()
                FROM tokens t
                WHERE tc.token_id = t.id
                  AND tc.global_token_id = :old_gt_id
                  AND t.base_asset = :base_asset
            """), {"new_gt_id": new_gt_id, "old_gt_id": str(gt_id), "base_asset": ba})

            moved = result.rowcount

            # Also update token_prices if they reference this global_token
            conn.execute(sa.text("""
                UPDATE token_prices tp
                SET global_token_id = :new_gt_id, updated_at = now()
                FROM tokens t
                WHERE tp.token_id = t.id
                  AND tp.global_token_id = :old_gt_id
                  AND t.base_asset = :base_asset
            """), {"new_gt_id": new_gt_id, "old_gt_id": str(gt_id), "base_asset": ba})

            # Create global_token_sources for the new global_token
            conn.execute(sa.text("""
                INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                SELECT gen_random_uuid(), :new_gt_id, s.id, now(), now()
                FROM (
                    SELECT DISTINCT t2.source_id as id
                    FROM token_chains tc2
                    JOIN tokens t2 ON t2.id = tc2.token_id
                    WHERE tc2.global_token_id = :new_gt_id
                ) s
                ON CONFLICT DO NOTHING
            """), {"new_gt_id": new_gt_id})

            print(f"  Split {ba} ({moved} chains) from {gt_symbol} ({gt_id}) -> new GT {new_gt_id}")
            total_splits += 1

    print(f"Total: split {total_splits} base_assets from {len(contaminated)} global_tokens")


def downgrade() -> None:
    # Downgrade not supported — data was corrupted before, no point reverting
    pass
