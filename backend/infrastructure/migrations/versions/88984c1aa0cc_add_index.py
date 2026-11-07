"""add_index

Revision ID: 88984c1aa0cc
Revises: 706e9a0273a3
Create Date: 2025-08-24 01:08:00.135826

"""
from typing import Sequence, Union

from alembic import op

__no_transaction__ = True

revision: str = '88984c1aa0cc'
down_revision: Union[str, None] = '706e9a0273a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_token_prices_gtid_upd
        ON token_prices (global_token_id, updated_at DESC);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_token_prices_gtid
        ON token_prices (global_token_id);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_token_chains_gtid
        ON token_chains (global_token_id);
    """)


def downgrade() -> None:
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_token_prices_gtid_upd;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_token_prices_gtid;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_token_chains_gtid;")
