"""Add UNIQUE constraint on global_tokens.symbol

Revision ID: g9h1i2j3k4l5
Revises: b8c9d0e1f2g3
Create Date: 2026-02-19 08:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'g9h1i2j3k4l5'
down_revision: Union[str, None] = 'b8c9d0e1f2g3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add partial UNIQUE index on global_tokens.symbol where is_deleted=false.
    
    This prevents creating duplicate global_tokens with the same symbol.
    Race conditions from parallel price-collectors were creating hundreds of duplicates.
    
    Before applying:
    1. Clean up existing duplicates (run cleanup script)
    2. Merge duplicate GT manually if needed
    """
    # Drop old non-unique index
    op.drop_index('ix_global_tokens_symbol', table_name='global_tokens')
    
    # Create partial UNIQUE index (only for non-deleted tokens)
    # This allows multiple deleted tokens with same symbol, but only ONE active token per symbol
    op.execute("""
        CREATE UNIQUE INDEX uq_global_tokens_symbol_active 
        ON global_tokens (symbol) 
        WHERE is_deleted = false
    """)
    
    # Also create regular index for deleted tokens (for queries)
    op.create_index(
        'ix_global_tokens_symbol_deleted',
        'global_tokens',
        ['symbol'],
        postgresql_where=sa.text('is_deleted = true')
    )


def downgrade() -> None:
    """Revert to old non-unique index."""
    op.drop_index('uq_global_tokens_symbol_active', table_name='global_tokens')
    op.drop_index('ix_global_tokens_symbol_deleted', table_name='global_tokens', if_exists=True)
    
    # Restore old index
    op.create_index('ix_global_tokens_symbol', 'global_tokens', ['symbol'])
