"""populate_global_token_id_in_blacklist

Revision ID: rde7sflewozy
Revises: a1797011e421
Create Date: 2026-01-26 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'rde7sflewozy'
down_revision: Union[str, None] = 'a1797011e421'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Populate global_token_id for existing token_blacklist records
    # by matching contract_address and chain_name with token_chains table
    op.execute("""
        UPDATE token_blacklist tb
        SET global_token_id = tc.global_token_id
        FROM token_chains tc
        JOIN chains c ON c.id = tc.chain_id
        WHERE tb.global_token_id IS NULL
          AND tb.chain_name IS NOT NULL
          AND tb.contract_address IS NOT NULL
          AND LOWER(tc.contract_address) = LOWER(tb.contract_address)
          AND c.name = tb.chain_name
    """)


def downgrade() -> None:
    # Optionally clear global_token_id for records that were populated by this migration
    # For safety, we'll leave them as-is rather than clearing them
    pass
