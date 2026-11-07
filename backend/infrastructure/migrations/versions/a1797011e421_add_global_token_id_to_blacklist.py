"""add_global_token_id_to_blacklist

Revision ID: a1797011e421
Revises: 19ed257eec9c
Create Date: 2026-01-26 10:24:06.880399

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1797011e421'
down_revision: Union[str, None] = '19ed257eec9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Make chain_name nullable
    op.alter_column('token_blacklist', 'chain_name',
                    existing_type=sa.String(length=32),
                    nullable=True)

    # Add global_token_id column with FK
    op.add_column('token_blacklist',
                  sa.Column('global_token_id', sa.UUID(), nullable=True))

    op.create_foreign_key(
        'fk_token_blacklist_global_token_id',
        'token_blacklist', 'global_tokens',
        ['global_token_id'], ['id'],
        ondelete='CASCADE'
    )

    # Create index on global_token_id
    op.create_index('ix_token_blacklist_global_token_id',
                    'token_blacklist',
                    ['global_token_id'])


def downgrade() -> None:
    # Drop index
    op.drop_index('ix_token_blacklist_global_token_id', 'token_blacklist')

    # Drop FK constraint
    op.drop_constraint('fk_token_blacklist_global_token_id',
                       'token_blacklist',
                       type_='foreignkey')

    # Drop column
    op.drop_column('token_blacklist', 'global_token_id')

    # Make chain_name non-nullable again
    op.alter_column('token_blacklist', 'chain_name',
                    existing_type=sa.String(length=32),
                    nullable=False)
