"""add_size

Revision ID: ef8f2acf3355
Revises: 5fc6983b2d93
Create Date: 2025-07-23 18:33:53.737419

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'ef8f2acf3355'
down_revision: Union[str, None] = '5fc6983b2d93'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tokens', sa.Column('contract_size', sa.Float(), nullable=True))
    op.alter_column(
        'tokens',
        'is_active',
        server_default=sa.text('true'),
        existing_type=sa.Boolean(),
    )
    op.execute("UPDATE tokens SET is_active = true")
    op.alter_column(
        'token_prices',
        'price_usd',
        nullable=True,
    )


def downgrade() -> None:
    op.drop_column('tokens', 'contract_size')
    op.alter_column(
        'tokens',
        'is_active',
        server_default=None,
        existing_type=sa.Boolean(),
    )
