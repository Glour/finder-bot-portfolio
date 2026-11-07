"""add_uuid

Revision ID: 2aa821bfc329
Revises: 43efc330247a
Create Date: 2025-08-26 02:47:24.428995

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '2aa821bfc329'
down_revision: Union[str, None] = '43efc330247a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    op.alter_column(
        'token_prices',
        'id',
        existing_type=postgresql.UUID(),
        server_default=sa.text("gen_random_uuid()"),
    )


def downgrade() -> None:
    op.alter_column(
        'token_prices',
        'id',
        existing_type=postgresql.UUID(),
        server_default=None,
    )
