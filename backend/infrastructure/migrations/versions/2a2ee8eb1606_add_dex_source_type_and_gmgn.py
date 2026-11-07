"""add_dex_source_type_and_gmgn

Revision ID: 2a2ee8eb1606
Revises: eef64955505e
Create Date: 2026-02-02 22:48:50.688465

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2a2ee8eb1606'
down_revision: Union[str, None] = 'eef64955505e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add DEX value to source_type_enum (uppercase to match CEX)
    op.execute("ALTER TYPE source_type_enum ADD VALUE IF NOT EXISTS 'DEX'")

    # Insert GMGN source
    op.execute("""
        INSERT INTO sources (id, name, type, created_at, updated_at)
        VALUES (gen_random_uuid(), 'gmgn', 'DEX', NOW(), NOW())
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    # Remove GMGN source
    op.execute("DELETE FROM sources WHERE name = 'gmgn'")

    # Note: Cannot remove enum value in PostgreSQL without recreating the type
    # This would require dropping all dependent columns first
    pass
