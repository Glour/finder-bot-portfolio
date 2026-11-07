"""add_exchange_blacklist

Revision ID: eef64955505e
Revises: rde7sflewozy
Create Date: 2026-01-27 01:14:16.553913

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eef64955505e'
down_revision: Union[str, None] = 'rde7sflewozy'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enums if they don't exist (safe for both new and existing DBs)
    op.execute("DO $$ BEGIN CREATE TYPE market_type_enum AS ENUM ('SPOT', 'FUTURES'); EXCEPTION WHEN duplicate_object THEN null; END $$;")
    op.execute("DO $$ BEGIN CREATE TYPE mute_type_enum AS ENUM ('PERMANENT', 'TEMPORARY'); EXCEPTION WHEN duplicate_object THEN null; END $$;")

    # Create exchange_blacklist table
    op.execute("""
        CREATE TABLE IF NOT EXISTS exchange_blacklist (
            id UUID PRIMARY KEY,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
            market_type market_type_enum,
            mute_type mute_type_enum NOT NULL,
            expires_at TIMESTAMP WITHOUT TIME ZONE,
            CONSTRAINT uq_exchange_blacklist UNIQUE (source_id, market_type)
        )
    """)

    # Create index on source_id
    op.execute("CREATE INDEX IF NOT EXISTS ix_exchange_blacklist_source_id ON exchange_blacklist (source_id)")


def downgrade() -> None:
    # Drop index
    op.execute("DROP INDEX IF EXISTS ix_exchange_blacklist_source_id")

    # Drop table
    op.execute("DROP TABLE IF EXISTS exchange_blacklist")

    # Note: We don't drop enums as they're used by other tables (token_blacklist)
