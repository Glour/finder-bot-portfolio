"""add_index

Revision ID: 19ed257eec9c
Revises: 2b0ffa8dd2ac
Create Date: 2025-10-03 12:41:01.398374

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '19ed257eec9c'
down_revision: Union[str, None] = '2b0ffa8dd2ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # global_tokens
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_global_tokens_is_deleted
    ON global_tokens(is_deleted);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_global_tokens_id_desc
    ON global_tokens(id DESC);
    """)

    # token_chains
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_chain_id
    ON token_chains(chain_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_global_token_id
    ON token_chains(global_token_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_global_chain
    ON token_chains(global_token_id, chain_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_chain_global
    ON token_chains(chain_id, global_token_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_contract_address
    ON token_chains(contract_address);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_chains_token_id
    ON token_chains(token_id);
    """)

    # token_blacklist
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_blacklist_chain_address
    ON token_blacklist(chain_name, contract_address);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_blacklist_expires_at
    ON token_blacklist(expires_at);
    """)

    # token_prices
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_prices_global_token_id
    ON token_prices(global_token_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_token_prices_global_token_id_updated_at
    ON token_prices(global_token_id, updated_at);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_prices_global_token_source
    ON token_prices(global_token_source_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_prices_token_id
    ON token_prices(token_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_prices_market_type
    ON token_prices(market_type);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_prices_composite
    ON token_prices(global_token_source_id, token_id, market_type);
    """)


def downgrade() -> None:
    # token_prices
    op.drop_index("ix_prices_composite", table_name="prices")
    op.drop_index("ix_prices_market_type", table_name="prices")
    op.drop_index("ix_prices_token_id", table_name="prices")
    op.drop_index("ix_prices_global_token_source", table_name="prices")
    op.drop_index("ix_token_prices_global_token_id_updated_at", table_name="token_prices")
    op.drop_index("ix_token_prices_global_token_id", table_name="token_prices")

    # token_blacklist
    op.drop_index("ix_token_blacklist_expires_at", table_name="token_blacklist")
    op.drop_index("ix_token_blacklist_chain_address", table_name="token_blacklist")

    # token_chains
    op.drop_index("ix_token_chains_chain_global", table_name="token_chains")
    op.drop_index("ix_token_chains_global_chain", table_name="token_chains")
    op.drop_index("ix_token_chains_global_token_id", table_name="token_chains")
    op.drop_index("ix_token_chains_chain_id", table_name="token_chains")
    op.drop_index("ix_token_chains_contract_address", table_name="token_chains")
    op.drop_index("ix_token_chains_token_id", table_name="token_chains")

    # chains
    op.drop_index("ix_chains_name", table_name="chains")

    # global_tokens
    op.drop_index("ix_global_tokens_id_desc", table_name="global_tokens")
    op.drop_index("ix_global_tokens_is_deleted", table_name="global_tokens")
