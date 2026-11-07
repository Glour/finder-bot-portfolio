"""add_global

Revision ID: 01d8ff15dd1c
Revises: 08c7d39261a7
Create Date: 2025-08-14 17:09:48.698467

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '01d8ff15dd1c'
down_revision: Union[str, None] = '08c7d39261a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('global_tokens',
                    sa.Column('symbol', sa.String(length=64), nullable=False),
                    sa.Column('created_at', sa.DateTime(), nullable=False),
                    sa.Column('updated_at', sa.DateTime(), nullable=False),
                    sa.Column('id', sa.UUID(), nullable=False),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('global_token_sources',
                    sa.Column('global_token_id', sa.UUID(), nullable=False),
                    sa.Column('source_id', sa.UUID(), nullable=False),
                    sa.Column('price_id', sa.UUID(), nullable=True),
                    sa.Column('created_at', sa.DateTime(), nullable=False),
                    sa.Column('updated_at', sa.DateTime(), nullable=False),
                    sa.Column('id', sa.UUID(), nullable=False),
                    sa.ForeignKeyConstraint(['global_token_id'], ['global_tokens.id'], ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['price_id'], ['token_prices.id'], ondelete='SET NULL'),
                    sa.ForeignKeyConstraint(['source_id'], ['sources.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('global_token_id', 'source_id', name='uq_global_token_source')
                    )
    op.create_index(op.f('ix_global_token_sources_global_token_id'), 'global_token_sources', ['global_token_id'],
                    unique=False)
    op.create_index(op.f('ix_global_token_sources_source_id'), 'global_token_sources', ['source_id'], unique=False)
    op.create_index(op.f('ix_global_tokens_symbol'), 'global_tokens', ['symbol'], unique=False)
    op.add_column('token_chains', sa.Column('global_token_id', sa.UUID(), nullable=False))
    op.create_index(op.f('ix_token_chains_global_token_id'), 'token_chains', ['global_token_id'], unique=False)
    op.create_foreign_key("token_chains_global_token_id_fkey", 'token_chains', 'global_tokens', ['global_token_id'],
                          ['id'], ondelete='CASCADE')
    op.add_column('token_prices', sa.Column('global_token_id', sa.UUID(), nullable=False))
    op.add_column('token_prices', sa.Column('global_token_source_id', sa.UUID(), nullable=True))
    op.drop_index('ix_token_prices_source_id', table_name='token_prices')
    op.drop_index('ix_token_prices_token_id', table_name='token_prices')
    op.drop_index('ix_token_prices_token_on_chain_id', table_name='token_prices')
    op.drop_constraint('uq_token_source', 'token_prices', type_='unique')
    op.create_index(op.f('ix_token_prices_global_token_id'), 'token_prices', ['global_token_id'], unique=False)
    op.create_unique_constraint('uq_token_chain_source', 'token_prices', ['global_token_id', 'global_token_source_id'])
    op.drop_constraint('token_prices_token_id_fkey', 'token_prices', type_='foreignkey')
    op.drop_constraint('token_prices_source_id_fkey', 'token_prices', type_='foreignkey')
    op.drop_constraint('token_prices_token_on_chain_id_fkey', 'token_prices', type_='foreignkey')
    op.create_foreign_key("token_prices_global_token_source_id_fkey", 'token_prices', 'global_token_sources',
                          ['global_token_source_id'], ['id'],
                          ondelete='CASCADE')
    op.create_foreign_key("token_prices_global_token_id_fkey", 'token_prices', 'global_tokens', ['global_token_id'],
                          ['id'], ondelete='CASCADE')
    op.drop_column('token_prices', 'token_id')
    op.drop_column('token_prices', 'token_on_chain_id')
    op.drop_column('token_prices', 'source_id')


def downgrade() -> None:
    op.add_column('token_prices', sa.Column('source_id', sa.UUID(), autoincrement=False, nullable=False))
    op.add_column('token_prices', sa.Column('token_on_chain_id', sa.UUID(), autoincrement=False, nullable=True))
    op.add_column('token_prices', sa.Column('token_id', sa.UUID(), autoincrement=False, nullable=False))
    op.drop_constraint("token_prices_global_token_source_id_fkey", 'token_prices', type_='foreignkey')
    op.drop_constraint("token_prices_global_token_id_fkey", 'token_prices', type_='foreignkey')
    op.create_foreign_key('token_prices_token_on_chain_id_fkey', 'token_prices', 'token_chains', ['token_on_chain_id'],
                          ['id'], ondelete='SET NULL')
    op.create_foreign_key('token_prices_source_id_fkey', 'token_prices', 'sources', ['source_id'], ['id'],
                          ondelete='CASCADE')
    op.create_foreign_key('token_prices_token_id_fkey', 'token_prices', 'tokens', ['token_id'], ['id'],
                          ondelete='CASCADE')
    op.drop_constraint('uq_token_chain_source', 'token_prices', type_='unique')
    op.drop_index(op.f('ix_token_prices_global_token_id'), table_name='token_prices')
    op.create_unique_constraint('uq_token_source', 'token_prices', ['token_id', 'source_id'])
    op.create_index('ix_token_prices_token_on_chain_id', 'token_prices', ['token_on_chain_id'], unique=False)
    op.create_index('ix_token_prices_token_id', 'token_prices', ['token_id'], unique=False)
    op.create_index('ix_token_prices_source_id', 'token_prices', ['source_id'], unique=False)
    op.drop_column('token_prices', 'global_token_source_id')
    op.drop_column('token_prices', 'global_token_id')
    op.drop_constraint("token_chains_global_token_id_fkey", 'token_chains', type_='foreignkey')
    op.drop_index(op.f('ix_token_chains_global_token_id'), table_name='token_chains')
    op.drop_column('token_chains', 'global_token_id')
    op.drop_index(op.f('ix_global_tokens_symbol'), table_name='global_tokens')
    op.drop_table('global_tokens')
    op.drop_index(op.f('ix_global_token_sources_source_id'), table_name='global_token_sources')
    op.drop_index(op.f('ix_global_token_sources_global_token_id'), table_name='global_token_sources')
    op.drop_table('global_token_sources')
