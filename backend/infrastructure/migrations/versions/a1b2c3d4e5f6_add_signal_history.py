"""add_signal_history

Revision ID: a1b2c3d4e5f6
Revises: 2a2ee8eb1606
Create Date: 2026-02-06 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '2a2ee8eb1606'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'signal_history',
        sa.Column('id', sa.BIGINT(), autoincrement=True, nullable=False),
        sa.Column('global_token_id', sa.dialects.postgresql.UUID(), nullable=False),
        sa.Column('symbol', sa.String(50), nullable=False),
        sa.Column('buy_from', sa.String(100), nullable=False),
        sa.Column('sell_to', sa.String(100), nullable=False),
        sa.Column('buy_price', sa.Float(), nullable=False),
        sa.Column('sell_price', sa.Float(), nullable=False),
        sa.Column('spread_percent', sa.Float(), nullable=False),
        sa.Column('profit', sa.Float(), nullable=False),
        sa.Column('profit_before_fee', sa.Float(), nullable=True),
        sa.Column('applied_fee', sa.Float(), server_default='0'),
        sa.Column('volume', sa.Float(), nullable=True),
        sa.Column('level', sa.String(20), nullable=False),
        sa.Column('analyzed_at', sa.BIGINT(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('NOW()')),
        sa.Column('details', JSONB(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_signal_history_analyzed_at', 'signal_history', ['analyzed_at'])
    op.create_index('ix_signal_history_symbol', 'signal_history', ['symbol'])
    op.create_index('ix_signal_history_level', 'signal_history', ['level'])
    op.create_index('ix_signal_history_created_at', 'signal_history', ['created_at'])


def downgrade() -> None:
    op.drop_index('ix_signal_history_created_at', table_name='signal_history')
    op.drop_index('ix_signal_history_level', table_name='signal_history')
    op.drop_index('ix_signal_history_symbol', table_name='signal_history')
    op.drop_index('ix_signal_history_analyzed_at', table_name='signal_history')
    op.drop_table('signal_history')
