"""Add buy_base_asset, sell_base_asset, telegram_link to signal_history

Revision ID: h1i2j3k4l5m6
Revises: g9h1i2j3k4l5
Create Date: 2026-03-04 09:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'h1i2j3k4l5m6'
down_revision: Union[str, None] = 'g9h1i2j3k4l5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('signal_history', sa.Column('buy_base_asset', sa.String(50), nullable=True))
    op.add_column('signal_history', sa.Column('sell_base_asset', sa.String(50), nullable=True))
    op.add_column('signal_history', sa.Column('telegram_link', sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column('signal_history', 'telegram_link')
    op.drop_column('signal_history', 'sell_base_asset')
    op.drop_column('signal_history', 'buy_base_asset')
