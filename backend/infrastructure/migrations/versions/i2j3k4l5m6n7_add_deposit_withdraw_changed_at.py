"""Add deposit_enabled_changed_at and withdraw_enabled_changed_at to token_chains

Revision ID: i2j3k4l5m6n7
Revises: h1i2j3k4l5m6
Create Date: 2026-03-06 13:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'i2j3k4l5m6n7'
down_revision: Union[str, None] = 'h1i2j3k4l5m6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('token_chains', sa.Column(
        'deposit_enabled_changed_at', sa.DateTime(), nullable=True
    ))
    op.add_column('token_chains', sa.Column(
        'withdraw_enabled_changed_at', sa.DateTime(), nullable=True
    ))


def downgrade() -> None:
    op.drop_column('token_chains', 'deposit_enabled_changed_at')
    op.drop_column('token_chains', 'withdraw_enabled_changed_at')
