"""fix_tc

Revision ID: 1c62e30b2413
Revises: 93e08dc2e7bb
Create Date: 2025-09-07 00:39:24.575316

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '1c62e30b2413'
down_revision: Union[str, None] = '93e08dc2e7bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Преобразуем все неконвертируемые значения в NULL
    op.execute("""
        UPDATE token_chains
        SET withdrawal_fee = NULL
        WHERE withdrawal_fee IS NULL OR withdrawal_fee = 'None' OR withdrawal_fee !~ '^[0-9.]+$'
    """)
    op.execute("""
        UPDATE token_chains
        SET withdraw_tax = NULL
        WHERE withdraw_tax IS NULL OR withdraw_tax = 'None' OR withdraw_tax !~ '^[0-9.]+$'
    """)

    # Теперь безопасно меняем тип
    op.execute("""
        ALTER TABLE token_chains 
        ALTER COLUMN withdrawal_fee TYPE DOUBLE PRECISION USING withdrawal_fee::double precision
    """)
    op.execute("""
        ALTER TABLE token_chains 
        ALTER COLUMN withdraw_tax TYPE DOUBLE PRECISION USING withdraw_tax::double precision
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE token_chains 
        ALTER COLUMN withdraw_tax TYPE VARCHAR
    """)
    op.execute("""
        ALTER TABLE token_chains 
        ALTER COLUMN withdrawal_fee TYPE VARCHAR
    """)
