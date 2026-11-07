"""add_global_token_pair_blacklist

Revision ID: b8c9d0e1f2g3
Revises: fix_contaminated_gt
Create Date: 2026-02-17 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b8c9d0e1f2g3'
down_revision: Union[str, None] = 'fix_contaminated_gt'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create global_token_pair_blacklist table
    op.create_table(
        'global_token_pair_blacklist',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        
        sa.Column('global_token_a_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('global_token_b_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('reason', sa.String(512), nullable=True),
        sa.Column('created_by', sa.String(128), nullable=True),
        
        sa.ForeignKeyConstraint(['global_token_a_id'], ['global_tokens.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['global_token_b_id'], ['global_tokens.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('global_token_a_id', 'global_token_b_id', name='uq_pair_blacklist'),
    )
    
    # Create indexes for fast lookup
    op.create_index('ix_pair_blacklist_gt_a', 'global_token_pair_blacklist', ['global_token_a_id'])
    op.create_index('ix_pair_blacklist_gt_b', 'global_token_pair_blacklist', ['global_token_b_id'])


def downgrade() -> None:
    op.drop_index('ix_pair_blacklist_gt_b', 'global_token_pair_blacklist')
    op.drop_index('ix_pair_blacklist_gt_a', 'global_token_pair_blacklist')
    op.drop_table('global_token_pair_blacklist')
