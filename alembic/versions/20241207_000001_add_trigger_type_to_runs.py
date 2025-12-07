"""Add trigger_type column to runs table

Revision ID: 002
Revises: 001
Create Date: 2024-12-07 00:00:01.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add trigger_type column to runs table
    op.add_column(
        'runs',
        sa.Column(
            'trigger_type',
            sa.String(50),
            nullable=False,
            server_default='on_demand'
        )
    )
    
    # Create index for trigger_type
    op.create_index('ix_runs_trigger_type', 'runs', ['trigger_type'])


def downgrade() -> None:
    op.drop_index('ix_runs_trigger_type', table_name='runs')
    op.drop_column('runs', 'trigger_type')

