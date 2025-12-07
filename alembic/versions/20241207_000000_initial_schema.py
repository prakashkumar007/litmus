"""Initial schema - Create tenants, connections, and datasets tables

Revision ID: 001
Revises: 
Create Date: 2024-12-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create tenants table
    op.create_table(
        'tenants',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('slug', sa.String(255), nullable=False, unique=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('snowflake_account', sa.String(255), nullable=True),
        sa.Column('snowflake_database', sa.String(255), nullable=True),
        sa.Column('slack_webhook_url', sa.String(500), nullable=True),
        sa.Column('slack_channel', sa.String(255), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.Column('settings', postgresql.JSONB(), nullable=True),
    )
    op.create_index('ix_tenants_slug', 'tenants', ['slug'])
    op.create_index('ix_tenants_is_active', 'tenants', ['is_active'])

    # Create connections table
    op.create_table(
        'connections',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('connection_type', sa.String(50), nullable=False, default='snowflake'),
        sa.Column('account', sa.String(255), nullable=True),
        sa.Column('warehouse', sa.String(255), nullable=True),
        sa.Column('database_name', sa.String(255), nullable=True),
        sa.Column('schema_name', sa.String(255), nullable=True),
        sa.Column('role_name', sa.String(255), nullable=True),
        sa.Column('secret_arn', sa.String(500), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
    )
    op.create_index('ix_connections_tenant_id', 'connections', ['tenant_id'])
    op.create_index('ix_connections_is_active', 'connections', ['is_active'])

    # Create datasets table
    op.create_table(
        'datasets',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('connection_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('connections.id'), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('database_name', sa.String(255), nullable=False),
        sa.Column('schema_name', sa.String(255), nullable=False),
        sa.Column('table_name', sa.String(255), nullable=False),
        sa.Column('quality_yaml', sa.Text(), nullable=True),
        sa.Column('drift_yaml', sa.Text(), nullable=True),
        sa.Column('schedule', sa.String(100), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.Column('last_quality_run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_drift_run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('settings', postgresql.JSONB(), nullable=True),
    )
    op.create_index('ix_datasets_tenant_id', 'datasets', ['tenant_id'])
    op.create_index('ix_datasets_connection_id', 'datasets', ['connection_id'])
    op.create_index('ix_datasets_is_active', 'datasets', ['is_active'])


def downgrade() -> None:
    op.drop_table('datasets')
    op.drop_table('connections')
    op.drop_table('tenants')

