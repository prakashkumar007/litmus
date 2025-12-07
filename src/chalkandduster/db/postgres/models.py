"""
Chalk and Duster - SQLAlchemy Models
"""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Tenant(Base, TimestampMixin):
    """Tenant model - represents an organization."""
    
    __tablename__ = "tenants"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Snowflake defaults
    snowflake_account: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    snowflake_database: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Slack integration
    slack_webhook_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    slack_channel: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Settings
    settings: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    connections: Mapped[List["Connection"]] = relationship(
        "Connection", back_populates="tenant", cascade="all, delete-orphan"
    )
    datasets: Mapped[List["Dataset"]] = relationship(
        "Dataset", back_populates="tenant", cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("ix_tenants_slug", "slug"),
        Index("ix_tenants_is_active", "is_active"),
    )


class Connection(Base, TimestampMixin):
    """Connection model - Snowflake connection configuration."""
    
    __tablename__ = "connections"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connection_type: Mapped[str] = mapped_column(String(50), default="snowflake")
    
    # Snowflake connection details
    account: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse: Mapped[str] = mapped_column(String(255), default="COMPUTE_WH")
    database_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), default="PUBLIC")
    role_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Secret reference (credentials stored in Secrets Manager)
    secret_arn: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_tested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_test_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="connections")
    datasets: Mapped[List["Dataset"]] = relationship(
        "Dataset", back_populates="connection", cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("ix_connections_tenant_id", "tenant_id"),
        Index("ix_connections_is_active", "is_active"),
    )


class Dataset(Base, TimestampMixin):
    """Dataset model - table/view to monitor."""
    
    __tablename__ = "datasets"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False,
    )
    
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Table reference
    database_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # YAML configurations
    quality_yaml: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    drift_yaml: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Scheduling (cron expressions)
    quality_schedule: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    drift_schedule: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Tags (stored as JSONB in PostgreSQL)
    tags: Mapped[List[str]] = mapped_column(JSONB, default=list)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Last run info
    last_quality_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_quality_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    last_drift_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_drift_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="datasets")
    connection: Mapped["Connection"] = relationship("Connection", back_populates="datasets")
    
    __table_args__ = (
        Index("ix_datasets_tenant_id", "tenant_id"),
        Index("ix_datasets_connection_id", "connection_id"),
        Index("ix_datasets_is_active", "is_active"),
    )


class Run(Base, TimestampMixin):
    """Run model - quality check or drift detection run."""

    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )

    run_type: Mapped[str] = mapped_column(String(50), nullable=False)  # quality, drift
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False, default="on_demand")  # on_demand, scheduled
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")

    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(nullable=True)

    # Results summary
    total_checks: Mapped[int] = mapped_column(default=0)
    passed_checks: Mapped[int] = mapped_column(default=0)
    failed_checks: Mapped[int] = mapped_column(default=0)
    error_checks: Mapped[int] = mapped_column(default=0)

    # Detailed results stored as JSON
    results: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_runs_dataset_id", "dataset_id"),
        Index("ix_runs_tenant_id", "tenant_id"),
        Index("ix_runs_run_type", "run_type"),
        Index("ix_runs_trigger_type", "trigger_type"),
        Index("ix_runs_status", "status"),
    )
