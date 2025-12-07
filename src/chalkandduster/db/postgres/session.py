"""
Chalk and Duster - PostgreSQL Session Management
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from chalkandduster.core.config import settings

# Create async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.APP_DEBUG,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

# Create async session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def init_db() -> None:
    """
    Initialize the database.
    
    Creates tables if they don't exist.
    Note: In production, use Alembic migrations instead.
    """
    from chalkandduster.db.postgres.models import Base
    
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncSession:
    """Get a new database session."""
    async with async_session_factory() as session:
        yield session

