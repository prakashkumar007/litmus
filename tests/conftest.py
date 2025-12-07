"""
Chalk and Duster - Test Fixtures
"""

import asyncio
import os
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Set test environment
os.environ["APP_ENV"] = "test"
os.environ["SNOWFLAKE_USE_LOCALSTACK"] = "true"
os.environ["LOCALSTACK_SNOWFLAKE_HOST"] = "localhost"
os.environ["LOCALSTACK_SNOWFLAKE_PORT"] = "4566"
os.environ["DATABASE_URL"] = "postgresql+asyncpg://chalkandduster:chalkandduster@localhost:5432/chalkandduster_test"

from chalkandduster.main import app
from chalkandduster.db.postgres.models import Base, Tenant, Connection, Dataset
from chalkandduster.db.postgres.session import async_session_factory
from chalkandduster.api.deps import get_db_session


# Test database engine
TEST_DATABASE_URL = os.environ["DATABASE_URL"]
test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
test_session_factory = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with test_session_factory() as session:
        yield session
        await session.rollback()
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Create a test client with database session override."""
    
    async def override_get_db():
        yield db_session
    
    app.dependency_overrides[get_db_session] = override_get_db
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    
    app.dependency_overrides.clear()


@pytest.fixture
def sync_client() -> Generator[TestClient, None, None]:
    """Create a synchronous test client."""
    with TestClient(app) as client:
        yield client


# =============================================================================
# Sample Data Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def sample_tenant(db_session: AsyncSession) -> Tenant:
    """Create a sample tenant."""
    tenant = Tenant(
        id=uuid4(),
        name="Test Tenant",
        slug="test-tenant",
        description="A test tenant for unit tests",
        snowflake_account="test_account",
        snowflake_database="TEST_DB",
        is_active=True,
    )
    db_session.add(tenant)
    await db_session.commit()
    await db_session.refresh(tenant)
    return tenant


@pytest_asyncio.fixture
async def sample_connection(
    db_session: AsyncSession, sample_tenant: Tenant
) -> Connection:
    """Create a sample connection."""
    connection = Connection(
        id=uuid4(),
        tenant_id=sample_tenant.id,
        name="Test Connection",
        connection_type="snowflake",
        account="test_account",
        warehouse="TEST_WH",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        is_active=True,
    )
    db_session.add(connection)
    await db_session.commit()
    await db_session.refresh(connection)
    return connection


@pytest_asyncio.fixture
async def sample_dataset(
    db_session: AsyncSession,
    sample_tenant: Tenant,
    sample_connection: Connection,
) -> Dataset:
    """Create a sample dataset."""
    dataset = Dataset(
        id=uuid4(),
        tenant_id=sample_tenant.id,
        connection_id=sample_connection.id,
        name="Test Dataset",
        description="A test dataset",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        table_name="TEST_TABLE",
        quality_yaml="""
checks for TEST_TABLE:
  - row_count > 0
  - missing_count(id) = 0
""",
        drift_yaml="""
monitors:
  - name: volume_monitor
    type: volume
    threshold: 3.0
""",
        is_active=True,
    )
    db_session.add(dataset)
    await db_session.commit()
    await db_session.refresh(dataset)
    return dataset


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_ollama_response():
    """Mock Ollama API response."""
    return {
        "model": "llama3.2",
        "message": {
            "role": "assistant",
            "content": """Here is the generated YAML:

```yaml
# quality_yaml
checks for TEST_TABLE:
  - row_count > 0
  - missing_count(id) = 0
```

```yaml
# drift_yaml
monitors:
  - name: volume_monitor
    type: volume
    threshold: 3.0
```

This configuration checks for row count and missing values.
""",
        },
    }


@pytest.fixture
def sample_quality_yaml() -> str:
    """Sample quality YAML for testing."""
    return """
checks for ORDERS:
  - row_count > 0
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - invalid_count(status) = 0:
      valid values: ['pending', 'completed', 'cancelled']
  - freshness(created_at) < 1d
"""


@pytest.fixture
def sample_drift_yaml() -> str:
    """Sample drift YAML for testing."""
    return """
monitors:
  - name: schema_monitor
    type: schema
    threshold: 0
  - name: volume_monitor
    type: volume
    threshold: 3.0
  - name: amount_distribution
    type: distribution
    column: amount
    threshold: 0.25
"""

