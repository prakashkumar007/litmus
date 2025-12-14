#!/usr/bin/env python3
"""
Setup test data in PostgreSQL and LocalStack for Lambda testing.

This script:
1. Creates a test tenant, connection, and dataset in PostgreSQL
2. Creates a secret in LocalStack Secrets Manager with Snowflake credentials
3. Creates the test database/tables in LocalStack Snowflake

Usage:
    docker compose exec api python scripts/setup_test_data.py
"""

import asyncio
import json
import os
import uuid

import boto3
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

# Fixed UUIDs for testing (matching tests/events/*.json)
TENANT_ID = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
CONNECTION_ID = uuid.UUID("550e8400-e29b-41d4-a716-446655440001")
DATASET_ID = uuid.UUID("550e8400-e29b-41d4-a716-446655440002")

# Snowflake LocalStack credentials
SNOWFLAKE_CREDENTIALS = {
    "user": "test",
    "password": "test",
}

# Quality YAML configuration (Great Expectations format)
# Note: Column names are uppercase to match Snowflake's default column naming
QUALITY_YAML = """
expectation_suite_name: orders_quality
expectations:
  - expectation_type: expect_table_row_count_to_be_between
    kwargs:
      min_value: 1
      max_value: 1000000
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: ORDER_ID
  - expectation_type: expect_column_values_to_be_unique
    kwargs:
      column: ORDER_ID
  - expectation_type: expect_column_values_to_be_in_set
    kwargs:
      column: STATUS
      value_set:
        - pending
        - completed
        - cancelled
""".strip()

# Drift YAML configuration (Evidently format)
# Note: Column names are uppercase to match Snowflake's default column naming
# Reference data is fetched using Snowflake Time Travel automatically
DRIFT_YAML = """
# Time Travel configuration for reference data
# Uses Snowflake Time Travel to query data from the past
time_travel_days: 1  # Look back 1 day for reference data

monitors:
  - name: amount_distribution_drift
    type: distribution
    column: AMOUNT
    threshold: 0.1
    stattest: ks
  - name: status_distribution_drift
    type: distribution
    column: STATUS
    threshold: 0.1
    stattest: chisquare
  - name: dataset_drift_check
    type: dataset
    threshold: 0.3
  - name: volume_check
    type: volume
    threshold: 0.5
  - name: schema_check
    type: schema
""".strip()


async def setup_postgres_data():
    """Create test tenant, connection, and dataset in PostgreSQL."""
    print("\n" + "=" * 60)
    print("  Setting up PostgreSQL test data")
    print("=" * 60)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not set")

    engine = create_async_engine(database_url)

    async with AsyncSession(engine) as session:
        # Check if tenant exists
        result = await session.execute(
            text("SELECT id FROM tenants WHERE id = :id"),
            {"id": str(TENANT_ID)}
        )
        if result.scalar():
            print(f"  Tenant {TENANT_ID} already exists, skipping...")
        else:
            # Create tenant
            await session.execute(
                text("""
                    INSERT INTO tenants (id, name, slug, description, is_active)
                    VALUES (:id, :name, :slug, :description, :is_active)
                """),
                {
                    "id": str(TENANT_ID),
                    "name": "Test Tenant",
                    "slug": "test-tenant",
                    "description": "Test tenant for Lambda testing",
                    "is_active": True,
                }
            )
            print(f"  ✅ Created tenant: {TENANT_ID}")

        # Check if connection exists
        result = await session.execute(
            text("SELECT id FROM connections WHERE id = :id"),
            {"id": str(CONNECTION_ID)}
        )
        if result.scalar():
            print(f"  Connection {CONNECTION_ID} already exists, skipping...")
        else:
            # Create connection
            secret_arn = f"arn:aws:secretsmanager:us-east-1:000000000000:secret:chalkandduster/snowflake/{TENANT_ID}"
            await session.execute(
                text("""
                    INSERT INTO connections (id, tenant_id, name, connection_type, account, warehouse, database_name, schema_name, secret_arn, is_active)
                    VALUES (:id, :tenant_id, :name, :connection_type, :account, :warehouse, :database_name, :schema_name, :secret_arn, :is_active)
                """),
                {
                    "id": str(CONNECTION_ID),
                    "tenant_id": str(TENANT_ID),
                    "name": "Test Snowflake Connection",
                    "connection_type": "snowflake",
                    "account": "test",
                    "warehouse": "test_warehouse",
                    "database_name": "test_db",
                    "schema_name": "public",
                    "secret_arn": secret_arn,
                    "is_active": True,
                }
            )
            print(f"  ✅ Created connection: {CONNECTION_ID}")

        # Check if dataset exists
        result = await session.execute(
            text("SELECT id FROM datasets WHERE id = :id"),
            {"id": str(DATASET_ID)}
        )
        if result.scalar():
            print(f"  Dataset {DATASET_ID} already exists, updating YAML configs...")
            await session.execute(
                text("""
                    UPDATE datasets 
                    SET quality_yaml = :quality_yaml, drift_yaml = :drift_yaml
                    WHERE id = :id
                """),
                {
                    "id": str(DATASET_ID),
                    "quality_yaml": QUALITY_YAML,
                    "drift_yaml": DRIFT_YAML,
                }
            )
        else:
            # Create dataset
            await session.execute(
                text("""
                    INSERT INTO datasets (id, tenant_id, connection_id, name, description, database_name, schema_name, table_name, quality_yaml, drift_yaml, is_active)
                    VALUES (:id, :tenant_id, :connection_id, :name, :description, :database_name, :schema_name, :table_name, :quality_yaml, :drift_yaml, :is_active)
                """),
                {
                    "id": str(DATASET_ID),
                    "tenant_id": str(TENANT_ID),
                    "connection_id": str(CONNECTION_ID),
                    "name": "Orders Dataset",
                    "description": "Test orders table for Lambda testing",
                    "database_name": "test_db",
                    "schema_name": "public",
                    "table_name": "orders",
                    "quality_yaml": QUALITY_YAML,
                    "drift_yaml": DRIFT_YAML,
                    "is_active": True,
                }
            )
            print(f"  ✅ Created dataset: {DATASET_ID}")

        await session.commit()
    print("  ✅ PostgreSQL data setup complete")


def setup_secrets_manager():
    """Create Snowflake credentials secret in LocalStack Secrets Manager."""
    print("\n" + "=" * 60)
    print("  Setting up Secrets Manager")
    print("=" * 60)

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    region = os.environ.get("AWS_REGION", "us-east-1")

    client = boto3.client(
        "secretsmanager",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    secret_name = f"chalkandduster/snowflake/{TENANT_ID}"

    try:
        # Try to get existing secret
        client.get_secret_value(SecretId=secret_name)
        print(f"  Secret {secret_name} already exists, updating...")
        client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(SNOWFLAKE_CREDENTIALS),
        )
    except client.exceptions.ResourceNotFoundException:
        # Create new secret
        client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(SNOWFLAKE_CREDENTIALS),
        )
        print(f"  ✅ Created secret: {secret_name}")

    print("  ✅ Secrets Manager setup complete")


async def setup_snowflake_data():
    """Create test database and tables in LocalStack Snowflake."""
    print("\n" + "=" * 60)
    print("  Setting up Snowflake test data")
    print("=" * 60)

    import snowflake.connector as sf

    # Connect without database (to create it)
    conn = sf.connect(
        user="test",
        password="test",
        account="test",
        host="snowflake.localhost.localstack.cloud",
    )
    cursor = conn.cursor()

    # Create database and warehouse
    cursor.execute("CREATE WAREHOUSE IF NOT EXISTS test_warehouse")
    print("  ✅ Created warehouse: test_warehouse")

    cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
    print("  ✅ Created database: test_db")

    cursor.execute("USE DATABASE test_db")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS public")
    print("  ✅ Created schema: public")

    # Create orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_date TIMESTAMP,
            amount DECIMAL(10,2),
            status VARCHAR(50)
        )
    """)
    print("  ✅ Created table: orders")

    # Check if data exists
    cursor.execute("SELECT COUNT(*) FROM public.orders")
    count = cursor.fetchone()[0]

    if count == 0:
        # Insert sample data
        cursor.execute("""
            INSERT INTO public.orders (order_id, customer_id, order_date, amount, status)
            VALUES
                (1, 1, '2024-01-15 10:30:00', 49.98, 'completed'),
                (2, 2, '2024-01-16 14:20:00', 29.99, 'completed'),
                (3, 1, '2024-01-17 09:15:00', 99.97, 'pending'),
                (4, 3, '2024-01-18 16:45:00', 19.99, 'completed'),
                (5, 2, '2024-01-19 11:00:00', 79.98, 'cancelled')
        """)
        print("  ✅ Inserted sample data: 5 rows")
    else:
        print(f"  Orders table already has {count} rows, skipping insert")

    cursor.close()
    conn.close()
    print("  ✅ Snowflake data setup complete")


async def main():
    print("\n" + "=" * 60)
    print("  Chalk and Duster - Test Data Setup")
    print("=" * 60)

    # Setup PostgreSQL data
    await setup_postgres_data()

    # Setup Secrets Manager
    setup_secrets_manager()

    # Setup Snowflake data
    await setup_snowflake_data()

    print("\n" + "=" * 60)
    print("  ✅ All test data setup complete!")
    print("=" * 60)
    print(f"""
  Test IDs:
    Tenant ID:     {TENANT_ID}
    Connection ID: {CONNECTION_ID}
    Dataset ID:    {DATASET_ID}

  You can now run Lambda tests with:
    make docker-lambda-baseline
    make docker-lambda-drift
    make docker-lambda-quality
""")


if __name__ == "__main__":
    asyncio.run(main())

