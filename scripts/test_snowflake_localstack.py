#!/usr/bin/env python3
"""
Manual test script for Snowflake LocalStack integration.

This script tests the Snowflake connector against the LocalStack Snowflake emulator.
Run inside Docker: docker compose exec api python scripts/test_snowflake_localstack.py

Environment Variables (passed from docker-compose.yaml):
- SNOWFLAKE_USE_LOCALSTACK: true/false
- LOCALSTACK_SNOWFLAKE_HOST: hostname (localstack)
- LOCALSTACK_SNOWFLAKE_PORT: port (4566)
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.
"""

import asyncio
import os
import sys


def print_header(text: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}")


def test_raw_snowflake_connection() -> bool:
    """Test raw snowflake-connector-python connection to LocalStack."""
    print_header("Testing Raw Snowflake Connection")

    try:
        import snowflake.connector as sf

        # LocalStack Snowflake uses snowflake.localhost.localstack.cloud DNS
        # Inside Docker, we need to use the localstack container hostname
        localstack_host = os.environ.get("LOCALSTACK_SNOWFLAKE_HOST", "localstack")

        # Try different connection approaches
        print(f"\n  LocalStack Host: {localstack_host}")

        # Approach 1: Direct connection using host parameter (as per LocalStack docs)
        # The key is to use the special DNS name that LocalStack provides
        print("\n  Attempting connection with snowflake.localhost.localstack.cloud...")

        try:
            conn = sf.connect(
                user="test",
                password="test",
                account="test",
                host="snowflake.localhost.localstack.cloud",
            )
            print("  ✅ Connected using snowflake.localhost.localstack.cloud!")

            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            print(f"  ✅ CURRENT_VERSION(): {result[0] if result else 'Unknown'}")

            # Create the test database and warehouse for subsequent tests
            print("\n  Setting up test database and warehouse...")
            cursor.execute("CREATE WAREHOUSE IF NOT EXISTS test_warehouse")
            print("  ✅ Created warehouse: test_warehouse")
            cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
            print("  ✅ Created database: test_db")
            cursor.execute("USE DATABASE test_db")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public")
            print("  ✅ Created schema: public")

            cursor.close()
            conn.close()
            return True

        except Exception as e1:
            print(f"  ❌ Failed with snowflake.localhost.localstack.cloud: {e1}")

            # Approach 2: Try with localstack hostname directly (for Docker internal)
            print(f"\n  Attempting connection with {localstack_host}:4566...")
            try:
                conn = sf.connect(
                    user="test",
                    password="test",
                    account="test",
                    database="test",
                    host=localstack_host,
                    port=4566,
                    protocol="http",
                    insecure_mode=True,
                )
                print(f"  ✅ Connected using {localstack_host}:4566!")

                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                result = cursor.fetchone()
                print(f"  ✅ CURRENT_VERSION(): {result[0] if result else 'Unknown'}")

                cursor.close()
                conn.close()
                return True

            except Exception as e2:
                print(f"  ❌ Failed with {localstack_host}:4566: {e2}")

                # Approach 3: Try snowflake.localstack (Docker DNS alias)
                print("\n  Attempting connection with snowflake.localstack...")
                try:
                    conn = sf.connect(
                        user="test",
                        password="test",
                        account="test",
                        database="test",
                        host="snowflake.localstack",
                    )
                    print("  ✅ Connected using snowflake.localstack!")

                    cursor = conn.cursor()
                    cursor.execute("SELECT CURRENT_VERSION()")
                    result = cursor.fetchone()
                    print(f"  ✅ CURRENT_VERSION(): {result[0] if result else 'Unknown'}")

                    cursor.close()
                    conn.close()
                    return True

                except Exception as e3:
                    print(f"  ❌ Failed with snowflake.localstack: {e3}")
                    return False

    except ImportError as e:
        print(f"  ❌ Import Error: {e}")
        print("     Make sure snowflake-connector-python is installed")
        return False


def print_env_config() -> None:
    """Print current environment configuration."""
    print_header("Environment Configuration")
    env_vars = [
        "SNOWFLAKE_USE_LOCALSTACK",
        "LOCALSTACK_SNOWFLAKE_HOST",
        "LOCALSTACK_SNOWFLAKE_PORT",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    for var in env_vars:
        value = os.environ.get(var, "<not set>")
        # Mask password
        if "PASSWORD" in var and value != "<not set>":
            value = "***"
        print(f"  {var}: {value}")


async def test_snowflake_connector() -> bool:
    """Test the Snowflake connector with LocalStack."""
    print_header("Testing Snowflake Connector")
    
    try:
        from chalkandduster.db.snowflake.connector import SnowflakeConnector
        from chalkandduster.core.config import settings
        
        print(f"\n  LocalStack Mode: {settings.SNOWFLAKE_USE_LOCALSTACK}")
        print(f"  Host: {settings.LOCALSTACK_SNOWFLAKE_HOST}")
        print(f"  Port: {settings.LOCALSTACK_SNOWFLAKE_PORT}")
        
        connector = SnowflakeConnector(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            use_localstack=settings.SNOWFLAKE_USE_LOCALSTACK,
        )
        
        print("\n  Connecting to Snowflake...")
        await connector.connect()
        print("  ✅ Connected successfully!")
        
        # Test queries
        print("\n  Running test queries...")
        
        # Query 1: Current version
        result = await connector.execute_query("SELECT CURRENT_VERSION()")
        version = result[0].get("CURRENT_VERSION()") if result else "Unknown"
        print(f"  ✅ CURRENT_VERSION(): {version}")
        
        # Query 2: Current warehouse
        result = await connector.execute_query("SELECT CURRENT_WAREHOUSE()")
        warehouse = result[0].get("CURRENT_WAREHOUSE()") if result else "Unknown"
        print(f"  ✅ CURRENT_WAREHOUSE(): {warehouse}")
        
        # Query 3: Current database
        result = await connector.execute_query("SELECT CURRENT_DATABASE()")
        database = result[0].get("CURRENT_DATABASE()") if result else "Unknown"
        print(f"  ✅ CURRENT_DATABASE(): {database}")
        
        # Disconnect
        await connector.disconnect()
        print("  ✅ Disconnected successfully!")
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Import Error: {e}")
        print("     Make sure snowflake-connector-python is installed")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


async def test_create_sample_tables() -> bool:
    """Create sample tables in Snowflake LocalStack."""
    print_header("Creating Sample Tables")
    
    try:
        from chalkandduster.db.snowflake.connector import SnowflakeConnector
        from chalkandduster.core.config import settings
        
        connector = SnowflakeConnector(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            use_localstack=settings.SNOWFLAKE_USE_LOCALSTACK,
        )
        
        await connector.connect()
        
        # Create database and schema
        print("  Creating database and schema...")
        await connector.execute_query("CREATE DATABASE IF NOT EXISTS test_db")
        await connector.execute_query("USE DATABASE test_db")
        await connector.execute_query("CREATE SCHEMA IF NOT EXISTS public")
        await connector.execute_query("USE SCHEMA public")
        print("  ✅ Database and schema created")
        
        # Create orders table
        print("  Creating orders table...")
        await connector.execute_query("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date TIMESTAMP_NTZ,
                amount DECIMAL(10, 2),
                status VARCHAR(50)
            )
        """)
        print("  ✅ Orders table created")
        
        # Insert sample data
        print("  Inserting sample data...")
        await connector.execute_query("""
            INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES
            (1, 1, '2024-01-15 10:30:00', 49.98, 'completed'),
            (2, 2, '2024-01-16 14:20:00', 29.99, 'completed'),
            (3, 1, '2024-01-17 09:15:00', 99.97, 'pending'),
            (4, 3, '2024-01-18 16:45:00', 19.99, 'completed'),
            (5, 2, '2024-01-19 11:00:00', 79.98, 'cancelled')
        """)
        print("  ✅ Sample data inserted")
        
        # Query the data
        print("  Querying data...")
        results = await connector.execute_query("SELECT * FROM orders ORDER BY order_id")
        print(f"  ✅ Found {len(results)} rows")
        for row in results:
            print(f"     {row}")
        
        await connector.disconnect()
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


async def main():
    """Run all tests."""
    print_header("Snowflake LocalStack Integration Test")
    print("  This script tests the Snowflake connection via LocalStack emulator")

    # Print environment config
    print_env_config()

    # Test 0: Raw snowflake connection (to find working connection params)
    success0 = test_raw_snowflake_connection()

    if not success0:
        print_header("Test Summary")
        print("  Raw Connection Test: ❌ FAILED")
        print("\n  ⚠️  Could not establish raw connection. Check LocalStack setup.")
        return 1

    # Test 1: Basic connection using our connector
    success1 = await test_snowflake_connector()

    # Test 2: Create tables and insert data
    if success1:
        success2 = await test_create_sample_tables()
    else:
        success2 = False

    # Summary
    print_header("Test Summary")
    print(f"  Raw Connection Test:     {'✅ PASSED' if success0 else '❌ FAILED'}")
    print(f"  Connector Test:          {'✅ PASSED' if success1 else '❌ FAILED'}")
    print(f"  Create Tables Test:      {'✅ PASSED' if success2 else '❌ FAILED'}")

    if success0 and success1 and success2:
        print("\n  🎉 All tests passed! Snowflake LocalStack integration is working.")
        return 0
    else:
        print("\n  ⚠️  Some tests failed. Check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

