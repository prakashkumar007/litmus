"""
Chalk and Duster - Snowflake Connector

Supports:
- LocalStack Snowflake Emulator (for local development)
- Real Snowflake (for production)
"""

import time
from typing import Any, Dict, List, Optional

import structlog

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import SnowflakeError
from chalkandduster.api.schemas.connection import ConnectionTestResult

logger = structlog.get_logger()


class SnowflakeConnector:
    """
    Snowflake connection manager.

    Supports:
    - LocalStack Snowflake Emulator for local development
    - Real Snowflake connections for production
    """

    def __init__(
        self,
        account: str,
        user: str,
        warehouse: str,
        database: str,
        schema: str,
        role: Optional[str] = None,
        private_key: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        password: Optional[str] = None,
        use_localstack: Optional[bool] = None,
    ):
        self.account = account
        self.user = user
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.private_key = private_key
        self.private_key_passphrase = private_key_passphrase
        self.password = password
        self._connection = None
        self._use_localstack = use_localstack if use_localstack is not None else settings.SNOWFLAKE_USE_LOCALSTACK

    @property
    def is_localstack_mode(self) -> bool:
        """Check if using LocalStack Snowflake emulator."""
        return self._use_localstack

    def _get_localstack_host(self) -> str:
        """Get the LocalStack Snowflake host URL."""
        host = settings.LOCALSTACK_SNOWFLAKE_HOST
        port = settings.LOCALSTACK_SNOWFLAKE_PORT
        return f"{host}:{port}"

    async def connect(self) -> None:
        """Establish connection to Snowflake (or LocalStack emulator)."""
        try:
            import snowflake.connector

            connect_params = {
                "user": self.user,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
            }

            if self.is_localstack_mode:
                # LocalStack Snowflake Emulator configuration
                # See: https://docs.localstack.cloud/snowflake/
                connect_params["account"] = "test"
                connect_params["password"] = self.password or "test"
                connect_params["host"] = settings.LOCALSTACK_SNOWFLAKE_HOST
                connect_params["port"] = settings.LOCALSTACK_SNOWFLAKE_PORT
                # Disable SSL for local development
                connect_params["protocol"] = "http"
                connect_params["insecure_mode"] = True

                logger.info(
                    "Connecting to LocalStack Snowflake emulator",
                    host=settings.LOCALSTACK_SNOWFLAKE_HOST,
                    port=settings.LOCALSTACK_SNOWFLAKE_PORT,
                )
            else:
                # Real Snowflake connection
                connect_params["account"] = self.account

                if self.role:
                    connect_params["role"] = self.role

                if self.private_key:
                    # Key-pair authentication
                    from cryptography.hazmat.backends import default_backend
                    from cryptography.hazmat.primitives import serialization

                    p_key = serialization.load_pem_private_key(
                        self.private_key.encode(),
                        password=self.private_key_passphrase.encode() if self.private_key_passphrase else None,
                        backend=default_backend(),
                    )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption(),
                    )

                    connect_params["private_key"] = pkb
                elif self.password:
                    connect_params["password"] = self.password

            self._connection = snowflake.connector.connect(**connect_params)
            logger.info(
                "Connected to Snowflake",
                account=self.account,
                localstack=self.is_localstack_mode,
            )

        except Exception as e:
            logger.error("Failed to connect to Snowflake", error=str(e))
            raise SnowflakeError(f"Connection failed: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from Snowflake")
    
    async def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results.

        Works with both real Snowflake and LocalStack emulator.
        """
        if not self._connection:
            await self.connect()

        try:
            cursor = self._connection.cursor()
            cursor.execute(query, params or {})

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                results = []

            cursor.close()
            return results
        except Exception as e:
            logger.error("Query execution failed", error=str(e), query=query[:100])
            raise SnowflakeError(f"Query failed: {str(e)}")
    
    async def get_table_schema(
        self, database: str, schema: str, table: str
    ) -> List[Dict[str, Any]]:
        """Get schema information for a table."""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
            ORDER BY ORDINAL_POSITION
        """.format(database=database)
        
        return await self.execute_query(query, {"schema": schema, "table": table})


async def test_snowflake_connection(connection) -> ConnectionTestResult:
    """Test a Snowflake connection (works with LocalStack emulator or real Snowflake)."""
    start_time = time.time()

    try:
        connector = SnowflakeConnector(
            account=connection.account,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=connection.warehouse,
            database=connection.database_name,
            schema=connection.schema_name,
            role=connection.role_name,
        )

        await connector.connect()

        # Test queries
        version = await connector.execute_query("SELECT CURRENT_VERSION()")
        warehouse = await connector.execute_query("SELECT CURRENT_WAREHOUSE()")
        database = await connector.execute_query("SELECT CURRENT_DATABASE()")
        role = await connector.execute_query("SELECT CURRENT_ROLE()")

        await connector.disconnect()

        latency_ms = (time.time() - start_time) * 1000

        mode = "LocalStack" if settings.SNOWFLAKE_USE_LOCALSTACK else "Snowflake"

        return ConnectionTestResult(
            success=True,
            message=f"Connection successful ({mode})",
            latency_ms=latency_ms,
            snowflake_version=version[0].get("CURRENT_VERSION()") if version else None,
            current_warehouse=warehouse[0].get("CURRENT_WAREHOUSE()") if warehouse else None,
            current_database=database[0].get("CURRENT_DATABASE()") if database else None,
            current_role=role[0].get("CURRENT_ROLE()") if role else None,
        )

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        return ConnectionTestResult(
            success=False,
            message=f"Connection failed: {str(e)}",
            latency_ms=latency_ms,
        )

