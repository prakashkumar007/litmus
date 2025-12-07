"""
Chalk and Duster - Snowflake Connector
"""

from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import structlog
import snowflake.connector
from snowflake.connector import DictCursor

from chalkandduster.core.config import settings

logger = structlog.get_logger()


class SnowflakeConnector:
    """
    Production-ready Snowflake connector with connection pooling.

    Supports both real Snowflake and LocalStack Snowflake emulator.
    Supports both password and key-pair authentication.
    """

    def __init__(
        self,
        account: str,
        user: str,
        warehouse: str,
        database: str,
        schema: str,
        password: Optional[str] = None,
        private_key: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        role: Optional[str] = None,
        use_localstack: bool = None,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.private_key = private_key
        self.private_key_passphrase = private_key_passphrase
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.use_localstack = use_localstack if use_localstack is not None else settings.SNOWFLAKE_USE_LOCALSTACK
        self._connection = None

    @classmethod
    def from_connection_model(cls, connection, credentials: Dict[str, str]) -> "SnowflakeConnector":
        """Create connector from a Connection database model."""
        return cls(
            account=connection.account,
            user=credentials.get("user", ""),
            password=credentials.get("password"),
            private_key=credentials.get("private_key"),
            private_key_passphrase=credentials.get("private_key_passphrase"),
            warehouse=connection.warehouse,
            database=connection.database_name,
            schema=connection.schema_name,
            role=connection.role_name,
        )
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for Snowflake."""
        params = {
            "user": self.user,
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }

        # Use private key authentication if available, otherwise password
        if self.private_key:
            # Load private key for key-pair authentication
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_bytes = self.private_key.encode()
            passphrase = self.private_key_passphrase.encode() if self.private_key_passphrase else None

            p_key = serialization.load_pem_private_key(
                private_key_bytes,
                password=passphrase,
                backend=default_backend(),
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            params["private_key"] = pkb
        elif self.password:
            params["password"] = self.password

        if self.role:
            params["role"] = self.role

        # LocalStack Snowflake emulator settings
        if self.use_localstack:
            params["host"] = settings.LOCALSTACK_SNOWFLAKE_HOST
            params["port"] = settings.LOCALSTACK_SNOWFLAKE_PORT
            params["protocol"] = "https"
            params["insecure_mode"] = True

        return params
    
    def connect(self):
        """Create a Snowflake connection."""
        if self._connection is None:
            params = self._get_connection_params()
            logger.info(
                "Connecting to Snowflake",
                account=self.account,
                database=self.database,
                schema=self.schema,
                use_localstack=self.use_localstack,
            )
            self._connection = snowflake.connector.connect(**params)
        return self._connection
    
    def close(self):
        """Close the Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    @asynccontextmanager
    async def get_cursor(self):
        """Get a cursor for executing queries."""
        conn = self.connect()
        cursor = conn.cursor(DictCursor)
        try:
            yield cursor
        finally:
            cursor.close()
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        async with self.get_cursor() as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchall()
    
    async def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        query = f'SELECT COUNT(*) as cnt FROM "{self.database}"."{self.schema}"."{table_name}"'
        results = await self.execute_query(query)
        return results[0]["CNT"] if results else 0
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a table."""
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
            FROM "{self.database}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.schema}' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        return await self.execute_query(query)
    
    async def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get statistics for a column."""
        query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT "{column_name}") as distinct_count,
                SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) as null_count
            FROM "{self.database}"."{self.schema}"."{table_name}"
        """
        results = await self.execute_query(query)
        return results[0] if results else {}
    
    async def get_value_distribution(self, table_name: str, column_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get value distribution for a column."""
        query = f"""
            SELECT "{column_name}" as value, COUNT(*) as count
            FROM "{self.database}"."{self.schema}"."{table_name}"
            GROUP BY "{column_name}"
            ORDER BY count DESC
            LIMIT {limit}
        """
        return await self.execute_query(query)
    
    async def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            await self.execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False

