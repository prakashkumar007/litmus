"""
Chalk and Duster - Baseline Storage Service

Stores and retrieves baseline data for Evidently drift detection.
Supports both LocalStack S3 (local development) and AWS S3 (production).
"""

import io
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import structlog

from chalkandduster.core.config import settings

logger = structlog.get_logger()


class BaselineStorage:
    """
    Storage service for Evidently baseline data.

    Baselines are stored as Parquet files in S3:
    s3://{bucket}/{tenant_id}/{dataset_id}/baseline.parquet

    Supports versioning with metadata:
    s3://{bucket}/{tenant_id}/{dataset_id}/baseline_v{version}.parquet
    """

    def __init__(
        self,
        bucket_name: str = None,
        endpoint_url: str = None,
    ):
        """
        Initialize the baseline storage.

        Args:
            bucket_name: S3 bucket name (default: from settings)
            endpoint_url: S3 endpoint URL (default: from settings, uses LocalStack if set)
        """
        self.bucket_name = bucket_name or getattr(settings, 'BASELINE_BUCKET', 'chalkandduster-baselines')
        self.endpoint_url = endpoint_url or getattr(settings, 'AWS_ENDPOINT_URL', None)
        self._client = None

    @property
    def client(self):
        """Lazy-load boto3 S3 client."""
        if self._client is None:
            import boto3

            client_kwargs = {}
            if self.endpoint_url:
                client_kwargs['endpoint_url'] = self.endpoint_url
                # LocalStack doesn't need real credentials
                client_kwargs['aws_access_key_id'] = 'test'
                client_kwargs['aws_secret_access_key'] = 'test'
                client_kwargs['region_name'] = 'us-east-1'

            self._client = boto3.client('s3', **client_kwargs)

            # Ensure bucket exists (for LocalStack)
            try:
                self._client.head_bucket(Bucket=self.bucket_name)
            except Exception:
                try:
                    self._client.create_bucket(Bucket=self.bucket_name)
                    logger.info("Created baseline bucket", bucket=self.bucket_name)
                except Exception as e:
                    logger.warning("Could not create bucket", error=str(e))

        return self._client

    def _get_baseline_key(self, tenant_id: UUID, dataset_id: UUID, version: int = None) -> str:
        """Get the S3 key for a baseline."""
        if version:
            return f"{tenant_id}/{dataset_id}/baseline_v{version}.parquet"
        return f"{tenant_id}/{dataset_id}/baseline.parquet"

    def _get_metadata_key(self, tenant_id: UUID, dataset_id: UUID) -> str:
        """Get the S3 key for baseline metadata."""
        return f"{tenant_id}/{dataset_id}/metadata.json"

    async def save_baseline(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
        data,  # pandas DataFrame
        metadata: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Save baseline data to S3.

        Args:
            tenant_id: Tenant UUID
            dataset_id: Dataset UUID
            data: Pandas DataFrame with baseline data
            metadata: Optional metadata (columns, row count, etc.)

        Returns:
            Dict with save result including version and location
        """
        import json
        import pandas as pd

        key = self._get_baseline_key(tenant_id, dataset_id)

        # Convert DataFrame to Parquet bytes
        buffer = io.BytesIO()
        data.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        # Upload to S3
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream',
        )

        # Save metadata
        meta = {
            "tenant_id": str(tenant_id),
            "dataset_id": str(dataset_id),
            "created_at": datetime.utcnow().isoformat(),
            "row_count": len(data),
            "columns": list(data.columns),
            "dtypes": {col: str(dtype) for col, dtype in data.dtypes.items()},
            **(metadata or {}),
        }

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self._get_metadata_key(tenant_id, dataset_id),
            Body=json.dumps(meta),
            ContentType='application/json',
        )

        logger.info(
            "Saved baseline",
            tenant_id=str(tenant_id),
            dataset_id=str(dataset_id),
            rows=len(data),
            columns=len(data.columns),
        )

        return {
            "status": "success",
            "location": f"s3://{self.bucket_name}/{key}",
            "row_count": len(data),
            "columns": list(data.columns),
            "created_at": meta["created_at"],
        }


    async def get_metadata(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
    ) -> Optional[Dict[str, Any]]:
        """Get baseline metadata."""
        import json

        key = self._get_metadata_key(tenant_id, dataset_id)

        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception:
            return None

    async def delete_baseline(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
    ) -> bool:
        """Delete baseline data."""
        key = self._get_baseline_key(tenant_id, dataset_id)
        metadata_key = self._get_metadata_key(tenant_id, dataset_id)

        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            self.client.delete_object(Bucket=self.bucket_name, Key=metadata_key)
            logger.info("Deleted baseline", dataset_id=str(dataset_id))
            return True
        except Exception as e:
            logger.error("Failed to delete baseline", error=str(e))
            return False

    async def baseline_exists(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
    ) -> bool:
        """Check if baseline exists."""
        key = self._get_baseline_key(tenant_id, dataset_id)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception:
            return False

    async def load_baseline(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
    ):
        """
        Load baseline data from S3.

        Returns:
            Pandas DataFrame or None if not found
        """
        import pandas as pd

        key = self._get_baseline_key(tenant_id, dataset_id)

        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            buffer = io.BytesIO(response['Body'].read())
            return pd.read_parquet(buffer)
        except self.client.exceptions.NoSuchKey:
            logger.info("No baseline found", dataset_id=str(dataset_id))
            return None
        except Exception as e:
            logger.error("Failed to load baseline", error=str(e))
            return None

