"""
Chalk and Duster - Report Storage Service

Stores generated HTML reports for drift detection and quality checks in S3.
Reports are accessible via pre-signed URLs.
"""

import io
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import structlog

from chalkandduster.core.config import settings

logger = structlog.get_logger()


class ReportStorage:
    """
    Storage service for HTML reports.

    Reports are stored in S3:
    s3://{bucket}/{tenant_id}/{dataset_id}/reports/
        drift/{run_id}.html
        quality/{run_id}.html
    """

    def __init__(
        self,
        bucket_name: str = None,
        endpoint_url: str = None,
    ):
        """
        Initialize the report storage.

        Args:
            bucket_name: S3 bucket name (default: from settings)
            endpoint_url: S3 endpoint URL (default: from settings, uses LocalStack if set)
        """
        self.bucket_name = bucket_name or getattr(settings, 'REPORTS_BUCKET', 'chalkandduster-reports')
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
                    logger.info("Created reports bucket", bucket=self.bucket_name)
                except Exception as e:
                    logger.warning("Could not create bucket", error=str(e))

        return self._client

    def _get_report_key(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
        report_type: str,
        run_id: str,
    ) -> str:
        """Get the S3 key for a report."""
        return f"{tenant_id}/{dataset_id}/reports/{report_type}/{run_id}.html"

    async def save_report(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
        report_type: str,  # 'drift' or 'quality'
        run_id: str,
        html_content: str,
        metadata: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Save HTML report to S3.

        Args:
            tenant_id: Tenant UUID
            dataset_id: Dataset UUID
            report_type: Type of report ('drift' or 'quality')
            run_id: Run UUID
            html_content: HTML report content
            metadata: Optional metadata

        Returns:
            Dict with save result including location
        """
        key = self._get_report_key(tenant_id, dataset_id, report_type, run_id)

        # Upload to S3
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=html_content.encode('utf-8'),
            ContentType='text/html',
            Metadata={
                'run_id': run_id,
                'report_type': report_type,
                'created_at': datetime.utcnow().isoformat(),
                **(metadata or {}),
            },
        )

        location = f"s3://{self.bucket_name}/{key}"
        
        logger.info(
            "Saved report",
            tenant_id=str(tenant_id),
            dataset_id=str(dataset_id),
            report_type=report_type,
            run_id=run_id,
            location=location,
        )

        return {
            "status": "success",
            "location": location,
            "report_type": report_type,
            "run_id": run_id,
            "created_at": datetime.utcnow().isoformat(),
        }

    def get_presigned_url(
        self,
        tenant_id: UUID,
        dataset_id: UUID,
        report_type: str,
        run_id: str,
        expires_in: int = 3600,
    ) -> Optional[str]:
        """Get a pre-signed URL for downloading a report."""
        key = self._get_report_key(tenant_id, dataset_id, report_type, run_id)
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': key},
                ExpiresIn=expires_in,
            )
            return url
        except Exception as e:
            logger.error("Failed to generate presigned URL", error=str(e))
            return None

