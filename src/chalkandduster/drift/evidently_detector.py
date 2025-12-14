"""
Chalk and Duster - Evidently Drift Detector

Drift detection using the Evidently open-source framework.
Supports data drift, target drift, and data quality checks.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import structlog

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import DriftDetectionError
from chalkandduster.drift.models import DriftResult, DriftRunResult

logger = structlog.get_logger()


def convert_decimal_columns(df):
    """Convert Decimal type columns to float64 for Evidently."""
    from decimal import Decimal
    df = df.copy()
    for col in df.columns:
        # Check if any value is a Decimal
        if df[col].apply(lambda x: isinstance(x, Decimal)).any():
            df[col] = df[col].astype(float)
        # Also convert object columns that look numeric
        elif df[col].dtype == object:
            try:
                df[col] = df[col].astype(float)
            except (ValueError, TypeError):
                pass  # Keep as-is if conversion fails
    return df


@dataclass
class EvidentlyConfig:
    """Configuration for Evidently drift detection."""

    drift_threshold: float = 0.1
    stattest: str = "ks"  # ks, chisquare, z, wasserstein, psi
    columns: Optional[List[str]] = None
    categorical_columns: Optional[List[str]] = None
    numerical_columns: Optional[List[str]] = None


class EvidentlyDriftDetector:
    """
    Drift detector using the Evidently framework.

    Provides sophisticated drift detection with support for:
    - Data drift (distribution changes)
    - Target drift (label distribution changes)
    - Feature importance drift
    - Statistical tests (KS, Chi-square, Wasserstein, PSI)

    Reference data is automatically fetched using Snowflake Time Travel.
    The time travel offset can be configured in drift_yaml:
    - time_travel_days: Number of days to look back (default: 1)

    Example drift_yaml:
        time_travel_days: 7  # Compare with data from 7 days ago
        monitors:
          - name: amount_drift
            type: distribution
            column: AMOUNT
    """

    def __init__(
        self,
        snowflake_connector: Any,
        config: Optional[EvidentlyConfig] = None,
    ):
        """
        Initialize the Evidently drift detector.

        Args:
            snowflake_connector: Snowflake connector for data access
            config: Evidently configuration
        """
        self.connector = snowflake_connector
        self.config = config or EvidentlyConfig(
            drift_threshold=settings.EVIDENTLY_DRIFT_THRESHOLD,
            stattest=settings.EVIDENTLY_STATTEST,
        )
    
    async def detect(
        self,
        dataset_id: UUID,
        drift_yaml: str,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        run_id: Optional[UUID] = None,
        tenant_id: Optional[UUID] = None,
    ) -> DriftRunResult:
        """
        Detect drift using Evidently with Snowflake Time Travel for reference data.

        Reference data is automatically fetched using Snowflake Time Travel.
        Configure the time travel offset in drift_yaml:
        - time_travel_days: Number of days to look back (default: 1)

        Args:
            dataset_id: The dataset ID
            drift_yaml: Drift detection YAML configuration
            table_name: The table to analyze
            database: Optional database name
            schema: Optional schema name
            run_id: Optional run ID
            tenant_id: Optional tenant ID (unused, kept for compatibility)

        Returns:
            DriftRunResult with detection results
        """
        run_id = run_id or uuid4()
        started_at = datetime.utcnow()

        logger.info(
            "Starting Evidently drift detection",
            run_id=str(run_id),
            dataset_id=str(dataset_id),
            table=table_name,
        )

        try:
            import yaml

            config = yaml.safe_load(drift_yaml)
            monitors = config.get("monitors", [])
            time_travel_days = config.get("time_travel_days", 1)

            # Fetch current data
            current_data = await self._fetch_data(table_name, database, schema)

            # Get reference data using Snowflake Time Travel
            reference_data = await self._get_reference_data_time_travel(
                table_name=table_name,
                database=database,
                schema=schema,
                time_travel_days=time_travel_days,
            )

            # Run Evidently drift detection
            results, html_report = await self._run_evidently_detection(
                baseline_data=reference_data,
                current_data=current_data,
                monitors=monitors,
            )

            return DriftRunResult(
                run_id=run_id,
                dataset_id=dataset_id,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                status="completed",
                results=results,
                html_report=html_report,
            )

        except Exception as e:
            logger.error("Evidently drift detection failed", error=str(e))
            raise DriftDetectionError(f"Evidently drift detection failed: {e}")

    async def _fetch_data(
        self,
        table_name: str,
        database: Optional[str],
        schema: Optional[str],
        where_clause: Optional[str] = None,
        limit: int = 10000,
    ):
        """Fetch data from Snowflake as pandas DataFrame."""
        import pandas as pd

        full_table_name = self._build_table_name(table_name, database, schema)
        query = f"SELECT * FROM {full_table_name}"

        if where_clause:
            query += f" WHERE {where_clause}"

        query += f" LIMIT {limit}"

        result = await self.connector.execute_query(query)
        return pd.DataFrame(result)

    async def _fetch_data_from_query(self, query: str):
        """Fetch data using a custom SQL query."""
        import pandas as pd

        result = await self.connector.execute_query(query)
        return pd.DataFrame(result)

    async def _get_reference_data_time_travel(
        self,
        table_name: str,
        database: Optional[str],
        schema: Optional[str],
        time_travel_days: int = 1,
        limit: int = 10000,
    ):
        """
        Get reference data using Snowflake Time Travel.

        Uses Snowflake's Time Travel feature to query historical data
        as it existed at a specific point in time.

        For LocalStack (development), Time Travel is not supported, so we
        use current data as a fallback for testing purposes only.

        Args:
            table_name: Current table name
            database: Database name
            schema: Schema name
            time_travel_days: Number of days to look back (default: 1)
            limit: Maximum rows to fetch

        Returns:
            Pandas DataFrame with reference data from the past

        Raises:
            DriftDetectionError: If Time Travel query fails
        """
        import pandas as pd

        full_table_name = self._build_table_name(table_name, database, schema)

        # Check if we're using LocalStack (development mode)
        is_localstack = self._is_localstack_connection()

        if is_localstack:
            # LocalStack doesn't support Time Travel - use current data for dev/testing
            logger.warning(
                "LocalStack detected - Time Travel not supported. "
                "Using current data as reference for development testing.",
                table=full_table_name,
                time_travel_days=time_travel_days,
            )
            return await self._fetch_data(table_name, database, schema)

        # Production Snowflake - use Time Travel
        # Calculate the offset in seconds (negative for past)
        offset_seconds = time_travel_days * 24 * 60 * 60

        # Build Time Travel query using OFFSET
        # OFFSET uses seconds in the past
        query = (
            f"SELECT * FROM {full_table_name} "
            f"AT(OFFSET => -{offset_seconds}) "
            f"LIMIT {limit}"
        )

        logger.info(
            "Using Snowflake Time Travel for reference data",
            table=full_table_name,
            time_travel_days=time_travel_days,
            offset_seconds=offset_seconds,
        )

        try:
            result = await self.connector.execute_query(query)
            df = pd.DataFrame(result)

            if df.empty:
                logger.warning(
                    "Time Travel returned no data, table may be new or data was purged",
                    table=full_table_name,
                    time_travel_days=time_travel_days,
                )
                raise DriftDetectionError(
                    f"No historical data available for table {full_table_name} "
                    f"at {time_travel_days} day(s) ago. Snowflake Time Travel "
                    "requires data to exist at the specified time point."
                )

            logger.info(
                "Fetched reference data via Time Travel",
                rows=len(df),
                time_travel_days=time_travel_days,
            )
            return df

        except Exception as e:
            error_msg = str(e)
            # Handle specific Snowflake Time Travel errors
            if "does not exist or not authorized" in error_msg.lower():
                raise DriftDetectionError(
                    f"Table {full_table_name} does not exist or Time Travel "
                    "is not available. Ensure the table exists and Time Travel "
                    "retention is configured."
                )
            elif "insufficient data retention" in error_msg.lower():
                raise DriftDetectionError(
                    f"Insufficient Time Travel retention for {full_table_name}. "
                    f"Requested {time_travel_days} days but data may have been purged. "
                    "Check Snowflake DATA_RETENTION_TIME_IN_DAYS setting."
                )
            else:
                # Re-raise with context
                raise DriftDetectionError(
                    f"Snowflake Time Travel query failed: {error_msg}"
                )

    def _is_localstack_connection(self) -> bool:
        """Check if we're connected to LocalStack Snowflake emulator."""
        # Check for is_localstack_mode property (db.snowflake.connector)
        if hasattr(self.connector, 'is_localstack_mode'):
            return bool(self.connector.is_localstack_mode)
        # Check for _use_localstack attribute (db.snowflake.connector)
        if hasattr(self.connector, '_use_localstack'):
            return bool(self.connector._use_localstack)
        # Check for use_localstack attribute (connectors.snowflake)
        if hasattr(self.connector, 'use_localstack'):
            return bool(self.connector.use_localstack)
        return False

    def _build_table_name(
        self,
        table_name: str,
        database: Optional[str],
        schema: Optional[str],
    ) -> str:
        """Build fully qualified table name."""
        parts = []
        if database:
            parts.append(database)
        if schema:
            parts.append(schema)
        parts.append(table_name)
        return ".".join(parts)

    async def _run_evidently_detection(
        self,
        baseline_data,
        current_data,
        monitors: List[Dict[str, Any]],
    ) -> tuple:
        """
        Run Evidently drift detection.

        Uses Evidently 0.7+ API with Dataset and Report for drift analysis.
        Returns tuple of (results, html_report).
        """
        results = []
        html_report = None

        try:
            from evidently import Dataset, Report
            from evidently.metrics import ValueDrift, DriftedColumnsCount
            from evidently.presets import DataDriftPreset
            import io

            baseline_data = convert_decimal_columns(baseline_data)
            current_data = convert_decimal_columns(current_data)

            # Create Evidently Dataset objects from pandas DataFrames
            ref_dataset = Dataset.from_pandas(baseline_data)
            cur_dataset = Dataset.from_pandas(current_data)

            # Generate native Evidently DataDrift report for HTML
            try:
                drift_report = Report([DataDriftPreset()])
                drift_snapshot = drift_report.run(cur_dataset, ref_dataset)
                html_report = drift_snapshot.get_html_str(as_iframe=False)
                logger.info("Generated native Evidently HTML report", size=len(html_report))
            except Exception as e:
                logger.warning("Failed to generate Evidently HTML report", error=str(e))

            for monitor in monitors:
                monitor_name = monitor.get("name", "unnamed")
                drift_type = monitor.get("type", "distribution")
                threshold = monitor.get("threshold", self.config.drift_threshold)
                column = monitor.get("column")

                if drift_type == "distribution" and column:
                    # Per-column drift detection
                    result = self._detect_column_drift(
                        ref_dataset=ref_dataset,
                        cur_dataset=cur_dataset,
                        column=column,
                        monitor_name=monitor_name,
                        threshold=threshold,
                    )
                    results.append(result)

                elif drift_type == "dataset":
                    # Dataset-level drift
                    result = self._detect_dataset_drift(
                        ref_dataset=ref_dataset,
                        cur_dataset=cur_dataset,
                        monitor_name=monitor_name,
                        threshold=threshold,
                    )
                    results.append(result)

                elif drift_type == "schema":
                    # Schema drift (column changes)
                    result = self._detect_schema_drift(
                        baseline_data=baseline_data,
                        current_data=current_data,
                        monitor_name=monitor_name,
                    )
                    results.append(result)

                elif drift_type == "volume":
                    # Row count drift
                    result = self._detect_volume_drift(
                        baseline_data=baseline_data,
                        current_data=current_data,
                        monitor_name=monitor_name,
                        threshold=threshold,
                    )
                    results.append(result)

        except ImportError as e:
            logger.warning("Evidently not available, skipping detection", error=str(e))
            results.append(DriftResult(
                monitor_name="import_error",
                drift_type="error",
                detected=False,
                message=f"Evidently package not installed: {e}",
            ))

        except Exception as e:
            logger.error("Evidently detection error", error=str(e))
            results.append(DriftResult(
                monitor_name="error",
                drift_type="error",
                detected=False,
                message=str(e),
            ))

        return results, html_report

    def _detect_column_drift(
        self,
        ref_dataset,
        cur_dataset,
        column: str,
        monitor_name: str,
        threshold: float,
    ) -> DriftResult:
        """Detect drift in a specific column using Evidently 0.7+ API."""
        try:
            from evidently import Report
            from evidently.metrics import ValueDrift

            # Create report with ValueDrift metric for the specific column
            report = Report([ValueDrift(column=column, threshold=threshold)])
            snapshot = report.run(cur_dataset, ref_dataset)

            # Extract results from snapshot
            result_dict = snapshot.dict()
            metrics = result_dict.get("metrics", [{}])

            if metrics:
                metric = metrics[0]
                # ValueDrift returns p-value; drift detected if p-value < threshold
                p_value = float(metric.get("value", 1.0))
                drift_detected = p_value < threshold

                severity = "critical" if p_value < threshold / 10 else "warning" if drift_detected else "info"

                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="distribution",
                    detected=drift_detected,
                    severity=severity,
                    metric_value=p_value,
                    threshold=threshold,
                    details={
                        "column": column,
                        "p_value": p_value,
                        "method": metric.get("config", {}).get("method", "auto"),
                    },
                    message=f"Column '{column}' drift p-value: {p_value:.6f} (threshold: {threshold})",
                )

            return DriftResult(
                monitor_name=monitor_name,
                drift_type="distribution",
                detected=False,
                message=f"No metrics returned for column '{column}'",
            )

        except Exception as e:
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="distribution",
                detected=False,
                message=f"Column drift detection failed: {e}",
            )

    def _detect_dataset_drift(
        self,
        ref_dataset,
        cur_dataset,
        monitor_name: str,
        threshold: float,
    ) -> DriftResult:
        """Detect dataset-level drift using Evidently 0.7+ API."""
        try:
            from evidently import Report
            from evidently.metrics import DriftedColumnsCount

            # Create report with DriftedColumnsCount metric
            report = Report([DriftedColumnsCount(drift_share=threshold)])
            snapshot = report.run(cur_dataset, ref_dataset)

            # Extract results from snapshot
            result_dict = snapshot.dict()
            metrics = result_dict.get("metrics", [{}])

            if metrics:
                metric = metrics[0]
                value = metric.get("value", {})
                drift_count = int(value.get("count", 0))
                drift_share = float(value.get("share", 0.0))
                drift_detected = drift_share >= threshold

                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="dataset",
                    detected=drift_detected,
                    severity="critical" if drift_detected else "info",
                    metric_value=drift_share,
                    threshold=threshold,
                    details={
                        "drifted_columns_count": drift_count,
                        "drift_share": drift_share,
                    },
                    message=f"Dataset drift: {drift_count} columns drifted ({drift_share:.2%})",
                )

            return DriftResult(
                monitor_name=monitor_name,
                drift_type="dataset",
                detected=False,
                message="No metrics returned for dataset drift",
            )

        except Exception as e:
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="dataset",
                detected=False,
                message=f"Dataset drift detection failed: {e}",
            )

    def _detect_schema_drift(
        self,
        baseline_data,
        current_data,
        monitor_name: str,
    ) -> DriftResult:
        """Detect schema changes between baseline and current data."""
        baseline_cols = set(baseline_data.columns)
        current_cols = set(current_data.columns)

        added = current_cols - baseline_cols
        removed = baseline_cols - current_cols

        drift_detected = bool(added or removed)

        return DriftResult(
            monitor_name=monitor_name,
            drift_type="schema",
            detected=drift_detected,
            severity="critical" if removed else "warning" if added else "info",
            details={
                "added_columns": list(added),
                "removed_columns": list(removed),
            },
            message=f"Schema changes: +{len(added)} added, -{len(removed)} removed",
        )

    def _detect_volume_drift(
        self,
        baseline_data,
        current_data,
        monitor_name: str,
        threshold: float,
    ) -> DriftResult:
        """Detect significant changes in data volume."""
        baseline_count = len(baseline_data)
        current_count = len(current_data)

        if baseline_count == 0:
            change_ratio = float("inf") if current_count > 0 else 0
        else:
            change_ratio = abs(current_count - baseline_count) / baseline_count

        drift_detected = change_ratio > threshold

        return DriftResult(
            monitor_name=monitor_name,
            drift_type="volume",
            detected=drift_detected,
            severity="warning" if drift_detected else "info",
            metric_value=change_ratio,
            threshold=threshold,
            details={
                "baseline_count": baseline_count,
                "current_count": current_count,
                "change_ratio": change_ratio,
            },
            message=f"Volume change: {baseline_count} -> {current_count} ({change_ratio:.2%})",
        )

