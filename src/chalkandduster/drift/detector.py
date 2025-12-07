"""
Chalk and Duster - Drift Detector
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import structlog
import yaml

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import DriftDetectionError
from chalkandduster.drift.statistical import (
    calculate_psi,
    calculate_chi_square,
    calculate_zscore,
    calculate_schema_diff,
)

logger = structlog.get_logger()


class DriftResult:
    """Result of a single drift check."""
    
    def __init__(
        self,
        monitor_name: str,
        drift_type: str,
        detected: bool,
        severity: str = "warning",
        metric_value: Optional[float] = None,
        threshold: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ):
        self.monitor_name = monitor_name
        self.drift_type = drift_type
        self.detected = detected
        self.severity = severity
        self.metric_value = metric_value
        self.threshold = threshold
        self.details = details or {}
        self.message = message
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "monitor_name": self.monitor_name,
            "drift_type": self.drift_type,
            "detected": self.detected,
            "severity": self.severity,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "details": self.details,
            "message": self.message,
        }


class DriftRunResult:
    """Result of a drift detection run."""
    
    def __init__(
        self,
        run_id: UUID,
        dataset_id: UUID,
        started_at: datetime,
        completed_at: Optional[datetime] = None,
        status: str = "pending",
        results: Optional[List[DriftResult]] = None,
    ):
        self.run_id = run_id
        self.dataset_id = dataset_id
        self.started_at = started_at
        self.completed_at = completed_at
        self.status = status
        self.results = results or []
    
    @property
    def total_monitors(self) -> int:
        return len(self.results)
    
    @property
    def drift_detected_count(self) -> int:
        return sum(1 for r in self.results if r.detected)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": str(self.run_id),
            "dataset_id": str(self.dataset_id),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "total_monitors": self.total_monitors,
            "drift_detected_count": self.drift_detected_count,
            "results": [r.to_dict() for r in self.results],
        }


class DriftDetector:
    """
    Detects data drift using various statistical methods.
    
    Supports:
    - Schema drift (column additions, removals, type changes)
    - Volume drift (row count anomalies)
    - Distribution drift (PSI for numeric, Chi-square for categorical)
    """
    
    def __init__(
        self,
        snowflake_connector,
        baseline_days: int = 30,
        mock_mode: bool = None,
    ):
        self.connector = snowflake_connector
        self.baseline_days = baseline_days
        self.mock_mode = mock_mode if mock_mode is not None else settings.SNOWFLAKE_MOCK_MODE
    
    async def detect(
        self,
        dataset_id: UUID,
        drift_yaml: str,
        table_name: str,
        database: str,
        schema: str,
    ) -> DriftRunResult:
        """
        Run drift detection for a dataset.
        
        Args:
            dataset_id: The dataset ID
            drift_yaml: The drift detection YAML configuration
            table_name: The table to check
            database: Database name
            schema: Schema name
            
        Returns:
            DriftRunResult with detection results
        """
        run_id = uuid4()
        started_at = datetime.utcnow()
        
        logger.info(
            "Starting drift detection",
            run_id=str(run_id),
            dataset_id=str(dataset_id),
            table=table_name,
        )
        
        try:
            config = yaml.safe_load(drift_yaml)
            monitors = config.get("monitors", [])
            
            results = []
            for monitor in monitors:
                result = await self._run_monitor(
                    monitor=monitor,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                )
                results.append(result)
            
            return DriftRunResult(
                run_id=run_id,
                dataset_id=dataset_id,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                status="completed",
                results=results,
            )
            
        except Exception as e:
            logger.error("Drift detection failed", error=str(e))
            raise DriftDetectionError(f"Drift detection failed: {str(e)}")
    
    async def _run_monitor(
        self,
        monitor: Dict[str, Any],
        table_name: str,
        database: str,
        schema: str,
    ) -> DriftResult:
        """Run a single drift monitor."""
        drift_type = monitor.get("type", "unknown")
        monitor_name = monitor.get("name", f"{drift_type}_monitor")
        threshold = monitor.get("threshold", self._default_threshold(drift_type))
        
        if self.mock_mode:
            return self._mock_monitor(monitor_name, drift_type, threshold)
        
        if drift_type == "schema":
            return await self._detect_schema_drift(monitor_name, table_name, database, schema)
        elif drift_type == "volume":
            return await self._detect_volume_drift(monitor_name, table_name, database, schema, threshold)
        elif drift_type == "distribution":
            column = monitor.get("column")
            return await self._detect_distribution_drift(
                monitor_name, table_name, database, schema, column, threshold
            )
        else:
            return DriftResult(
                monitor_name=monitor_name,
                drift_type=drift_type,
                detected=False,
                message=f"Unknown drift type: {drift_type}",
            )
    
    def _default_threshold(self, drift_type: str) -> float:
        """Get default threshold for drift type."""
        defaults = {
            "schema": 0,  # Any change is drift
            "volume": 3.0,  # Z-score threshold
            "distribution": 0.25,  # PSI threshold
        }
        return defaults.get(drift_type, 0.25)
    
    def _mock_monitor(
        self, monitor_name: str, drift_type: str, threshold: float
    ) -> DriftResult:
        """Return mock drift result for testing."""
        import random
        detected = random.random() < 0.3  # 30% chance of drift
        
        return DriftResult(
            monitor_name=monitor_name,
            drift_type=drift_type,
            detected=detected,
            severity="warning" if detected else "info",
            metric_value=random.uniform(0, 0.5),
            threshold=threshold,
            message=f"Mock {drift_type} drift {'detected' if detected else 'not detected'}",
        )
    
    async def _detect_schema_drift(
        self, monitor_name: str, table_name: str, database: str, schema: str
    ) -> DriftResult:
        """Detect schema changes by comparing current schema with baseline."""
        try:
            # Get current schema
            current_schema = await self.connector.get_table_schema(table_name)

            if not current_schema:
                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="schema",
                    detected=False,
                    severity="info",
                    message=f"No schema found for table {table_name}",
                )

            # For now, we compare against stored baseline (future: store in DB)
            # If no baseline exists, this is the first run - no drift
            baseline_schema = getattr(self, '_schema_baseline', None)

            if baseline_schema is None:
                # Store current as baseline for future comparisons
                self._schema_baseline = current_schema
                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="schema",
                    detected=False,
                    severity="info",
                    details={"columns": len(current_schema)},
                    message=f"Baseline established with {len(current_schema)} columns",
                )

            # Compare schemas
            diff = calculate_schema_diff(baseline_schema, current_schema)
            has_drift = len(diff.get("added", [])) > 0 or len(diff.get("removed", [])) > 0 or len(diff.get("modified", [])) > 0

            return DriftResult(
                monitor_name=monitor_name,
                drift_type="schema",
                detected=has_drift,
                severity="critical" if has_drift else "info",
                details=diff,
                message=f"Schema drift: {len(diff.get('added', []))} added, {len(diff.get('removed', []))} removed, {len(diff.get('modified', []))} modified" if has_drift else "No schema drift detected",
            )

        except Exception as e:
            logger.error("Schema drift detection failed", error=str(e))
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="schema",
                detected=False,
                severity="error",
                message=f"Schema drift detection failed: {str(e)}",
            )

    async def _detect_volume_drift(
        self, monitor_name: str, table_name: str, database: str, schema: str, threshold: float
    ) -> DriftResult:
        """Detect volume anomalies using Z-score comparison."""
        try:
            # Get current row count
            current_count = await self.connector.get_row_count(table_name)

            # Get historical counts (future: from stored history)
            # For now, use a simple comparison with stored baseline
            baseline_count = getattr(self, '_volume_baseline', None)
            historical_counts = getattr(self, '_volume_history', [])

            if baseline_count is None:
                # Store current as baseline
                self._volume_baseline = current_count
                self._volume_history = [current_count]
                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="volume",
                    detected=False,
                    severity="info",
                    metric_value=float(current_count),
                    threshold=threshold,
                    details={"current_count": current_count},
                    message=f"Baseline established with {current_count} rows",
                )

            # Calculate Z-score
            if len(historical_counts) >= 2:
                z_score = calculate_zscore(current_count, historical_counts)
            else:
                # Simple percentage change if not enough history
                pct_change = abs(current_count - baseline_count) / max(baseline_count, 1) * 100
                z_score = pct_change / 10  # Approximate Z-score

            has_drift = abs(z_score) > threshold

            # Update history
            self._volume_history.append(current_count)
            if len(self._volume_history) > self.baseline_days:
                self._volume_history = self._volume_history[-self.baseline_days:]

            return DriftResult(
                monitor_name=monitor_name,
                drift_type="volume",
                detected=has_drift,
                severity="warning" if has_drift else "info",
                metric_value=abs(z_score),
                threshold=threshold,
                details={
                    "current_count": current_count,
                    "baseline_count": baseline_count,
                    "z_score": z_score,
                },
                message=f"Volume drift detected: Z-score {z_score:.2f} exceeds threshold {threshold}" if has_drift else f"Volume stable: {current_count} rows (Z-score: {z_score:.2f})",
            )

        except Exception as e:
            logger.error("Volume drift detection failed", error=str(e))
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="volume",
                detected=False,
                severity="error",
                message=f"Volume drift detection failed: {str(e)}",
            )

    async def _detect_distribution_drift(
        self, monitor_name: str, table_name: str, database: str, schema: str,
        column: Optional[str], threshold: float
    ) -> DriftResult:
        """Detect distribution drift using PSI (Population Stability Index)."""
        if not column:
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="distribution",
                detected=False,
                severity="error",
                message="Column name required for distribution drift detection",
            )

        try:
            # Get current distribution
            current_dist = await self.connector.get_value_distribution(table_name, column)

            if not current_dist:
                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="distribution",
                    detected=False,
                    severity="info",
                    message=f"No data found for column {column}",
                )

            # Get baseline distribution (future: from stored history)
            baseline_key = f'_dist_baseline_{column}'
            baseline_dist = getattr(self, baseline_key, None)

            if baseline_dist is None:
                # Store current as baseline
                setattr(self, baseline_key, current_dist)
                return DriftResult(
                    monitor_name=monitor_name,
                    drift_type="distribution",
                    detected=False,
                    severity="info",
                    details={"unique_values": len(current_dist)},
                    message=f"Baseline established for column {column} with {len(current_dist)} unique values",
                )

            # Calculate PSI
            psi = calculate_psi(baseline_dist, current_dist)
            has_drift = psi > threshold

            return DriftResult(
                monitor_name=monitor_name,
                drift_type="distribution",
                detected=has_drift,
                severity="warning" if has_drift else "info",
                metric_value=psi,
                threshold=threshold,
                details={
                    "column": column,
                    "psi": psi,
                    "current_unique_values": len(current_dist),
                    "baseline_unique_values": len(baseline_dist),
                },
                message=f"Distribution drift detected: PSI {psi:.4f} exceeds threshold {threshold}" if has_drift else f"Distribution stable for {column} (PSI: {psi:.4f})",
            )

        except Exception as e:
            logger.error("Distribution drift detection failed", error=str(e), column=column)
            return DriftResult(
                monitor_name=monitor_name,
                drift_type="distribution",
                detected=False,
                severity="error",
                message=f"Distribution drift detection failed: {str(e)}",
            )

