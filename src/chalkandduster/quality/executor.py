"""
Chalk and Duster - Quality Check Executor
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import structlog
import yaml

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import QualityCheckError

logger = structlog.get_logger()


class CheckResult:
    """Result of a single quality check."""
    
    def __init__(
        self,
        check_name: str,
        check_type: str,
        status: str,
        severity: str = "warning",
        expected: Optional[str] = None,
        actual: Optional[str] = None,
        failure_count: int = 0,
        message: Optional[str] = None,
    ):
        self.check_name = check_name
        self.check_type = check_type
        self.status = status
        self.severity = severity
        self.expected = expected
        self.actual = actual
        self.failure_count = failure_count
        self.message = message
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "status": self.status,
            "severity": self.severity,
            "expected": self.expected,
            "actual": self.actual,
            "failure_count": self.failure_count,
            "message": self.message,
        }


class QualityRunResult:
    """Result of a quality check run."""
    
    def __init__(
        self,
        run_id: UUID,
        dataset_id: UUID,
        started_at: datetime,
        completed_at: Optional[datetime] = None,
        status: str = "pending",
        results: Optional[List[CheckResult]] = None,
    ):
        self.run_id = run_id
        self.dataset_id = dataset_id
        self.started_at = started_at
        self.completed_at = completed_at
        self.status = status
        self.results = results or []
    
    @property
    def total_checks(self) -> int:
        return len(self.results)
    
    @property
    def passed_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "passed")
    
    @property
    def failed_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")
    
    @property
    def error_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "error")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": str(self.run_id),
            "dataset_id": str(self.dataset_id),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "error_checks": self.error_checks,
            "results": [r.to_dict() for r in self.results],
        }


class QualityExecutor:
    """
    Executes data quality checks using Soda Core.
    
    Supports both real Soda Core execution and mock mode for testing.
    """
    
    def __init__(
        self,
        connection_config: Dict[str, Any],
        mock_mode: bool = None,
    ):
        self.connection_config = connection_config
        self.mock_mode = mock_mode if mock_mode is not None else settings.SNOWFLAKE_MOCK_MODE
    
    async def execute(
        self,
        dataset_id: UUID,
        quality_yaml: str,
        table_name: str,
    ) -> QualityRunResult:
        """
        Execute quality checks for a dataset.
        
        Args:
            dataset_id: The dataset ID
            quality_yaml: The quality check YAML configuration
            table_name: The table to check
            
        Returns:
            QualityRunResult with check results
        """
        run_id = uuid4()
        started_at = datetime.utcnow()
        
        logger.info(
            "Starting quality check",
            run_id=str(run_id),
            dataset_id=str(dataset_id),
            table=table_name,
        )
        
        if self.mock_mode:
            return await self._execute_mock(run_id, dataset_id, quality_yaml, started_at)
        
        return await self._execute_soda(run_id, dataset_id, quality_yaml, table_name, started_at)
    
    async def _execute_mock(
        self,
        run_id: UUID,
        dataset_id: UUID,
        quality_yaml: str,
        started_at: datetime,
    ) -> QualityRunResult:
        """Execute mock quality checks for testing."""
        # Parse YAML to count checks
        config = yaml.safe_load(quality_yaml)
        checks = config.get("checks", {})
        
        results = []
        for table_name, table_checks in checks.items():
            for i, check in enumerate(table_checks):
                check_name = f"check_{i}" if isinstance(check, str) else list(check.keys())[0]
                
                # Simulate some failures for testing
                status = "passed" if i % 3 != 0 else "failed"
                
                results.append(CheckResult(
                    check_name=check_name,
                    check_type="mock",
                    status=status,
                    severity="warning" if status == "failed" else "info",
                    expected="mock_expected",
                    actual="mock_actual",
                    message=f"Mock check result for {check_name}",
                ))
        
        return QualityRunResult(
            run_id=run_id,
            dataset_id=dataset_id,
            started_at=started_at,
            completed_at=datetime.utcnow(),
            status="completed",
            results=results,
        )
    
    async def _execute_soda(
        self,
        run_id: UUID,
        dataset_id: UUID,
        quality_yaml: str,
        table_name: str,
        started_at: datetime,
    ) -> QualityRunResult:
        """Execute real Soda Core quality checks."""
        try:
            from soda.scan import Scan
            
            # Create temporary files for Soda configuration
            with tempfile.TemporaryDirectory() as tmpdir:
                tmppath = Path(tmpdir)
                
                # Write checks YAML
                checks_file = tmppath / "checks.yml"
                checks_file.write_text(quality_yaml)
                
                # Write configuration YAML
                config_yaml = self._generate_soda_config()
                config_file = tmppath / "configuration.yml"
                config_file.write_text(config_yaml)
                
                # Create and run scan
                scan = Scan()
                scan.set_data_source_name("snowflake")
                scan.add_configuration_yaml_file(str(config_file))
                scan.add_sodacl_yaml_file(str(checks_file))
                scan.execute()
                
                # Parse results
                results = self._parse_soda_results(scan)
                
                return QualityRunResult(
                    run_id=run_id,
                    dataset_id=dataset_id,
                    started_at=started_at,
                    completed_at=datetime.utcnow(),
                    status="completed" if not scan.has_error_logs() else "failed",
                    results=results,
                )
                
        except Exception as e:
            logger.error("Soda Core execution failed", error=str(e))
            raise QualityCheckError(f"Quality check failed: {str(e)}")
    
    def _generate_soda_config(self) -> str:
        """Generate Soda Core configuration YAML."""
        config = {
            "data_source snowflake": {
                "type": "snowflake",
                "account": self.connection_config.get("account"),
                "username": self.connection_config.get("user"),
                "password": self.connection_config.get("password", ""),
                "database": self.connection_config.get("database"),
                "schema": self.connection_config.get("schema"),
                "warehouse": self.connection_config.get("warehouse"),
                "role": self.connection_config.get("role"),
            }
        }
        return yaml.dump(config)
    
    def _parse_soda_results(self, scan) -> List[CheckResult]:
        """Parse Soda Core scan results."""
        results = []
        
        for check in scan.get_checks_fail():
            results.append(CheckResult(
                check_name=check.name,
                check_type=check.check_type,
                status="failed",
                severity="critical" if check.is_critical else "warning",
                expected=str(check.expected_value) if hasattr(check, "expected_value") else None,
                actual=str(check.actual_value) if hasattr(check, "actual_value") else None,
                message=check.message if hasattr(check, "message") else None,
            ))
        
        for check in scan.get_checks_pass():
            results.append(CheckResult(
                check_name=check.name,
                check_type=check.check_type,
                status="passed",
                severity="info",
            ))
        
        return results

