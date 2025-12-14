"""
Chalk and Duster - Great Expectations Executor

Data quality executor using Great Expectations framework.
Uses pandas DataFrames for validation (compatible with GX 0.18+).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import structlog
import yaml

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import QualityCheckError
from chalkandduster.quality.models import CheckResult, QualityRunResult

logger = structlog.get_logger()


class GreatExpectationsExecutor:
    """
    Executes data quality checks using Great Expectations.

    Uses pandas DataFrames for validation, which is compatible with all GX versions.
    Fetches data from Snowflake using snowflake-connector-python, then validates
    the DataFrame using GX expectations.
    """

    def __init__(
        self,
        connection_config: Dict[str, Any],
        data_context_root: Optional[str] = None,
    ):
        """
        Initialize the Great Expectations executor.

        Args:
            connection_config: Snowflake connection configuration
            data_context_root: Path to Great Expectations data context (unused)
        """
        self.connection_config = connection_config
        self.data_context_root = data_context_root or settings.GE_DATA_CONTEXT_ROOT
        self._connector = None

    def _get_snowflake_connector(self):
        """Get Snowflake connector for data fetching."""
        if self._connector is None:
            import snowflake.connector as sf

            # Check for LocalStack Snowflake emulator
            use_localstack = settings.SNOWFLAKE_USE_LOCALSTACK
            host = None
            if use_localstack:
                localstack_host = settings.LOCALSTACK_SNOWFLAKE_HOST or "localhost"
                # If running inside Docker, use the special LocalStack DNS name
                # that resolves correctly within the Docker network
                if localstack_host == "localstack":
                    # Inside Docker: use snowflake.localstack (resolves within network)
                    host = "snowflake.localstack"
                else:
                    # Outside Docker: use the standard LocalStack DNS name
                    host = "snowflake.localhost.localstack.cloud"
                logger.info("Connecting to LocalStack Snowflake emulator", host=host)

            self._connector = sf.connect(
                user=self.connection_config.get("user"),
                password=self.connection_config.get("password"),
                account=self.connection_config.get("account"),
                warehouse=self.connection_config.get("warehouse"),
                database=self.connection_config.get("database"),
                schema=self.connection_config.get("schema"),
                role=self.connection_config.get("role"),
                host=host,
            )
            logger.info("Connected to Snowflake for quality checks")

        return self._connector
    
    async def execute(
        self,
        dataset_id: UUID,
        quality_yaml: str,
        table_name: str,
    ) -> QualityRunResult:
        """
        Execute quality checks using Great Expectations.
        
        Args:
            dataset_id: The dataset ID
            quality_yaml: Quality check YAML configuration
            table_name: The table to validate
            
        Returns:
            QualityRunResult with validation results
        """
        run_id = uuid4()
        started_at = datetime.utcnow()
        
        logger.info(
            "Starting Great Expectations validation",
            run_id=str(run_id),
            dataset_id=str(dataset_id),
            table=table_name,
        )
        
        try:
            # Parse YAML config and convert to expectations
            config = yaml.safe_load(quality_yaml)
            expectations = self._convert_yaml_to_expectations(config, table_name)

            # Execute validation
            results, html_report = await self._run_validation(table_name, expectations)

            return QualityRunResult(
                run_id=run_id,
                dataset_id=dataset_id,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                status="completed",
                results=results,
                html_report=html_report,
            )

        except Exception as e:
            logger.error("Great Expectations validation failed", error=str(e))
            raise QualityCheckError(f"Validation failed: {e}")
    
    def _convert_yaml_to_expectations(
        self,
        config: Dict[str, Any],
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Convert YAML config to Great Expectations expectations.

        Supports two formats:
        1. Native GE format with 'expectations' key containing expectation dicts
        2. Soda-style YAML with 'checks' key containing check strings/dicts
        """
        expectations = []

        # Check for native Great Expectations format first
        if "expectations" in config:
            # Native GE format: list of expectation dicts
            for exp in config.get("expectations", []):
                if isinstance(exp, dict) and "expectation_type" in exp:
                    expectations.append(exp)
            return expectations

        # Fall back to Soda-style format
        checks = config.get("checks", {})

        # Get checks for the specified table or use generic checks
        table_checks = checks.get(table_name, checks.get(f"checks for {table_name}", []))

        for check in table_checks:
            if isinstance(check, str):
                expectation = self._parse_check_string(check)
                if expectation:
                    expectations.append(expectation)
            elif isinstance(check, dict):
                for check_name, check_config in check.items():
                    expectation = self._parse_check_dict(check_name, check_config)
                    if expectation:
                        expectations.append(expectation)

        return expectations
    
    def _parse_check_string(self, check: str) -> Optional[Dict[str, Any]]:
        """Parse a simple check string into a GE expectation."""
        check = check.strip()
        
        # row_count > 0
        if check.startswith("row_count"):
            return {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1},
            }
        
        # missing_count(column) = 0
        if "missing_count" in check:
            import re
            match = re.search(r"missing_count\((\w+)\)", check)
            if match:
                column = match.group(1)
                return {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": column},
                }
        
        # duplicate_count(column) = 0
        if "duplicate_count" in check:
            import re
            match = re.search(r"duplicate_count\((\w+)\)", check)
            if match:
                column = match.group(1)
                return {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": column},
                }

        return None

    def _parse_check_dict(
        self,
        check_name: str,
        check_config: Any,
    ) -> Optional[Dict[str, Any]]:
        """Parse a dictionary check into a GE expectation."""
        # Handle invalid_count with valid values
        if "invalid_count" in check_name:
            import re
            match = re.search(r"invalid_count\((\w+)\)", check_name)
            if match and isinstance(check_config, dict):
                column = match.group(1)
                valid_values = check_config.get("valid values", [])
                return {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": column, "value_set": valid_values},
                }

        # Handle freshness checks
        if "freshness" in check_name:
            import re
            match = re.search(r"freshness\((\w+)\)", check_name)
            if match:
                column = match.group(1)
                return {
                    "expectation_type": "expect_column_max_to_be_between",
                    "kwargs": {
                        "column": column,
                        "min_value": None,  # Will be computed at runtime
                    },
                }

        return None

    async def _run_validation(
        self,
        table_name: str,
        expectations: List[Dict[str, Any]],
    ) -> tuple:
        """
        Run Great Expectations validation using pandas DataFrame.

        Fetches data from Snowflake into a DataFrame, then validates
        each expectation individually using GX's expect_* methods.
        Returns tuple of (results, html_report).
        """
        results = []
        html_report = None

        try:
            import pandas as pd

            # Fetch data from Snowflake
            df = await self._fetch_data(table_name)

            if df.empty:
                results.append(CheckResult(
                    check_name="data_fetch",
                    check_type="great_expectations",
                    status="warning",
                    severity="warning",
                    message=f"No data found in table {table_name}",
                ))
                return results, html_report

            # Validate each expectation
            for exp in expectations:
                check_result = self._validate_expectation(df, exp)
                results.append(check_result)

            # Generate native Great Expectations HTML report
            try:
                html_report = self._generate_html_report(df, expectations, results, table_name)
                logger.info("Generated native Great Expectations HTML report")
            except Exception as e:
                logger.warning("Failed to generate GE HTML report", error=str(e))

        except ImportError as e:
            logger.warning("Great Expectations not available", error=str(e))
            results.append(CheckResult(
                check_name="import_error",
                check_type="great_expectations",
                status="error",
                severity="critical",
                message="Great Expectations package not installed",
            ))

        except Exception as e:
            logger.error("GE validation error", error=str(e))
            results.append(CheckResult(
                check_name="validation_error",
                check_type="great_expectations",
                status="error",
                severity="critical",
                message=str(e),
            ))

        return results, html_report

    def _generate_html_report(
        self,
        df,
        expectations: List[Dict[str, Any]],
        results: List[CheckResult],
        table_name: str,
    ) -> Optional[str]:
        """
        Generate native Great Expectations HTML validation report using Data Docs.

        Uses GX 1.x File Data Context with ValidationDefinition and Data Docs
        to generate the native HTML report.
        """
        import glob
        import os
        import tempfile

        import great_expectations as gx

        try:
            # Create a temporary directory for file context
            with tempfile.TemporaryDirectory() as tmpdir:
                # Create a file context (required for Data Docs)
                context = gx.get_context(mode="file", project_root_dir=tmpdir)

                # Add pandas datasource and asset
                datasource = context.data_sources.add_pandas(name="chalkandduster_ds")
                asset = datasource.add_dataframe_asset(name=table_name)
                batch_def = asset.add_batch_definition_whole_dataframe("batch_def")

                # Create expectation suite from our expectations
                suite = gx.ExpectationSuite(name=f"{table_name}_suite")

                for exp in expectations:
                    exp_type = exp.get("expectation_type")
                    kwargs = exp.get("kwargs", {})

                    try:
                        gx_exp = self._create_gx_expectation(exp_type, kwargs)
                        if gx_exp:
                            suite.add_expectation(gx_exp)
                    except Exception as e:
                        logger.debug(
                            "Could not create GX expectation",
                            exp_type=exp_type,
                            error=str(e),
                        )

                # Add at least one expectation if suite is empty
                if len(suite.expectations) == 0:
                    suite.add_expectation(
                        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0)
                    )

                suite = context.suites.add(suite)

                # Create validation definition
                val_def = gx.ValidationDefinition(
                    name=f"{table_name}_validation",
                    data=batch_def,
                    suite=suite,
                )
                val_def = context.validation_definitions.add(val_def)

                # Run validation
                val_def.run(batch_parameters={"dataframe": df})

                # Build data docs
                context.build_data_docs()

                # Find and read the validation HTML file
                data_docs_dir = os.path.join(
                    tmpdir,
                    "gx",
                    "uncommitted",
                    "data_docs",
                    "local_site",
                    "validations",
                )
                html_files = glob.glob(
                    os.path.join(data_docs_dir, "**/*.html"), recursive=True
                )

                if html_files:
                    # Read the most recent validation HTML
                    with open(html_files[0], "r") as f:
                        html_content = f.read()

                    logger.info(
                        "Generated native GX HTML report", size=len(html_content)
                    )
                    return html_content

                logger.warning("No GX Data Docs HTML files found")
                return None

        except Exception as e:
            logger.warning("Failed to generate GX native HTML report", error=str(e))
            return None

    def _create_gx_expectation(self, exp_type: str, kwargs: Dict[str, Any]):
        """Create a GX expectation object from type and kwargs."""
        import great_expectations as gx

        if exp_type == "expect_table_row_count_to_be_between":
            return gx.expectations.ExpectTableRowCountToBeBetween(**kwargs)
        elif exp_type == "expect_column_values_to_not_be_null":
            return gx.expectations.ExpectColumnValuesToNotBeNull(**kwargs)
        elif exp_type == "expect_column_values_to_be_unique":
            return gx.expectations.ExpectColumnValuesToBeUnique(**kwargs)
        elif exp_type == "expect_column_values_to_be_in_set":
            return gx.expectations.ExpectColumnValuesToBeInSet(**kwargs)
        elif exp_type == "expect_column_max_to_be_between":
            return gx.expectations.ExpectColumnMaxToBeBetween(**kwargs)
        else:
            logger.debug("Unknown GX expectation type", exp_type=exp_type)
            return None

    async def _fetch_data(self, table_name: str, limit: int = 10000):
        """Fetch data from Snowflake as pandas DataFrame."""
        import pandas as pd

        conn = self._get_snowflake_connector()
        cursor = conn.cursor()

        database = self.connection_config.get("database")
        schema = self.connection_config.get("schema")

        # Build fully qualified table name
        parts = []
        if database:
            parts.append(database)
        if schema:
            parts.append(schema)
        parts.append(table_name)
        full_table_name = ".".join(parts)

        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        return pd.DataFrame(rows, columns=columns)

    def _validate_expectation(
        self,
        df,
        expectation: Dict[str, Any],
    ) -> CheckResult:
        """
        Validate a single expectation against a DataFrame.

        Uses pure pandas validation since GX 1.x removed the PandasDataset API.
        """
        exp_type = expectation.get("expectation_type")
        kwargs = expectation.get("kwargs", {})

        try:
            # Implement common expectations using pandas
            if exp_type == "expect_table_row_count_to_be_between":
                min_val = kwargs.get("min_value", 0)
                max_val = kwargs.get("max_value", float("inf"))
                row_count = len(df)
                success = min_val <= row_count <= max_val
                return CheckResult(
                    check_name=exp_type,
                    check_type="great_expectations",
                    status="passed" if success else "failed",
                    severity="info" if success else "warning",
                    expected=f"row_count between {min_val} and {max_val}",
                    actual=str(row_count),
                    message=f"Row count: {row_count} (expected: {min_val}-{max_val})",
                )

            elif exp_type == "expect_column_values_to_not_be_null":
                column = kwargs.get("column")
                if column not in df.columns:
                    return CheckResult(
                        check_name=exp_type,
                        check_type="great_expectations",
                        status="error",
                        severity="warning",
                        message=f"Column '{column}' not found in table",
                    )
                null_count = df[column].isnull().sum()
                success = null_count == 0
                return CheckResult(
                    check_name=exp_type,
                    check_type="great_expectations",
                    status="passed" if success else "failed",
                    severity="info" if success else "warning",
                    expected="0 null values",
                    actual=str(null_count),
                    message=f"Column '{column}': {null_count} null values",
                )

            elif exp_type == "expect_column_values_to_be_unique":
                column = kwargs.get("column")
                if column not in df.columns:
                    return CheckResult(
                        check_name=exp_type,
                        check_type="great_expectations",
                        status="error",
                        severity="warning",
                        message=f"Column '{column}' not found in table",
                    )
                duplicate_count = df[column].duplicated().sum()
                success = duplicate_count == 0
                return CheckResult(
                    check_name=exp_type,
                    check_type="great_expectations",
                    status="passed" if success else "failed",
                    severity="info" if success else "warning",
                    expected="0 duplicates",
                    actual=str(duplicate_count),
                    message=f"Column '{column}': {duplicate_count} duplicate values",
                )

            elif exp_type == "expect_column_values_to_be_in_set":
                column = kwargs.get("column")
                value_set = kwargs.get("value_set", [])
                if column not in df.columns:
                    return CheckResult(
                        check_name=exp_type,
                        check_type="great_expectations",
                        status="error",
                        severity="warning",
                        message=f"Column '{column}' not found in table",
                    )
                invalid_values = df[~df[column].isin(value_set)][column].unique()
                success = len(invalid_values) == 0
                return CheckResult(
                    check_name=exp_type,
                    check_type="great_expectations",
                    status="passed" if success else "failed",
                    severity="info" if success else "warning",
                    expected=f"values in {value_set}",
                    actual=str(list(invalid_values)[:5]) if len(invalid_values) > 0 else "all valid",
                    message=f"Column '{column}': {len(invalid_values)} invalid values" if not success else f"Column '{column}': all values valid",
                )

            else:
                return CheckResult(
                    check_name=exp_type,
                    check_type="great_expectations",
                    status="error",
                    severity="warning",
                    message=f"Unknown expectation type: {exp_type}",
                )

        except Exception as e:
            return CheckResult(
                check_name=exp_type,
                check_type="great_expectations",
                status="error",
                severity="critical",
                message=f"Expectation failed: {str(e)}",
            )
