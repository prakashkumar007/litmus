"""
Chalk and Duster - YAML Validator

Validates Great Expectations and Evidently YAML configurations.
"""

from typing import Any, Dict, List

import structlog
import yaml

from chalkandduster.core.schemas import DatasetValidation

logger = structlog.get_logger()

# Valid Great Expectations expectation types
VALID_EXPECTATION_TYPES = {
    "expect_table_row_count_to_be_between",
    "expect_table_row_count_to_equal",
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex",
    "expect_column_values_to_be_between",
    "expect_column_min_to_be_between",
    "expect_column_max_to_be_between",
    "expect_column_mean_to_be_between",
    "expect_column_median_to_be_between",
    "expect_column_stdev_to_be_between",
    "expect_column_distinct_values_to_be_in_set",
    "expect_column_distinct_values_to_contain_set",
    "expect_column_proportion_of_unique_values_to_be_between",
    "expect_table_columns_to_match_ordered_list",
    "expect_table_columns_to_match_set",
}

# Valid Evidently drift monitor types
VALID_DRIFT_TYPES = {
    "schema",
    "volume",
    "distribution",
    "dataset",
}

# Valid Evidently statistical tests
VALID_STATTESTS = {
    "ks",
    "chisquare",
    "z",
    "wasserstein",
    "psi",
    "jensenshannon",
    "kl_div",
    "anderson",
    "cramer_von_mises",
}


def validate_quality_yaml(yaml_content: str) -> DatasetValidation:
    """
    Validate a Great Expectations quality YAML configuration.

    Expected format:
    ```yaml
    expectation_suite_name: my_suite
    expectations:
      - expectation_type: expect_table_row_count_to_be_between
        kwargs:
          min_value: 1
      - expectation_type: expect_column_values_to_not_be_null
        kwargs:
          column: id
    ```

    Returns validation result with errors and warnings.
    """
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    check_count = 0

    try:
        config = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return DatasetValidation(
            valid=False,
            errors=[{"type": "parse_error", "message": f"Invalid YAML: {str(e)}"}],
        )

    if not isinstance(config, dict):
        return DatasetValidation(
            valid=False,
            errors=[{"type": "structure_error", "message": "YAML must be a dictionary"}],
        )

    # Check for required 'expectations' key
    if "expectations" not in config:
        errors.append({
            "type": "missing_key",
            "message": "Missing required 'expectations' key",
        })
        return DatasetValidation(valid=False, errors=errors)

    expectations = config.get("expectations", [])

    if not isinstance(expectations, list):
        errors.append({
            "type": "structure_error",
            "message": "'expectations' must be a list",
        })
        return DatasetValidation(valid=False, errors=errors)

    # Check for expectation suite name
    if "expectation_suite_name" not in config:
        warnings.append({
            "type": "missing_suite_name",
            "message": "No 'expectation_suite_name' specified, using default",
        })

    # Validate each expectation
    for i, expectation in enumerate(expectations):
        if not isinstance(expectation, dict):
            errors.append({
                "type": "invalid_expectation",
                "message": f"Expectation at index {i} must be a dictionary",
            })
            continue

        check_count += 1

        # Check required fields
        if "expectation_type" not in expectation:
            errors.append({
                "type": "missing_field",
                "message": f"Expectation at index {i} missing 'expectation_type' field",
            })
        else:
            exp_type = expectation["expectation_type"]
            if exp_type not in VALID_EXPECTATION_TYPES:
                warnings.append({
                    "type": "unknown_expectation",
                    "message": f"Unknown expectation type '{exp_type}' at index {i}",
                })

        # Check for kwargs
        if "kwargs" not in expectation:
            warnings.append({
                "type": "missing_kwargs",
                "message": f"Expectation at index {i} has no 'kwargs'",
            })

    # Check for data asset configuration
    if "data_asset_type" in config:
        logger.info("Data asset type specified", data_asset_type=config["data_asset_type"])

    return DatasetValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        check_count=check_count,
    )


def validate_drift_yaml(yaml_content: str) -> DatasetValidation:
    """
    Validate an Evidently drift detection YAML configuration.

    Expected format:
    ```yaml
    monitors:
      - name: data_drift_monitor
        type: distribution
        column: feature_column
        threshold: 0.1
        stattest: ks
      - name: dataset_drift_monitor
        type: dataset
        threshold: 0.3
      - name: schema_monitor
        type: schema
      - name: volume_monitor
        type: volume
        threshold: 0.2
    ```

    Returns validation result with errors and warnings.
    """
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    monitor_count = 0

    try:
        config = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return DatasetValidation(
            valid=False,
            errors=[{"type": "parse_error", "message": f"Invalid YAML: {str(e)}"}],
        )

    if not isinstance(config, dict):
        return DatasetValidation(
            valid=False,
            errors=[{"type": "structure_error", "message": "YAML must be a dictionary"}],
        )

    # Check for required 'monitors' key
    if "monitors" not in config:
        errors.append({
            "type": "missing_key",
            "message": "Missing required 'monitors' key",
        })
        return DatasetValidation(valid=False, errors=errors)

    monitors = config.get("monitors", [])

    if not isinstance(monitors, list):
        errors.append({
            "type": "structure_error",
            "message": "'monitors' must be a list",
        })
        return DatasetValidation(valid=False, errors=errors)

    # Validate each monitor
    for i, monitor in enumerate(monitors):
        if not isinstance(monitor, dict):
            errors.append({
                "type": "invalid_monitor",
                "message": f"Monitor at index {i} must be a dictionary",
            })
            continue

        monitor_count += 1

        # Check required fields
        if "type" not in monitor:
            errors.append({
                "type": "missing_field",
                "message": f"Monitor at index {i} missing 'type' field",
            })
        elif monitor["type"] not in VALID_DRIFT_TYPES:
            errors.append({
                "type": "invalid_type",
                "message": f"Invalid drift type '{monitor['type']}' at index {i}",
                "valid_types": list(VALID_DRIFT_TYPES),
            })

        # Check for name
        if "name" not in monitor:
            warnings.append({
                "type": "missing_name",
                "message": f"Monitor at index {i} has no 'name' - using auto-generated name",
            })

        # Validate stattest if provided
        if "stattest" in monitor:
            if monitor["stattest"] not in VALID_STATTESTS:
                warnings.append({
                    "type": "unknown_stattest",
                    "message": f"Unknown statistical test '{monitor['stattest']}' at index {i}",
                    "valid_stattests": list(VALID_STATTESTS),
                })

        # Check for threshold
        if "threshold" not in monitor:
            warnings.append({
                "type": "missing_threshold",
                "message": f"Monitor at index {i} has no threshold - using Evidently defaults",
            })

        # For distribution monitors, column is required
        if monitor.get("type") == "distribution" and "column" not in monitor:
            errors.append({
                "type": "missing_column",
                "message": f"Distribution monitor at index {i} requires 'column' field",
            })

    return DatasetValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        monitor_count=monitor_count,
    )

