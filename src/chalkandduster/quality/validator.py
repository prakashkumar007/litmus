"""
Chalk and Duster - YAML Validator
"""

from typing import Any, Dict, List

import yaml
import structlog

from chalkandduster.api.schemas.dataset import DatasetValidation

logger = structlog.get_logger()

# Valid Soda Core check types
VALID_CHECK_TYPES = {
    "row_count",
    "missing_count",
    "missing_percent",
    "duplicate_count",
    "duplicate_percent",
    "invalid_count",
    "invalid_percent",
    "valid_count",
    "valid_percent",
    "min",
    "max",
    "avg",
    "sum",
    "stddev",
    "variance",
    "freshness",
    "schema",
    "values_in",
    "values_not_in",
    "regex_match",
    "reference",
}

# Valid drift monitor types
VALID_DRIFT_TYPES = {
    "schema",
    "volume",
    "distribution",
    "statistical",
}


def validate_quality_yaml(yaml_content: str) -> DatasetValidation:
    """
    Validate a data quality YAML configuration.
    
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
    
    # Check for required 'checks' key
    if "checks" not in config:
        errors.append({
            "type": "missing_key",
            "message": "Missing required 'checks' key",
        })
        return DatasetValidation(valid=False, errors=errors)
    
    checks = config.get("checks", {})
    
    if not isinstance(checks, dict):
        errors.append({
            "type": "structure_error",
            "message": "'checks' must be a dictionary with table names as keys",
        })
        return DatasetValidation(valid=False, errors=errors)
    
    # Validate each table's checks
    for table_name, table_checks in checks.items():
        if not isinstance(table_checks, list):
            errors.append({
                "type": "structure_error",
                "message": f"Checks for '{table_name}' must be a list",
                "table": table_name,
            })
            continue
        
        for i, check in enumerate(table_checks):
            if isinstance(check, str):
                # Simple check like "row_count > 0"
                check_count += 1
            elif isinstance(check, dict):
                # Complex check
                check_count += 1
                
                # Validate check structure
                for check_name, check_config in check.items():
                    if check_name.startswith("-"):
                        # Disabled check
                        warnings.append({
                            "type": "disabled_check",
                            "message": f"Check '{check_name}' is disabled",
                            "table": table_name,
                        })
            else:
                errors.append({
                    "type": "invalid_check",
                    "message": f"Invalid check at index {i}",
                    "table": table_name,
                })
    
    # Check for optional configurations
    if "configurations" in config:
        configs = config["configurations"]
        if not isinstance(configs, list):
            warnings.append({
                "type": "structure_warning",
                "message": "'configurations' should be a list",
            })
    
    return DatasetValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        check_count=check_count,
    )


def validate_drift_yaml(yaml_content: str) -> DatasetValidation:
    """
    Validate a drift detection YAML configuration.
    
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
        
        # Check for threshold
        if "threshold" not in monitor:
            warnings.append({
                "type": "missing_threshold",
                "message": f"Monitor at index {i} has no threshold - using defaults",
            })
    
    return DatasetValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        monitor_count=monitor_count,
    )

