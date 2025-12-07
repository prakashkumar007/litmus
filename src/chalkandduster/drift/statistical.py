"""
Chalk and Duster - Statistical Drift Detection Methods
"""

import math
from typing import Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()


def calculate_psi(
    baseline: List[Dict],
    current: List[Dict],
    buckets: int = 10,
) -> float:
    """
    Calculate Population Stability Index (PSI) for distribution drift.

    PSI measures how much a distribution has shifted from a baseline.

    Args:
        baseline: Baseline distribution as list of {value, count} dicts
        current: Current distribution as list of {value, count} dicts
        buckets: Number of buckets for binning (unused for categorical)

    Returns:
        PSI value (float)

    Interpretation:
        PSI < 0.1: No significant change
        0.1 <= PSI < 0.25: Moderate change
        PSI >= 0.25: Significant change
    """
    if not baseline or not current:
        return 0.0

    # Convert to dictionaries for comparison
    baseline_dict = {}
    current_dict = {}

    for item in baseline:
        val = item.get("VALUE") or item.get("value")
        cnt = item.get("COUNT") or item.get("count", 0)
        if val is not None:
            baseline_dict[str(val)] = cnt

    for item in current:
        val = item.get("VALUE") or item.get("value")
        cnt = item.get("COUNT") or item.get("count", 0)
        if val is not None:
            current_dict[str(val)] = cnt

    # Get all values
    all_values = set(baseline_dict.keys()) | set(current_dict.keys())

    if not all_values:
        return 0.0

    # Calculate totals
    baseline_total = sum(baseline_dict.values()) or 1
    current_total = sum(current_dict.values()) or 1

    # Calculate PSI
    epsilon = 0.0001
    psi = 0.0

    for val in all_values:
        baseline_pct = (baseline_dict.get(val, 0) / baseline_total) + epsilon
        current_pct = (current_dict.get(val, 0) / current_total) + epsilon
        psi += (current_pct - baseline_pct) * math.log(current_pct / baseline_pct)

    return abs(psi)


def calculate_chi_square(
    baseline_counts: Dict[str, int],
    current_counts: Dict[str, int],
) -> Tuple[float, float, str]:
    """
    Calculate Chi-Square statistic for categorical drift.
    
    Args:
        baseline_counts: Category counts from baseline
        current_counts: Category counts from current data
        
    Returns:
        Tuple of (chi_square value, p_value, interpretation)
    """
    # Get all categories
    all_categories = set(baseline_counts.keys()) | set(current_counts.keys())
    
    if len(all_categories) < 2:
        return 0.0, 1.0, "insufficient_categories"
    
    # Calculate totals
    baseline_total = sum(baseline_counts.values())
    current_total = sum(current_counts.values())
    
    if baseline_total == 0 or current_total == 0:
        return 0.0, 1.0, "insufficient_data"
    
    # Calculate chi-square
    chi_square = 0.0
    for category in all_categories:
        baseline_count = baseline_counts.get(category, 0)
        current_count = current_counts.get(category, 0)
        
        # Expected count based on baseline proportion
        expected = (baseline_count / baseline_total) * current_total
        
        if expected > 0:
            chi_square += ((current_count - expected) ** 2) / expected
    
    # Degrees of freedom
    df = len(all_categories) - 1
    
    # Approximate p-value using chi-square distribution
    # For simplicity, using critical values
    # df=1: 3.84 (0.05), 6.63 (0.01)
    # df=2: 5.99 (0.05), 9.21 (0.01)
    # df=3: 7.81 (0.05), 11.34 (0.01)
    
    critical_05 = 3.84 + (df - 1) * 2.0  # Rough approximation
    critical_01 = 6.63 + (df - 1) * 2.5
    
    if chi_square < critical_05:
        p_value = 0.1  # Not significant
        interpretation = "no_significant_change"
    elif chi_square < critical_01:
        p_value = 0.05
        interpretation = "moderate_change"
    else:
        p_value = 0.01
        interpretation = "significant_change"
    
    return chi_square, p_value, interpretation


def calculate_zscore(
    current_value: float,
    historical_values: List[float],
) -> float:
    """
    Calculate Z-score for volume anomaly detection.

    Args:
        current_value: Current metric value
        historical_values: List of historical values for baseline

    Returns:
        Z-score (float)
    """
    if not historical_values:
        return 0.0

    n = len(historical_values)
    mean = sum(historical_values) / n

    if n < 2:
        # Not enough data for std dev, use simple comparison
        if mean == 0:
            return 0.0 if current_value == 0 else float('inf')
        return abs(current_value - mean) / mean * 3  # Scale to approximate Z-score

    # Calculate standard deviation
    variance = sum((x - mean) ** 2 for x in historical_values) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0

    if std == 0:
        if current_value == mean:
            return 0.0
        else:
            return float("inf")

    z_score = (current_value - mean) / std
    return z_score


def calculate_schema_diff(
    baseline_schema: List[Dict],
    current_schema: List[Dict],
) -> Dict[str, List[str]]:
    """
    Calculate schema differences between baseline and current.

    Args:
        baseline_schema: List of column info dicts from Snowflake
        current_schema: List of column info dicts from Snowflake

    Returns:
        Dictionary with added, removed, and modified columns
    """
    # Convert to dictionaries {column_name: data_type}
    def schema_to_dict(schema_list: List[Dict]) -> Dict[str, str]:
        result = {}
        for col in schema_list:
            col_name = col.get("COLUMN_NAME") or col.get("column_name")
            data_type = col.get("DATA_TYPE") or col.get("data_type")
            if col_name:
                result[col_name] = data_type
        return result

    baseline_dict = schema_to_dict(baseline_schema)
    current_dict = schema_to_dict(current_schema)

    baseline_cols = set(baseline_dict.keys())
    current_cols = set(current_dict.keys())

    added = list(current_cols - baseline_cols)
    removed = list(baseline_cols - current_cols)

    modified = []
    for col in baseline_cols & current_cols:
        if baseline_dict[col] != current_dict[col]:
            modified.append(f"{col}: {baseline_dict[col]} -> {current_dict[col]}")

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
    }

