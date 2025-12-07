"""
Chalk and Duster - Statistical Drift Detection Tests
"""

import pytest

from chalkandduster.drift.statistical import (
    calculate_psi,
    calculate_chi_square,
    calculate_zscore,
    calculate_schema_diff,
)


class TestPSI:
    """Tests for Population Stability Index calculation."""
    
    def test_identical_distributions(self):
        """Test PSI for identical distributions."""
        baseline = [1.0, 2.0, 3.0, 4.0, 5.0] * 100
        current = [1.0, 2.0, 3.0, 4.0, 5.0] * 100
        
        psi, interpretation = calculate_psi(baseline, current)
        
        assert psi < 0.1
        assert interpretation == "no_significant_change"
    
    def test_moderate_drift(self):
        """Test PSI for moderate distribution shift."""
        baseline = [1.0, 2.0, 3.0, 4.0, 5.0] * 100
        current = [2.0, 3.0, 4.0, 5.0, 6.0] * 100
        
        psi, interpretation = calculate_psi(baseline, current)
        
        # Should detect some change
        assert psi > 0
    
    def test_significant_drift(self):
        """Test PSI for significant distribution shift."""
        baseline = [1.0, 2.0, 3.0] * 100
        current = [10.0, 11.0, 12.0] * 100
        
        psi, interpretation = calculate_psi(baseline, current)
        
        assert psi >= 0.25
        assert interpretation == "significant_change"
    
    def test_empty_baseline(self):
        """Test PSI with empty baseline."""
        psi, interpretation = calculate_psi([], [1.0, 2.0, 3.0])
        
        assert psi == 0.0
        assert interpretation == "insufficient_data"
    
    def test_no_variance(self):
        """Test PSI when all values are the same."""
        baseline = [5.0] * 100
        current = [5.0] * 100
        
        psi, interpretation = calculate_psi(baseline, current)
        
        assert psi == 0.0
        assert interpretation == "no_variance"


class TestChiSquare:
    """Tests for Chi-Square calculation."""
    
    def test_identical_distributions(self):
        """Test Chi-Square for identical categorical distributions."""
        baseline = {"A": 100, "B": 100, "C": 100}
        current = {"A": 100, "B": 100, "C": 100}
        
        chi_sq, p_value, interpretation = calculate_chi_square(baseline, current)
        
        assert chi_sq < 1.0
        assert interpretation == "no_significant_change"
    
    def test_significant_change(self):
        """Test Chi-Square for significant categorical shift."""
        baseline = {"A": 100, "B": 100, "C": 100}
        current = {"A": 10, "B": 200, "C": 90}
        
        chi_sq, p_value, interpretation = calculate_chi_square(baseline, current)
        
        assert chi_sq > 0
    
    def test_new_category(self):
        """Test Chi-Square when new category appears."""
        baseline = {"A": 100, "B": 100}
        current = {"A": 80, "B": 80, "C": 40}
        
        chi_sq, p_value, interpretation = calculate_chi_square(baseline, current)
        
        assert chi_sq > 0
    
    def test_insufficient_categories(self):
        """Test Chi-Square with insufficient categories."""
        baseline = {"A": 100}
        current = {"A": 100}
        
        chi_sq, p_value, interpretation = calculate_chi_square(baseline, current)
        
        assert interpretation == "insufficient_categories"


class TestZScore:
    """Tests for Z-Score calculation."""
    
    def test_normal_value(self):
        """Test Z-Score for normal value."""
        z_score, interpretation = calculate_zscore(
            current_value=100,
            baseline_mean=100,
            baseline_std=10,
        )
        
        assert abs(z_score) < 3.0
        assert interpretation == "normal"
    
    def test_anomaly_high(self):
        """Test Z-Score for high anomaly."""
        z_score, interpretation = calculate_zscore(
            current_value=150,
            baseline_mean=100,
            baseline_std=10,
        )
        
        assert z_score > 3.0
        assert interpretation == "anomaly"
    
    def test_anomaly_low(self):
        """Test Z-Score for low anomaly."""
        z_score, interpretation = calculate_zscore(
            current_value=50,
            baseline_mean=100,
            baseline_std=10,
        )
        
        assert z_score < -3.0
        assert interpretation == "anomaly"
    
    def test_zero_std(self):
        """Test Z-Score with zero standard deviation."""
        z_score, interpretation = calculate_zscore(
            current_value=100,
            baseline_mean=100,
            baseline_std=0,
        )
        
        assert z_score == 0.0
        assert interpretation == "no_change"


class TestSchemaDiff:
    """Tests for schema difference calculation."""
    
    def test_identical_schemas(self):
        """Test schema diff for identical schemas."""
        baseline = {"id": "INTEGER", "name": "VARCHAR"}
        current = {"id": "INTEGER", "name": "VARCHAR"}
        
        diff = calculate_schema_diff(baseline, current)
        
        assert len(diff["added"]) == 0
        assert len(diff["removed"]) == 0
        assert len(diff["type_changed"]) == 0
    
    def test_added_column(self):
        """Test schema diff when column is added."""
        baseline = {"id": "INTEGER"}
        current = {"id": "INTEGER", "name": "VARCHAR"}
        
        diff = calculate_schema_diff(baseline, current)
        
        assert "name" in diff["added"]
        assert len(diff["removed"]) == 0
    
    def test_removed_column(self):
        """Test schema diff when column is removed."""
        baseline = {"id": "INTEGER", "name": "VARCHAR"}
        current = {"id": "INTEGER"}
        
        diff = calculate_schema_diff(baseline, current)
        
        assert "name" in diff["removed"]
        assert len(diff["added"]) == 0
    
    def test_type_changed(self):
        """Test schema diff when column type changes."""
        baseline = {"id": "INTEGER", "amount": "INTEGER"}
        current = {"id": "INTEGER", "amount": "DECIMAL"}
        
        diff = calculate_schema_diff(baseline, current)
        
        assert len(diff["type_changed"]) == 1
        assert "amount: INTEGER -> DECIMAL" in diff["type_changed"]

