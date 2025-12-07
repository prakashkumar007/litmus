"""
Chalk and Duster - YAML Validator Tests
"""

import pytest

from chalkandduster.quality.validator import validate_quality_yaml, validate_drift_yaml


class TestQualityYAMLValidator:
    """Tests for quality YAML validation."""
    
    def test_valid_quality_yaml(self, sample_quality_yaml: str):
        """Test validation of valid quality YAML."""
        result = validate_quality_yaml(sample_quality_yaml)
        
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.check_count > 0
    
    def test_invalid_yaml_syntax(self):
        """Test validation of invalid YAML syntax."""
        invalid_yaml = """
checks for TABLE:
  - row_count > 0
    invalid indentation
"""
        result = validate_quality_yaml(invalid_yaml)
        
        assert result.valid is False
        assert len(result.errors) > 0
        assert result.errors[0]["type"] == "parse_error"
    
    def test_missing_checks_key(self):
        """Test validation when 'checks' key is missing."""
        yaml_content = """
monitors:
  - name: test
"""
        result = validate_quality_yaml(yaml_content)
        
        assert result.valid is False
        assert any(e["type"] == "missing_key" for e in result.errors)
    
    def test_empty_yaml(self):
        """Test validation of empty YAML."""
        result = validate_quality_yaml("")
        
        assert result.valid is False
    
    def test_checks_not_dict(self):
        """Test validation when checks is not a dictionary."""
        yaml_content = """
checks:
  - row_count > 0
"""
        result = validate_quality_yaml(yaml_content)
        
        assert result.valid is False
        assert any(e["type"] == "structure_error" for e in result.errors)


class TestDriftYAMLValidator:
    """Tests for drift YAML validation."""
    
    def test_valid_drift_yaml(self, sample_drift_yaml: str):
        """Test validation of valid drift YAML."""
        result = validate_drift_yaml(sample_drift_yaml)
        
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.monitor_count > 0
    
    def test_invalid_yaml_syntax(self):
        """Test validation of invalid YAML syntax."""
        invalid_yaml = """
monitors:
  - name: test
    type: volume
      invalid: indentation
"""
        result = validate_drift_yaml(invalid_yaml)
        
        assert result.valid is False
        assert len(result.errors) > 0
    
    def test_missing_monitors_key(self):
        """Test validation when 'monitors' key is missing."""
        yaml_content = """
checks:
  - row_count > 0
"""
        result = validate_drift_yaml(yaml_content)
        
        assert result.valid is False
        assert any(e["type"] == "missing_key" for e in result.errors)
    
    def test_invalid_drift_type(self):
        """Test validation with invalid drift type."""
        yaml_content = """
monitors:
  - name: test_monitor
    type: invalid_type
    threshold: 0.5
"""
        result = validate_drift_yaml(yaml_content)
        
        assert result.valid is False
        assert any(e["type"] == "invalid_type" for e in result.errors)
    
    def test_missing_threshold_warning(self):
        """Test that missing threshold generates a warning."""
        yaml_content = """
monitors:
  - name: test_monitor
    type: volume
"""
        result = validate_drift_yaml(yaml_content)
        
        assert result.valid is True
        assert any(w["type"] == "missing_threshold" for w in result.warnings)

