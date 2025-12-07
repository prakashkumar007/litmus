"""
Chalk and Duster - YAML Generator Tests
"""

import pytest
from unittest.mock import AsyncMock, patch

from chalkandduster.llm.yaml_generator import YAMLGenerator, generate_yaml_from_description


class TestYAMLGenerator:
    """Tests for YAML generation from natural language."""
    
    @pytest.fixture
    def generator(self):
        """Create a YAML generator instance."""
        return YAMLGenerator()
    
    @pytest.mark.asyncio
    async def test_generate_quality_yaml(self, generator, mock_ollama_response):
        """Test generating quality YAML from description."""
        with patch.object(
            generator.client, "chat", new_callable=AsyncMock
        ) as mock_chat:
            mock_chat.return_value = mock_ollama_response
            
            result = await generator.generate(
                description="Check that the orders table has no missing order IDs",
                table_name="ORDERS",
                columns=["order_id", "customer_id", "amount", "status"],
            )
            
            assert result.quality_yaml is not None
            assert "checks" in result.quality_yaml.lower() or "row_count" in result.quality_yaml.lower()
    
    @pytest.mark.asyncio
    async def test_generate_drift_yaml(self, generator, mock_ollama_response):
        """Test generating drift YAML from description."""
        with patch.object(
            generator.client, "chat", new_callable=AsyncMock
        ) as mock_chat:
            mock_chat.return_value = mock_ollama_response
            
            result = await generator.generate(
                description="Monitor for volume changes in the orders table",
                table_name="ORDERS",
                columns=["order_id", "customer_id", "amount"],
            )
            
            assert result.drift_yaml is not None
            assert "monitors" in result.drift_yaml.lower() or "volume" in result.drift_yaml.lower()
    
    @pytest.mark.asyncio
    async def test_generate_with_empty_description(self, generator):
        """Test that empty description raises error."""
        with pytest.raises(ValueError):
            await generator.generate(
                description="",
                table_name="ORDERS",
                columns=["order_id"],
            )
    
    @pytest.mark.asyncio
    async def test_generate_with_no_columns(self, generator, mock_ollama_response):
        """Test generating YAML without column information."""
        with patch.object(
            generator.client, "chat", new_callable=AsyncMock
        ) as mock_chat:
            mock_chat.return_value = mock_ollama_response
            
            result = await generator.generate(
                description="Basic quality checks for orders",
                table_name="ORDERS",
                columns=None,
            )
            
            # Should still generate something
            assert result.quality_yaml is not None or result.drift_yaml is not None


class TestGenerateYAMLFromDescription:
    """Tests for the convenience function."""
    
    @pytest.mark.asyncio
    async def test_convenience_function(self, mock_ollama_response):
        """Test the convenience function."""
        with patch(
            "chalkandduster.llm.yaml_generator.YAMLGenerator.generate",
            new_callable=AsyncMock,
        ) as mock_generate:
            from chalkandduster.llm.yaml_generator import GeneratedYAML
            
            mock_generate.return_value = GeneratedYAML(
                quality_yaml="checks for TABLE:\n  - row_count > 0",
                drift_yaml="monitors:\n  - name: test\n    type: volume",
                explanation="Generated basic checks",
            )
            
            result = await generate_yaml_from_description(
                description="Check for missing values",
                table_name="TABLE",
            )
            
            assert result.quality_yaml is not None
            mock_generate.assert_called_once()

