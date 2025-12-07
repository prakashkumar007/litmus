"""
Chalk and Duster - YAML Generator using LLM
"""

import re
from typing import Any, Dict, Optional

import structlog

from chalkandduster.api.schemas.llm import YAMLGenerateResponse
from chalkandduster.llm.client import get_llm_client
from chalkandduster.llm.prompts import YAML_GENERATOR_SYSTEM, format_yaml_prompt
from chalkandduster.quality.validator import validate_quality_yaml, validate_drift_yaml

logger = structlog.get_logger()


async def generate_yaml_from_description(
    description: str,
    table_name: Optional[str] = None,
    schema_info: Optional[Dict[str, Any]] = None,
    include_quality: bool = True,
    include_drift: bool = True,
) -> YAMLGenerateResponse:
    """
    Generate data quality and drift YAML from natural language description.
    
    Uses LLM to convert plain English requirements into valid YAML configuration.
    
    Args:
        description: Natural language description of quality requirements
        table_name: Optional table name for context
        schema_info: Optional schema information (columns, types)
        include_quality: Whether to generate quality YAML
        include_drift: Whether to generate drift YAML
        
    Returns:
        YAMLGenerateResponse with generated YAML and explanation
    """
    client = get_llm_client()
    
    # Format the prompt
    prompt = format_yaml_prompt(
        description=description,
        table_name=table_name,
        schema_info=schema_info,
        include_quality=include_quality,
        include_drift=include_drift,
    )
    
    logger.info("Generating YAML from description", table=table_name)
    
    try:
        # Generate response from LLM
        response = await client.generate(
            prompt=prompt,
            system_prompt=YAML_GENERATOR_SYSTEM,
            temperature=0.3,  # Lower temperature for more consistent output
        )
        
        # Parse the response to extract YAML blocks
        quality_yaml = None
        drift_yaml = None
        explanation = ""
        warnings = []
        
        # Extract quality YAML
        if include_quality:
            quality_yaml = extract_yaml_block(response, "quality_yaml")
            if quality_yaml:
                validation = validate_quality_yaml(quality_yaml)
                if not validation.valid:
                    warnings.append(f"Quality YAML validation warnings: {validation.errors}")
        
        # Extract drift YAML
        if include_drift:
            drift_yaml = extract_yaml_block(response, "drift_yaml")
            if drift_yaml:
                validation = validate_drift_yaml(drift_yaml)
                if not validation.valid:
                    warnings.append(f"Drift YAML validation warnings: {validation.errors}")
        
        # Extract explanation (text outside code blocks)
        explanation = extract_explanation(response)
        
        # Count checks and monitors
        check_count = count_checks(quality_yaml) if quality_yaml else 0
        monitor_count = count_monitors(drift_yaml) if drift_yaml else 0
        
        return YAMLGenerateResponse(
            success=True,
            quality_yaml=quality_yaml,
            drift_yaml=drift_yaml,
            explanation=explanation or "YAML generated successfully.",
            check_count=check_count,
            monitor_count=monitor_count,
            warnings=warnings,
        )
        
    except Exception as e:
        logger.error("YAML generation failed", error=str(e))
        return YAMLGenerateResponse(
            success=False,
            explanation=f"Failed to generate YAML: {str(e)}",
            warnings=[str(e)],
        )


def extract_yaml_block(text: str, block_name: str) -> Optional[str]:
    """Extract a named YAML block from LLM response."""
    # Try to find labeled code block
    pattern = rf"```(?:yaml)?\s*#?\s*{block_name}\s*\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    
    if match:
        return match.group(1).strip()
    
    # Try to find any YAML block with the name mentioned before it
    pattern = rf"{block_name}[:\s]*\n```(?:yaml)?\s*\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    
    if match:
        return match.group(1).strip()
    
    # Fallback: find any YAML block
    pattern = r"```(?:yaml)?\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    
    if matches:
        # Return first for quality, second for drift
        if block_name == "quality_yaml" and len(matches) >= 1:
            return matches[0].strip()
        elif block_name == "drift_yaml" and len(matches) >= 2:
            return matches[1].strip()
    
    return None


def extract_explanation(text: str) -> str:
    """Extract explanation text from LLM response."""
    # Remove code blocks
    cleaned = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    # Clean up whitespace
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def count_checks(yaml_content: str) -> int:
    """Count the number of checks in quality YAML."""
    if not yaml_content:
        return 0
    
    # Count lines that look like checks (start with -)
    lines = yaml_content.split("\n")
    count = 0
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("-") and not stripped.startswith("- name:"):
            count += 1
    return count


def count_monitors(yaml_content: str) -> int:
    """Count the number of monitors in drift YAML."""
    if not yaml_content:
        return 0
    
    # Count "- name:" or "- type:" occurrences
    return yaml_content.count("- name:") or yaml_content.count("- type:")

