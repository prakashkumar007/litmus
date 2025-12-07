"""
Chalk and Duster - Alert Enhancement using LLM
"""

import re
from typing import Any, Dict, List, Optional

import structlog

from chalkandduster.api.schemas.llm import AlertEnhanceResponse
from chalkandduster.llm.client import get_llm_client
from chalkandduster.llm.prompts import ALERT_ENHANCER_SYSTEM, format_alert_prompt

logger = structlog.get_logger()


async def enhance_alert(
    dataset_name: str,
    run_id: str,
    failures: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> AlertEnhanceResponse:
    """
    Enhance a raw alert with LLM-generated insights.
    
    Provides human-readable summary, root cause hints, and recommendations.
    
    Args:
        dataset_name: Name of the dataset with failures
        run_id: The run ID for reference
        failures: List of check failures with details
        context: Optional additional context (historical data, patterns)
        
    Returns:
        AlertEnhanceResponse with enhanced alert information
    """
    client = get_llm_client()
    
    # Format the prompt
    prompt = format_alert_prompt(
        dataset_name=dataset_name,
        run_id=run_id,
        failures=failures,
        context=context,
    )
    
    logger.info("Enhancing alert", dataset=dataset_name, failure_count=len(failures))
    
    try:
        # Generate response from LLM
        response = await client.generate(
            prompt=prompt,
            system_prompt=ALERT_ENHANCER_SYSTEM,
            temperature=0.5,
        )
        
        # Parse the response
        summary = extract_section(response, "summary", default="Data quality issues detected.")
        root_causes = extract_list_section(response, "root cause")
        recommendations = extract_list_section(response, "recommend")
        severity = extract_severity(response)
        slack_message = format_slack_message(
            dataset_name=dataset_name,
            run_id=run_id,
            summary=summary,
            failures=failures,
            severity=severity,
        )
        
        return AlertEnhanceResponse(
            success=True,
            summary=summary,
            root_cause_hints=root_causes,
            recommended_actions=recommendations,
            severity=severity,
            slack_message=slack_message,
        )
        
    except Exception as e:
        logger.error("Alert enhancement failed", error=str(e))
        
        # Return basic alert without LLM enhancement
        return AlertEnhanceResponse(
            success=False,
            summary=f"Data quality issues detected in {dataset_name}",
            root_cause_hints=[],
            recommended_actions=["Review the failed checks manually"],
            severity="warning",
            slack_message=format_basic_slack_message(dataset_name, run_id, failures),
        )


def extract_section(text: str, section_name: str, default: str = "") -> str:
    """Extract a section from the LLM response."""
    # Try to find numbered section
    pattern = rf"\d+\.\s*{section_name}[:\s]*(.*?)(?=\d+\.|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    
    # Try to find labeled section
    pattern = rf"{section_name}[:\s]*(.*?)(?=\n\n|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    
    return default


def extract_list_section(text: str, section_name: str) -> List[str]:
    """Extract a list section from the LLM response."""
    section = extract_section(text, section_name)
    
    if not section:
        return []
    
    # Extract bullet points
    items = re.findall(r"[-•*]\s*(.+?)(?=\n[-•*]|\n\n|$)", section, re.DOTALL)
    
    return [item.strip() for item in items if item.strip()]


def extract_severity(text: str) -> str:
    """Extract severity from the LLM response."""
    text_lower = text.lower()
    
    if "critical" in text_lower:
        return "critical"
    elif "warning" in text_lower:
        return "warning"
    elif "info" in text_lower:
        return "info"
    
    return "warning"


def format_slack_message(
    dataset_name: str,
    run_id: str,
    summary: str,
    failures: List[Dict[str, Any]],
    severity: str,
) -> str:
    """Format a Slack message for the alert."""
    emoji = {
        "critical": "🚨",
        "warning": "⚠️",
        "info": "ℹ️",
    }.get(severity, "⚠️")
    
    message = f"{emoji} *Data Quality Alert: {dataset_name}*\n\n"
    message += f"*Summary:* {summary}\n\n"
    message += f"*Failed Checks ({len(failures)}):*\n"
    
    for failure in failures[:5]:  # Limit to 5 failures
        check_name = failure.get("check_name", "Unknown")
        check_message = failure.get("message", "No details")
        message += f"• `{check_name}`: {check_message}\n"
    
    if len(failures) > 5:
        message += f"_...and {len(failures) - 5} more_\n"
    
    message += f"\n_Run ID: {run_id}_"
    
    return message


def format_basic_slack_message(
    dataset_name: str,
    run_id: str,
    failures: List[Dict[str, Any]],
) -> str:
    """Format a basic Slack message without LLM enhancement."""
    message = f"⚠️ *Data Quality Alert: {dataset_name}*\n\n"
    message += f"*{len(failures)} check(s) failed*\n\n"
    
    for failure in failures[:5]:
        check_name = failure.get("check_name", "Unknown")
        message += f"• `{check_name}`\n"
    
    if len(failures) > 5:
        message += f"_...and {len(failures) - 5} more_\n"
    
    message += f"\n_Run ID: {run_id}_"
    
    return message

