"""
Chalk and Duster - Drift Explanation using LLM
"""

import re
from typing import Any, Dict, List, Optional

import structlog

from chalkandduster.api.schemas.llm import DriftExplainResponse
from chalkandduster.llm.client import get_llm_client
from chalkandduster.llm.prompts import DRIFT_EXPLAINER_SYSTEM, format_drift_prompt

logger = structlog.get_logger()


async def explain_drift(
    dataset_name: str,
    drift_results: List[Dict[str, Any]],
    baseline_info: Optional[Dict[str, Any]] = None,
) -> DriftExplainResponse:
    """
    Explain drift detection results in plain English.
    
    Provides clear explanation of what changed and potential impact.
    
    Args:
        dataset_name: Name of the dataset with drift
        drift_results: List of detected drift events
        baseline_info: Optional baseline statistics for comparison
        
    Returns:
        DriftExplainResponse with explanation and recommendations
    """
    client = get_llm_client()
    
    # Format the prompt
    prompt = format_drift_prompt(
        dataset_name=dataset_name,
        drift_results=drift_results,
        baseline_info=baseline_info,
    )
    
    logger.info("Explaining drift", dataset=dataset_name, drift_count=len(drift_results))
    
    try:
        # Generate response from LLM
        response = await client.generate(
            prompt=prompt,
            system_prompt=DRIFT_EXPLAINER_SYSTEM,
            temperature=0.5,
        )
        
        # Parse the response
        summary = extract_summary(response)
        changes = extract_changes(response)
        impact = extract_impact(response)
        recommendations = extract_recommendations(response)
        
        return DriftExplainResponse(
            success=True,
            summary=summary,
            changes=changes,
            impact_assessment=impact,
            recommendations=recommendations,
        )
        
    except Exception as e:
        logger.error("Drift explanation failed", error=str(e))
        
        # Return basic explanation without LLM
        return DriftExplainResponse(
            success=False,
            summary=f"Drift detected in {dataset_name}. {len(drift_results)} monitor(s) triggered.",
            changes=[],
            impact_assessment="Unable to assess impact automatically.",
            recommendations=["Review the drift results manually."],
        )


def extract_summary(text: str) -> str:
    """Extract summary from the LLM response."""
    # Try to find summary section
    pattern = r"(?:summary|what changed)[:\s]*(.*?)(?=\n\n|\d+\.|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    
    # Return first paragraph as summary
    paragraphs = text.split("\n\n")
    if paragraphs:
        return paragraphs[0].strip()
    
    return "Drift detected in the dataset."


def extract_changes(text: str) -> List[Dict[str, str]]:
    """Extract list of changes from the LLM response."""
    changes = []
    
    # Find changes section
    pattern = r"(?:changes|specific changes)[:\s]*(.*?)(?=\n\n\d+\.|\n\n[A-Z]|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        section = match.group(1)
        
        # Extract bullet points
        items = re.findall(r"[-•*]\s*(.+?)(?=\n[-•*]|\n\n|$)", section, re.DOTALL)
        
        for item in items:
            item = item.strip()
            if ":" in item:
                parts = item.split(":", 1)
                changes.append({
                    "change": parts[0].strip(),
                    "explanation": parts[1].strip(),
                })
            else:
                changes.append({
                    "change": item,
                    "explanation": "",
                })
    
    return changes


def extract_impact(text: str) -> str:
    """Extract impact assessment from the LLM response."""
    pattern = r"(?:impact|business impact)[:\s]*(.*?)(?=\n\n\d+\.|\n\n[A-Z]|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    
    return ""


def extract_recommendations(text: str) -> List[str]:
    """Extract recommendations from the LLM response."""
    recommendations = []
    
    # Find recommendations section
    pattern = r"(?:recommend|suggestion|action)[:\s]*(.*?)(?=\n\n\d+\.|\n\n[A-Z]|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        section = match.group(1)
        
        # Extract bullet points
        items = re.findall(r"[-•*]\s*(.+?)(?=\n[-•*]|\n\n|$)", section, re.DOTALL)
        
        recommendations = [item.strip() for item in items if item.strip()]
    
    return recommendations

