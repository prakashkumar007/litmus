"""
Chalk and Duster - LLM Routes
"""

import structlog
from fastapi import APIRouter, Depends, HTTPException, status

from chalkandduster.api.deps import get_rate_limiter, RateLimiter
from chalkandduster.api.schemas.llm import (
    YAMLGenerateRequest,
    YAMLGenerateResponse,
    AlertEnhanceRequest,
    AlertEnhanceResponse,
    DriftExplainRequest,
    DriftExplainResponse,
)
from chalkandduster.llm.yaml_generator import generate_yaml_from_description
from chalkandduster.llm.alert_enhancer import enhance_alert
from chalkandduster.llm.drift_explainer import explain_drift
from chalkandduster.core.config import settings

logger = structlog.get_logger()
router = APIRouter()


@router.post("/generate-yaml", response_model=YAMLGenerateResponse)
async def generate_yaml(
    request: YAMLGenerateRequest,
    rate_limiter: RateLimiter = Depends(
        lambda: get_rate_limiter(settings.RATE_LIMIT_LLM_CALLS_PER_HOUR)
    ),
) -> YAMLGenerateResponse:
    """
    Generate data quality and drift YAML from natural language description.
    
    Uses LLM to convert plain English requirements into valid YAML configuration.
    """
    # Check rate limit
    # TODO: Use tenant ID for rate limiting
    if not await rate_limiter.check("global"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded for LLM calls",
        )
    
    try:
        result = await generate_yaml_from_description(
            description=request.description,
            table_name=request.table_name,
            schema_info=request.schema_info,
            include_quality=request.include_quality,
            include_drift=request.include_drift,
        )
        
        logger.info(
            "YAML generated",
            check_count=result.check_count,
            monitor_count=result.monitor_count,
        )
        
        return result
    
    except Exception as e:
        logger.error("YAML generation failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate YAML: {str(e)}",
        )


@router.post("/enhance-alert", response_model=AlertEnhanceResponse)
async def enhance_alert_endpoint(
    request: AlertEnhanceRequest,
    rate_limiter: RateLimiter = Depends(
        lambda: get_rate_limiter(settings.RATE_LIMIT_LLM_CALLS_PER_HOUR)
    ),
) -> AlertEnhanceResponse:
    """
    Enhance a raw alert with LLM-generated insights.
    
    Provides human-readable summary, root cause hints, and recommendations.
    """
    if not await rate_limiter.check("global"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded for LLM calls",
        )
    
    try:
        result = await enhance_alert(
            dataset_name=request.dataset_name,
            run_id=request.run_id,
            failures=request.failures,
            context=request.context,
        )
        
        logger.info(
            "Alert enhanced",
            dataset=request.dataset_name,
            severity=result.severity,
        )
        
        return result
    
    except Exception as e:
        logger.error("Alert enhancement failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enhance alert: {str(e)}",
        )


@router.post("/explain-drift", response_model=DriftExplainResponse)
async def explain_drift_endpoint(
    request: DriftExplainRequest,
    rate_limiter: RateLimiter = Depends(
        lambda: get_rate_limiter(settings.RATE_LIMIT_LLM_CALLS_PER_HOUR)
    ),
) -> DriftExplainResponse:
    """
    Explain drift detection results in plain English.
    
    Provides clear explanation of what changed and potential impact.
    """
    if not await rate_limiter.check("global"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded for LLM calls",
        )
    
    try:
        result = await explain_drift(
            dataset_name=request.dataset_name,
            drift_results=request.drift_results,
            baseline_info=request.baseline_info,
        )
        
        logger.info("Drift explained", dataset=request.dataset_name)
        
        return result
    
    except Exception as e:
        logger.error("Drift explanation failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to explain drift: {str(e)}",
        )

