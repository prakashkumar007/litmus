"""LLM Integration module - Ollama client and prompt templates."""

from chalkandduster.llm.client import OllamaClient
from chalkandduster.llm.yaml_generator import generate_yaml_from_description
from chalkandduster.llm.alert_enhancer import enhance_alert
from chalkandduster.llm.drift_explainer import explain_drift

__all__ = [
    "OllamaClient",
    "generate_yaml_from_description",
    "enhance_alert",
    "explain_drift",
]

