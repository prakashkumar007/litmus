"""
Chalk and Duster - Ollama LLM Client
"""

from typing import Any, Dict, List, Optional

import httpx
import structlog

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import LLMError

logger = structlog.get_logger()


class OllamaClient:
    """
    Client for interacting with Ollama LLM.
    
    Supports both local Ollama and remote endpoints.
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        self.base_url = base_url or settings.OLLAMA_BASE_URL
        self.model = model or settings.OLLAMA_MODEL
        self.timeout = timeout or settings.OLLAMA_TIMEOUT
    
    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
    ) -> str:
        """
        Generate text using Ollama.
        
        Args:
            prompt: The user prompt
            system_prompt: Optional system prompt for context
            temperature: Sampling temperature (0-1)
            max_tokens: Maximum tokens to generate
            
        Returns:
            Generated text response
        """
        messages = []
        
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        
        messages.append({"role": "user", "content": prompt})
        
        return await self.chat(messages, temperature, max_tokens)
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2048,
    ) -> str:
        """
        Chat with Ollama using message history.

        Uses the /api/generate endpoint which is more reliable than /api/chat.

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature (0-1)
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text response
        """
        # Convert messages to a single prompt for /api/generate
        prompt_parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                prompt_parts.append(f"System: {content}")
            elif role == "user":
                prompt_parts.append(f"User: {content}")
            elif role == "assistant":
                prompt_parts.append(f"Assistant: {content}")

        prompt = "\n\n".join(prompt_parts) + "\n\nAssistant:"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": temperature,
                            "num_predict": max_tokens,
                        },
                    },
                )

                if response.status_code != 200:
                    raise LLMError(
                        f"Ollama API error: {response.status_code} - {response.text}",
                        provider="ollama",
                    )

                data = response.json()
                return data.get("response", "")
                
        except httpx.TimeoutException:
            logger.error("Ollama request timed out", timeout=self.timeout)
            raise LLMError("LLM request timed out", provider="ollama")
        except httpx.RequestError as e:
            logger.error("Ollama request failed", error=str(e))
            raise LLMError(f"LLM request failed: {str(e)}", provider="ollama")
    
    async def is_available(self) -> bool:
        """Check if Ollama is available."""
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(f"{self.base_url}/api/tags")
                return response.status_code == 200
        except Exception:
            return False
    
    async def list_models(self) -> List[str]:
        """List available models."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(f"{self.base_url}/api/tags")
                if response.status_code == 200:
                    data = response.json()
                    return [m["name"] for m in data.get("models", [])]
                return []
        except Exception:
            return []


# Singleton client instance
_client: Optional[OllamaClient] = None


def get_llm_client() -> OllamaClient:
    """Get the LLM client instance."""
    global _client
    if _client is None:
        _client = OllamaClient()
    return _client

