import json
import os
from enum import Enum
from pathlib import Path
from typing import List, Tuple

from langchain_openai import ChatOpenAI
from pydantic import BaseModel


class ModelProvider(str, Enum):
    """Supported model providers (OpenAI-compatible endpoints)."""

    ALIBABA = "Alibaba"
    ANTHROPIC = "Anthropic"
    DEEPSEEK = "DeepSeek"
    GOOGLE = "Google"
    GROQ = "Groq"
    META = "Meta"
    MISTRAL = "Mistral"
    OPENAI = "OpenAI"
    OLLAMA = "Ollama"
    OPENROUTER = "OpenRouter"
    GIGACHAT = "GigaChat"
    AZURE_OPENAI = "Azure OpenAI"
    XAI = "xAI"
    QWEN = "Qwen"
    GLM = "GLM"
    MINIMAX = "MiniMax"


class LLMModel(BaseModel):
    """Represents an LLM model configuration."""

    display_name: str
    model_name: str
    provider: ModelProvider

    def to_choice_tuple(self) -> Tuple[str, str, str]:
        return (self.display_name, self.model_name, self.provider.value)

    def is_custom(self) -> bool:
        return self.model_name == "-"

    def has_json_mode(self) -> bool:
        # Keep JSON mode enabled for OpenAI-compatible hosted models.
        # Ollama compatibility varies by model.
        return not self.is_ollama()

    def is_deepseek(self) -> bool:
        return self.model_name.startswith("deepseek")

    def is_gemini(self) -> bool:
        return self.model_name.startswith("gemini")

    def is_ollama(self) -> bool:
        return self.provider == ModelProvider.OLLAMA


def _normalize_provider(provider: ModelProvider | str | None) -> ModelProvider:
    if isinstance(provider, ModelProvider):
        return provider

    value = str(provider or "").strip()
    if not value:
        return ModelProvider.DEEPSEEK

    for item in ModelProvider:
        if value == item.value or value.lower() == item.value.lower() or value.upper().replace(" ", "_") == item.name:
            return item

    aliases = {
        "tongyi": ModelProvider.QWEN,
        "qwen": ModelProvider.QWEN,
        "zhipu": ModelProvider.GLM,
        "glm": ModelProvider.GLM,
        "minimax": ModelProvider.MINIMAX,
    }
    return aliases.get(value.lower(), ModelProvider.DEEPSEEK)


def load_models_from_json(json_path: str) -> List[LLMModel]:
    """Load models from a JSON file."""
    with open(json_path, "r", encoding="utf-8-sig") as f:
        models_data = json.load(f)

    models: List[LLMModel] = []
    for model_data in models_data:
        provider_enum = _normalize_provider(model_data.get("provider"))
        models.append(
            LLMModel(
                display_name=model_data["display_name"],
                model_name=model_data["model_name"],
                provider=provider_enum,
            )
        )
    return models


current_dir = Path(__file__).parent
models_json_path = current_dir / "api_models.json"
ollama_models_json_path = current_dir / "ollama_models.json"

AVAILABLE_MODELS = load_models_from_json(str(models_json_path))
OLLAMA_MODELS = load_models_from_json(str(ollama_models_json_path))

LLM_ORDER = [model.to_choice_tuple() for model in AVAILABLE_MODELS]
OLLAMA_LLM_ORDER = [model.to_choice_tuple() for model in OLLAMA_MODELS]


def get_model_info(model_name: str, model_provider: str | ModelProvider) -> LLMModel | None:
    """Get model information by model_name/provider pair."""
    provider = _normalize_provider(model_provider)
    all_models = AVAILABLE_MODELS + OLLAMA_MODELS
    return next((model for model in all_models if model.model_name == model_name and model.provider == provider), None)


def find_model_by_name(model_name: str) -> LLMModel | None:
    """Find a model by its name across all available models."""
    all_models = AVAILABLE_MODELS + OLLAMA_MODELS
    return next((model for model in all_models if model.model_name == model_name), None)


def get_models_list():
    """Get the list of models for API responses."""
    return [
        {
            "display_name": model.display_name,
            "model_name": model.model_name,
            "provider": model.provider.value,
        }
        for model in AVAILABLE_MODELS
    ]


_AGENT_MODEL_ROUTING: dict[str, tuple[str, ModelProvider]] = {
    # Force all active agents to use DeepSeek via SiliconFlow.
    "portfolio_manager": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "risk_management_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "warren_buffett_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "stanley_druckenmiller_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "fundamentals_analyst_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "growth_analyst_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "peter_lynch_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "charlie_munger_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    "soros_agent": (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
}


def resolve_agent_model(agent_name: str) -> tuple[str, ModelProvider]:
    """Resolve model config by agent role for A-share deployment."""
    return _AGENT_MODEL_ROUTING.get(
        agent_name,
        (os.getenv("DEEPSEEK_MODEL", "deepseek-ai/DeepSeek-V3"), ModelProvider.DEEPSEEK),
    )


def _provider_runtime_config(provider: ModelProvider) -> tuple[str, str]:
    """Return (base_url, api_key_env_name) for each provider."""
    if provider == ModelProvider.DEEPSEEK:
        return (os.getenv("DEEPSEEK_BASE_URL", "https://api.siliconflow.cn/v1"), "SILICONFLOW_API_KEY")
    if provider == ModelProvider.QWEN:
        return (
            os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            "QWEN_API_KEY",
        )
    if provider == ModelProvider.GLM:
        return (os.getenv("GLM_BASE_URL", "https://open.bigmodel.cn/api/paas/v4"), "GLM_API_KEY")
    if provider == ModelProvider.MINIMAX:
        return (os.getenv("MINIMAX_BASE_URL", "https://api.minimax.chat/v1"), "MINIMAX_API_KEY")
    if provider == ModelProvider.OLLAMA:
        return (os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"), "OLLAMA_API_KEY")

    # Compatibility fallback: keep OpenAI-compatible usage for any legacy provider.
    return (os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1"), "OPENAI_API_KEY")


def get_model(
    model_name: str,
    model_provider: ModelProvider | str,
    api_keys: dict | None = None,
) -> ChatOpenAI:
    """Create a ChatOpenAI client for all providers via base_url/api_key routing."""
    provider = _normalize_provider(model_provider)
    base_url, api_key_env = _provider_runtime_config(provider)

    api_key = (api_keys or {}).get(api_key_env) or os.getenv(api_key_env)
    if provider == ModelProvider.DEEPSEEK and not api_key:
        # Compatibility fallback: accept DEEPSEEK_API_KEY as alias.
        api_key = (api_keys or {}).get("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API_KEY")
    if provider == ModelProvider.OLLAMA and not api_key:
        api_key = "ollama"

    if not api_key:
        if provider == ModelProvider.DEEPSEEK:
            raise ValueError(
                "DeepSeek API key not found. Please set SILICONFLOW_API_KEY (or DEEPSEEK_API_KEY) in .env."
            )
        raise ValueError(
            f"{provider.value} API key not found. Please set {api_key_env} in .env or pass it through request api_keys."
        )

    return ChatOpenAI(
        model=model_name,
        api_key=api_key,
        base_url=base_url,
        temperature=0,
    )
