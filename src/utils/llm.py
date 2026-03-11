"""Helper functions for LLM."""

from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.messages import SystemMessage
from pydantic import BaseModel

from src.graph.state import AgentState
from src.llm.models import get_model, get_model_info, resolve_agent_model
from src.rag.master_retrieval import retrieve_for_agent_call
from src.utils.progress import progress


def _build_a_share_system_rules(agent_name: str | None) -> str:
    """Build mandatory A-share constraints injected into every LLM call."""
    common_rules = [
        "You must output in Chinese for all reasoning and conclusions.",
        "You are analyzing China A-share market only; stock symbols must be 6-digit codes (e.g., 000001, 600519).",
        "You must follow A-share rules: T+1 and limit-up/limit-down constraints (typically 10%, and 20% for STAR/ChiNext).",
        "You must speak in the localized Chinese persona of your assigned role; never output English personal names.",
    ]

    agent_specific_rules: list[str] = []
    if agent_name in {"portfolio_manager", "risk_management_agent"}:
        agent_specific_rules.append(
            "When producing trade and position advice, explicitly enforce T+1 and price-limit constraints."
        )
    if agent_name in {"fundamentals_analyst_agent", "growth_analyst_agent", "soros_agent"}:
        agent_specific_rules.append(
            "Include China-specific dimensions: northbound fund flow, national-team capital, policy signals (NDRC/PBOC/CSRC), theme speculation, and hot-money sentiment."
        )

    rule_lines = common_rules + agent_specific_rules
    return "[A-Share System Rules]\n" + "\n".join(f"{idx}. {line}" for idx, line in enumerate(rule_lines, start=1))


def _inject_a_share_rules(prompt: Any, agent_name: str | None):
    """Inject system constraints into either chat prompts or plain-text prompts."""
    rules = _build_a_share_system_rules(agent_name)

    if hasattr(prompt, "messages"):
        messages = list(prompt.messages)
        if messages and getattr(messages[0], "type", "") == "system":
            first_content = messages[0].content
            if not isinstance(first_content, str):
                first_content = str(first_content)
            messages[0] = SystemMessage(content=f"{rules}\n\n{first_content}")
        else:
            messages.insert(0, SystemMessage(content=rules))
        return messages

    if isinstance(prompt, str):
        return f"{rules}\n\n{prompt}"

    return prompt


def _prompt_to_query_text(prompt: Any) -> str:
    """Build retrieval query text from prompt payload."""
    if hasattr(prompt, "messages"):
        chunks = []
        for msg in list(prompt.messages):
            content = getattr(msg, "content", "")
            if isinstance(content, list):
                content = " ".join(str(x) for x in content)
            if content:
                chunks.append(str(content))
        return "\n".join(chunks).strip()

    if isinstance(prompt, list):
        chunks = []
        for msg in prompt:
            content = getattr(msg, "content", "")
            if content:
                chunks.append(str(content))
        return "\n".join(chunks).strip()

    if isinstance(prompt, str):
        return prompt
    return str(prompt)


def _inject_retrieval_context(prompt: Any, retrieval_bundle: dict[str, Any]) -> Any:
    if not retrieval_bundle.get("enabled"):
        return prompt

    master = retrieval_bundle.get("master")
    collection = retrieval_bundle.get("collection")
    hit_ids = retrieval_bundle.get("hit_ids") or []
    context = retrieval_bundle.get("context") or ""
    insufficient = retrieval_bundle.get("insufficient", True)

    evidence_header = (
        "[Retrieval-First]\n"
        f"- master: {master}\n"
        f"- collection: {collection}\n"
        f"- hit_ids: {', '.join(hit_ids) if hit_ids else 'none'}\n"
        "- hard_rules:\n"
        "  1) 仅可使用当前master命中的证据，不得引用其他大师观点。\n"
        "  2) 若证据不足，必须明确输出“证据不足”，禁止编造。\n"
    )
    evidence_block = (
        evidence_header + "\n当前检索结果为空。"
        if insufficient
        else evidence_header + f"\n证据正文:\n{context}"
    )

    if hasattr(prompt, "messages"):
        messages = list(prompt.messages)
        insert_idx = 1 if messages and getattr(messages[0], "type", "") == "system" else 0
        messages.insert(insert_idx, SystemMessage(content=evidence_block))
        return messages

    if isinstance(prompt, list):
        messages = list(prompt)
        insert_idx = 1 if messages and getattr(messages[0], "type", "") == "system" else 0
        messages.insert(insert_idx, SystemMessage(content=evidence_block))
        return messages

    if isinstance(prompt, str):
        return f"{evidence_block}\n\n{prompt}"

    return prompt


def _create_evidence_insufficient_response(model_class: type[BaseModel]) -> BaseModel:
    """Safe fallback when retrieval evidence is insufficient."""
    payload: dict[str, Any] = {}
    message = "证据不足：未检索到当前大师有效语料，已触发安全降级。"
    for field_name, field in model_class.model_fields.items():
        name = field_name.lower()
        ann = field.annotation

        if name == "signal":
            payload[field_name] = "neutral"
            continue
        if name == "action":
            payload[field_name] = "hold"
            continue
        if "reasoning" in name:
            payload[field_name] = message
            continue
        if "confidence" in name:
            payload[field_name] = 20 if ann == int else 20.0
            continue
        if name in {"quantity", "shares"}:
            payload[field_name] = 0
            continue
        if hasattr(ann, "__origin__") and ann.__origin__ == dict:
            payload[field_name] = {}
            continue
        if hasattr(ann, "__origin__") and ann.__origin__ == list:
            payload[field_name] = []
            continue
        if ann == str:
            payload[field_name] = message
            continue
        if ann == float:
            payload[field_name] = 0.0
            continue
        if ann == int:
            payload[field_name] = 0
            continue
        if hasattr(ann, "__args__") and ann.__args__:
            payload[field_name] = ann.__args__[0]
            continue
        payload[field_name] = None
    return model_class(**payload)


def _sanitize_chinese_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return value
    alpha_count = len(re.findall(r"[A-Za-z]", value))
    han_count = len(re.findall(r"[\u4e00-\u9fff]", value))
    if alpha_count > 0 and alpha_count > han_count:
        return "中文输出约束触发：模型返回英文内容，已按规则降级。"
    return value


def _sanitize_result_payload(payload: Any) -> Any:
    if isinstance(payload, str):
        return _sanitize_chinese_text(payload)
    if isinstance(payload, list):
        return [_sanitize_result_payload(x) for x in payload]
    if isinstance(payload, dict):
        return {k: _sanitize_result_payload(v) for k, v in payload.items()}
    return payload


def _sanitize_model_result(result: BaseModel, model_class: type[BaseModel]) -> BaseModel:
    try:
        if hasattr(result, "model_dump"):
            raw_payload = result.model_dump()
        elif hasattr(result, "dict"):
            raw_payload = result.dict()
        elif hasattr(result, "__dict__"):
            raw_payload = dict(result.__dict__)
        else:
            return result
        clean_payload = _sanitize_result_payload(raw_payload)
        return model_class(**clean_payload)
    except Exception:
        return result


def call_llm(
    prompt: Any,
    pydantic_model: type[BaseModel],
    agent_name: str | None = None,
    state: AgentState | None = None,
    max_retries: int = 3,
    default_factory=None,
) -> BaseModel:
    """
    Make an LLM call with retry logic.

    Args:
        prompt: Prompt payload for the model.
        pydantic_model: Output schema.
        agent_name: Agent id/name for routing and progress updates.
        state: Graph state.
        max_retries: Max retries.
        default_factory: Optional fallback factory on final failure.
    """
    if state and agent_name:
        model_name, model_provider = get_agent_model_config(state, agent_name)
    else:
        model_name, model_provider = ("deepseek-ai/DeepSeek-V3", "DeepSeek")

    api_keys = None
    if state:
        request = state.get("metadata", {}).get("request")
        if request and hasattr(request, "api_keys"):
            api_keys = request.api_keys

    retrieval_bundle = retrieve_for_agent_call(
        state=state,
        agent_name=agent_name,
        query=_prompt_to_query_text(prompt),
    )
    if retrieval_bundle.get("enabled") and retrieval_bundle.get("insufficient"):
        if agent_name:
            progress.update_status(agent_name, None, "证据不足，已降级")
        if default_factory:
            try:
                return _sanitize_model_result(default_factory(), pydantic_model)
            except Exception:
                pass
        return _create_evidence_insufficient_response(pydantic_model)

    model_info = get_model_info(model_name, model_provider)
    llm = get_model(model_name, model_provider, api_keys)
    prompt = _inject_retrieval_context(prompt, retrieval_bundle)
    prompt = _inject_a_share_rules(prompt, agent_name)

    if not (model_info and not model_info.has_json_mode()):
        llm = llm.with_structured_output(
            pydantic_model,
            method="json_mode",
        )

    for attempt in range(max_retries):
        try:
            result = llm.invoke(prompt)
            if model_info and not model_info.has_json_mode():
                parsed_result = extract_json_from_response(result.content)
                if parsed_result:
                    return _sanitize_model_result(pydantic_model(**parsed_result), pydantic_model)
            else:
                return _sanitize_model_result(result, pydantic_model)
        except Exception as exc:
            if agent_name:
                progress.update_status(agent_name, None, f"Error - retry {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                print(f"Error in LLM call after {max_retries} attempts: {exc}")
                if default_factory:
                    return _sanitize_model_result(default_factory(), pydantic_model)
                return create_default_response(pydantic_model)

    return create_default_response(pydantic_model)


def create_default_response(model_class: type[BaseModel]) -> BaseModel:
    """Create a safe default response based on schema fields."""
    default_values = {}
    for field_name, field in model_class.model_fields.items():
        if field.annotation == str:
            default_values[field_name] = "分析异常，使用默认值"
        elif field.annotation == float:
            default_values[field_name] = 0.0
        elif field.annotation == int:
            default_values[field_name] = 0
        elif hasattr(field.annotation, "__origin__") and field.annotation.__origin__ == dict:
            default_values[field_name] = {}
        elif hasattr(field.annotation, "__origin__") and field.annotation.__origin__ == list:
            default_values[field_name] = []
        elif hasattr(field.annotation, "__args__") and field.annotation.__args__:
            default_values[field_name] = field.annotation.__args__[0]
        else:
            default_values[field_name] = None

    return model_class(**default_values)


def extract_json_from_response(content: str) -> dict | None:
    """Extract JSON from model text response."""
    try:
        direct = content.strip()
        if direct.startswith("{") and direct.endswith("}"):
            return json.loads(direct)

        json_start = content.find("```json")
        if json_start != -1:
            json_text = content[json_start + 7 :]
            json_end = json_text.find("```")
            if json_end != -1:
                return json.loads(json_text[:json_end].strip())

        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(content[start : end + 1])
    except Exception as exc:
        print(f"Error extracting JSON from response: {exc}")
    return None


def get_agent_model_config(state: AgentState, agent_name: str) -> tuple[str, str]:
    """Get model config for a specific agent via A-share role routing."""
    routed_model, routed_provider = resolve_agent_model(agent_name)
    return routed_model, routed_provider.value
