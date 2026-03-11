from __future__ import annotations

import json

from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from typing_extensions import Literal

from src.graph.state import AgentState, show_agent_reasoning
from src.tools.api import get_financial_metrics
from src.utils.api_key import get_api_key_from_state
from src.utils.llm import call_llm
from src.utils.progress import progress


class FundamentalLLMOutput(BaseModel):
    signal: Literal["bullish", "bearish", "neutral"]
    confidence: int = Field(description="0-100")
    conclusion: str
    basis: str
    risk: str
    trigger: str
    evidence_ids: list[str] = Field(default_factory=list)


def _latest_evidence_ids(state: AgentState, agent_id: str) -> list[str]:
    logs = state.get("data", {}).get("retrieval_logs", [])
    for row in reversed(logs):
        if row.get("agent") == agent_id:
            return list(row.get("hit_ids") or [])
    return []


def fundamentals_analyst_agent(state: AgentState, agent_id: str = "fundamentals_analyst_agent"):
    """Analyze fundamentals and generate Chinese A-share signals."""
    data = state["data"]
    end_date = data["end_date"]
    tickers = data["tickers"]
    api_key = get_api_key_from_state(state, "AKSHARE_API_KEY")
    fundamental_analysis: dict[str, dict] = {}

    for ticker in tickers:
        progress.update_status(agent_id, ticker, "正在获取财务指标")
        financial_metrics = get_financial_metrics(
            ticker=ticker,
            end_date=end_date,
            period="ttm",
            limit=10,
            api_key=api_key,
            agent_name=agent_id,
        )
        if not financial_metrics:
            fundamental_analysis[ticker] = {
                "signal": "neutral",
                "confidence": 20,
                "reasoning": {
                    "conclusion": "证据不足",
                    "basis": "未获取到有效财务指标",
                    "risk": "数据源可能缺失或接口波动",
                    "trigger": "补齐财报后再评估",
                    "evidence_ids": [],
                },
            }
            progress.update_status(agent_id, ticker, "证据不足，已降级")
            continue

        metrics = financial_metrics[0]
        return_on_equity = metrics.return_on_equity
        net_margin = metrics.net_margin
        operating_margin = metrics.operating_margin
        revenue_growth = metrics.revenue_growth
        earnings_growth = metrics.earnings_growth
        current_ratio = metrics.current_ratio
        debt_to_equity = metrics.debt_to_equity
        pe_ratio = metrics.price_to_earnings_ratio
        pb_ratio = metrics.price_to_book_ratio
        current_price = metrics.current_price
        intrinsic_value = metrics.intrinsic_value_estimate

        summary_payload = {
            "ticker": ticker,
            "report_period": metrics.report_period,
            "roe": return_on_equity,
            "net_margin": net_margin,
            "operating_margin": operating_margin,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "current_ratio": current_ratio,
            "debt_to_equity": debt_to_equity,
            "pe_ttm": pe_ratio,
            "pb": pb_ratio,
            "current_price": current_price,
            "intrinsic_value_estimate": intrinsic_value,
            "a_share_context": {
                "northbound_capital": "结合北向资金近20日净流入与持仓变化",
                "national_team_capital": "关注汇金/社保/证金等国家队资金信号",
                "policy_guidance": "跟踪发改委、央行、证监会政策导向",
                "theme_and_hot_money": "识别题材炒作与游资情绪的退潮风险",
            },
        }

        progress.update_status(agent_id, ticker, "正在分析")
        template = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "你是A股基本面分析师。仅基于输入数据给出结论，禁止编造。"
                    "必须输出中文，并给出结论/依据/风险/触发条件/置信度/引用证据ID。",
                ),
                (
                    "human",
                    "请分析以下数据：\n{analysis_data}\n\n"
                    "返回JSON：\n"
                    "{{\n"
                    '  "signal": "bullish" | "bearish" | "neutral",\n'
                    '  "confidence": 0-100,\n'
                    '  "conclusion": "一句话结论",\n'
                    '  "basis": "核心依据（含PE/PB/ROE等）",\n'
                    '  "risk": "主要风险",\n'
                    '  "trigger": "触发条件",\n'
                    '  "evidence_ids": ["证据ID"]\n'
                    "}}",
                ),
            ]
        )
        prompt = template.invoke({"analysis_data": json.dumps(summary_payload, ensure_ascii=False, indent=2)})

        def _default():
            return FundamentalLLMOutput(
                signal="neutral",
                confidence=35,
                conclusion="证据不足",
                basis="关键财务证据不足，无法形成高置信判断",
                risk="补充财报前存在误判风险",
                trigger="获取完整财报与估值后再决策",
                evidence_ids=[],
            )

        llm_out = call_llm(
            prompt=prompt,
            pydantic_model=FundamentalLLMOutput,
            agent_name=agent_id,
            state=state,
            default_factory=_default,
        )
        evidence_ids = llm_out.evidence_ids or _latest_evidence_ids(state, agent_id)

        fundamental_analysis[ticker] = {
            "signal": llm_out.signal,
            "confidence": llm_out.confidence,
            "reasoning": {
                "conclusion": llm_out.conclusion,
                "basis": llm_out.basis,
                "risk": llm_out.risk,
                "trigger": llm_out.trigger,
                "evidence_ids": evidence_ids,
                "metric_snapshot": summary_payload,
            },
        }
        progress.update_status(
            agent_id,
            ticker,
            "已完成",
            analysis=json.dumps(fundamental_analysis[ticker]["reasoning"], ensure_ascii=False, indent=2),
        )

    message = HumanMessage(
        content=json.dumps(fundamental_analysis, ensure_ascii=False, indent=2),
        name=agent_id,
    )

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(fundamental_analysis, "基本面分析师")

    state["data"]["analyst_signals"][agent_id] = fundamental_analysis
    progress.update_status(agent_id, None, "已完成")
    return {
        "messages": [message],
        "data": data,
    }
