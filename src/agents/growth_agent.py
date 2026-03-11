from __future__ import annotations

"""Growth Agent."""

import json

from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from typing_extensions import Literal

from src.graph.state import AgentState, show_agent_reasoning
from src.tools.api import get_financial_metrics, get_insider_trades
from src.utils.api_key import get_api_key_from_state
from src.utils.llm import call_llm
from src.utils.progress import progress


class GrowthLLMOutput(BaseModel):
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


def growth_analyst_agent(state: AgentState, agent_id: str = "growth_analyst_agent"):
    """Run growth analysis and generate Chinese A-share signals."""
    data = state["data"]
    end_date = data["end_date"]
    tickers = data["tickers"]
    api_key = get_api_key_from_state(state, "AKSHARE_API_KEY")
    growth_analysis: dict[str, dict] = {}

    for ticker in tickers:
        progress.update_status(agent_id, ticker, "正在获取财务数据")
        financial_metrics = get_financial_metrics(
            ticker=ticker,
            end_date=end_date,
            period="ttm",
            limit=12,
            api_key=api_key,
            agent_name=agent_id,
        )
        if not financial_metrics or len(financial_metrics) < 3:
            growth_analysis[ticker] = {
                "signal": "neutral",
                "confidence": 20,
                "reasoning": {
                    "conclusion": "证据不足",
                    "basis": "成长数据样本不足（少于3期）",
                    "risk": "样本太少会导致增长斜率失真",
                    "trigger": "补齐历史财务后再评估",
                    "evidence_ids": [],
                },
            }
            progress.update_status(agent_id, ticker, "证据不足，已降级")
            continue

        recent = financial_metrics[0]
        previous = financial_metrics[1]
        older = financial_metrics[2]

        insider_trades = get_insider_trades(
            ticker=ticker,
            end_date=end_date,
            limit=200,
            api_key=api_key,
            agent_name=agent_id,
        )
        insider_buy = sum(
            1 for t in insider_trades if t.transaction_shares is not None and t.transaction_shares > 0
        )
        insider_sell = sum(
            1 for t in insider_trades if t.transaction_shares is not None and t.transaction_shares < 0
        )

        snapshot = {
            "ticker": ticker,
            "current_price": recent.current_price,
            "intrinsic_value_estimate": recent.intrinsic_value_estimate,
            "revenue_growth_t0": recent.revenue_growth,
            "revenue_growth_t1": previous.revenue_growth,
            "revenue_growth_t2": older.revenue_growth,
            "earnings_growth_t0": recent.earnings_growth,
            "earnings_growth_t1": previous.earnings_growth,
            "earnings_growth_t2": older.earnings_growth,
            "peg_ratio": recent.peg_ratio,
            "pe_ttm": recent.price_to_earnings_ratio,
            "pb": recent.price_to_book_ratio,
            "gross_margin": recent.gross_margin,
            "operating_margin": recent.operating_margin,
            "debt_to_equity": recent.debt_to_equity,
            "current_ratio": recent.current_ratio,
            "insider_buy_count": insider_buy,
            "insider_sell_count": insider_sell,
            "a_share_context": {
                "northbound_capital": "关注北向资金对成长赛道的持续性偏好",
                "policy_guidance": "关注新质生产力、算力、半导体等政策催化",
                "theme_and_hot_money": "题材拥挤时防范一致性反转",
            },
        }

        progress.update_status(agent_id, ticker, "正在分析")
        template = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "你是A股成长风格分析师。只允许依据输入数据给出结论，禁止编造。"
                    "必须输出中文，并给出结论/依据/风险/触发条件/置信度/引用证据ID。",
                ),
                (
                    "human",
                    "请分析以下成长数据：\n{analysis_data}\n\n"
                    "返回JSON：\n"
                    "{{\n"
                    '  "signal": "bullish" | "bearish" | "neutral",\n'
                    '  "confidence": 0-100,\n'
                    '  "conclusion": "一句话结论",\n'
                    '  "basis": "成长依据（增速、估值、情绪）",\n'
                    '  "risk": "主要风险",\n'
                    '  "trigger": "触发条件",\n'
                    '  "evidence_ids": ["证据ID"]\n'
                    "}}",
                ),
            ]
        )
        prompt = template.invoke({"analysis_data": json.dumps(snapshot, ensure_ascii=False, indent=2)})

        def _default():
            return GrowthLLMOutput(
                signal="neutral",
                confidence=35,
                conclusion="证据不足",
                basis="当前成长证据不足以给出高置信判断",
                risk="赛道拥挤与估值波动风险并存",
                trigger="业绩与订单增速再次确认后再行动",
                evidence_ids=[],
            )

        llm_out = call_llm(
            prompt=prompt,
            pydantic_model=GrowthLLMOutput,
            agent_name=agent_id,
            state=state,
            default_factory=_default,
        )
        evidence_ids = llm_out.evidence_ids or _latest_evidence_ids(state, agent_id)

        growth_analysis[ticker] = {
            "signal": llm_out.signal,
            "confidence": llm_out.confidence,
            "reasoning": {
                "conclusion": llm_out.conclusion,
                "basis": llm_out.basis,
                "risk": llm_out.risk,
                "trigger": llm_out.trigger,
                "evidence_ids": evidence_ids,
                "metric_snapshot": snapshot,
            },
        }
        progress.update_status(
            agent_id,
            ticker,
            "已完成",
            analysis=json.dumps(growth_analysis[ticker]["reasoning"], ensure_ascii=False, indent=2),
        )

    message = HumanMessage(content=json.dumps(growth_analysis, ensure_ascii=False, indent=2), name=agent_id)
    if state["metadata"].get("show_reasoning"):
        show_agent_reasoning(growth_analysis, "成长风格分析师")

    state["data"]["analyst_signals"][agent_id] = growth_analysis
    progress.update_status(agent_id, None, "已完成")
    return {"messages": [message], "data": data}
