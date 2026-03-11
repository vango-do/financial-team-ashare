import json
import statistics
from typing_extensions import Literal

from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from src.graph.state import AgentState, show_agent_reasoning
from src.tools.api import get_company_news, get_financial_metrics, get_prices
from src.utils.api_key import get_api_key_from_state
from src.utils.llm import call_llm
from src.utils.progress import progress


class GeorgeSorosSignal(BaseModel):
    signal: Literal["bullish", "bearish", "neutral"]
    confidence: int = Field(description="Confidence 0-100")
    reasoning: str


def george_soros_agent(state: AgentState, agent_id: str = "soros_agent"):
    """
    Analyze stocks in a Soros-style reflexivity framework.
    Focuses on narrative/price feedback loop, momentum, and sentiment shifts.
    """
    data = state["data"]
    start_date = data["start_date"]
    end_date = data["end_date"]
    tickers = data["tickers"]
    api_key = get_api_key_from_state(state, "AKSHARE_API_KEY")

    soros_analysis: dict[str, dict] = {}

    for ticker in tickers:
        progress.update_status(agent_id, ticker, "Fetching price data")
        prices = get_prices(
            ticker,
            start_date=start_date,
            end_date=end_date,
            api_key=api_key,
            agent_name=agent_id,
        )

        progress.update_status(agent_id, ticker, "Fetching financial metrics")
        metrics = get_financial_metrics(
            ticker,
            end_date=end_date,
            period="ttm",
            limit=5,
            api_key=api_key,
            agent_name=agent_id,
        )

        progress.update_status(agent_id, ticker, "Fetching company news")
        news = get_company_news(
            ticker,
            end_date=end_date,
            start_date=start_date,
            limit=30,
            api_key=api_key,
            agent_name=agent_id,
        )

        progress.update_status(agent_id, ticker, "Evaluating reflexivity factors")
        factor_snapshot = _build_factor_snapshot(prices=prices, metrics=metrics, news=news)
        pre_signal, confidence_hint = _make_pre_signal(factor_snapshot)

        progress.update_status(agent_id, ticker, "Generating George Soros analysis")
        llm_signal = _generate_soros_output(
            ticker=ticker,
            factor_snapshot=factor_snapshot,
            pre_signal=pre_signal,
            confidence_hint=confidence_hint,
            state=state,
            agent_id=agent_id,
        )

        soros_analysis[ticker] = {
            "signal": llm_signal.signal,
            "confidence": llm_signal.confidence,
            "reasoning": llm_signal.reasoning,
        }
        progress.update_status(agent_id, ticker, "Done", analysis=llm_signal.reasoning)

    message = HumanMessage(content=json.dumps(soros_analysis, ensure_ascii=False, indent=2), name=agent_id)
    if state["metadata"].get("show_reasoning"):
        show_agent_reasoning(soros_analysis, "George Soros Agent")

    state["data"]["analyst_signals"][agent_id] = soros_analysis
    progress.update_status(agent_id, None, "Done")
    return {"messages": [message], "data": state["data"]}


def _build_factor_snapshot(prices, metrics, news) -> dict:
    close_prices = [p.close for p in sorted(prices, key=lambda x: x.time) if p.close is not None] if prices else []
    returns = []
    for idx in range(1, len(close_prices)):
        prev = close_prices[idx - 1]
        cur = close_prices[idx]
        if prev and prev > 0:
            returns.append((cur - prev) / prev)

    momentum = 0.0
    volatility = 0.0
    if len(close_prices) >= 2 and close_prices[0] > 0:
        momentum = (close_prices[-1] - close_prices[0]) / close_prices[0]
    if returns:
        volatility = statistics.pstdev(returns)

    latest_metric = metrics[0] if metrics else None
    pe = latest_metric.price_to_earnings_ratio if latest_metric else None
    pb = latest_metric.price_to_book_ratio if latest_metric else None
    revenue_growth = latest_metric.revenue_growth if latest_metric else None
    earnings_growth = latest_metric.earnings_growth if latest_metric else None

    negative_keywords = ["减持", "立案", "亏损", "诉讼", "风险", "暴雷", "调查", "违约"]
    negative_count = 0
    total_news = len(news) if news else 0
    if news:
        for item in news:
            title = (item.title or "").lower()
            if any(kw in title for kw in negative_keywords):
                negative_count += 1
    negative_ratio = (negative_count / total_news) if total_news > 0 else 0.0

    return {
        "momentum": momentum,
        "volatility": volatility,
        "pe_ttm": pe,
        "pb": pb,
        "revenue_growth": revenue_growth,
        "earnings_growth": earnings_growth,
        "news_total": total_news,
        "negative_news_ratio": negative_ratio,
    }


def _make_pre_signal(snapshot: dict) -> tuple[str, int]:
    score = 0
    momentum = snapshot.get("momentum") or 0.0
    volatility = snapshot.get("volatility") or 0.0
    pe = snapshot.get("pe_ttm")
    pb = snapshot.get("pb")
    neg_ratio = snapshot.get("negative_news_ratio") or 0.0

    if momentum >= 0.12:
        score += 3
    elif momentum >= 0.03:
        score += 2
    elif momentum <= -0.08:
        score -= 2

    if volatility >= 0.045:
        score -= 2
    elif volatility <= 0.02:
        score += 1

    if pe is not None:
        if pe > 80:
            score -= 1
        elif pe > 0 and pe < 30:
            score += 1

    if pb is not None:
        if pb > 15:
            score -= 1
        elif pb > 0 and pb < 6:
            score += 1

    if neg_ratio > 0.3:
        score -= 2
    elif neg_ratio < 0.1 and snapshot.get("news_total", 0) >= 5:
        score += 1

    if score >= 3:
        signal = "bullish"
    elif score <= -2:
        signal = "bearish"
    else:
        signal = "neutral"

    confidence = int(max(35, min(90, 55 + score * 8)))
    return signal, confidence


def _generate_soros_output(
    ticker: str,
    factor_snapshot: dict,
    pre_signal: str,
    confidence_hint: int,
    state: AgentState,
    agent_id: str,
) -> GeorgeSorosSignal:
    template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "你是乔治·索罗斯风格分析师。使用反身性框架：叙事影响预期，预期影响价格，价格再反哺叙事。\n"
                "只输出JSON，理由用中文且不超过120字。置信度必须使用给定值，不得修改。",
            ),
            (
                "human",
                "代码：{ticker}\n"
                "因子快照：{factors}\n"
                "预信号：{pre_signal}\n"
                "置信度：{confidence}\n"
                "严格返回：\n"
                "{{\n"
                '  "signal": "bullish" | "bearish" | "neutral",\n'
                f'  "confidence": {confidence_hint},\n'
                '  "reasoning": "中文依据"\n'
                "}}",
            ),
        ]
    )
    prompt = template.invoke(
        {
            "ticker": ticker,
            "factors": json.dumps(factor_snapshot, ensure_ascii=False, indent=2),
            "pre_signal": pre_signal,
            "confidence": confidence_hint,
        }
    )

    def _default():
        return GeorgeSorosSignal(signal=pre_signal, confidence=confidence_hint, reasoning="数据有限，维持预判。")

    return call_llm(
        prompt=prompt,
        pydantic_model=GeorgeSorosSignal,
        agent_name=agent_id,
        state=state,
        default_factory=_default,
    )
