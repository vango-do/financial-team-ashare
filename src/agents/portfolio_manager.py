import json
import time
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.graph.state import AgentState, show_agent_reasoning
from pydantic import BaseModel, Field
from typing_extensions import Literal
from src.utils.progress import progress
from src.utils.llm import call_llm
from src.rag.master_retrieval import prepare_chief_memory_records


class PortfolioDecision(BaseModel):
    action: Literal["buy", "sell", "cover", "hold"]
    quantity: int = Field(description="Number of shares to trade")
    confidence: int = Field(description="Confidence 0-100")
    reasoning: str = Field(description="Reasoning for the decision")


class PortfolioManagerOutput(BaseModel):
    decisions: dict[str, PortfolioDecision] = Field(description="Dictionary of ticker to trading decisions")


class PortfolioReasoningOutput(BaseModel):
    reasons: dict[str, str] = Field(description="Dictionary of ticker to Chinese reasoning text")


##### Portfolio Management Agent #####
def portfolio_management_agent(state: AgentState, agent_id: str = "portfolio_manager"):
    """Makes final trading decisions and generates orders for multiple tickers"""

    portfolio = state["data"]["portfolio"]
    analyst_signals = state["data"]["analyst_signals"]
    tickers = state["data"]["tickers"]

    position_limits = {}
    current_prices = {}
    max_shares = {}
    signals_by_ticker = {}
    for ticker in tickers:
        progress.update_status(agent_id, ticker, "Processing analyst signals")

        # Find the corresponding risk manager for this portfolio manager
        if agent_id.startswith("portfolio_manager_"):
            suffix = agent_id.split('_')[-1]
            risk_manager_id = f"risk_management_agent_{suffix}"
        else:
            risk_manager_id = "risk_management_agent"  # Fallback for CLI

        risk_data = analyst_signals.get(risk_manager_id, {}).get(ticker, {})
        position_limits[ticker] = risk_data.get("remaining_position_limit", 0.0)
        current_prices[ticker] = float(risk_data.get("current_price", 0.0))

        # Calculate maximum shares allowed based on position limit and price
        if current_prices[ticker] > 0:
            max_shares[ticker] = int(position_limits[ticker] // current_prices[ticker])
        else:
            max_shares[ticker] = 0

        # Compress analyst signals to {sig, conf}
        ticker_signals = {}
        for agent, signals in analyst_signals.items():
            if not agent.startswith("risk_management_agent") and ticker in signals:
                sig = signals[ticker].get("signal")
                conf = signals[ticker].get("confidence")
                if sig is not None and conf is not None:
                    ticker_signals[agent] = {"sig": sig, "conf": conf}
        signals_by_ticker[ticker] = ticker_signals

    state["data"]["current_prices"] = current_prices
    state["data"]["chief_memory_records"] = prepare_chief_memory_records(
        tickers=tickers,
        analyst_signals=analyst_signals,
    )

    progress.update_status(agent_id, None, "Generating trading decisions")

    result = generate_trading_decision(
        tickers=tickers,
        signals_by_ticker=signals_by_ticker,
        current_prices=current_prices,
        max_shares=max_shares,
        portfolio=portfolio,
        agent_id=agent_id,
        state=state,
    )
    message = HumanMessage(
        content=json.dumps(
            {ticker: decision.model_dump() for ticker, decision in result.decisions.items()},
            ensure_ascii=False,
            indent=2,
        ),
        name=agent_id,
    )

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning({ticker: decision.model_dump() for ticker, decision in result.decisions.items()},
                             "Portfolio Manager")

    progress.update_status(agent_id, None, "Done")

    return {
        "messages": state["messages"] + [message],
        "data": state["data"],
    }


def compute_allowed_actions(
        tickers: list[str],
        current_prices: dict[str, float],
        max_shares: dict[str, int],
        portfolio: dict[str, float],
) -> dict[str, dict[str, int | dict[str, object]]]:
    """Compute allowed actions and max quantities for each ticker deterministically."""
    allowed = {}
    cash = float(portfolio.get("cash", 0.0))
    positions = portfolio.get("positions", {}) or {}
    a_share_constraints = portfolio.get("a_share_constraints", {}) or {}
    t_plus_one_blocked = {
        str(x).strip()
        for x in (a_share_constraints.get("t_plus_one_blocked_tickers", []) or [])
        if str(x).strip()
    }
    today_buys = a_share_constraints.get("today_buys", {}) or {}
    price_limit_status = a_share_constraints.get("price_limit_status", {}) or {}

    def _default_limit_pct(symbol: str) -> float:
        # STAR/ChiNext -> 20%, others -> 10%
        if str(symbol).startswith(("688", "300")):
            return 0.20
        return 0.10

    for ticker in tickers:
        price = float(current_prices.get(ticker, 0.0))
        pos = positions.get(
            ticker,
            {"long": 0, "long_cost_basis": 0.0, "short": 0, "short_cost_basis": 0.0},
        )
        long_shares = int(pos.get("long", 0) or 0)
        short_shares = int(pos.get("short", 0) or 0)
        max_qty = int(max_shares.get(ticker, 0) or 0)
        ticker_limit_info = price_limit_status.get(ticker, {}) if isinstance(price_limit_status, dict) else {}
        if not isinstance(ticker_limit_info, dict):
            ticker_limit_info = {}
        limit_pct = float(ticker_limit_info.get("limit_pct") or _default_limit_pct(ticker))
        is_limit_up = bool(ticker_limit_info.get("is_limit_up") or ticker_limit_info.get("limit_up"))
        is_limit_down = bool(ticker_limit_info.get("is_limit_down") or ticker_limit_info.get("limit_down"))

        raw_today_buy_qty = 0
        if isinstance(today_buys, dict):
            raw_today_buy_qty = today_buys.get(ticker, 0) or 0
        pos_today_buy_qty = pos.get("today_bought", 0) or 0
        try:
            today_buy_qty = int(max(0, float(raw_today_buy_qty), float(pos_today_buy_qty)))
        except Exception:
            today_buy_qty = 0
        is_t_plus_one_blocked = (ticker in t_plus_one_blocked) or (today_buy_qty > 0)
        sellable_long_shares = max(0, long_shares - today_buy_qty) if is_t_plus_one_blocked else long_shares

        # Start with zeros
        actions = {"buy": 0, "sell": 0, "cover": 0, "hold": 0}

        # Long side + A-share hard gates:
        # 1) T+1: today's buy quantity cannot be sold on the same day.
        # 2) Limit-down: block sell when marked as limit-down.
        if sellable_long_shares > 0 and not is_limit_down:
            actions["sell"] = sellable_long_shares

        # Buy gate: block opening buys when marked as limit-up.
        if cash > 0 and price > 0 and not is_limit_up:
            max_buy_cash = int(cash // price)
            max_buy = max(0, min(max_qty, max_buy_cash))
            if max_buy > 0:
                actions["buy"] = max_buy

        # Cover only used for legacy short positions (no new short opening allowed).
        if short_shares > 0 and not is_limit_up:
            actions["cover"] = short_shares

        # Hold always valid
        actions["hold"] = 0

        # Prune zero-capacity actions to reduce tokens, keep hold
        pruned = {"hold": 0}
        for k, v in actions.items():
            if k != "hold" and v > 0:
                pruned[k] = v
        # A-share hard-constraint placeholders for downstream visibility/audit.
        pruned["_a_share_guard"] = {
            "short_disabled": True,
            "t_plus_one_blocked": bool(is_t_plus_one_blocked),
            "today_buy_qty": int(today_buy_qty),
            "is_limit_up": bool(is_limit_up),
            "is_limit_down": bool(is_limit_down),
            "limit_pct": float(limit_pct),
        }

        allowed[ticker] = pruned

    return allowed


def _compact_signals(signals_by_ticker: dict[str, dict]) -> dict[str, dict]:
    """Keep only {agent: {sig, conf}} and drop empty agents."""
    out = {}
    for t, agents in signals_by_ticker.items():
        if not agents:
            out[t] = {}
            continue
        compact = {}
        for agent, payload in agents.items():
            sig = payload.get("sig") or payload.get("signal")
            conf = payload.get("conf") if "conf" in payload else payload.get("confidence")
            if sig is not None and conf is not None:
                compact[agent] = {"sig": sig, "conf": conf}
        out[t] = compact
    return out


def _normalize_signal(signal: str | None) -> str:
    value = str(signal or "").strip().lower()
    if value in {"bullish", "buy", "long"}:
        return "bullish"
    if value in {"bearish", "sell", "short"}:
        return "bearish"
    return "neutral"


def _majority_vote(signal_payload: dict[str, dict]) -> tuple[str, dict[str, int], int]:
    counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    conf_bucket: dict[str, list[float]] = {"bullish": [], "bearish": [], "neutral": []}

    for payload in signal_payload.values():
        sig = _normalize_signal(payload.get("sig"))
        counts[sig] += 1
        try:
            conf_val = float(payload.get("conf", 0))
        except Exception:
            conf_val = 0.0
        conf_bucket[sig].append(max(0.0, min(100.0, conf_val)))

    total = sum(counts.values())
    if total <= 0:
        return "neutral", counts, 50

    max_count = max(counts.values())
    leaders = [sig for sig, cnt in counts.items() if cnt == max_count]
    # Strict majority rule: ties are forced to neutral.
    majority = leaders[0] if len(leaders) == 1 else "neutral"

    ratio = (max_count / total) if total else 0.0
    majority_confs = conf_bucket.get(majority) or []
    avg_conf = (sum(majority_confs) / len(majority_confs)) if majority_confs else 50.0
    confidence = int(round((ratio * 0.6 + (avg_conf / 100.0) * 0.4) * 100))
    if len(leaders) != 1:
        confidence = min(confidence, 55)
    confidence = max(0, min(100, confidence))
    return majority, counts, confidence


def _action_from_majority_signal(
    majority_signal: str,
    allowed_actions: dict[str, int | dict[str, object]],
) -> tuple[str, int]:
    allowed = dict(allowed_actions or {})

    if majority_signal == "bullish":
        if int(allowed.get("buy", 0)) > 0:
            return "buy", int(allowed["buy"])
        if int(allowed.get("cover", 0)) > 0:
            return "cover", int(allowed["cover"])
        return "hold", 0

    if majority_signal == "bearish":
        if int(allowed.get("sell", 0)) > 0:
            return "sell", int(allowed["sell"])
        return "hold", 0

    return "hold", 0


def _fallback_reasoning_text(
    ticker: str,
    majority_signal: str,
    counts: dict[str, int],
    action: str,
) -> str:
    signal_cn = {"bullish": "看多", "bearish": "看空", "neutral": "中性"}.get(majority_signal, "中性")
    action_cn = {"buy": "买入", "sell": "卖出", "cover": "回补", "hold": "持有"}.get(action, "持有")
    return (
        f"按多数票规则执行：{signal_cn}为主。"
        f"票数看多/看空/中性={counts.get('bullish', 0)}/{counts.get('bearish', 0)}/{counts.get('neutral', 0)}，"
        f"因此执行{action_cn}。"
    )


def generate_trading_decision(
        tickers: list[str],
        signals_by_ticker: dict[str, dict],
        current_prices: dict[str, float],
        max_shares: dict[str, int],
        portfolio: dict[str, float],
        agent_id: str,
        state: AgentState,
) -> PortfolioManagerOutput:
    """Strict majority-vote execution; LLM only writes final Chinese reasoning."""
    allowed_actions_full = compute_allowed_actions(tickers, current_prices, max_shares, portfolio)
    compact_signals = _compact_signals({t: signals_by_ticker.get(t, {}) for t in tickers})

    deterministic_decisions: dict[str, PortfolioDecision] = {}
    reasoning_payload: dict[str, dict] = {}
    for ticker in tickers:
        ticker_signals = compact_signals.get(ticker, {})
        majority_signal, vote_counts, confidence = _majority_vote(ticker_signals)
        action, quantity = _action_from_majority_signal(
            majority_signal=majority_signal,
            allowed_actions=allowed_actions_full.get(ticker, {"hold": 0}),
        )
        deterministic_decisions[ticker] = PortfolioDecision(
            action=action,
            quantity=int(quantity),
            confidence=int(confidence),
            reasoning="",
        )
        reasoning_payload[ticker] = {
            "majority_signal": majority_signal,
            "vote_counts": vote_counts,
            "selected_action": action,
            "selected_quantity": int(quantity),
            "selected_confidence": int(confidence),
            "allowed_actions": allowed_actions_full.get(ticker, {"hold": 0}),
        }

    # LLM writes only the reasoning for fixed decisions.
    template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "你是A股投资组合经理的理由撰写器。\n"
                "交易动作、股数、置信度都已由多数票规则预先确定，你不得修改这些值。\n"
                "你只能为每个代码补充中文理由，每个理由不超过120字。\n"
                "必须遵守A股规则描述：T+1、主板10%与科创/创业板20%涨跌停，并且禁止做空开仓。\n"
                "只输出JSON。"
            ),
            (
                "human",
                "请基于以下已确定决策生成理由：\n{decision_payload}\n\n"
                "输出格式：\n"
                "{{\n"
                '  "reasons": {{\n'
                '    "TICKER": "中文理由"\n'
                "  }}\n"
                "}}"
            ),
        ]
    )

    prompt_data = {
        "decision_payload": json.dumps(reasoning_payload, ensure_ascii=False, indent=2),
    }
    prompt = template.invoke(prompt_data)

    def create_default_reason_output():
        return PortfolioReasoningOutput(
            reasons={t: _fallback_reasoning_text(
                ticker=t,
                majority_signal=reasoning_payload[t]["majority_signal"],
                counts=reasoning_payload[t]["vote_counts"],
                action=reasoning_payload[t]["selected_action"],
            ) for t in tickers}
        )

    reason_out = call_llm(
        prompt=prompt,
        pydantic_model=PortfolioReasoningOutput,
        agent_name=agent_id,
        state=state,
        default_factory=create_default_reason_output,
    )

    reasons = dict((reason_out.reasons or {}))
    for ticker in tickers:
        deterministic_decisions[ticker].reasoning = (
            str(reasons.get(ticker) or "").strip()
            or _fallback_reasoning_text(
                ticker=ticker,
                majority_signal=reasoning_payload[ticker]["majority_signal"],
                counts=reasoning_payload[ticker]["vote_counts"],
                action=reasoning_payload[ticker]["selected_action"],
            )
        )

    return PortfolioManagerOutput(decisions=deterministic_decisions)
