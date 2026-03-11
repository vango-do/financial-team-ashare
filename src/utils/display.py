from __future__ import annotations

import json
import os
import re
import textwrap

from colorama import Fore, Style
from tabulate import tabulate

from .analysts import ANALYST_ORDER, get_agent_display_name


SIGNAL_TEXT = {
    "BULLISH": "\u770b\u591a",
    "BEARISH": "\u770b\u7a7a",
    "NEUTRAL": "\u4e2d\u6027",
}

ACTION_TEXT = {
    "BUY": "\u4e70\u5165",
    "SELL": "\u5356\u51fa",
    "HOLD": "\u6301\u6709",
    "SHORT": "\u505a\u7a7a",
    "COVER": "\u56de\u8865",
}

_ANALYST_KEY_ORDER = {analyst_key: idx for idx, (_, analyst_key) in enumerate(ANALYST_ORDER)}


def _agent_sort_key(agent_node: str) -> int:
    if agent_node == "risk_management_agent":
        return len(_ANALYST_KEY_ORDER) + 1
    if agent_node.endswith("_agent"):
        analyst_key = agent_node[: -len("_agent")]
        return _ANALYST_KEY_ORDER.get(analyst_key, 999)
    return _ANALYST_KEY_ORDER.get(agent_node, 999)


def _format_reasoning(reasoning) -> str:
    if reasoning is None:
        return ""
    if isinstance(reasoning, str):
        text = reasoning
    elif isinstance(reasoning, (dict, list)):
        text = json.dumps(reasoning, ensure_ascii=False, indent=2)
    else:
        text = str(reasoning)

    alpha_count = len(re.findall(r"[A-Za-z]", text))
    han_count = len(re.findall(r"[\u4e00-\u9fff]", text))
    if han_count == 0 and alpha_count > 0:
        text = "\u4e2d\u6587\u8f93\u51fa\u7ea6\u675f\u89e6\u53d1\uff1a\u68c0\u6d4b\u5230\u82f1\u6587\u5185\u5bb9\uff0c\u5df2\u964d\u7ea7\u5c55\u793a\u3002"

    wrapped_lines = [textwrap.fill(line, width=88, break_long_words=False) for line in text.splitlines() or [text]]
    return "\n".join(wrapped_lines)


def _signal_text(signal: str) -> str:
    return SIGNAL_TEXT.get((signal or "").upper(), signal or "")


def _action_text(action: str) -> str:
    return ACTION_TEXT.get((action or "").upper(), action or "")


def _color_by_signal(signal_type: str) -> str:
    return {
        "BULLISH": Fore.GREEN,
        "BEARISH": Fore.RED,
        "NEUTRAL": Fore.YELLOW,
    }.get(signal_type.upper(), Fore.WHITE)


def _color_by_action(action: str) -> str:
    return {
        "BUY": Fore.GREEN,
        "COVER": Fore.GREEN,
        "SELL": Fore.RED,
        "SHORT": Fore.RED,
        "HOLD": Fore.YELLOW,
    }.get(action.upper(), Fore.WHITE)


def _print_reasoning_block(reasoning, indent: str = "    ") -> None:
    formatted = _format_reasoning(reasoning)
    if not formatted:
        print(f"{indent}\uff08\u65e0\uff09")
        return
    for line in formatted.splitlines():
        print(f"{indent}{line}")


def print_trading_output(result: dict) -> None:
    """Print trading results in section-style Chinese output."""
    decisions = result.get("decisions")
    if not decisions:
        print(f"{Fore.RED}\u6682\u65e0\u53ef\u7528\u4ea4\u6613\u51b3\u7b56{Style.RESET_ALL}")
        return

    analyst_signals = result.get("analyst_signals", {})

    for ticker, decision in decisions.items():
        print(
            f"\n{Fore.WHITE}{Style.BRIGHT}\u4ee3\u7801 {Fore.CYAN}{ticker}{Style.RESET_ALL}"
            f"{Fore.WHITE}{Style.BRIGHT} \u5206\u6790\u7ed3\u679c{Style.RESET_ALL}"
        )
        print(f"{Fore.WHITE}{Style.BRIGHT}{'=' * 72}{Style.RESET_ALL}")
        print(f"{Fore.WHITE}{Style.BRIGHT}\u5206\u6790\u5e08\u7ed3\u8bba\uff08\u9010\u4f4d\uff09\uff1a{Style.RESET_ALL}")

        idx = 0
        for agent, signals in sorted(analyst_signals.items(), key=lambda item: _agent_sort_key(item[0])):
            if agent == "risk_management_agent" or ticker not in signals:
                continue

            idx += 1
            signal = signals[ticker] or {}
            signal_type = str(signal.get("signal", "")).upper()
            confidence = signal.get("confidence", 0)
            confidence_text = f"{confidence:.2f}%" if isinstance(confidence, (int, float)) else f"{confidence}%"
            role = get_agent_display_name(agent)

            print(f"\n{Fore.CYAN}{idx}. {role}{Style.RESET_ALL}")
            print(
                f"  \u4fe1\u53f7\uff1a{_color_by_signal(signal_type)}{_signal_text(signal_type)}{Style.RESET_ALL}   "
                f"\u4fe1\u5fc3\u6307\u6570\uff1a{Fore.WHITE}{confidence_text}{Style.RESET_ALL}"
            )
            print("  \u51b3\u7b56\u903b\u8f91\uff1a")
            _print_reasoning_block(signal.get("reasoning"), indent="    ")

        action = str(decision.get("action", "")).upper()
        action_text = _action_text(action)
        action_color = _color_by_action(action)
        confidence = decision.get("confidence", 0)
        confidence_text = f"{confidence:.1f}%" if isinstance(confidence, (int, float)) else f"{confidence}%"

        print(f"\n{Fore.WHITE}{Style.BRIGHT}\u4ea4\u6613\u51b3\u7b56\uff1a{Style.RESET_ALL}")
        print(f"  \u4ee3\u7801\uff1a{Fore.CYAN}{ticker}{Style.RESET_ALL}")
        print(f"  \u64cd\u4f5c\u5efa\u8bae\uff1a{action_color}{action_text}{Style.RESET_ALL}")
        print(f"  \u80a1\u6570\uff1a{action_color}{decision.get('quantity', 0)}{Style.RESET_ALL}")
        print(f"  \u4fe1\u5fc3\u6307\u6570\uff1a{Fore.WHITE}{confidence_text}{Style.RESET_ALL}")
        print("  \u51b3\u7b56\u903b\u8f91\uff1a")
        _print_reasoning_block(decision.get("reasoning"), indent="    ")

    print(f"\n{Fore.WHITE}{Style.BRIGHT}\u6295\u8d44\u7ec4\u5408\u6c47\u603b\uff1a{Style.RESET_ALL}")
    portfolio_reasoning = None

    for _, decision in decisions.items():
        if decision.get("reasoning"):
            portfolio_reasoning = decision.get("reasoning")
            break

    for i, (ticker, decision) in enumerate(decisions.items(), start=1):
        action = str(decision.get("action", "")).upper()
        confidence = decision.get("confidence", 0)
        confidence_text = f"{confidence:.1f}%" if isinstance(confidence, (int, float)) else f"{confidence}%"

        bullish = 0
        bearish = 0
        neutral = 0
        for agent, signals in analyst_signals.items():
            if agent == "risk_management_agent" or ticker not in signals:
                continue
            signal = str((signals[ticker] or {}).get("signal", "")).upper()
            if signal == "BULLISH":
                bullish += 1
            elif signal == "BEARISH":
                bearish += 1
            elif signal == "NEUTRAL":
                neutral += 1

        print(
            f"{i}. \u4ee3\u7801 {Fore.CYAN}{ticker}{Style.RESET_ALL}\uff0c"
            f"\u5efa\u8bae {_action_text(action)}\uff0c"
            f"\u80a1\u6570 {decision.get('quantity', 0)}\uff0c"
            f"\u4fe1\u5fc3 {confidence_text}\uff0c"
            f"\u770b\u591a/\u770b\u7a7a/\u4e2d\u6027 = {bullish}/{bearish}/{neutral}"
        )

    if portfolio_reasoning:
        print(f"\n{Fore.WHITE}{Style.BRIGHT}\u7ec4\u5408\u7b56\u7565\u8bf4\u660e\uff1a{Style.RESET_ALL}")
        _print_reasoning_block(portfolio_reasoning, indent="  ")


def print_backtest_results(table_rows: list) -> None:
    """Print the backtest results in a formatted table."""
    os.system("cls" if os.name == "nt" else "clear")

    ticker_rows = []
    summary_rows = []
    for row in table_rows:
        label = str(row[1]) if len(row) > 1 else ""
        if "PORTFOLIO SUMMARY" in label or "\u6295\u8d44\u7ec4\u5408\u6c47\u603b" in label:
            summary_rows.append(row)
        else:
            ticker_rows.append(row)

    if summary_rows:
        latest_summary = max(summary_rows, key=lambda r: r[0])
        print(f"\n{Fore.WHITE}{Style.BRIGHT}\u6295\u8d44\u7ec4\u5408\u6c47\u603b:{Style.RESET_ALL}")

        position_str = latest_summary[7].split("$")[1].split(Style.RESET_ALL)[0].replace(",", "")
        cash_str = latest_summary[8].split("$")[1].split(Style.RESET_ALL)[0].replace(",", "")
        total_str = latest_summary[9].split("$")[1].split(Style.RESET_ALL)[0].replace(",", "")

        print(f"\u73b0\u91d1\u4f59\u989d: {Fore.CYAN}${float(cash_str):,.2f}{Style.RESET_ALL}")
        print(f"\u6301\u4ed3\u5e02\u503c: {Fore.YELLOW}${float(position_str):,.2f}{Style.RESET_ALL}")
        print(f"\u7ec4\u5408\u603b\u503c: {Fore.WHITE}${float(total_str):,.2f}{Style.RESET_ALL}")
        print(f"\u7ec4\u5408\u6536\u76ca: {latest_summary[10]}")
        if len(latest_summary) > 14 and latest_summary[14]:
            print(f"\u57fa\u51c6\u6536\u76ca: {latest_summary[14]}")
        if latest_summary[11]:
            print(f"\u590f\u666e\u6bd4\u7387: {latest_summary[11]}")
        if latest_summary[12]:
            print(f"\u7d22\u63d0\u8bfa\u6bd4\u7387: {latest_summary[12]}")
        if latest_summary[13]:
            print(f"\u6700\u5927\u56de\u64a4: {latest_summary[13]}")

    print("\n" * 2)
    print(
        tabulate(
            ticker_rows,
            headers=[
                "\u65e5\u671f",
                "\u4ee3\u7801",
                "\u64cd\u4f5c",
                "\u6570\u91cf",
                "\u4ef7\u683c",
                "\u591a\u5934\u6301\u4ed3",
                "\u7a7a\u5934\u6301\u4ed3",
                "\u6301\u4ed3\u5e02\u503c",
            ],
            tablefmt="grid",
            colalign=("left", "left", "center", "right", "right", "right", "right", "right"),
        )
    )
    print("\n" * 4)


def format_backtest_row(
    date: str,
    ticker: str,
    action: str,
    quantity: float,
    price: float,
    long_shares: float = 0,
    short_shares: float = 0,
    position_value: float = 0,
    is_summary: bool = False,
    total_value: float = None,
    return_pct: float = None,
    cash_balance: float = None,
    total_position_value: float = None,
    sharpe_ratio: float = None,
    sortino_ratio: float = None,
    max_drawdown: float = None,
    benchmark_return_pct: float | None = None,
) -> list[any]:
    """Format a row for backtest output."""
    action_color = _color_by_action(action)

    if is_summary:
        return_color = Fore.GREEN if return_pct >= 0 else Fore.RED
        benchmark_str = ""
        if benchmark_return_pct is not None:
            bench_color = Fore.GREEN if benchmark_return_pct >= 0 else Fore.RED
            benchmark_str = f"{bench_color}{benchmark_return_pct:+.2f}%{Style.RESET_ALL}"
        return [
            date,
            f"{Fore.WHITE}{Style.BRIGHT}\u6295\u8d44\u7ec4\u5408\u6c47\u603b{Style.RESET_ALL}",
            "",
            "",
            "",
            "",
            "",
            f"{Fore.YELLOW}${total_position_value:,.2f}{Style.RESET_ALL}",
            f"{Fore.CYAN}${cash_balance:,.2f}{Style.RESET_ALL}",
            f"{Fore.WHITE}${total_value:,.2f}{Style.RESET_ALL}",
            f"{return_color}{return_pct:+.2f}%{Style.RESET_ALL}",
            f"{Fore.YELLOW}{sharpe_ratio:.2f}{Style.RESET_ALL}" if sharpe_ratio is not None else "",
            f"{Fore.YELLOW}{sortino_ratio:.2f}{Style.RESET_ALL}" if sortino_ratio is not None else "",
            f"{Fore.RED}{max_drawdown:.2f}%{Style.RESET_ALL}" if max_drawdown is not None else "",
            benchmark_str,
        ]

    return [
        date,
        f"{Fore.CYAN}{ticker}{Style.RESET_ALL}",
        f"{action_color}{action.upper()}{Style.RESET_ALL}",
        f"{action_color}{quantity:,.0f}{Style.RESET_ALL}",
        f"{Fore.WHITE}{price:,.2f}{Style.RESET_ALL}",
        f"{Fore.GREEN}{long_shares:,.0f}{Style.RESET_ALL}",
        f"{Fore.RED}{short_shares:,.0f}{Style.RESET_ALL}",
        f"{Fore.YELLOW}{position_value:,.2f}{Style.RESET_ALL}",
    ]
