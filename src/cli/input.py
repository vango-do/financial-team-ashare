from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import questionary
from colorama import Fore, Style
from dateutil.relativedelta import relativedelta

from src.llm.models import LLM_ORDER, OLLAMA_LLM_ORDER, ModelProvider, find_model_by_name, get_model_info
from src.utils.analysts import ANALYST_ORDER
from src.utils.ollama import ensure_ollama_and_model


def add_common_args(
    parser: argparse.ArgumentParser,
    *,
    require_tickers: bool = False,
    include_analyst_flags: bool = True,
    include_ollama: bool = True,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--tickers",
        type=str,
        required=require_tickers,
        help="A股代码列表，逗号分隔，例如：600519,000001",
    )
    if include_analyst_flags:
        parser.add_argument(
            "--analysts",
            type=str,
            required=False,
            help="逗号分隔的分析师键名，例如：warren_buffett,soros",
        )
        parser.add_argument(
            "--analysts-all",
            action="store_true",
            help="启用全部分析师（优先级高于 --analysts）",
        )
    if include_ollama:
        parser.add_argument("--ollama", action="store_true", help="使用 Ollama 本地模型")
    parser.add_argument("--model", type=str, required=False, help="指定模型名，例如 deepseek-ai/DeepSeek-V3")
    return parser


def add_date_args(parser: argparse.ArgumentParser, *, default_months_back: int | None = None) -> argparse.ArgumentParser:
    if default_months_back is None:
        parser.add_argument("--start-date", type=str, help="开始日期 (YYYY-MM-DD)")
        parser.add_argument("--end-date", type=str, help="结束日期 (YYYY-MM-DD)")
    else:
        parser.add_argument(
            "--end-date",
            type=str,
            default=datetime.now().strftime("%Y-%m-%d"),
            help="结束日期 (YYYY-MM-DD)",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            default=(datetime.now() - relativedelta(months=default_months_back)).strftime("%Y-%m-%d"),
            help="开始日期 (YYYY-MM-DD)",
        )
    return parser


def parse_tickers(tickers_arg: str | None) -> list[str]:
    if not tickers_arg:
        return []
    return [ticker.strip() for ticker in tickers_arg.split(",") if ticker.strip()]


def resolve_tickers_interactive(tickers: list[str]) -> list[str]:
    if tickers:
        return tickers
    if not sys.stdin.isatty():
        return ["600519"]
    raw = input("请输入A股代码（多个用逗号分隔，默认600519）：").strip()
    parsed = parse_tickers(raw)
    return parsed or ["600519"]


def select_analysts(flags: dict | None = None) -> list[str]:
    if flags and flags.get("analysts_all"):
        return [a[1] for a in ANALYST_ORDER]
    if flags and flags.get("analysts"):
        return [a.strip() for a in str(flags["analysts"]).split(",") if a.strip()]
    if not sys.stdin.isatty():
        return [a[1] for a in ANALYST_ORDER]

    try:
        choices = questionary.checkbox(
            "请选择要启用的 AI 分析师：",
            choices=[questionary.Choice(display, value=value) for display, value in ANALYST_ORDER],
            instruction=(
                "\n\n操作说明：\n"
                "1. 空格键：选择/取消分析师\n"
                "2. a 键：全选/取消全选\n"
                "3. Enter 键：确认"
            ),
            validate=lambda x: len(x) > 0 or "至少需要选择一位分析师。",
            style=questionary.Style(
                [
                    ("checkbox-selected", "fg:green"),
                    ("selected", "fg:green noinherit"),
                    ("highlighted", "noinherit"),
                    ("pointer", "noinherit"),
                ]
            ),
        ).ask()
    except Exception:
        return [a[1] for a in ANALYST_ORDER]

    if not choices:
        print("\n\n已取消，程序退出。")
        sys.exit(0)

    selected_name_map = dict(ANALYST_ORDER)
    selected_display_names = [selected_name_map.get(analyst_key, analyst_key) for analyst_key in choices]
    print(f"\n已选择分析师：{', '.join(Fore.GREEN + n + Style.RESET_ALL for n in selected_display_names)}\n")
    return choices


def select_model(use_ollama: bool, model_flag: str | None = None) -> tuple[str, str]:
    model_name: str = ""
    model_provider: str | None = None

    if model_flag:
        model = find_model_by_name(model_flag)
        if model:
            print(
                f"\n使用指定模型：{Fore.CYAN}{model.provider.value}{Style.RESET_ALL}"
                f" - {Fore.GREEN + Style.BRIGHT}{model.model_name}{Style.RESET_ALL}\n"
            )
            return model.model_name, model.provider.value
        print(f"{Fore.RED}未找到模型 '{model_flag}'，将进入手动选择。{Style.RESET_ALL}")

    if not sys.stdin.isatty():
        default_model = find_model_by_name("deepseek-ai/DeepSeek-V3")
        if default_model:
            return default_model.model_name, default_model.provider.value
        return "deepseek-ai/DeepSeek-V3", ModelProvider.DEEPSEEK.value

    if use_ollama:
        print(f"{Fore.CYAN}当前使用 Ollama 本地推理。{Style.RESET_ALL}")
        try:
            model_name = questionary.select(
                "请选择 Ollama 模型：",
                choices=[questionary.Choice(display, value=value) for display, value, _ in OLLAMA_LLM_ORDER],
                style=questionary.Style(
                    [
                        ("selected", "fg:green bold"),
                        ("pointer", "fg:green bold"),
                        ("highlighted", "fg:green"),
                        ("answer", "fg:green bold"),
                    ]
                ),
            ).ask()
        except Exception:
            model_name = OLLAMA_LLM_ORDER[0][1] if OLLAMA_LLM_ORDER else ""

        if not model_name:
            print("\n\n已取消，程序退出。")
            sys.exit(0)

        if model_name == "-":
            model_name = questionary.text("请输入自定义模型名：").ask()
            if not model_name:
                print("\n\n已取消，程序退出。")
                sys.exit(0)

        if not ensure_ollama_and_model(model_name):
            print(f"{Fore.RED}未检测到 Ollama 或模型不可用，无法继续。{Style.RESET_ALL}")
            sys.exit(1)

        model_provider = ModelProvider.OLLAMA.value
        print(f"\n已选择 {Fore.CYAN}Ollama{Style.RESET_ALL} 模型：{Fore.GREEN + Style.BRIGHT}{model_name}{Style.RESET_ALL}\n")
    else:
        try:
            model_choice = questionary.select(
                "请选择 LLM 模型：",
                choices=[questionary.Choice(display, value=(name, provider)) for display, name, provider in LLM_ORDER],
                style=questionary.Style(
                    [
                        ("selected", "fg:green bold"),
                        ("pointer", "fg:green bold"),
                        ("highlighted", "fg:green"),
                        ("answer", "fg:green bold"),
                    ]
                ),
            ).ask()
        except Exception:
            model_choice = ("deepseek-ai/DeepSeek-V3", ModelProvider.DEEPSEEK.value)

        if not model_choice:
            print("\n\n已取消，程序退出。")
            sys.exit(0)

        model_name, model_provider = model_choice
        model_info = get_model_info(model_name, model_provider)
        if model_info and model_info.is_custom():
            model_name = questionary.text("请输入自定义模型名：").ask()
            if not model_name:
                print("\n\n已取消，程序退出。")
                sys.exit(0)

        if model_info:
            print(
                f"\n已选择 {Fore.CYAN}{model_provider}{Style.RESET_ALL} 模型："
                f"{Fore.GREEN + Style.BRIGHT}{model_name}{Style.RESET_ALL}\n"
            )
        else:
            model_provider = "Unknown"
            print(f"\n已选择模型：{Fore.GREEN + Style.BRIGHT}{model_name}{Style.RESET_ALL}\n")

    return model_name, model_provider or ""


def resolve_dates(
    start_date: str | None,
    end_date: str | None,
    *,
    default_months_back: int | None = None,
) -> tuple[str, str]:
    if start_date:
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("开始日期必须是 YYYY-MM-DD 格式") from exc
    if end_date:
        try:
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("结束日期必须是 YYYY-MM-DD 格式") from exc

    final_end = end_date or datetime.now().strftime("%Y-%m-%d")
    if start_date:
        final_start = start_date
    else:
        months = default_months_back if default_months_back is not None else 3
        end_date_obj = datetime.strptime(final_end, "%Y-%m-%d")
        final_start = (end_date_obj - relativedelta(months=months)).strftime("%Y-%m-%d")
    return final_start, final_end


@dataclass
class CLIInputs:
    tickers: list[str]
    selected_analysts: list[str]
    model_name: str
    model_provider: str
    start_date: str
    end_date: str
    initial_cash: float
    margin_requirement: float
    show_reasoning: bool = False
    show_agent_graph: bool = False
    raw_args: Optional[argparse.Namespace] = None


def parse_cli_inputs(
    *,
    description: str,
    require_tickers: bool,
    default_months_back: int | None,
    include_graph_flag: bool = False,
    include_reasoning_flag: bool = False,
) -> CLIInputs:
    parser = argparse.ArgumentParser(description=description)
    add_common_args(parser, require_tickers=require_tickers, include_analyst_flags=True, include_ollama=True)
    add_date_args(parser, default_months_back=default_months_back)

    parser.add_argument(
        "--initial-cash",
        "--initial-capital",
        dest="initial_cash",
        type=float,
        default=100000.0,
        help="初始现金（别名: --initial-capital），默认 100000.0",
    )
    parser.add_argument(
        "--margin-requirement",
        dest="margin_requirement",
        type=float,
        default=0.0,
        help="空头保证金比例（例如 0.5 代表 50%%），默认 0.0",
    )
    if include_reasoning_flag:
        parser.add_argument("--show-reasoning", action="store_true", help="显示每位分析师推理")
    if include_graph_flag:
        parser.add_argument("--show-agent-graph", action="store_true", help="显示 Agent 图")

    args = parser.parse_args()

    tickers = resolve_tickers_interactive(parse_tickers(getattr(args, "tickers", None)))
    selected_analysts = select_analysts(
        {
            "analysts_all": getattr(args, "analysts_all", False),
            "analysts": getattr(args, "analysts", None),
        }
    )
    model_name, model_provider = select_model(getattr(args, "ollama", False), getattr(args, "model", None))
    start_date, end_date = resolve_dates(
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        default_months_back=default_months_back,
    )

    return CLIInputs(
        tickers=tickers,
        selected_analysts=selected_analysts,
        model_name=model_name,
        model_provider=model_provider,
        start_date=start_date,
        end_date=end_date,
        initial_cash=getattr(args, "initial_cash", 100000.0),
        margin_requirement=getattr(args, "margin_requirement", 0.0),
        show_reasoning=getattr(args, "show_reasoning", False),
        show_agent_graph=getattr(args, "show_agent_graph", False),
        raw_args=args,
    )
