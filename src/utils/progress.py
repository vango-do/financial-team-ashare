from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from rich.console import Console
from rich.live import Live
from rich.style import Style
from rich.table import Table
from rich.text import Text

console = Console()


class AgentProgress:
    """Manages progress tracking for multiple agents."""

    def __init__(self):
        self.agent_status: Dict[str, Dict[str, str]] = {}
        self.table = Table(show_header=False, box=None, padding=(0, 1))
        self.live = Live(self.table, console=console, refresh_per_second=4)
        self.started = False
        self.update_handlers: List[Callable[[str, Optional[str], str], None]] = []

    def register_handler(self, handler: Callable[[str, Optional[str], str], None]):
        self.update_handlers.append(handler)
        return handler

    def unregister_handler(self, handler: Callable[[str, Optional[str], str], None]):
        if handler in self.update_handlers:
            self.update_handlers.remove(handler)

    def start(self):
        if not self.started:
            try:
                self.live.start()
                self.started = True
            except Exception:
                self.started = False

    def stop(self):
        if self.started:
            try:
                self.live.stop()
            except Exception:
                pass
            finally:
                self.started = False

    def update_status(self, agent_name: str, ticker: Optional[str] = None, status: str = "", analysis: Optional[str] = None):
        if agent_name not in self.agent_status:
            self.agent_status[agent_name] = {"status": "", "ticker": None}

        if ticker:
            self.agent_status[agent_name]["ticker"] = ticker
        if status:
            self.agent_status[agent_name]["status"] = self._translate_status(status)
        if analysis:
            self.agent_status[agent_name]["analysis"] = analysis

        timestamp = datetime.now(timezone.utc).isoformat()
        self.agent_status[agent_name]["timestamp"] = timestamp

        for handler in self.update_handlers:
            handler(agent_name, ticker, status, analysis, timestamp)

        self._refresh_display()

    def get_all_status(self):
        return {
            agent_name: {
                "ticker": info["ticker"],
                "status": info["status"],
                "display_name": self._get_display_name(agent_name),
            }
            for agent_name, info in self.agent_status.items()
        }

    def _get_display_name(self, agent_name: str) -> str:
        try:
            from src.utils.analysts import get_agent_display_name

            return get_agent_display_name(agent_name)
        except Exception:
            return agent_name.replace("_agent", "").replace("_", " ").title()

    def _translate_status(self, status: str) -> str:
        if not status:
            return status
        text = status.strip()

        exact_map = {
            "Done": "已完成",
            "Completed": "已完成",
            "Failed": "失败",
            "Fetching data...": "正在获取数据...",
            "Analyzing...": "正在分析...",
            "Generating trading decisions": "正在生成交易决策",
            "Processing analyst signals": "正在处理分析师信号",
            "Warning: No price data found": "警告：未找到价格数据",
            "No valid price data": "无有效价格数据",
        }
        if text in exact_map:
            return exact_map[text]

        lower = text.lower()
        if lower.startswith("error - retry"):
            return text.replace("Error - retry", "错误，重试").replace("error - retry", "错误，重试")
        if lower.startswith("generating "):
            return "正在生成 " + text[len("Generating ") :]
        if lower.startswith("processing "):
            return "正在处理 " + text[len("Processing ") :]
        if lower.startswith("fetching "):
            return "正在获取 " + text[len("Fetching ") :]
        if lower.startswith("analyzing "):
            return "正在分析 " + text[len("Analyzing ") :]
        if lower.startswith("calculating "):
            return "正在计算 " + text[len("Calculating ") :]

        return text

    def _refresh_display(self):
        self.table.columns.clear()
        self.table.add_column(width=100)

        def sort_key(item):
            agent_name = item[0]
            if "risk_management" in agent_name:
                return (2, agent_name)
            if "portfolio" in agent_name:
                return (3, agent_name)
            return (1, agent_name)

        done_values = {"done", "completed", "已完成", "完成"}
        error_values = {"error", "failed", "失败"}

        for agent_name, info in sorted(self.agent_status.items(), key=sort_key):
            status = (info.get("status") or "").strip()
            ticker = info.get("ticker")
            status_key = status.lower()

            if status_key in done_values or status in done_values:
                style = Style(color="green", bold=True)
                symbol = "[OK]"
            elif status_key in error_values or status in error_values:
                style = Style(color="red", bold=True)
                symbol = "[ER]"
            else:
                style = Style(color="yellow")
                symbol = "[..]"

            agent_display = self._get_display_name(agent_name)
            text = Text()
            text.append(f"{symbol} ", style=style)
            text.append(f"{agent_display:<20}", style=Style(bold=True))

            if ticker:
                text.append(f"[{ticker}] ", style=Style(color="cyan"))
            text.append(status, style=style)
            self.table.add_row(text)


progress = AgentProgress()
