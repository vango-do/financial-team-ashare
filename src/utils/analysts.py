"""Analyst configuration and display helpers."""

from src.agents.charlie_munger import charlie_munger_agent
from src.agents.fundamentals import fundamentals_analyst_agent
from src.agents.george_soros import george_soros_agent
from src.agents.growth_agent import growth_analyst_agent
from src.agents.peter_lynch import peter_lynch_agent
from src.agents.stanley_druckenmiller import stanley_druckenmiller_agent
from src.agents.warren_buffett import warren_buffett_agent


ANALYST_CONFIG = {
    "warren_buffett": {
        "display_name": "沃伦·巴菲特",
        "description": "价值投资大师",
        "investing_style": "重视护城河、现金流与长期复利能力。",
        "agent_func": warren_buffett_agent,
        "type": "analyst",
        "order": 0,
    },
    "stanley_druckenmiller": {
        "display_name": "斯坦利·德鲁肯米勒",
        "description": "宏观交易大师",
        "investing_style": "强调趋势、赔率与风险收益非对称。",
        "agent_func": stanley_druckenmiller_agent,
        "type": "analyst",
        "order": 1,
    },
    "fundamentals_analyst": {
        "display_name": "基本面分析师",
        "description": "财务报表专家",
        "investing_style": "聚焦三大报表质量、盈利能力与偿债能力。",
        "agent_func": fundamentals_analyst_agent,
        "type": "analyst",
        "order": 2,
    },
    "growth_analyst": {
        "display_name": "成长风格分析师",
        "description": "成长投资专家",
        "investing_style": "关注收入与利润增速、成长持续性和估值匹配度。",
        "agent_func": growth_analyst_agent,
        "type": "analyst",
        "order": 3,
    },
    "peter_lynch": {
        "display_name": "彼得·林奇",
        "description": "成长价值投资大师",
        "investing_style": "强调可理解商业模式与长期成长潜力。",
        "agent_func": peter_lynch_agent,
        "type": "analyst",
        "order": 4,
    },
    "charlie_munger": {
        "display_name": "查理·芒格",
        "description": "多元思维投资大师",
        "investing_style": "重视企业质量、管理层与长期确定性。",
        "agent_func": charlie_munger_agent,
        "type": "analyst",
        "order": 5,
    },
    "soros": {
        "display_name": "乔治·索罗斯",
        "description": "反身性交易大师",
        "investing_style": "关注叙事反馈、预期差与市场情绪拐点。",
        "agent_func": george_soros_agent,
        "type": "analyst",
        "order": 6,
    },
}

ANALYST_ORDER = [
    (config["display_name"], key)
    for key, config in sorted(ANALYST_CONFIG.items(), key=lambda item: item[1]["order"])
]

_SPECIAL_AGENT_NAMES = {
    "risk_management_agent": "风险管理师",
    "portfolio_manager": "投资组合经理",
}


def get_analyst_display_name(analyst_key: str) -> str:
    """Get analyst display name by analyst key."""
    return ANALYST_CONFIG.get(analyst_key, {}).get("display_name", analyst_key)


def get_agent_display_name(agent_node: str) -> str:
    """Get graph node display name."""
    if agent_node in _SPECIAL_AGENT_NAMES:
        return _SPECIAL_AGENT_NAMES[agent_node]
    if agent_node.endswith("_agent"):
        return get_analyst_display_name(agent_node[: -len("_agent")])
    return agent_node


def get_analyst_nodes():
    """Get analyst node map for workflow wiring."""
    return {key: (f"{key}_agent", config["agent_func"]) for key, config in ANALYST_CONFIG.items()}


def get_agents_list():
    """Get sorted agents list for API responses."""
    return [
        {
            "key": key,
            "display_name": config["display_name"],
            "description": config["description"],
            "investing_style": config["investing_style"],
            "order": config["order"],
        }
        for key, config in sorted(ANALYST_CONFIG.items(), key=lambda item: item[1]["order"])
    ]
