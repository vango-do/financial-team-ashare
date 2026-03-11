**免责声明：本项目仅用于研究与工程验证，不构成任何投资建议。**

# 金融团队（A股版）

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Poetry](https://img.shields.io/badge/Dependency-Poetry-60A5FA?logo=poetry&logoColor=white)

`金融团队（A股版）` 是一个面向中国 A 股市场的多 Agent 投研决策系统，深度 fork 并改造自 [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund)。

## 致敬原作者与本地化改造

感谢原作者提供的开源框架。本项目在其多 Agent 思路上完成了 A 股场景重构：

- 交易制度本地化：支持 A 股 `T+1` 与主板 `10%`、科创/创业板 `20%` 涨跌停规则。
- 决策流程本地化：`7 位大师分析师 + 风险管理 + 组合经理` 的完整投研链路。
- 证据机制本地化：`Retrieval-First`，每次 LLM 调用前强制先检索本地语料。
- 风格污染防护：大师语料隔离、查询隔离、会话隔离，跨角色命中即丢弃并审计。
- 执行规则固化：组合经理执行“严格多数票”，LLM 仅输出决策逻辑与理由。

## 核心能力与架构

### 多 Agent 主链路

1. 输入股票代码和分析参数。
2. 获取行情、财务、估值、新闻等结构化数据。
3. 七位大师分别输出信号、置信度与证据。
4. 风险管理模块施加仓位与执行边界。
5. 组合经理按多数票给出最终动作。
6. 自动导出中文综合研报（`.docx`）。

### Retrieval-First（本地 RAG）

- 每次 LLM 调用前必须先检索对应大师本地语料。
- 检索结果会校验 `master` 身份，跨大师证据直接丢弃。
- 证据不足时触发安全降级，明确输出“证据不足”，禁止编造。

### 策略引擎特色

- 每周集中反思：系统按周沉淀一轮策略反思与复盘。
- 胜率优先：执行层优先保证决策胜率与稳定性。
- 进化边界受控：策略进化仅限参数微调，并可叠加过滤条件，不做任意风格漂移。

## 技术边界与极端行情提示

- 本系统不是交易撮合系统，无法保证真实成交。
- 在极端行情（如千股涨停/跌停、连续一字板、临停）下，可能出现“建议可交易但实际无法成交”的偏差。
- 当流动性恶化时，系统优先执行风控约束与安全降级逻辑。

## 环境前置要求

- Python `3.10+`（推荐 `3.11/3.12`；如遇上游轮子兼容问题，请先使用 `3.11`）
- Poetry `>=1.8`
- Git
- 可访问模型服务与东方财富相关数据接口的网络环境

可选依赖（仅动态爬取链路需要）：

- 本地 `scrapling` 库路径（通过 `SCRAPLING_CN_LIB` 指向）

## 快速开始

### 1) 克隆与安装

```bash
git clone <your-repo-url>
cd 金融团队
pip install poetry
poetry install
```

### 2) 配置环境变量

```bash
cp .env.example .env
```

按需填写 `.env`：

```env
SILICONFLOW_API_KEY=your_api_key
DEEPSEEK_API_KEY=your_api_key
DEEPSEEK_BASE_URL=https://api.siliconflow.cn/v1
DEEPSEEK_MODEL=deepseek-ai/DeepSeek-V3

MASTER_LIBRARY_ROOT=./AgentLibrary
MASTER_RAG_LOG_PATH=./outputs/retrieval_logs.jsonl
MASTER_RAG_TOP_K=6
MASTER_RAG_MIN_SCORE=0.03
```

### 3) 运行项目

交互模式（程序会提示输入股票代码）：

```bash
poetry run python src/main.py
```

非交互模式：

```bash
poetry run python src/main.py --tickers 688578 --analysts-all --model deepseek-ai/DeepSeek-V3 --start-date 2025-01-01 --end-date 2026-03-11
```

## 输出目录说明

- 综合报告：`./outputs/YYYYMMDD_HHMM_综合决策报告.docx`
- 桌面副本（可选）：`./outputs/{股票简称}_综合研报.docx`
- 检索日志：`./outputs/retrieval_logs.jsonl`

## 项目结构（关键脚手架）

```text
.
├─ src/
├─ tests/
├─ 数据获取/
├─ pyproject.toml
├─ poetry.lock
├─ .env.example
├─ .gitignore
├─ LICENSE
└─ README.md
```

## 安全与开源发布建议

- 提交前检查 `.env`、API Key、Token、Cookie 是否泄露。
- 不提交本地报告、缓存、日志、语料库数据。
- 提交前使用 `git status` 和 `git diff` 二次确认变更。
- 安全问题反馈流程见 `SECURITY.md`。

## 许可协议

本项目采用 MIT License（见 `LICENSE`），并致敬原项目 [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund) 的开源贡献。

## 联系方式

- GitHub Issues（推荐）
- 邮箱：`juzhouq@gmail.com`
