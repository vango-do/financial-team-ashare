**免责声明：本项目仅用于研究与工程验证，不构成任何投资建议。**

# 金融团队（A股版）
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Poetry](https://img.shields.io/badge/Dependency-Poetry-60A5FA?logo=poetry&logoColor=white)

本项目深度 Fork 并改造自 [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund)，面向中国 A 股市场构建了可运行、可复现、可扩展的多 Agent 投研决策系统。

## 致敬原作者与本地化改造
感谢原作者提供的开源框架。本项目在其基础上完成了 A 股本地化重构：

- 交易规则本地化：支持 `T+1`，主板 `10%`、科创/创业板 `20%` 涨跌停约束。
- 决策链路本地化：`7 位大师分析师 + 风险管理 + 组合经理` 的完整流程。
- 检索优先机制：每次 LLM 调用前强制本地检索（Retrieval-First）。
- 风格污染防护：语料隔离、查询隔离、会话隔离、越界证据拦截。
- 决策硬规则：组合经理按“严格多数票”决定动作，LLM 只负责解释理由。

## 项目最核心特色：大师 A 股化经历体系
这是本项目区别于通用量化 Agent 的核心能力。

- 七位大师均有独立的 A 股化“经历画像”与本地语料库，不是共用模板提示词。
- 每位大师只读取自己对应的语料 collection，禁止跨大师读取，避免风格污染。
- 每次分析会优先检索该大师的 A 股历史认知、偏好与证据，再触发 LLM 推理。
- 证据不足时会显式降级输出“证据不足”，不允许模型编造结论。

## 核心能力与架构
### 多 Agent 主流程
1. 输入股票代码与分析参数。
2. 拉取行情、估值、财务、公告、新闻与情绪等结构化数据。
3. 七位大师分别输出：信号、置信度、证据与风险。
4. 风险管理模块执行仓位与交易约束校验。
5. 组合经理基于多数票输出最终动作。
6. 自动导出中文综合报告（`.docx`）。

### Retrieval-First（本地 RAG）
- LLM 前置检索为强制步骤。
- 命中证据按 `master` 字段做一致性校验，跨大师命中直接丢弃。
- 支持检索日志与审计，便于复盘。

## A 股交易规则说明
- 当日买入不可当日卖出（`T+1`）。
- 主板默认 `10%` 涨跌停，科创/创业板默认 `20%`。
- 禁止做空动作（仅允许买入/持有/卖出可成交逻辑）。

## 技术边界与极端行情提示
- 本系统不是交易所撮合系统，不保证真实成交。
- 极端行情下（如连续一字板、临停、流动性枯竭）可能出现“建议可交易但无法成交”的情况。
- 当数据缺失或证据不足时，系统会触发安全降级，优先控制风险。

## 环境前置要求
- Python `3.10+`（建议 `3.11`）。
- Poetry `>=1.8`。
- Git。
- 可访问模型服务与东方财富相关数据接口的网络环境。

可选依赖（仅动态爬取链路需要）：
- 本地 `scrapling_cn_lib` 路径（通过 `SCRAPLING_CN_LIB` 指向）。

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
SILICONFLOW_API_KEY=
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.siliconflow.cn/v1
DEEPSEEK_MODEL=deepseek-ai/DeepSeek-V3

MASTER_LIBRARY_ROOT=./AgentLibrary
MASTER_RAG_LOG_PATH=./outputs/retrieval_logs.jsonl
MASTER_RAG_TOP_K=6
MASTER_RAG_MIN_SCORE=0.03

EASTMONEY_TOKEN=
DESKTOP_OUTPUT_DIR=
ENABLE_RAG_QUERY_LOGGING=false
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

## 输出目录
- 综合报告：`./outputs/YYYYMMDD_HHMM_综合决策报告.docx`
- 桌面副本（可选）：由 `DESKTOP_OUTPUT_DIR` 控制
- 检索日志：`./outputs/retrieval_logs.jsonl`

## 仓库安全保护指南
- 提交前检查 `.env`、API Key、Token、Cookie 是否泄露。
- 不提交本地报告、缓存、日志、语料库数据。
- 提交前执行 `git status` 与 `git diff` 二次确认。
- 安全问题反馈流程见 `SECURITY.md`（如存在）。

## 项目结构（关键脚手架）
```text
.
├── src/
├── tests/
├── data_fetching/
├── pyproject.toml
├── poetry.lock
├── .env.example
├── .gitignore
├── LICENSE
└── README.md
```

## 许可证
本项目采用 MIT License（见 `LICENSE`），并致敬原项目 [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund) 的开源贡献。

## 联系方式
- GitHub Issues（推荐）
- 邮箱：`juzhouq@gmail.com`
