# overall_statistics

## 验收打勾
- [ ] 350条报告生成完毕，无 Token 截断导致的半截文字。
- [x] Vector 目录中的数据已严格剔除“事后验证”模块。
- [ ] 包含了至少 10 只历史退市/爆雷股票的避险或错误买入复盘。
- [x] JSON 文件中已包含 tags 字段供高级检索使用。

## 摘要
- 报告校验: 报告数=264（目标=350），异常条目=8
- 向量污染: memory 行数=264，污染条目=0
- 退市覆盖: 识别到退市/爆雷相关样本=9（目标>=10）
- 标签字段: JSON tags 缺失=0/264

## 分布统计
- 按大师报告数:
  - Buffett: 50
  - Druckenmiller: 50
  - Fundamental: 50
  - Growth: 50
  - Lynch: 20
  - Munger: 19
  - Soros: 25
- 按决策类型:
  - 买入: 81
  - 卖出: 51
  - 回避: 68
  - 持有: 64

## 报告异常样例（最多30条）
```text
buffett_001.json: core_logic 过短
buffett_002.json: core_logic 过短
buffett_009.json: core_logic 过短
buffett_015.json: core_logic 过短
buffett_020.json: core_logic 过短
buffett_025.json: core_logic 过短
soros_025.json: reflection 过短
soros_025.json: post_validation 过短
```

## 退市/爆雷样本名单（部分）
*ST宇顺，万得微盘股指数ETF，中际旭创，工业富联，平安银行，康得新，康美药业，立讯精密，隆基绿能


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1828_综合决策报告.md
- run_time: 2026-03-10 18:28:27
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1830_综合决策报告.md
- run_time: 2026-03-10 18:30:27
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1832_综合决策报告.md
- run_time: 2026-03-10 18:32:32
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1834_综合决策报告.md
- run_time: 2026-03-10 18:34:31
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1837_综合决策报告.md
- run_time: 2026-03-10 18:37:03
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_1839_综合决策报告.md
- run_time: 2026-03-10 18:39:45
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260310_2146_综合决策报告.md
- run_time: 2026-03-10 21:46:41
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1442_综合决策报告.md
- run_time: 2026-03-11 14:42:44
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1447_综合决策报告.md
- run_time: 2026-03-11 14:47:53
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1450_综合决策报告.md
- run_time: 2026-03-11 14:50:57
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1525_综合决策报告.md
- run_time: 2026-03-11 15:25:42
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1555_综合决策报告.md
- run_time: 2026-03-11 15:55:03
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1600_综合决策报告.docx
- run_time: 2026-03-11 16:00:54
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: D:\桌面\大师agent\outputs\20260311_1609_综合决策报告.docx
- run_time: 2026-03-11 16:10:00
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no


## Latest Fusion Run
- report_path: ./outputs/20260311_2248_综合决策报告.docx
- run_time: 2026-03-11 22:48:15
- retrieval_calls: 8
- cross_master_drop_events: 0
- style_pollution: no
