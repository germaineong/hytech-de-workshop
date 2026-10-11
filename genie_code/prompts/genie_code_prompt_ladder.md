# Lab 05 · Genie Code — 让 AI 助手按 Hytech 规范写代码 (Lab 05 · Genie Code — an AI pair-engineer that follows Hytech conventions)

**准备 (Before you start)**

- **`hytech-de-conventions`** 技能已在工作区范围内安装（`/Workspace/.assistant/skills/`）。
  它教导 Genie Code Hytech 的命名、MT5 字段语义、*append vs MERGE* 规则、期望模式和治理规则。
  打开并浏览一下（2 分钟）— 这是一个团队共享规范的方式。<br>
- The **`hytech-de-conventions`** skill is installed workspace-wide (`/Workspace/.assistant/skills/`).
  It teaches Genie Code Hytech's naming, MT5 field semantics, the *append vs MERGE* rule, the expectation
  patterns and the governance rules. Open it and skim it (2 min) — this is how a team shares conventions.
- 打开 Genie Code 面板并切换到 **Agent 模式**。你可以用中文写提示词。<br>
- Open the Genie Code panel and switch to **Agent mode**. You can write prompts in Chinese.
- 使用 `@` 给出上下文（例如 `@silver_mt5_deals_current`、`@ref_symbols`）。<br>
- Use `@` to give context (e.g. `@silver_mt5_deals_current`, `@ref_symbols`).

按照这个梯子一步步完成。每一步都有一个**预期成果** — 在继续之前检查一下。

Work through the ladder. Each step has an **expected outcome** — check it before moving on.

---

### L1 · 新的 gold 物化视图 (a new gold MV) — *Lakeflow Pipelines Editor, your `03_pipeline`*

> 在 transformations 里新增一个 gold 物化视图 `gold_country_asset_class_daily`：按 deal_date、country、asset_class
> 统计交易笔数、手数、美元名义金额和客户盈亏，只统计 BUY/SELL 成交，客户属性取当前版本。遵循 hytech-de-conventions。

> Create a new gold materialized view `gold_country_asset_class_daily` in transformations: grouped by deal_date, country, asset_class,
> count trades, volume in lots, USD notional, and client P&L; only BUY/SELL deals; use current user attributes. Follow hytech-de-conventions.

✅ **预期成果：** 一个新的 `CREATE OR REFRESH MATERIALIZED VIEW`，它
- 读取 `silver_mt5_deals_current`，在 `(server_id, login)` 和 `ref_symbols` 上连接**当前**用户（`__END_AT IS NULL`）
- 计算 `notional_usd = lots × contract_size × price × rate_profit`
- 拥有 `COMMENT` 和 `quality = gold`

✅ **Expected:** a new `CREATE OR REFRESH MATERIALIZED VIEW` that
- reads `silver_mt5_deals_current`, joins **current** users (`__END_AT IS NULL`) on `(server_id, login)` and `ref_symbols`
- computes `notional_usd = lots × contract_size × price × rate_profit`
- has a `COMMENT` and `quality = gold`

运行管道并查询新表。

Run the pipeline and query the new table.

### L2 · 数据质量期望 (expectations)

> 给 `silver_mt5_positions` 增加数据质量期望：lots > 0、price_open > 0，违反时丢弃该行；并解释 AUTO CDC 表上的期望应该放在哪里。

> Add data quality expectations to `silver_mt5_positions`: lots > 0, price_open > 0, drop rows on violation; explain where expectations belong on AUTO CDC tables.

✅ **预期成果：** 约束条件添加到流式表定义中，加上一条说明：期望适用于
*流向*目标的行（CDC 源查询）。

✅ **Expected:** constraints added to the streaming-table definition, plus a note that expectations apply to
the rows *flowing into* the target (the CDC source query).

### L3 · 解释 (explain) — `/explain`

> /explain 为什么 `silver_mt5_users` 用 `TRACK HISTORY ON` 只跟踪部分列？如果跟踪全部列，会有什么后果？

> /explain why `silver_mt5_users` uses `TRACK HISTORY ON` to track only some columns, not all. What would happen if all columns were tracked?

✅ **预期成果：** `LastAccess` 在每次登录时都会改变，所以跟踪所有列会在每次登录时创建一个新的 SCD2 版本：
表臃肿、历史记录误导、成本更高。

✅ **Expected:** `LastAccess` changes on every login, so tracking all columns would create a new SCD2 version per
login: table bloat, misleading history, higher cost.

### L4 · 诊断失败 (diagnose a failed task) — *Jobs run page, the failed `reconcile_server` iteration*

> 这个任务为什么失败？我应该如何只重跑失败的部分？

> Why did this task fail? How do I re-run only the failed part?

✅ **预期成果：** 它识别模拟的失败（`fail_server`）并推荐**修复运行**，清除
`fail_server`，保持 `servers` 不变（for-each 循环迭代次数相同）。

✅ **Expected:** it identifies the simulated failure (`fail_server`) and recommends **Repair run** with
`fail_server` cleared, keeping `servers` unchanged (same number of for-each iterations).

### L5 · 成本查询 (cost query) — *SQL editor*

> 用 `hytech_ws_catalog.ops.billing_usage` 和 `ops.list_prices` 计算我的管道过去 7 天每天的 DBU 和标价成本（美元），
> 管道名以 trade_lakehouse 开头、由我创建（`ops.pipelines`）。

> Using `hytech_ws_catalog.ops.billing_usage` and `ops.list_prices`, calculate daily DBU and list-price cost (USD) for my pipelines over the past 7 days,
> where pipeline name starts with trade_lakehouse and created by me (`ops.pipelines`).

✅ **预期成果：** 在 `sku_name` 上的连接，带有价格有效窗口、`usage_quantity × pricing.default`，
以及对 `usage_metadata.dlt_pipeline_id` 的过滤。

✅ **Expected:** a join on `sku_name` with the price validity window, `usage_quantity × pricing.default`,
and a filter on `usage_metadata.dlt_pipeline_id`.

### L6 · 挑战 (stretch) — dashboard

> 基于 L5 的查询和 `gold_daily_symbol_volume`，创建一个 AI/BI 仪表盘：第 1 页”交易概览”（名义金额趋势、Top 品种、客户盈亏），
> 第 2 页”管道健康与成本”（每日成本、运行时长、数据质量丢弃行数）。

> Based on the L5 query and `gold_daily_symbol_volume`, create an AI/BI dashboard: page 1 “Trade Summary” (notional trend, top symbols, client P&L),
> page 2 “Pipeline Health & Cost” (daily cost, run duration, DQ dropped rows).

---

**讨论 (Discuss)：** Genie Code 在哪里应用了技能（命名、注释、append 规则）？你在哪里需要更正它？
Hytech 的技能还应该包含哪些其他规范，例如他们的真实 catalog、GitLab 工作流、列标签框架？

**讨论 (Discuss):** where did Genie Code apply the skill (naming, comments, the append rule)? Where did you have to
correct it? Which other conventions should Hytech's skill contain, e.g. their real catalogs, the GitLab
workflow, the column-tag framework?
