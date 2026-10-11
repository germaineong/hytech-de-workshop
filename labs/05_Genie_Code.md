# Lab 05 · Genie Code — 让 AI 助手按 Hytech 规范写代码 (an AI pair-engineer that follows Hytech conventions)

**准备 (Before you start)**

- 工作区已安装 **`hytech-de-conventions`** 技能（`/Workspace/.assistant/skills/`）。
  它教 Genie Code Hytech 的命名规范、MT5 字段语义、追加 vs MERGE 规则、期望模式和治理规则。打开并浏览（2 分钟）——这是团队分享规范的方式。
- 打开 Genie Code 面板，切换到 **Agent 模式**。你可以用中文写提示词。
- 使用 `@` 提供上下文（例如 `@silver_mt5_deals_current`、`@ref_symbols`）。

  The **`hytech-de-conventions`** skill is installed workspace-wide (`/Workspace/.assistant/skills/`). It teaches Genie Code Hytech's naming, MT5 field semantics, the *append vs MERGE* rule, the expectation patterns and the governance rules. Open it and skim it (2 min) — this is how a team shares conventions.
- Open the Genie Code panel and switch to **Agent mode**. You can write prompts in Chinese.
- Use `@` to give context (e.g. `@silver_mt5_deals_current`, `@ref_symbols`).

逐步完成。每一步都有 **预期结果** —— 在继续之前检查一下。

Work through the ladder. Each step has an **expected outcome** — check it before moving on.

---

### L1 · 新的 gold 物化视图 (a new gold MV) — *Lakeflow Pipelines Editor, your `03_pipeline`*

> 在 transformations 里新增一个 gold 物化视图 `gold_country_asset_class_daily`：按 deal_date、country、asset_class
> 统计交易笔数、手数、美元名义金额和客户盈亏，只统计 BUY/SELL 成交，客户属性取当前版本。遵循 hytech-de-conventions。
>
> In transformations, add a gold materialized view `gold_country_asset_class_daily`: group by deal_date, country, asset_class
> and count deals, lots, notional USD and client P&L, filtering to BUY/SELL only, using current user attributes. Follow hytech-de-conventions.

✅ **预期结果：** 一个新的 `CREATE OR REFRESH MATERIALIZED VIEW`，它：
- 读取 `silver_mt5_deals_current`，用 `(server_id, login)` 联接 **当前** 用户（`__END_AT IS NULL`）和 `ref_symbols`
- 计算 `notional_usd = lots × contract_size × price × rate_profit`
- 有 `COMMENT` 和 `quality = gold`

运行管道，查询新表。

**Expected:** a new `CREATE OR REFRESH MATERIALIZED VIEW` that reads `silver_mt5_deals_current`, joins **current** users (`__END_AT IS NULL`) on `(server_id, login)` and `ref_symbols`, computes `notional_usd = lots × contract_size × price × rate_profit`, and has a `COMMENT` and `quality = gold`.

Run the pipeline and query the new table.

### L2 · 数据质量期望 (expectations)

> 给 `silver_mt5_positions` 增加数据质量期望：lots > 0、price_open > 0，违反时丢弃该行；并解释 AUTO CDC 表上的期望应该放在哪里。
>
> Add data quality expectations to `silver_mt5_positions`: lots > 0, price_open > 0, drop rows on violation; and explain where expectations should go on AUTO CDC tables.

✅ **预期结果：** 约束条件添加到流式表定义，加上一个注记说明期望应用于*流入*目标的行（CDC 源查询）。

**Expected:** constraints added to the streaming-table definition, plus a note that expectations apply to the rows *flowing into* the target (the CDC source query).

### L3 · 解释 (explain) — `/explain`

> /explain 为什么 `silver_mt5_users` 用 `TRACK HISTORY ON` 只跟踪部分列？如果跟踪全部列，会有什么后果？
>
> /explain why `silver_mt5_users` uses `TRACK HISTORY ON` to track only some columns. What would happen if we tracked all columns?

✅ **预期结果：** `LastAccess` 在每次登录时改变，所以跟踪所有列会为每次登录创建一个新的 SCD2 版本：表膨胀、历史记录误导、成本升高。

**Expected:** `LastAccess` changes on every login, so tracking all columns would create a new SCD2 version per login: table bloat, misleading history, higher cost.

### L4 · 诊断失败 (diagnose a failed task) — *Jobs run page, the failed `reconcile_server` iteration*

> 这个任务为什么失败？我应该如何只重跑失败的部分？
>
> Why did this task fail? How do I re-run just the failed part?

✅ **预期结果：** 它识别出模拟失败（`fail_server`），建议使用 **修复运行 (Repair run)**，清除 `fail_server`，保持 `servers` 不变（for-each 迭代数相同）。

**Expected:** it identifies the simulated failure (`fail_server`) and recommends **Repair run** with `fail_server` cleared, keeping `servers` unchanged (same number of for-each iterations).

### L5 · 成本查询 (cost query) — *SQL editor*

> 用 `hytech_ws_catalog.ops.billing_usage` 和 `ops.list_prices` 计算我的管道过去 7 天每天的 DBU 和标价成本（美元），
> 管道名以 trade_lakehouse 开头、由我创建（`ops.pipelines`）。
>
> Use `hytech_ws_catalog.ops.billing_usage` and `ops.list_prices` to calculate my pipeline's daily DBU and list-price cost (USD) for the past 7 days, filtering to pipelines starting with trade_lakehouse and created by me (from `ops.pipelines`).

✅ **预期结果：** 按 `sku_name` 与价格有效期窗口连接，`usage_quantity × pricing.default`，按 `usage_metadata.dlt_pipeline_id` 过滤。

**Expected:** a join on `sku_name` with the price validity window, `usage_quantity × pricing.default`, and a filter on `usage_metadata.dlt_pipeline_id`.

### L6 · 挑战 (stretch) — dashboard

> 基于 L5 的查询和 `gold_daily_symbol_volume`，创建一个 AI/BI 仪表盘：第 1 页”交易概览”（名义金额趋势、Top 品种、客户盈亏），
> 第 2 页”管道健康与成本”（每日成本、运行时长、数据质量丢弃行数）。
>
> Build an AI/BI dashboard from L5's query and `gold_daily_symbol_volume`: Page 1 “Trading overview” (notional trends, top symbols, client P&L), 
> Page 2 “Pipeline health & cost” (daily cost, run duration, data quality drop counts).

---

**讨论 (Discuss)：** Genie Code 在哪里应用了技能（命名、注释、追加规则）？你在哪里必须纠正它？Hytech 的技能还应该包含哪些规范，比如他们真实的 catalog、GitLab 工作流、列标签框架？

**Discuss:** where did Genie Code apply the skill (naming, comments, the append rule)? Where did you have to correct it? Which other conventions should Hytech's skill contain, e.g. their real catalogs, the GitLab workflow, the column-tag framework?
