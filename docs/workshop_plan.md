# 工作坊计划 — Hytech "Data Engineering with Databricks" (Shenzhen) (Workshop Plan — Hytech "Data Engineering with Databricks" (Shenzhen))

*选项 1：MT5 交易湖仓 · 2026 年 10 月 13–14 日 · 2 × 半天 · 普通话授课 · 约 20 名学员*
*讲师：Tan Zhi Han、Ong Germaine · Hytech 管理员：Triones*

*Option 1: MT5 Trade Lakehouse · 13–14 Oct 2026 · 2 × half-day · Mandarin · ~20 participants*
*Instructors: Zhi Han Tan, Germaine Ong · Hytech admin: Triones*

---

## 1. 目标 (Goals)

**学员能够掌握：**

**Participants leave able to:**

1. 使用 CTAS、COPY INTO 和 Auto Loader 接入 DMS 风格的 CDC 文件和 JSON，并解释救援数据。

   Ingest DMS-style CDC files and JSON with CTAS, COPY INTO and Auto Loader, and explain rescued data.

2. 搭建 Lakeflow Spark 声明式管道：bronze 流式表、AUTO CDC（SCD1/SCD2）、只追加事实表、期望和 gold 物化视图。

   Build a Lakeflow Spark Declarative Pipeline: bronze streaming tables, AUTO CDC (SCD1/SCD2), an append-only fact table, expectations and gold materialized views.

3. 为成本选择正确的模式：**只追加用于不可变事实，AUTO CDC 仅用于可变实体**。

   Choose the right pattern for cost: **append for immutable facts, AUTO CDC only for mutable entities**.

4. 将管道编排为生产作业：条件任务 DQ 门、循环任务、运行条件、触发器、重试和修复运行。

   Orchestrate the pipeline as a production job: if/else DQ gate, for-each, Run-if, triggers, retries and repair runs.

5. 用 Unity Catalog 治理数据：授权、所有权、标签、列掩码和行过滤器。

   Govern data with Unity Catalog: grants, ownership, tags, column masks and row filters.

6. 用系统表测量他们构建的内容（每次运行/天的成本、运行时间轴、血缘）。

   Measure what they built with system tables (cost per run/day, run timeline, lineage).

7. 在日常工作中使用 Genie Code（带 Hytech 约定技能）和 AI Functions。

   Use Genie Code (with a Hytech conventions skill) and AI Functions in their daily work.

**成功标准 (Success criteria)**

- 至少 80% 的学员在第 2 天结束时在自己的 schema 中有绿色的管道更新和绿色的作业运行。

  At least 80% of participants have a green pipeline update and a green job run in their own schema by the end of Day 2.

- 每个学员都对自己的管道运行成本查询。

  Every participant runs a cost query against their own pipeline.

- Hytech 之后保留三样东西：作为参考模板的代码库、Genie Code 技能和作业模式。

  Hytech keeps three things afterwards: the repo as a reference template, the Genie Code skill, and the job pattern.

## 2. 故事线 (Storyline)

> 你是 Hytech 新入职的数据工程师。4 台 MT5 交易服务器的 MySQL 通过 AWS DMS 把变更数据（CDC）以 Parquet 文件写入云存储。
> 你的任务：搭建交易湖仓（Trade Lakehouse），为每日交易报表、客户盈亏、IB 业绩和风险敞口提供可信数据，并像生产系统一样调度、监控与治理它。

> You are a new Hytech data engineer. Four MT5 trading servers replicate MySQL changes to cloud storage as Parquet files via AWS DMS. Build the trade lakehouse behind the daily trading, client P&L, IB performance and exposure reports, and run, monitor and govern it like production.

```
 MT5 servers x4 (synthetic)                          App events (Sensors-style JSON)
 mt5-sg-01 · mt5-sg-02 · mt5-uk-01 · mt5-cy-01
        │ DMS-style CDC Parquet: LOAD00000001.parquet + yyyymmdd-hhmmssfff.parquet (Op = I/U/D, cdc_ts)
        ▼
 /Volumes/hytech_ws_catalog/raw/landing/mt5/<server>/<table>/      /landing/app_events/<date>/   (shared history)
        │  lab 00 copies it per participant · 00b_Live_Data adds new DMS files on demand
        ▼
 /Volumes/hytech_ws_catalog/u_<name>/landing/mt5/<server>/<table>/   /landing/app_events/<date>/
        │
        │  Lakeflow Spark Declarative Pipeline (serverless, per participant schema u_<name>)
        ▼
 BRONZE  bronze_mt5_users · bronze_mt5_deals · bronze_mt5_positions · bronze_app_events   (Auto Loader)
 SILVER  silver_mt5_users (AUTO CDC SCD2) · silver_mt5_positions (AUTO CDC SCD1)
         silver_mt5_deals (append-only + expectations) · silver_mt5_deal_corrections (AUTO CDC)
         silver_mt5_deals_current · silver_app_events (dedupe)
 GOLD    gold_daily_symbol_volume · gold_client_daily_pnl · gold_ib_daily_performance
         gold_net_exposure_by_symbol · gold_funding_daily
        │
        ▼
 Lakeflow Job: daily_trading_reporting  →  DQ gate · for-each server reconciliation · AI daily commentary (中文) · alerts · audit
 System tables → cost / runs / lineage of *your* pipeline · Genie Code · AI Functions
```

## 3. 环境 (Environment)

| 项目 (Item) | 值（默认值，全部可配置）(Value (defaults, all configurable)) |
|---|---|
| 构建/测试工作区 (Build/test workspace) | `fe-vm-zh-serverless-ws`（AWS us-west-2，无服务器）<br>`fe-vm-zh-serverless-ws` (AWS us-west-2, serverless) |
| Hytech 工作区 (Hytech workspace) | 与 Triones 待定 (TBC with Triones) (AWS us-east-1 or ap-southeast-1) |
| 目录 (Catalog) | `hytech_ws_catalog` |
| Schema | `raw` (volumes `landing`, `ref`, `producer`) · `solutions`（讲师解决方案 / instructor solution） · `ops`（系统表的受治理视图 / governed views over system tables） · `u_<name>`（每个学员一个，由他们所有 / one per participant, owned by them） |
| 计算 (Compute) | 无服务器笔记本、作业和管道；一个无服务器 SQL 仓库（小型，扩展到 3 个集群）<br>Serverless notebooks, jobs and pipelines; one serverless SQL warehouse (Small, scaling to 3 clusters) |
| 大语言模型端点 (LLM endpoint) | `databricks-claude-sonnet-4-5`（可配置）用于 `ai_query`<br>`databricks-claude-sonnet-4-5` (configurable) for `ai_query` |
| 学员组 (Participant group) | `de_workshop_sz` |

**数据集 (Dataset)**（合成、预设、约 90 天历史、不到 1 GB）<br>**Dataset** (synthetic, seeded, about 90 days of history, under 1 GB):

| 表/文件 (Table / files) | 粒度 (Grain) | 大小 (Size) | 内置"课程"(Built-in "lessons") |
|---|---|---|---|
| `mt5_users` | 每台服务器的客户账户 (client account per server) | 约 6k (about 6k) | SCD2 变更（杠杆、组别、IB、状态、KYC）、罕见删除<br>SCD2 changes (leverage, group, IB, status, KYC), rare deletes |
| `mt5_deals` | MT5 成交（开/平、入/出金平衡成交）(MT5 deal (trade in/out, deposit/withdrawal balance deals)) | 约 1M (about 1M) | 约 99.9% 插入、约 0.1% 修正；约 0.2% 无效行用于期望；中英文入金评论<br>About 99.9% inserts, about 0.1% corrections; about 0.2% invalid rows for expectations; Chinese/English funding comments |
| `mt5_positions` | 持仓 (open position) | 约 3k 持仓 (about 3k open) | 更新频繁的 CDC、平仓时删除<br>Update-heavy CDC, deletes on close |
| `app_events` | 传感器风格的 JSON 事件 (Sensors-style JSON event) | 约 200k (about 200k) | 嵌套 JSON；周中新字段；类型不匹配 → `_rescued_data`；约 1% 重复；中英文反馈文本<br>Nested JSON; a new field mid-week; type mismatches → `_rescued_data`; about 1% duplicates; zh/en feedback text |
| `ref/symbols.csv` | 交易品种 (symbol) | 40 | 资产类别和合约规模（COPY INTO）<br>Asset class and contract size (COPY INTO) |
| `ref/ib_hierarchy.csv` | IB | 约 60 (about 60) | IB 树和返佣计划<br>IB tree and rebate plan |
| `ref/fx_rates_daily.csv` | 日 × 货币 (day × ccy) | 约 900 (about 900) | 报价货币 → USD（CTAS）<br>Quote-currency → USD (CTAS) |
| 实时数据 `00b_Live_Data` (Live data) | — | 按需 (on demand) | 每位学员向自己的落地区写入新的 CDC 文件；可上线新服务器（`mt5-hk-01`）并注入坏批次。中心滴灌作业是可选的<br>Each participant writes new CDC files into their own landing zone; can onboard a new server (`mt5-hk-01`) and inject a bad batch. The central drip job is optional |

## 4. 现场表演流程 (Run of show)

一位讲师主讲，另一位在教室中巡回；每个模块交换。有 20 名新人，浮动讲师是必需的。每个模块的建议主讲已标记；与 Germaine 确认。

One instructor presents while the other circulates; swap per module. With 20 newcomers, the floater is essential. Proposed lead per module is marked; confirm with Germaine.

### 第 1 天 — 周二 10 月 13 日 (09:30–13:00) (Day 1 — Tue 13 Oct (09:30–13:00))

| 时间 (Time) | 模块 (Module) | 概念 - 10–15 分钟 (Concept (10–15 min)) | 演示 (Demo) | 实践实验 (Hands-on lab) | 输出 (Output) |
|---|---|---|---|---|---|
| 09:30 | **M1 平台概览 (Platform overview)** (30) | 湖仓、Unity Catalog、无服务器、Lakeflow（Connect、Pipelines、Jobs）、Genie。Hytech 的真实架构与实验对比<br>Lakehouse, Unity Catalog, serverless, Lakeflow (Connect, Pipelines, Jobs), Genie. Hytech's real architecture next to the lab | 工作区导览 (Workspace tour) | `labs/00_Start_Here`：创建/验证自己的 schema、浏览登陆 volume<br>`labs/00_Start_Here`: create/verify own schema, browse the landing volume | Schema `u_<name>` 已准备好<br>Schema `u_<name>` ready |
| 10:00 | **M2 Unity Catalog** (30) | 3 级命名空间、所有权、权限、volume、标签、掩码、行过滤器、血缘<br>3-level namespace, ownership, privileges, volumes, tags, masks, row filters, lineage | 基于属性的访问控制和受治理标签（讲师演示）、访问请求<br>ABAC with governed tags (instructor), access requests | `labs/01_Unity_Catalog`：授权、标签、电子邮件/电话列掩码、按品牌行过滤器<br>`labs/01_Unity_Catalog`: grants, tags, column mask on email/phone, row filter by brand | 掩盖的 `users_snapshot` 表<br>Masked `users_snapshot` table |
| 10:30 | **M3 数据接入 (Ingestion)** (30) | 批量 vs 增量 vs 流式；CTAS / COPY INTO / Auto Loader；元数据列；救援数据；企业连接器。缩短为 30 分钟：Lakeflow Connect 只做 5 分钟演示<br>Batch vs incremental vs streaming; CTAS / COPY INTO / Auto Loader; metadata columns; rescued data; enterprise connectors. Shortened to 30 min: Lakeflow Connect is a 5-min demo | Lakeflow Connect NetSuite（正式发布）、MySQL CDC（预览）<br>Lakeflow Connect NetSuite (GA), MySQL CDC (preview) | `labs/02_Ingestion`：CTAS FX 汇率、COPY INTO 交易品种（×2 用于幂等性）、Auto Loader DMS Parquet、JSON 带救援数据<br>`labs/02_Ingestion`: CTAS FX rates, COPY INTO symbols (×2 for idempotency), Auto Loader DMS Parquet, JSON with rescued data | 4 个 bronze 表<br>4 bronze tables |
| 11:00 | **M4 Spark 声明式管道 (Spark Declarative Pipelines)** (90) | 流式表 vs 物化视图、AUTO CDC SCD1/2、期望、事件日志；**追加 vs MERGE 成本课程**；数据质量期望（WARN / DROP / FAIL）、隔离表、血缘、事件日志、作业中的 Depends on 与 Run if<br>Streaming tables vs MVs, AUTO CDC SCD1/2, expectations, event log; **append vs MERGE cost lesson**; data-quality expectations (WARN / DROP / FAIL), quarantine, lineage, event log, Depends on and Run if in a job | Lakeflow 管道编辑器；Lakeflow Designer（IB 周报）<br>Lakeflow Pipelines Editor; Lakeflow Designer (IB weekly report) | `labs/03_pipeline`：读懂 bronze/silver/gold 中的要点，创建并运行管道；然后 `labs/03b_Explore_Pipeline`；然后 `labs/03c_Data_Quality_and_Monitoring`（20）和 `labs/03d_Run_If_Dependencies`（15）<br>`labs/03_pipeline`: read the key points in bronze/silver/gold, create and run the pipeline; then `labs/03b_Explore_Pipeline`; then `labs/03c_Data_Quality_and_Monitoring` (20) and `labs/03d_Run_If_Dependencies` (15) | 绿色管道更新；gold 集市；预发布通道 + 隔离表；作业 `run_if_lab_<name>`<br>Green pipeline update; gold marts; staging lane + quarantine; job `run_if_lab_<name>` |
| 12:30 | **第 1 天问答 (Q&A Day 1)** (30) | — | 学员运行 `00b_Live_Data`：观看新 CDC 在下次更新时落地<br>People run `00b_Live_Data`: watch new CDC land on the next update | 追进度：复制解决方案文件；离开前确认 03c 的最后一个检查是 ✅（管道是绿色的）<br>Catch-up: copy solution files; before leaving, check that 03c's last check is ✅ (a green pipeline) | 每个人都有一个管道<br>Everyone has a pipeline |

### 第 2 天 — 周三 10 月 14 日 (09:30–13:00) (Day 2 — Wed 14 Oct (09:30–13:00))

| 时间 (Time) | 模块 (Module) | 概念 (Concept) | 演示 (Demo) | 实践实验 (Hands-on lab) | 输出 (Output) |
|---|---|---|---|---|---|
| 09:30 | **M5 Lakeflow 作业 (Lakeflow Jobs)** (60) | 任务、DAG、参数、任务值、条件任务、循环任务、运行条件、触发器、重试、修复、通知；使用声明式自动化包的 CI/CD<br>Tasks, DAGs, parameters, task values, if/else, for-each, Run-if, triggers, retries, repair, notifications; CI/CD with Declarative Automation Bundles | 作为服务主体的 `databricks bundle deploy -t cicd`（`docs/cicd_demo.md`）<br>`databricks bundle deploy -t cicd` as a service principal (`docs/cicd_demo.md`) | `labs/04_Lakeflow_Jobs`：在 UI 中构建 `daily_trading_reporting`。运行 → `mt5-uk-01` 的迭代按设计失败（`fail_server`）→ **修复运行**（清空 `fail_server`）；然后 `labs/04c_Failure_Recovery_and_Backfill`（25）：自己的坏批次 → 重试 → 修复运行 → 回填<br>`labs/04_Lakeflow_Jobs`: build `daily_trading_reporting` in the UI. Run → the `mt5-uk-01` iteration fails by design (`fail_server`) → **repair run** (clear `fail_server`); then `labs/04c_Failure_Recovery_and_Backfill` (25): your own bad batch → retries → repair run → backfill | 绿色作业运行加修复运行；修复后变绿的运行 + 回填的日报<br>Green job run plus a repaired run; a run made green by the repair + backfilled daily reports |
| 10:30 | **M6 Genie Code + 系统表 (Genie Code + system tables)** (60) | M6a Genie Code（30）+ M6b 系统表（30）；回顾放到第 2 天问答开始时（5 分钟回顾），知识测验 08 作为课后作业<br>M6a Genie Code (30) + M6b system tables (30); the recap moves to the start of the Day 2 Q&A (a 5-minute recap), and knowledge check 08 becomes homework | 安装 `hytech-de-conventions` 技能；成本和健康仪表盘（由设置发布）<br>Install the `hytech-de-conventions` skill; Cost & health dashboard (published by setup) | `labs/05_Genie_Code` 提示词梯度：从中文提示生成新的 gold 物化视图、添加期望、修复失败的任务；`labs/06_System_Tables`：你的管道的 $/day、任务持续时间、血缘图<br>`labs/05_Genie_Code` prompt ladder: new gold MV from a Chinese prompt, add expectations, fix a failed task; `labs/06_System_Tables`: your pipeline's $/day, task durations, lineage graph | 自己管道中的新物化视图；成本和健康查询<br>New MV in own pipeline; Cost and health queries |
| 11:30 | **M7 AI/BI 仪表盘、Lakehouse RT 与 Genie (AI/BI dashboards, Lakehouse RT and Genie)** (60) | 实验 09（15）· Lakehouse RT 讲解（5）· 实验 10（25）· Lark 演示（10）· 缓冲（5）<br>Lab 09 (15) · Lakehouse RT talk (5) · Lab 10 (25) · Lark demo (10) · buffer (5) | Lark 中的 Genie（讲师自己的环境）<br>Genie in Lark (the instructor's own environment) | `labs/09_AIBI_Dashboard_Lakehouse_RT`：搭建仪表盘，把计算资源切换到 Lakehouse RT；`labs/10_Genie_Agent`：配置 Genie Agent，Genie One<br>`labs/09_AIBI_Dashboard_Lakehouse_RT`: build a dashboard and switch its compute to Lakehouse RT; `labs/10_Genie_Agent`: configure a Genie Agent, Genie One | 已发布的仪表盘；能回答中文问题的 Genie Agent<br>A published dashboard; a Genie Agent that answers Chinese questions |
| 12:30 | **第 2 天问答 (Q&A Day 2)** (30) | 路线图：Genie ZeroOps（私密预览）<br>Roadmap: Genie ZeroOps (private preview) | — | 先做 5 分钟回顾；反馈表；实验 07（AI 函数）和测验 08 作为可选的课后练习<br>A 5-minute recap first; feedback form; lab 07 (AI Functions) and quiz 08 as optional homework | — |

议程按客户 10 月 10 日的更新：M3 30 分钟、M4 90 分钟、M5 60 分钟、M7 60 分钟，AI 函数不在议程中。<br>Agenda per the customer's 10 Oct update: M3 30 min, M4 90, M5 60, M7 60; AI Functions is not on the agenda.

讲师没有 Hytech 工作区的权限，课堂上也不需要集中运行任何作业：每位学员在自己的 schema 里用 `00b_Live_Data` 生成实时数据。<br>Instructors have no access to Hytech's workspace, and nothing needs to run centrally during class: every participant generates their own live data with `00b_Live_Data` in their own schema.

### 关键"精彩时刻" (Key "wow" moments)

- **M3/M4：** `00b_Live_Data` 写入 MySQL 更改，它在下次管道更新时出现在 silver 层，SCD2 历史可见。

  **M3/M4:** `00b_Live_Data` writes a MySQL change, and it appears in silver on the next pipeline update, with SCD2 history visible.

- **M4：** 在只追加的成交表上使用 `DESCRIBE HISTORY`，与基于 MERGE 的修正表对比，加上 POC 成本数字：成本随*更改的数据*而不是*存储的数据*扩展。

  **M4:** `DESCRIBE HISTORY` on the append-only deals table next to the MERGE-based corrections table, plus the POC cost numbers: cost scales with *data changed, not data stored*.

- **M5：** 实时上线新的 MT5 服务器。作业失败，然后修复运行变成绿色，无需更改管道，因为 bronze 通配符是元数据驱动的。

  **M5:** onboard a new MT5 server live. The job fails, then the repair run goes green with no pipeline change, because the bronze glob is metadata-driven.

- **M6：** Genie Code 从中文提示生成一个集市，第 1 天的管道成本精确到美分。

  **M6:** Genie Code writes a mart from a Chinese prompt, and the Day-1 pipeline cost shows up to the cent.

- **M4（03c）：** 学员自己的坏批次让 silver 失败，而隔离表照常完成，显示出这 5 行。

  **M4 (03c):** a participant's own bad batch fails the silver table, while the quarantine completes and shows the 5 rows.

- **M5（04c）：** 同一次运行修复后变绿，坏着时到达的文件一行不丢。

  **M5 (04c):** the same run turns green after the repair, and the files that arrived while it was broken lose no row.

- **M7（09）：** 同一个仪表盘切换到 Lakehouse RT。

  **M7 (09):** the same dashboard switched to Lakehouse RT.

- **M7（10）：** 用中文问 Genie Agent，看它用上你写的同义词和说明。

  **M7 (10):** ask the Genie Agent in Chinese and watch it use the synonyms and instructions you wrote.

## 5. 实验设计规则 (Lab design rules)

- `labs/` 中的每个实验都包含**完整代码**，关键步骤有「要点」注释；`solutions/` 中保留一份原始副本。

  Every lab in `labs/` has the **complete code**, with "Key point" comments on the key steps; `solutions/` keeps an original copy.

- 每个模块都有**追进度路径**：直接运行完整代码，作业模块用 `04b_Jobs_Catch_Up`，这样不会有人卡住而耽误下一个模块。

  Every module has a **catch-up path**: run the complete code, or `04b_Jobs_Catch_Up` for the jobs module, so nobody blocks the next module.

- Markdown 先用中文，英文在方括号中；代码、标识符和列名用英文。

  Markdown is in Chinese first with English in brackets; code, identifiers and column names are in English.

- 实验为新人计时：概念 ≤ 15 分钟，实验 20–30 分钟。

  Labs are timed for newcomers: concept ≤ 15 min, lab 20–30 min.

- 没有第三方 Python 包，因为 Hytech 的无服务器出站可能受到限制。合成数据仅使用 numpy/pandas/pyarrow。

  No third-party Python packages, because Hytech's serverless egress may be restricted. Synthetic data uses only numpy/pandas/pyarrow.

- 一切都是幂等且安全可重新运行的。每个学员的隔离来自他们自己的 schema 和管道。

  Everything is idempotent and safe to re-run. Per-participant isolation comes from each person's own schema and pipeline.

## 6. 构建计划 (Build plan)（代码库 `germaineong/hytech-de-workshop`）

| # | 组件 (Component) | 路径 (Path) | 状态 (Status) |
|---|---|---|---|
| 1 | 计划（此文档）、README | `docs/`、`README.md` | ✅ |
| 2 | 配置 + 合成生成器（MT5 CDC、应用事件、参考）<br>Config + synthetic generators (MT5 CDC, app events, refs) | `src/hytech_workshop/` | ✅ |
| 3 | 设置笔记本（目录、数据、学员、ops 视图、技能安装）+ 清理<br>Setup notebooks (catalog, data, participants, ops views, skill install) + teardown | `setup/` | ✅ |
| 4 | 滴灌程序作业<br>Drip producer job | `setup/03_drip_producer.py` | ✅ |
| 5 | 参考答案管道（SQL + Python）<br>Solution pipeline (SQL + Python) | `solutions/pipeline/` | ✅ |
| 6 | 作业任务笔记本（DQ 门、调和、AI 总结、通知、审计）<br>Job task notebooks (DQ gate, reconcile, AI summary, notify, audit) | `jobs/` | ✅ |
| 7 | 声明式自动化包（设置、程序、参考答案管道和作业；dev/prod）<br>Declarative Automation Bundle (setup, producer, solution pipeline and job; dev/prod) | `databricks.yml`, `resources/` | ✅ |
| 8 | 学员实验 00–07 + 解决方案<br>Participant labs 00–07 + solutions | `labs/`, `solutions/` | ✅ |
| 9 | Genie Code 技能 + 提示词梯度<br>Genie Code skill + prompt ladder | `genie_code/` | ✅ |
| 10 | 讲师指南、学员指南（中文）、Triones 设置指南、故障排查<br>Facilitator guide, participant guide (zh), Triones setup guide, troubleshooting | `docs/` | ✅ |
| 11 | 在 `fe-vm-zh-serverless-ws` 上的端到端测试（设置 → 管道 → 作业 → 修复 → 系统表 → AI）<br>End-to-end test on `fe-vm-zh-serverless-ws` (setup → pipeline → job → repair → system tables → AI) | `docs/` 中的证据 (evidence in `docs/`) | ✅ |
| 12 | 知识测验（15 道题 + 加分题）和映射到考试指南的答案与讲解<br>Knowledge check (15 questions + bonus) and answer key mapped to the exam guide | `labs/08_Knowledge_Check.md`, `docs/knowledge_check_answers.md` | ✅ |
| 13 | 成本和健康仪表盘（AI/BI），由设置作业发布<br>Cost & health dashboard (AI/BI), published by the setup job | `dashboards/`, `setup/07_cost_dashboard.py` | ✅ |
| 14 | CI/CD 演示：`cicd` 目标（服务主体）、GitLab CI 模板、演示脚本<br>CI/CD demo: `cicd` target (service principal), GitLab CI template, demo script | `databricks.yml`, `cicd/`, `docs/cicd_demo.md` | ✅ |
| 15 | 一键安装笔记本：不用 CLI 或 bundle，创建并运行全部作业和管道<br>Master setup notebook: creates and runs every job and the pipeline without the CLI or a bundle | `setup/00_master_setup.py` | ✅ |

**FEVM 上的测试计划 (Test plan on FEVM)**

1. 部署包（dev）并运行设置作业；检查文件计数和行计数。

   Deploy the bundle (dev) and run the setup job; check file counts and row counts.

2. 运行参考答案管道。检查期望、SCD2 历史和 gold 行计数。

   Run the solution pipeline. Check expectations, SCD2 history and gold row counts.

3. 运行参考答案作业。检查 DQ 门真路径、跨 4 台服务器的循环任务、运行条件和审计。

   Run the solution job. Check the DQ gate true path, the for-each across 4 servers, Run-if and audit.

4. 用 `new_server=mt5-hk-01` 和坏批次启动程序。检查错误路径、失败 → 修复和增量拾取。

   Start the producer with `new_server=mt5-hk-01` and a bad batch. Check the false path, the failure → repair, and incremental pickup.

5. 针对解决方案运行系统表和 AI 函数实验。

   Run the system-tables and AI-functions labs against the solution.

6. 使用新的 `u_` schema 和实验文件（UI 流程）进行学员试运行。

   Do a participant dry run with a fresh `u_` schema and the lab files (UI flow).

## 7. 交付时间表 (Timeline to delivery)

| 日期 (Date) | 里程碑 (Milestone) |
|---|---|
| 10 月 3–4 日周六–周日 (Sat 3 – Sun 4 Oct) | 代码库、数据生成器、设置、参考答案管道、作业（FEVM 上的核心路径绿色）<br>Repo, data generator, setup, solution pipeline, job (core path green on FEVM) |
| 10 月 5 日周一 (Mon 5 Oct) | Germaine 与 Triones 确认：域、工作区/地区、系统表访问、学员列表<br>Germaine confirms with Triones: domain, workspace/region, system-table access, participant list |
| 10 月 6–7 日周二–周三 (Tue 6 – Wed 7 Oct) | 学员实验、Genie Code 技能、指南（中文）；Germaine 起草幻灯片<br>Participant labs, Genie Code skill, guides (zh); Germaine drafts slides |
| 10 月 8–9 日周四–周五 (Thu 8 – Fri 9 Oct) | 联合审查和模块分工；交给 Triones 设置指南；Triones 运行设置并从深圳办公室测试访问<br>Joint review and module split; hand Triones the setup guide; Triones runs setup and tests access from the Shenzhen office |
| 10 月 12 日周一 (Mon 12 Oct) | 出差；在 Hytech 工作区进行晚间试运行<br>Travel; evening dry run in Hytech's workspace |
| 10 月 13–14 日周二–周三 (Tue 13 – Wed 14 Oct) | 工作坊<br>Workshop |

## 8. 风险和缓解措施 (Risks and mitigations)

| 风险 (Risk) | 缓解措施 (Mitigation) |
|---|---|
| Hytech 工作区 IP-ACL 阻止深圳办公室<br>Hytech workspace IP-ACL blocks the Shenzhen office | Triones 在 10 月 9 日前从场地 Wi-Fi 测试；要求备用热点/VPN<br>Triones tests from the venue Wi-Fi by 9 Oct; ask for a fallback hotspot/VPN |
| Google Docs/Slides 在中国大陆被阻止<br>Google Docs/Slides blocked in mainland China | 借用笔记本电脑上的 PDF/PPTX 幻灯片；实验文本存储在笔记本中<br>Slides as PDF/PPTX on the loaner laptop; lab text lives in notebooks |
| FMAPI 模型在 Hytech 的地区未提供<br>FMAPI model not served in Hytech's region | `llm_endpoint` 是一个参数；检查跨地区路由；回退到任何可用的聊天模型<br>`llm_endpoint` is a parameter; check cross-geo routing; fallback to any available chat model |
| 学员无法读取系统表<br>Participants can't read system tables | `ops` 中的受治理视图（定义者的权利），过滤到培训工作区<br>Governed views in `ops` (definer's rights), filtered to the training workspace |
| 新人在 M3–M4 中落后<br>Newcomers fall behind in M3–M4 | 追进度单元格和解决方案文件；浮动讲师<br>Catch-up cells and solution files; the floater instructor |
| 20 个带文件到达触发器的作业<br>20 jobs with file-arrival triggers | 每个触发器只看自己的落地区，只有自己的 00b 会触发它；演示后暂停<br>Each trigger watches only its own landing zone, so only that person's 00b fires it; pause it after the demo |
| 20 位学员同时运行实验 00 的复制（每人约 760 个文件、约 90 MB）<br>20 people run lab 00's copy at once (about 760 files and 90 MB each) | 在演练时计时；复制在云端的无服务器计算上运行，不经过会场网络；中断后重新运行即可（跳过已复制的文件）<br>Time it at the dry run; the copy runs on serverless compute in the cloud, not over the venue network; re-run it after an interruption (files already copied are skipped) |
| 账户级组 / 受治理标签需要账户管理员<br>Account-level groups / governed tags need account admin | 设置正常降级：授权被跳过并显示警告；ABAC 仅限讲师演示<br>Setup degrades gracefully: grants are skipped with a warning; ABAC is instructor-demo only |
| 账单数据延迟数小时<br>Billing data lags by hours | 在第 2 天针对第 1 天的运行运行成本实验<br>Run the cost lab on Day 2 against Day-1 runs |
| Lakehouse RT（Beta）需要客户团队为账户开启，然后在 Previews 中开启<br>Lakehouse RT (Beta) needs the account team to enable it for the account, then the Previews toggle | 尽早申请；没有时实验 09 用 `hytech_workshop_sql` 完成，步骤相同<br>Ask early; without it lab 09 runs on `hytech_workshop_sql` with the same steps |
| 在 ap-southeast-1 看不到 Genie 的 Agent 模式或 Genie One 对话<br>Genie Agent mode or Genie One chat missing on ap-southeast-1 | 账户管理员开启跨地域处理；预演时用学员账号确认<br>An account admin enables cross-Geo processing; confirm with a participant account at the dry run |

## 9. 未解决的问题 (Open questions)（给 Triones，10 月 5 日周一 / for Triones, Mon 5 Oct）

1. 哪个工作区和地区？是否启用了无服务器、FMAPI（哪些模型）、Genie Code 和 Genie？深圳办公室是否在 IP 允许列表中？

   Which workspace and region? Are serverless, FMAPI (which models), Genie Code and Genie enabled? Is the Shenzhen office on the IP allow list?

2. 可以用合成数据镜像 MT5/DMS 表形吗？有要避免的名称吗？

   Is it OK to mirror MT5/DMS table shapes with synthetic data? Any names to avoid?

3. 学员列表和用户名。会有账户级组（`de_workshop_sz`）吗？

   Participant list and usernames. Will there be an account-level group (`de_workshop_sz`)?

4. `ops` schema 能通过过滤视图公开系统表吗？谁批准（Ray/Robin）？

   Can the `ops` schema expose system tables via filtered views? Who approves (Ray/Robin)?

5. 他们是否希望 CI/CD 演示与他们的 GitLab 设置保持一致（邀请 Robin 参加 M5）？GitLab 模板已准备好：`cicd/gitlab-ci.yml`。

   Do they want the CI/CD demo aligned to their GitLab setup (invite Robin to M5)? A GitLab template is ready: `cicd/gitlab-ci.yml`.

6. Triones 的团队问题民调返回了什么？

   What came back from Triones' poll of team questions?
