# Hytech · Data Engineering workshop with Databricks 数据工程培训

一份完整的、实践性强的 **2 × 半天工作坊（普通话授课）**，面向 Hytech 深圳数据工程团队。学员将按照 Hytech 生产系统的架构模式，搭建一个**交易湖仓**。

A self-contained, hands-on **2 × half-day workshop (Mandarin)** for Hytech's Shenzhen data-engineering team.
Participants build a production-style **trade lakehouse** on the same pattern Hytech runs in production:

> MT5 MySQL → AWS DMS (full load + CDC Parquet) → **Auto Loader** → **Lakeflow Spark Declarative Pipelines**
> (bronze → silver with AUTO CDC + expectations → gold marts) → **Lakeflow Jobs** (DQ gate, for-each, Run-if,
> repair) — governed by **Unity Catalog**, observed with **system tables**, assisted by **Genie Code** and
> **AI Functions**.

所有数据都是**合成数据**（预设种子，无真实客户）。全部无服务器架构、幂等且端到端验证（见下文 *Verified* 部分）。

All data is **synthetic** (seeded, no real clients). Everything is serverless, idempotent and verified end to
end (see *Verified* below).

```
 mt5-sg-01 · mt5-sg-02 · mt5-uk-01 · mt5-cy-01 (+ mt5-hk-01 onboarded live)      app events (JSON)
        │  DMS-style CDC Parquet  (LOAD00000001.parquet + yyyymmdd-hhmmssfff.parquet, Op = I/U/D)
        ▼
 /Volumes/hytech_ws_catalog/raw/landing/mt5/<server>/<table>/            shared history, written by setup
        ▼  lab 00 copies it per participant · 00b_Live_Data adds new DMS files on demand
 /Volumes/hytech_ws_catalog/u_<name>/landing/mt5/<server>/<table>/
        ▼  Spark Declarative Pipeline (per participant schema u_<name>)
 bronze_mt5_{users,deals,positions} · bronze_app_events
 silver_mt5_users (SCD2) · silver_mt5_positions (SCD1) · silver_mt5_deals (append + expectations)
 silver_mt5_deal_corrections (AUTO CDC) · silver_mt5_deals_current · silver_app_events
 gold_daily_symbol_volume · gold_client_daily_pnl · gold_ib_daily_performance · gold_net_exposure_by_symbol · gold_funding_daily
        ▼  Lakeflow Job
 run_pipeline → dq_gate → if/else → AI daily summary (中文) | DQ alert · for-each server reconciliation · Run-if alert/audit
```

## 快速开始：讲师与 Hytech 管理员 (Quick start: instructors / Hytech admin)

1. 把代码库放进工作区（Git 文件夹），然后二选一：<br>
   Put the repo in the workspace (Git folder), then choose one:

   **方式 A（推荐）：一键安装笔记本。** 打开 `setup/00_master_setup` → 选择 **Serverless** → 填写顶部参数 → **Run all**（约 15 分钟）。它会创建全部作业和管道，再依次运行 setup 作业和参考答案作业。不需要 CLI，也不需要修改 `databricks.yml`。

   **Option A (recommended): the master setup notebook.** Open `setup/00_master_setup` → attach **Serverless** → fill in the widgets at the top → **Run all** (about 15 minutes). It creates every job and the pipeline, then runs the setup job and the solution job in order. No CLI needed, and no change to `databricks.yml`.

   **方式 B：bundle（讲师开发和 CI/CD 演示用）。** 在本地电脑上部署：

   **Option B: the bundle (for instructor development and the CI/CD demo).** Deploy from your laptop:

   ```bash
   databricks bundle deploy -t dev            # or -t prod for the shared deployment
   databricks bundle run hytech_ws_setup      # catalog, synthetic data, ops views, cost dashboard, Genie Code skill (~10 min)
   databricks bundle run hytech_daily_trading_reporting_solution   # fills the solutions schema (~4 min)
   ```

   设置 `databricks.yml` 中的 `workspace.host` 和变量 `catalog`、`llm_endpoint`、`participant_group`。

   Set `workspace.host` in `databricks.yml` and the variables `catalog`, `llm_endpoint`, `participant_group`.

2. 课堂上不需要运行任何作业：每位学员在实验 00 中复制自己的落地区，需要新数据时自己运行 `labs/00b_Live_Data`（见讲师指南）。作业 `hytech_ws_drip_producer` 是可选的：它只写共享落地区，供讲师在自己的测试工作区里使用。

   Nothing needs to run during class: in lab 00 each participant copies their own landing zone, and runs `labs/00b_Live_Data` whenever a lab needs new data (see the facilitator guide). The job `hytech_ws_drip_producer` is optional: it writes only to the shared landing zone, for an instructor's own test workspace.

3. 学员从 **`labs/00_Start_Here`** 开始。

   Participants start at **`labs/00_Start_Here`**.

Hytech 管理员的完整检查清单：[`docs/setup_guide_triones.md`](docs/setup_guide_triones.md)。

Full checklist for Hytech's admin: [`docs/setup_guide_triones.md`](docs/setup_guide_triones.md).

## 文档 (Documents)

| 文档 (Doc) | 用途 (For) |
|---|---|
| [`docs/workshop_plan.md`](docs/workshop_plan.md) | 目标、故事线、议程、构建计划、风险<br>Goals, storyline, agenda, build plan, risks |
| [`docs/facilitator_guide.md`](docs/facilitator_guide.md) | 现场表演流程、讲师控制、精彩时刻、验证证据<br>Run of show, instructor controls, wow moments, verification evidence |
| [`docs/setup_guide_triones.md`](docs/setup_guide_triones.md) | Hytech 管理员的工作区设置<br>Workspace setup by the Hytech admin |
| [`docs/participant_guide_zh.md`](docs/participant_guide_zh.md) | 学员快速开始（中文）<br>Participant quick start (Chinese) |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | 已知问题和解决方案<br>Known errors and fixes |
| [`docs/cicd_demo.md`](docs/cicd_demo.md) | CI/CD 演示：作为服务主体部署、GitLab 模板<br>CI/CD demo: deploy as a service principal, GitLab template |
| [`docs/knowledge_check_answers.md`](docs/knowledge_check_answers.md) | 测验答案和考试部分映射（讲师用）<br>Quiz answers and exam-section mapping (instructors) |

## 实验 (Labs)

| 模块 (Module) | 实验 (`labs/`) | 参考答案 (Solution) |
|---|---|---|
| M1 | `00_Start_Here` — 自己的 schema 和落地区、浏览 DMS 文件、复制实验<br>`00_Start_Here` — own schema and landing zone, explore DMS files, copy labs | — |
| M3–M5 实时数据 (Live data) | `00b_Live_Data` — 需要新数据时运行：向自己的落地区写入新的 DMS 文件（可上线 `mt5-hk-01`、写入坏批次）<br>`00b_Live_Data` — run it whenever a lab needs new data: writes new DMS files into your own landing zone (can onboard `mt5-hk-01` or write a bad batch) | — |
| M2 Unity Catalog | `01_Unity_Catalog` — 授权、视图、标签、列掩码、行过滤器<br>`01_Unity_Catalog` — grants, views, tags, column mask, row filter | `solutions/01_Unity_Catalog` |
| M3 数据接入 (Ingestion) | `02_Ingestion` — CTAS、COPY INTO、Auto Loader、JSON + 救援数据<br>`02_Ingestion` — CTAS, COPY INTO, Auto Loader, JSON + rescued data | `solutions/02_Ingestion` |
| M4 管道 (Pipelines) | `03_pipeline/` + `03b_Explore_Pipeline` | `solutions/pipeline/` |
| M4 数据质量与依赖 (Data quality and dependencies) | `03c_Data_Quality_and_Monitoring` — 预发布通道 `mt5-hk-01`：WARN / DROP / FAIL、隔离表、血缘、事件日志、两种恢复<br>`03c_Data_Quality_and_Monitoring` — the `mt5-hk-01` staging lane: WARN / DROP / FAIL, quarantine, lineage, event log, two recoveries<br>`03d_Run_If_Dependencies` — 作业 `run_if_lab_<name>`：Depends on 与全部 6 种 Run if<br>`03d_Run_If_Dependencies` — job `run_if_lab_<name>`: Depends on and all six Run if conditions | `solutions/03c_Data_Quality_and_Monitoring`, `solutions/03d_Run_If_Dependencies`, `solutions/pipeline/staging/` |
| M5 作业 (Jobs) | `04_Lakeflow_Jobs.md` (UI) + `04b_Jobs_Catch_Up` | `resources/solution.job.yml` |
| M5 失败与恢复 (Failure and recovery) | `04c_Failure_Recovery_and_Backfill` — 重试、Succeeded with failures、修复运行、按日期回填（`report_date`）<br>`04c_Failure_Recovery_and_Backfill` — retries, Succeeded with failures, a repair run, backfill by date (`report_date`) | `solutions/04c_Failure_Recovery_and_Backfill` |
| M6 Genie Code | `05_Genie_Code.md` 提示词梯度 + 技能 `genie_code/.assistant/skills/hytech-de-conventions`<br>`05_Genie_Code.md` prompt ladder + skill `genie_code/.assistant/skills/hytech-de-conventions` | — |
| M6 系统表 (System tables) | `06_System_Tables`（通过受治理的 `ops` 视图）<br>`06_System_Tables` (via governed `ops` views) | `solutions/06_System_Tables` |
| M7 仪表盘 (Dashboards) | `09_AIBI_Dashboard_Lakehouse_RT.md`（UI）— 在自己的 gold 表上搭建仪表盘，把计算资源切换到 Lakehouse RT<br>`09_AIBI_Dashboard_Lakehouse_RT.md` (UI) — a dashboard on your own gold tables, compute switched to Lakehouse RT | `09b_Dashboard_Catch_Up`（参考仪表盘 / reference dashboard） |
| M7 Genie | `10_Genie_Agent.md`（UI）— 创建并配置 Genie Agent，再通过 Genie One 提问<br>`10_Genie_Agent.md` (UI) — create and configure a Genie Agent, then ask through Genie One | `10b_Genie_Catch_Up`（参考 Agent / reference agent） |
| AI 函数（可选，本次议程不包括）(AI Functions — optional, not on this agenda) | `07_AI_Functions` — `ai_query` (中文), `ai_classify`, `ai_mask`, 情感分析<br>`07_AI_Functions` — `ai_query` (中文), `ai_classify`, `ai_mask`, sentiment | `solutions/07_AI_Functions` |
| M6c 总结 (Recap)（测验作为课后作业 / quiz as homework）| `08_Knowledge_Check` — 15 道题 + 加分题，映射到考试指南<br>`08_Knowledge_Check` — 15 questions + bonus, mapped to the exam guide | `docs/knowledge_check_answers.md` |

## 代码库布局 (Repository layout)

| 路径 (Path) | 说明 (What) |
|---|---|
| `databricks.yml`, `resources/` | 声明式自动化包：设置作业、滴灌程序、参考答案管道和作业（`dev`、`prod` 和 `cicd` 目标）<br>Declarative Automation Bundle: setup job, drip producer, solution pipeline and job (`dev`, `prod` and `cicd` targets) |
| `setup/` | `00_master_setup`（一键创建并运行全部作业）、`01`–`08` 设置笔记本（`07` 发布成本仪表盘，`08` 找到或创建工作坊 SQL 仓库 `hytech_workshop_sql` 并授权，开启 Lakehouse RT 时再创建 `hytech_workshop_rt`）、`03_drip_producer`、`99_teardown`<br>`00_master_setup` (creates and runs every job in one go), `01`–`08` setup notebooks (`07` publishes the cost dashboard; `08` finds or creates the workshop SQL warehouse `hytech_workshop_sql` and grants it, and creates `hytech_workshop_rt` when Lakehouse RT is on), `03_drip_producer`, `99_teardown` |
| `dashboards/` | AI/BI 成本和健康仪表盘（JSON 格式），基于 `ops` 视图<br>AI/BI cost & health dashboard (JSON) over the `ops` views |
| `cicd/` | GitLab CI 模板（在合并请求时验证、作为服务主体部署）和 `deploy_as_service_principal.sh` 实时演示脚本<br>GitLab CI template (validate on merge requests, deploy as a service principal) and `deploy_as_service_principal.sh` for the live demo |
| `src/hytech_workshop/` | 合成数据生成器：MT5 用户/成交/持仓（作为 DMS CDC）、应用事件、参考数据、滴灌程序<br>Synthetic data generator: MT5 users/deals/positions as DMS CDC, app events, reference data, drip producer |
| `solutions/pipeline/transformations/` | 参考管道（Python Auto Loader bronze + SQL silver/gold）<br>Reference pipeline (Python Auto Loader bronze + SQL silver/gold) |
| `jobs/` | 作业任务笔记本：`dq_gate`、`reconcile_server`、`publish_daily_summary`、`notify`、`audit`、`task_logger`（实验 03d）<br>Job task notebooks: `dq_gate`, `reconcile_server`, `publish_daily_summary`, `notify`, `audit`, `task_logger` (lab 03d) |
| `labs/` | 学员笔记本：完整代码，关键步骤有「要点」注释；辅助笔记本 `live_feed`（00、00b）、`staging_feed`（03c、04c）、`run_if_spec`（03d）、`dashboard_spec`（09b）、`genie_spec`（10b）由实验用 `%run` 加载；`03_pipeline/staging/` 是 03c 的预发布通道文件<br>Participant notebooks: complete code, with "Key point" comments on the key steps; helper notebooks `live_feed` (00, 00b), `staging_feed` (03c, 04c), `run_if_spec` (03d), `dashboard_spec` (09b) and `genie_spec` (10b) are loaded by the labs with `%run`; `03_pipeline/staging/` holds 03c's staging-lane file |
| `genie_code/` | Genie Code 技能 + 提示词梯度<br>Genie Code skill + prompt ladder |
| `tests/` | 本地测试：生成器、实验辅助函数、作业和设置定义、仪表盘和 Genie 定义、文档一致性（`pytest tests/`）<br>Local tests: generator, lab helpers, job and setup definitions, dashboard and Genie definitions, docs consistency (`pytest tests/`) |

## 值得了解的设计选择 (Design choices worth knowing)

- **追加 vs MERGE。** 成交（约 99.9% 是插入）是一个只追加的流式表。少数修正和删除进入一个小的 AUTO CDC 表，在物化视图中合并。这反映了 Hytech 实时 POC 的重新设计。

  **Append vs MERGE.** Deals (~99.9% inserts) are an append-only streaming table. The rare corrections and
  deletes go to a small AUTO CDC table, combined in an MV. This mirrors the redesign from Hytech's real-time POC.

- **数据契约 + 救援数据。** 应用事件使用显式 schema。漂移字段和类型不匹配进入 `_rescued_data` 并在 silver 层恢复。

  **Data contract + rescued data.** App events use an explicit schema. Drifted fields and type mismatches land
  in `_rescued_data` and are recovered in silver.

- **受治理的可观测性。** 学员只能通过 `ops.*` 视图查询系统表：仅限该工作区、无 SQL 文本、其他用户身份已掩盖。

  **Governed observability.** Participants query system tables only through `ops.*` views: this workspace only,
  no SQL text, other users' identities masked.

- **大语言模型数据。** AI 总结在 SQL 中预先计算每个数字；模型只对其进行措辞（第 07 个实验中的课程）。

  **LLM numbers.** The AI summary pre-computes every figure in SQL; the model only words it (lesson in lab 07).

- **每位学员自己的失败。** 实验 03c 和 04c 在学员自己的 schema 和 volume 中写入坏批次，并在自己的管道和作业中失败与恢复。讲师不需要访问学员的环境，这些实验也不依赖滴灌程序。

  **Every participant breaks their own lane.** Labs 03c and 04c write bad batches into the participant's own schema
  and volume, and fail and recover in their own pipeline and job. No instructor needs access to the participant's
  environment, and these labs don't depend on the drip producer.

- **每位学员自己的实时数据。** 实验 00 把共享历史复制到学员自己的 volume；`00b_Live_Data` 用同一个生成器按需向其中写入新的 DMS 文件。所以 Auto Loader 增量、SCD2、文件到达触发器、新服务器上线和坏批次都由学员自己完成，课堂上不需要集中运行任何作业。

  **Every participant has their own live data.** Lab 00 copies the shared history into the participant's own volumes,
  and `00b_Live_Data` writes new DMS files into them on demand with the same generator. Auto Loader increments, SCD2,
  the file-arrival trigger, onboarding a new server and the bad batch are all done by each participant, and nothing
  runs centrally during class.

- **自检的实验和参考副本。** 代码实验的每一步以 ✅ / ❌ 检查结束；`auto = true` 代你完成手动步骤（也是我们的验证方式）。补课笔记本 09b 和 10b 创建标题带「· 参考 (reference)」的副本，不会覆盖你自己搭建的仪表盘或 Genie Agent。

  **Self-checking labs and reference copies.** Each step of a code lab ends with ✅ / ❌ checks; `auto = true` does the
  hand steps for you (it is also how we verify the labs). The catch-up notebooks 09b and 10b create copies titled
  "· 参考 (reference)", so they never overwrite the dashboard or Genie Agent you built yourself.

## 在 `fe-vm-zh-serverless-ws` (AWS us-west-2) 上验证，2026 年 10 月 3 日 (Verified on `fe-vm-zh-serverless-ws` (AWS us-west-2), 3 Oct 2026)

- **设置 (Setup):** 跨 4 台服务器约 78.5 万笔成交，14.1 万条应用事件。

  **Setup:** ~785k deals across 4 servers, 141k app events.

- **参考答案管道 (Solution pipeline):** 绿色。期望过滤了 1,238 笔无效交易；SCD2 历史、修正和救援数据均已验证。

  **Solution pipeline:** green. Expectations dropped 1,238 invalid trades; SCD2 history, corrections and rescued
  data all verified.

- **参考答案作业 (Solution job):**
  - 正常路径 → 中文 AI 总结 (happy path → Chinese AI summary)
  - 循环任务失败 → 运行条件和审计 (for-each failure → Run-if alert and audit)
  - 上线 `mt5-hk-01` → 修复运行 (onboard `mt5-hk-01` → repair run)
  - 坏批次 → DQ 告警分支 (bad batch → DQ alert branch)

- **学员流程 (Participant flow):** 实验 00 → 自己的管道 → 追进度作业 → 模拟失败 → 修复 → 绿色。

  Lab 00 → own pipeline → catch-up job → simulated failure → repair → green.

- **笔记本 (Notebooks):** 实验 01、02、03b、06 和 07 无头执行。

  Labs 01, 02, 03b, 06 and 07 executed headless.

- **成本仪表盘 (Cost dashboard):** 由设置作业发布；在浏览器中检查了两个页面。

  Published by the setup job; both pages checked in the browser.

- **CI/CD:** `cicd` 目标由服务主体通过 OAuth M2M 部署和运行，如 `cicd/gitlab-ci.yml` 所做的那样。

  The `cicd` target deployed and run by a service principal over OAuth M2M, as `cicd/gitlab-ci.yml` does.

- **一键安装 (Master setup):** `setup/00_master_setup` 不用 CLI 创建并运行了全部作业和管道；重复运行时原地更新。

  `setup/00_master_setup` created and ran every job and the pipeline without the CLI; a re-run updated them in place.

详情：[`docs/facilitator_guide.md`](docs/facilitator_guide.md#在-fevm-上的验证结果-verified-on-fevm-fe-vm-zh-serverless-ws-3-oct-2026)。

Details: [`docs/facilitator_guide.md`](docs/facilitator_guide.md#在-fevm-上的验证结果-verified-on-fevm-fe-vm-zh-serverless-ws-3-oct-2026).

## 新增实验的验证，2026 年 10 月 10 日 (New labs verified, 10 Oct 2026)

在一个测试工作区（AWS us-east-1）上，以学员身份运行（`auto = true`，每个至少两次）：

On a test workspace (AWS us-east-1), run as a participant (`auto = true`, each at least twice):

| 实验 (Lab) | 结果 (Result) |
|---|---|
| `03c_Data_Quality_and_Monitoring` | 22/22 项检查通过：WARN 保留、DROP 进入隔离表、FAIL 只让预发布 silver 失败而下游跳过；策略修复和源头修复<br>22/22 checks: WARN keeps, DROP quarantines, FAIL fails only the staging silver and skips downstream; policy and source fixes |
| `03d_Run_If_Dependencies` | 11/11 个任务符合预期，运行状态 Succeeded with failures<br>11/11 tasks as expected, run status Succeeded with failures |
| `04c_Failure_Recovery_and_Backfill` | 23/23 项检查通过：重试、修复运行后同一次运行变绿、没有丢失批次、回填两天且每天正好一份日报；没有成交的日期也能正常回填<br>23/23 checks: retries, the same run green after the repair, no lost batch, two backfilled days with exactly one summary each; a day with no trades backfills cleanly |
| `setup/08_workshop_warehouses` | 创建 `hytech_workshop_sql`（serverless，Small）和 `hytech_workshop_rt`（Real-Time，Small），重新运行不会重复<br>Created `hytech_workshop_sql` (serverless, Small) and `hytech_workshop_rt` (Real-Time, Small); a re-run makes no duplicate |
| `09b_Dashboard_Catch_Up` | 参考仪表盘在 Real-Time 仓库上发布；三个数据集的查询都成功<br>Reference dashboard published on the Real-Time warehouse; all three dataset queries succeed |
| `10b_Genie_Catch_Up` | 参考 Genie Agent 用简体中文回答，遵守说明（"昨天" = 数据中最近的一天，完整数字，黄金 = XAUUSD）<br>Reference Genie Agent answers in Simplified Chinese and follows the instructions ("yesterday" = the latest day in the data, full numbers, 黄金 = XAUUSD) |
