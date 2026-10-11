# 学员指南 · Hytech「Data Engineering with Databricks」工作坊

**时间:** 10 月 13–14 日 09:30–13:00 · **地点:** 深圳南山区后海联想中心（Hytech 办公室）· **语言:** 中文

**Time:** Oct 13–14, 09:30–13:00 · **Location:** Lenovo Houhai Centre, Nanshan District, Shenzhen (Hytech office) · **Language:** Mandarin Chinese

## 课前准备 (Before the workshop)

1. 用公司账号登录 Databricks 工作区（Triones 会发链接）。在**办公室网络**下确认能打开。
   Log in to the Databricks workspace with your company account (Triones will send the link). Confirm you can access it from the **office network**.

2. 推荐预习（自学，中文）：[Databricks Fundamentals (Mandarin Chinese)](https://www.databricks.com/training/catalog/databricks-fundamentals-mandarin-chinese-5940)
   Recommended pre-workshop study (self-paced, in Mandarin): [Databricks Fundamentals (Mandarin Chinese)](https://www.databricks.com/training/catalog/databricks-fundamentals-mandarin-chinese-5940)

3. 基础要求：SQL、中级 Python、Spark DataFrame 基础。
   Prerequisites: SQL, intermediate Python, basic Spark DataFrame knowledge.

## 第一步 (Step 1)

打开 `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here` → 右上角选择 **Serverless** → 逐个运行单元格。它会：

- 创建你的 schema：`hytech_ws_catalog.u_<你的名字>`
- 把共享的历史数据复制到你自己的落地区 `/Volumes/hytech_ws_catalog/u_<你的名字>/landing`（第 1b 步，通常 1–2 分钟）
- 把所有实验复制到 `/Users/<你>/hytech_de_lab/`。之后**只在你自己的副本里操作**。

Open `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here` → select **Serverless** in the top right → run each cell. It will:

- Create your schema: `hytech_ws_catalog.u_<your_name>`
- Copy the shared history into your own landing zone `/Volumes/hytech_ws_catalog/u_<your_name>/landing` (step 1b, usually 1–2 minutes)
- Copy all labs to `/Users/<you>/hytech_de_lab/`. After that, **work only in your own copy**.

## 实验地图 (Lab map)

| 模块 (Module) | 实验 (Lab) | 要点 (Topics) |
|---|---|---|
| M2 | `01_Unity_Catalog` | 授权给组、视图、标签、列掩码、行过滤、血缘<br>Grants to groups, views, tags, column masks, row filters, lineage |
| M3–M5 | `00b_Live_Data` | 需要新数据时运行：向你自己的落地区写入新的 DMS 文件（实验 02、03、04）<br>Run it whenever a lab needs new data: writes new DMS files into your own landing zone (labs 02, 03, 04) |
| M3 | `02_Ingestion` | CTAS / COPY INTO / Auto Loader / JSON + `_rescued_data` |
| M4 | `03_pipeline/`（先读 README）+ `03b_Explore_Pipeline`<br>`03_pipeline/` (read README first) + `03b_Explore_Pipeline` | 声明式管道：bronze → silver（AUTO CDC、期望）→ gold<br>Declarative pipelines: bronze → silver (AUTO CDC, expectations) → gold |
| M4 | `03c_Data_Quality_and_Monitoring` · `03d_Run_If_Dependencies` | 数据质量规则（WARN / DROP / FAIL）、隔离表、血缘、事件日志；作业的 Depends on 与 Run if<br>Data-quality rules (WARN / DROP / FAIL), quarantine, lineage, event log; Depends on and Run if in a job |
| M5 | `04_Lakeflow_Jobs.md` | 作业：if/else、for-each、Run if、修复运行、触发器<br>Jobs: if/else, for-each, Run if, repair runs, triggers |
| M5 | `04c_Failure_Recovery_and_Backfill` | 管道失败之后：重试、修复运行、按日期回填<br>After a pipeline failure: retries, a repair run, backfill by date |
| M6 | `05_Genie_Code.md` · `06_System_Tables` | Genie Code 提示词阶梯；成本 / 运行 / 血缘<br>Genie Code prompt ladder; cost / runs / lineage |
| M6c（课后作业 / homework） | `08_Knowledge_Check.md` | 知识测验：15 道单选题 + 加分题，对应认证考试大纲<br>Knowledge check: 15 multiple-choice questions + bonus, aligned to certification exam |
| M7 | `09_AIBI_Dashboard_Lakehouse_RT.md` · `10_Genie_Agent.md` | AI/BI 仪表盘 + Lakehouse RT；Genie Agent + Genie One（补课：`09b`、`10b` 会创建单独的参考副本）<br>AI/BI dashboard + Lakehouse RT; Genie Agent + Genie One (catch-up: `09b` and `10b` create separate reference copies) |
| AI（可选 / optional） | `07_AI_Functions` | `ai_query`（中文简报）、`ai_classify`、`ai_mask`<br>`ai_query` (Mandarin summary), `ai_classify`, `ai_mask` |

每个实验都包含完整代码：逐个运行单元格，结合说明和「要点」注释理解每一步。**跟不上没关系**：直接运行下一个单元格即可。改坏了代码？从 `solutions/` 复制原始版本。

Every lab has the complete code: run the cells one by one and use the notes and "Key point" comments to follow each step. **If you fall behind, no problem**: just run the next cell. Broke the code? Copy the original from `solutions/`.

代码实验（03c、03d、04c）在每一步结束时用 ✅ / ❌ 检查你的结果；❌ 后面写着怎么修改。想跳过手动步骤，把顶部的 `auto` 设为 `true`。

The code labs (03c, 03d, 04c) check your result with ✅ / ❌ at the end of each step; each ❌ says how to fix it. To skip the hand steps, set `auto` at the top to `true`.

## 数据是什么 (What is the data?)

**模拟数据**（不是真实客户数据），按 Hytech 的真实架构设计：

- 4 台 MT5 交易服务器（`mt5-sg-01`、`mt5-sg-02`、`mt5-uk-01`、`mt5-cy-01`）
- AWS DMS 格式的 Parquet 文件：全量 `LOAD00000001.parquet` + 增量 CDC 文件，带 `Op`（I/U/D）和 `cdc_ts`
- 表：`mt5_users`（客户账户）、`mt5_deals`（成交与出入金）、`mt5_positions`（持仓）、App 埋点事件 JSON
- 需要新数据时，运行 `00b_Live_Data`：它向你自己的落地区写入新的 CDC 文件，模拟实时数据（只有你的管道会读到）

**Synthetic data** (not real customer data), designed following Hytech's actual architecture:

- 4 MT5 trading servers (`mt5-sg-01`, `mt5-sg-02`, `mt5-uk-01`, `mt5-cy-01`)
- Parquet files in AWS DMS format: full load `LOAD00000001.parquet` + incremental CDC files with `Op` (I/U/D) and `cdc_ts`
- Tables: `mt5_users` (client accounts), `mt5_deals` (trades and deposits/withdrawals), `mt5_positions` (positions), app event JSON
- When you need new data, run `00b_Live_Data`: it writes new CDC files into your own landing zone to simulate streaming data (only your pipeline sees them)

## 小贴士 (Tips)

- Genie Code 可以用中文提问，选择 **Agent mode**，用 `@表名` 提供上下文。
  Genie Code accepts Mandarin questions; select **Agent mode** and use `@table_name` for context.

- 管道报错时，先看错误信息里的文件和行号；如果改过代码，就从 `solutions/pipeline/transformations/` 复制原始文件。
  When a pipeline fails, check the file and line number in the error message first; if you changed the code, copy the original file from `solutions/pipeline/transformations/`.

- 不要修改 `raw`、`solutions`、`ops` 下的任何东西（你也没有权限）。
  Do not modify anything under `raw`, `solutions`, or `ops` (you don't have permissions anyway).

- `00b_Live_Data` 一次只运行一个；`new_server=mt5-hk-01` 等完成 03d 之后再用；写入坏批次后马上运行一次你的作业。
  Run one `00b_Live_Data` at a time; use `new_server=mt5-hk-01` only after 03d; after a bad batch, run your job straight away.

- 课后两周内环境保留，可以继续练习。
  The environment is kept for 2 weeks after the workshop for additional practice.
