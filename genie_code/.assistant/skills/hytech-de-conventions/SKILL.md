---
name: hytech-de-conventions
description: >-
  为 Hytech MT5 交易数据湖（catalog hytech_ws_catalog 或 Hytech 的生产 catalog）编写、审查或调试数据工程代码时使用：Lakeflow Spark 声明式管道（bronze/silver/gold 流式表和物化视图）、AUTO CDC、期望、Auto Loader、Lakeflow 作业、Unity Catalog 授权/标签/掩码和系统表成本查询。编码 Hytech 命名、MT5 数据语义（成交、持仓、用户、DMS CDC）、成本感知模式和治理规则。
  Use when writing, reviewing or debugging data-engineering code for the Hytech MT5 trade lakehouse (catalog hytech_ws_catalog or Hytech's production catalogs): Lakeflow Spark Declarative Pipelines (bronze/silver/gold streaming tables and materialized views), AUTO CDC, expectations, Auto Loader, Lakeflow Jobs, Unity Catalog grants/tags/masks, and system-table cost queries. Encodes Hytech naming, MT5 data semantics (deals, positions, users, DMS CDC), cost-aware patterns and governance rules.
  Triggers: pipeline, streaming table, materialized view, AUTO CDC, expectation, Auto Loader, job, MT5, deals, positions, users, DMS, CDC, 管道, 流表, 物化视图, 数据质量, 作业.
---

# Hytech 数据工程规范 — MT5 交易数据湖 (Hytech data-engineering conventions — MT5 trade lakehouse)

为 Hytech 交易数据湖生成或更改代码时遵循这些规则。如果用户的请求与规则冲突，请说明并提出符合规范的替代方案。

Follow these rules whenever you generate or change code for the Hytech trade lakehouse. If the user's request conflicts with a rule, say so and propose the compliant alternative.

## 1. 事物的位置 (Where things live)

- 着陆区（只读）: `/Volumes/hytech_ws_catalog/raw/landing/`；每位学员自己的副本（学员管道的 `landing_root`，`00b_Live_Data` 向其中写入新文件）: `/Volumes/hytech_ws_catalog/u_<name>/landing/`<br>
Landing zone (read-only): `/Volumes/hytech_ws_catalog/raw/landing/`; each participant's own copy (their pipeline's `landing_root`, where `00b_Live_Data` writes new files): `/Volumes/hytech_ws_catalog/u_<name>/landing/`
  - `mt5/<server_id>/<table>/` — AWS DMS Parquet: `LOAD0000000N.parquet`（全量）和
    `yyyymmdd-hhmmssfff.parquet`（CDC）。每行都有 `Op`（`I`/`U`/`D`）和 `cdc_ts`（提交时间）。
    表：`mt5_users`, `mt5_deals`, `mt5_positions`。服务器：`mt5-sg-01`, `mt5-sg-02`, `mt5-uk-01`,
    `mt5-cy-01`（+ 会随时间增加新的 — 数据接入中永不硬编码列表）。
  - `mt5/<server_id>/<table>/` — AWS DMS Parquet: `LOAD0000000N.parquet` (full load) and
    `yyyymmdd-hhmmssfff.parquet` (CDC). Every row has `Op` (`I`/`U`/`D`) and `cdc_ts` (commit time).
    Tables: `mt5_users`, `mt5_deals`, `mt5_positions`. Servers: `mt5-sg-01`, `mt5-sg-02`, `mt5-uk-01`,
    `mt5-cy-01` (+ new ones over time — never hard-code the list in ingestion).
  - `app_events/<yyyy-mm-dd>/events-*.json` — 传感器风格的 JSON 行（嵌套 `properties`）。<br>
    `app_events/<yyyy-mm-dd>/events-*.json` — Sensors-style JSON lines (nested `properties`).
- 参考 CSV：`/Volumes/hytech_ws_catalog/raw/ref/{symbols,ib_hierarchy,servers,fx_rates}/`。<br>
Reference CSVs: `/Volumes/hytech_ws_catalog/raw/ref/{symbols,ib_hierarchy,servers,fx_rates}/`.
- 每个工程师在他们**自己的 schema** `u_<name>` 中进行构建。永不写入 `raw`、`solutions` 或 `ops`。<br>
Each engineer builds in their **own schema** `u_<name>`. Never write to `raw`, `solutions` or `ops`.
- 管道配置键：`landing_root`、`ref_root` — 使用 `${landing_root}`（SQL）或
  `spark.conf.get("landing_root")`（Python）而不是字面路径。<br>
Pipeline configuration keys: `landing_root`, `ref_root` — use `${landing_root}` (SQL) or
  `spark.conf.get("landing_root")` (Python) instead of literal paths.

## 2. 命名和分层 (Naming and layering)

| 层 (Layer) | 名称模式 (Name pattern) | 内容 (Contents) |
|---|---|---|
| Bronze | `bronze_<source>_<entity>` 例如 `bronze_mt5_deals` | 源列按原样（MT5 PascalCase）+ `server_id`（来自路径）、`source_file`、`ingested_at`<br>Source columns as-is (MT5 PascalCase) + `server_id` (from path), `source_file`, `ingested_at` |
| Silver | `silver_<source>_<entity>` | snake_case、有类型、强制执行键、期望<br>snake_case, typed, keys enforced, expectations |
| Gold | `gold_<subject>_<grain>` 例如 `gold_daily_symbol_volume` | 用于 BI / Genie 的业务集市<br>Business marts for BI / Genie |
| Reference | `ref_<name>` | 在参考 CSV 上的物化视图<br>Materialized views over the reference CSVs |
| Ops | `ops_<name>` | 作业输出：数据质量结果、对账、告警、审计<br>Job outputs: DQ results, reconciliation, alerts, audit |

- 列名称中的单位：`_usd`、`lots`、`_at`（时间戳）、`_date`、`_pct`。<br>
Units in column names: `_usd`, `lots`, `_at` (timestamp), `_date`, `_pct`.
- 每个表 / MV 都有一个 `COMMENT` 和 `TBLPROPERTIES ('quality' = 'bronze' | 'silver' | 'gold')`。<br>
Every table / MV has a `COMMENT` and `TBLPROPERTIES ('quality' = 'bronze' | 'silver' | 'gold')`.

## 3. MT5 语义学（准确获取这些） (MT5 semantics (get these exactly right))

- `Volume` 单位是 1/10000 手 → `lots = Volume / 10000.0`。<br>
`Volume` is in 1/10000 lot → `lots = Volume / 10000.0`.
- `Action`: 0 = BUY、1 = SELL、2 = BALANCE（如果 `Profit > 0` 则入金，如果 `Profit < 0` 则出金）。<br>
`Action`: 0 = BUY, 1 = SELL, 2 = BALANCE (deposit if `Profit > 0`, withdrawal if `Profit < 0`).
- `Entry`: 0 = IN（开仓）、1 = OUT（平仓）。已实现盈亏（`Profit`，美元）仅在 OUT 成交时出现。<br>
`Entry`: 0 = IN (opens a position), 1 = OUT (closes it). Realised P&L (`Profit`, USD) is on OUT deals only.
- 成本：`Commission`（按边收费）、`Storage` = 交换。`Profit`、`Commission`、`Storage` 以美元计。<br>
Costs: `Commission` (charged per side), `Storage` = swap. `Profit`, `Commission`, `Storage` are in USD.
- 美元名义金额 = `lots * ContractSize * Price * RateProfit`（`RateProfit` 将报价货币转换为美元）。<br>
USD notional = `lots * ContractSize * Price * RateProfit` (`RateProfit` converts quote currency → USD).
- 用户：`Group` = `real\<Brand>\<TYPE>-USD` → `brand = split_part(Group, '\\', 2)`，
  `account_type = split_part(split_part(Group, '\\', 3), '-', 1)`。`Agent` = IB 登录名，0 = 没有 IB。<br>
Users: `Group` = `real\<Brand>\<TYPE>-USD` → `brand = split_part(Group, '\\', 2)`,
  `account_type = split_part(split_part(Group, '\\', 3), '-', 1)`. `Agent` = IB login, 0 = no IB.
- 登录名、成交 id 和持仓 id 仅**按服务器**唯一：键始终是
  `(server_id, login)`、`(server_id, deal_id)`、`(server_id, position_id)`。<br>
Logins, deal ids and position ids are unique **per server** only: keys are always
  `(server_id, login)`, `(server_id, deal_id)`, `(server_id, position_id)`.
- 零售客户通常亏损：正的总 `client_pnl_usd` 不寻常且值得标记。<br>
Retail clients usually lose: a positive total `client_pnl_usd` is unusual and worth flagging.

## 4. 成本感知的模式（Hytech 实时 POC 的经验教训）(Cost-aware patterns (lesson from Hytech's real-time POC))

- **不可变事实（成交，~99.9% 插入）→ 只追加**流式表，来自 bronze `WHERE Op = 'I'`。
  将罕见的 `U`/`D` 行路由到小的 AUTO CDC 表（`silver_mt5_deal_corrections`），并在
  物化视图（`silver_mt5_deals_current`）中组合它们。**不要** AUTO CDC / MERGE 整个成交表：
  MERGE 会重新扫描目标，因此成本随存储的数据增长，而不是更改的数据。<br>
**Immutable facts (deals, ~99.9% inserts) → append-only** streaming table from bronze `WHERE Op = 'I'`.
  Route the rare `U`/`D` rows to a small AUTO CDC table (`silver_mt5_deal_corrections`) and combine
  them in a materialized view (`silver_mt5_deals_current`). **Do not** AUTO CDC / MERGE the entire deals
  table: a MERGE rescans the target, so cost grows with data *stored*, not data *changed*.
- **可变实体 → AUTO CDC**：用户作为 SCD 类型 2，仅在 `TRACK HISTORY ON` 业务列
  （group、account_type、leverage、ib_login、status — 不包括 `LastAccess`）；持仓作为 SCD 类型 1，
  `APPLY AS DELETE WHEN _op = 'D'`。始终 `SEQUENCE BY cdc_ts`。<br>
**Mutable entities → AUTO CDC**: users as SCD Type 2 with `TRACK HISTORY ON` business columns only
  (group, account_type, leverage, ib_login, status — not `LastAccess`); positions as SCD Type 1 with
  `APPLY AS DELETE WHEN _op = 'D'`. Always `SEQUENCE BY cdc_ts`.
- 默认触发的管道；仅当 SLA 需要时才使用连续模式。无服务器计算。
  不为计划工作使用多用途集群。<br>
Triggered pipelines by default; continuous mode only if an SLA requires it. Serverless compute.
  No all-purpose clusters for scheduled work.

## 5. 数据质量 (Data quality)

- Silver 上的期望：<br>
Expectations on silver:
  - `ON VIOLATION DROP ROW` 用于无效成交：`login IS NOT NULL`，且除非 `deal_type = 'BALANCE'`：
    `volume_raw > 0`、`symbol IS NOT NULL`、`price > 0`。<br>
  `ON VIOLATION DROP ROW` for invalid trades: `login IS NOT NULL`, and unless `deal_type = 'BALANCE'`:
    `volume_raw > 0`, `symbol IS NOT NULL`, `price > 0`.
  - 仅警告（无 `ON VIOLATION`）用于软规则，例如 `deal_time <= current_timestamp() + INTERVAL 1 HOUR`。<br>
  Warn only (no `ON VIOLATION`) for soft rules, e.g. `deal_time <= current_timestamp() + INTERVAL 1 HOUR`.
  - `ON VIOLATION FAIL UPDATE` 仅用于必须停止管道的合同破裂。<br>
  `ON VIOLATION FAIL UPDATE` only for contract breaks that must stop the pipeline.
- 数据质量指标来自管道事件日志：`details:flow_progress.data_quality.dropped_records` 和
  `details:flow_progress.data_quality.expectations`。<br>
DQ metrics come from the pipeline event log: `details:flow_progress.data_quality.dropped_records` and
  `details:flow_progress.data_quality.expectations`.
- 应用程序事件：在 bronze 中保持一个合同 schema，带有 `rescuedDataColumn => '_rescued_data'`；
  在 silver 中按 `event_id` 去重。<br>
App events: keep a contract schema in bronze with `rescuedDataColumn => '_rescued_data'`;
  de-duplicate on `event_id` in silver.

## 6. 治理（Unity Catalog）(Governance (Unity Catalog))

- PII 列（`email`、`phone`、`first_name`、`last_name`）获得标签 `pii_category`（值 = PII
  类型）和列掩码。在生产中使用 Hytech 数据治理团队定义的**受管标签**键和值 — 受管标签拒绝不在其
  允许列表中的值（`UC_TAG_POLICY_VALUE_NOT_ALLOWED`）。掩码和行过滤函数使用 `is_account_group_member('<group>')`。<br>
PII columns (`email`, `phone`, `first_name`, `last_name`) get the tag `pii_category` (value = the PII
  type) and a column mask. In production use the **governed tag** keys and values defined by Hytech's data
  governance team — governed tags reject values outside their allowed list (`UC_TAG_POLICY_VALUE_NOT_ALLOWED`).
  Mask and row-filter functions use `is_account_group_member('<group>')`.
- 授予**组**，永不授予个人用户。最小权限：`USE CATALOG`、`USE SCHEMA`、`SELECT`。<br>
Grant to **groups**, never to individual users. Least privilege: `USE CATALOG`, `USE SCHEMA`, `SELECT`.
- 代码中没有密钥或令牌。Webhook URL 来自作业参数或密钥作用域。<br>
No secrets or tokens in code. Webhook URLs come from job parameters or a secret scope.

## 7. Lakeflow 作业模式 (Lakeflow Jobs pattern)

`run_pipeline`（管道任务，1 次重试）→ `dq_gate`（设置任务值）→ 在
`{{tasks.dq_gate.values.dq_drop_pct}}` 上的 `if/else` → `publish_daily_summary` | `notify_dq_owner`；
`for each` 服务器 → `reconcile_server`；`alert_on_failure`，带有*运行条件：至少一个失败*；
`write_audit_row`，带有*运行条件：全部完成*。生产部署通过 CI/CD 使用声明式自动化包，
以服务主体身份运行 — 生产环境中没有手动编辑。

`run_pipeline` (pipeline task, 1 retry) → `dq_gate` (sets task values) → `if/else` on
`{{tasks.dq_gate.values.dq_drop_pct}}` → `publish_daily_summary` | `notify_dq_owner`;
`for each` server → `reconcile_server`; `alert_on_failure` with *Run if: At least one failed*;
`write_audit_row` with *Run if: All done*. Production deployments use Declarative Automation Bundles
through CI/CD, run as a service principal — no manual edits in prod.

## 8. 回答风格 (Answer style)

- 用用户的语言回答（中文提问就用中文回答）；保持 SQL/Python 标识符为英文。<br>
Reply in the user's language (中文提问就用中文回答); keep SQL/Python identifiers in English.
- 首先显示代码，然后在 1-3 短句中解释。指出上述哪条规则适用。<br>
Show the code first, then explain in 1–3 short lines. Point out which rule above you applied.
