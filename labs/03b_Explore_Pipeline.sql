-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03b · 探索你的管道结果 (Explore your pipeline)
-- MAGIC
-- MAGIC 先完成实验 03：按 `03_pipeline/README.md`（在你的副本 `hytech_de_lab/03_pipeline/` 中）创建并运行管道 `trade_lakehouse_<你的名字>`。实验 03 不是笔记本，而是你在 Lakeflow 管道编辑器中用这个文件夹里的文件搭建的管道。管道成功运行之后，再运行这里的查询；所有查询都在你自己的 schema 里。
-- MAGIC
-- MAGIC Finish lab 03 first: build and run the pipeline `trade_lakehouse_<your_name>` by following `03_pipeline/README.md` (in your copy, `hytech_de_lab/03_pipeline/`). Lab 03 isn't a notebook: it's a pipeline you build in the Lakeflow Pipelines Editor from the files in that folder. Run the queries here once the pipeline has run successfully; they all run in your own schema.
-- MAGIC
-- MAGIC | 节 (Section) | 你会看到 (What you'll see) |
-- MAGIC |---|---|
-- MAGIC | 1 | 每一层有多少行，以及为什么不一样<br>How many rows each layer has, and why they differ |
-- MAGIC | 2 | 数据质量期望的结果在哪里，以及 DROP 和 WARN 的区别<br>Where the data-quality results live, and DROP versus WARN |
-- MAGIC | 3 | SCD Type 2：客户的变更历史<br>SCD Type 2: a client's change history |
-- MAGIC | 4–5 | 修正怎样不改写成交表，以及追加和 MERGE 的写入成本<br>How corrections avoid rewriting the deals table, and what append and MERGE cost to write |
-- MAGIC | 6 | 新鲜度：变更从 DMS 到 bronze 要多久<br>Freshness: how long a change takes from DMS to bronze |
-- MAGIC | 7 | JSON 契约和救援数据<br>The JSON contract and rescued data |
-- MAGIC | 8 | gold 业务表<br>The gold business tables |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 0 · 准备 (Setup)
-- MAGIC
-- MAGIC **这一步看什么：** 这个单元格算出你的 schema 名（`u_` 加上邮箱 @ 前面的部分），并设为当前 catalog 和 schema，所以后面的查询直接写表名。
-- MAGIC
-- MAGIC **为什么重要：** 每位学员的管道都写进自己的 schema，所以你看到的是你自己的数据，不会影响别人。
-- MAGIC
-- MAGIC **What this shows:** this cell works out your schema name (`u_` plus the part of your email before the @) and makes it the current catalog and schema, so the queries below can use bare table names.
-- MAGIC
-- MAGIC **Why it matters:** every participant's pipeline writes to their own schema, so you see your own data and affect nobody else's.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE my_schema STRING DEFAULT
  'u_' || regexp_replace(lower(split_part(current_user(), '@', 1)), '[^a-z0-9]', '_');
USE CATALOG hytech_ws_catalog;
USE SCHEMA IDENTIFIER(my_schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · 各层行数 (Rows per layer)
-- MAGIC
-- MAGIC **这一步看什么：** 管道每一层有多少行。
-- MAGIC - `bronze_mt5_deals`：DMS 文件原样落地的每一行，包括全量加载和所有 I / U / D 变更。
-- MAGIC - `silver_mt5_deals`：只保留插入（`Op = 'I'`），并删除了无效的交易行，所以比 bronze 少。
-- MAGIC - `silver_mt5_deal_corrections`：很少的修正 (U) 和删除 (D)。
-- MAGIC - `silver_mt5_deals_current`：应用修正、去掉删除之后的成交，是所有 gold 表的来源。
-- MAGIC - `silver_mt5_users`：SCD Type 2，所以“所有版本”比“当前”多，多出来的就是历史。
-- MAGIC - `silver_mt5_positions`：SCD Type 1，只保留当前未平仓的持仓；平仓的持仓被删除。
-- MAGIC - `gold_daily_symbol_volume`：按日期、服务器和品种汇总。
-- MAGIC
-- MAGIC **为什么重要：** 每一层回答一个不同的问题：bronze 是“到了什么”，silver 是“哪些是对的、现在是什么样”，gold 是“业务要看什么”。
-- MAGIC
-- MAGIC **想一想：** bronze 和 silver 之间少掉的行去了哪里？第 2 节和第 4 节会给出答案。
-- MAGIC
-- MAGIC **What this shows:** how many rows each layer of your pipeline has.
-- MAGIC - `bronze_mt5_deals`: every row of the DMS files exactly as it arrived, the full load plus every I / U / D change.
-- MAGIC - `silver_mt5_deals`: only the inserts (`Op = 'I'`), with the invalid trade rows dropped, so it is smaller than bronze.
-- MAGIC - `silver_mt5_deal_corrections`: the few corrections (U) and deletions (D).
-- MAGIC - `silver_mt5_deals_current`: the deals with corrections applied and deletions removed; every gold table reads it.
-- MAGIC - `silver_mt5_users`: SCD Type 2, so "all versions" is larger than "current"; the difference is the history.
-- MAGIC - `silver_mt5_positions`: SCD Type 1, only the positions open right now; closed positions are deleted.
-- MAGIC - `gold_daily_symbol_volume`: totals per day, server and symbol.
-- MAGIC
-- MAGIC **Why it matters:** each layer answers a different question: bronze is "what arrived", silver is "what is valid and current", gold is "what the business looks at".
-- MAGIC
-- MAGIC **Think about it:** where did the rows go that are in bronze but not in silver? Sections 2 and 4 answer this.

-- COMMAND ----------

SELECT 'bronze_mt5_deals' AS table_name, count(*) AS rows FROM bronze_mt5_deals
UNION ALL SELECT 'silver_mt5_deals', count(*) FROM silver_mt5_deals
UNION ALL SELECT 'silver_mt5_deal_corrections', count(*) FROM silver_mt5_deal_corrections
UNION ALL SELECT 'silver_mt5_deals_current', count(*) FROM silver_mt5_deals_current
UNION ALL SELECT 'silver_mt5_users (all versions)', count(*) FROM silver_mt5_users
UNION ALL SELECT 'silver_mt5_users (current)', count(*) FROM silver_mt5_users WHERE __END_AT IS NULL
UNION ALL SELECT 'silver_mt5_positions (open)', count(*) FROM silver_mt5_positions
UNION ALL SELECT 'gold_daily_symbol_volume', count(*) FROM gold_daily_symbol_volume;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · 数据质量 (Data quality) — 期望在事件日志里 (expectations live in the event log)
-- MAGIC
-- MAGIC 前提：管道设置中已开启 *Publish event log*，表名 `pipeline_event_log`。
-- MAGIC
-- MAGIC **这一步看什么：** 每条期望在所有管道更新中通过和失败的行数。期望的结果不在数据表里，而在管道的事件日志里：每个 `flow_progress` 事件的 `data_quality` 部分。
-- MAGIC
-- MAGIC **注意：** `valid_login`、`valid_trade_volume`、`valid_trade_symbol` 和 `valid_trade_price` 用 `ON VIOLATION DROP ROW`，失败的行不会进入 silver。`deal_time_not_in_future` 没有 `ON VIOLATION`，只是警告：失败的行仍然进入 silver。bronze 和 silver 之间少掉的行 = 这里被删除的无效行 + 修正和删除的行（第 4 节）。
-- MAGIC
-- MAGIC **为什么重要：** 你不用自己写检查查询，也能知道每次运行删除了多少、按哪条规则删除。实验 04 的数据质量闸门 (`dq_gate`) 读的就是这些数字。
-- MAGIC
-- MAGIC Prerequisite: *Publish event log* is enabled in pipeline settings, table name is `pipeline_event_log`.
-- MAGIC
-- MAGIC **What this shows:** how many rows passed and failed each expectation, over all pipeline updates. The results aren't in a data table: they live in the pipeline's event log, in the `data_quality` part of each `flow_progress` event.
-- MAGIC
-- MAGIC **Notice:** `valid_login`, `valid_trade_volume`, `valid_trade_symbol` and `valid_trade_price` use `ON VIOLATION DROP ROW`, so failing rows never reach silver. `deal_time_not_in_future` has no `ON VIOLATION`, so it only warns: failing rows still go into silver. The rows in bronze but not in silver = the invalid rows dropped here + the corrections and deletions (section 4).
-- MAGIC
-- MAGIC **Why it matters:** you can see how many rows each run dropped, and by which rule, without writing your own checks. Lab 04's data-quality gate (`dq_gate`) reads exactly these numbers.

-- COMMAND ----------

SELECT e.dataset, e.name AS expectation, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM (
  SELECT explode(from_json(details:flow_progress.data_quality.expectations,
         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM pipeline_event_log
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL
)
GROUP BY ALL ORDER BY failed DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **这一步看什么：** gold 里日期在今天之后的成交。`deal_time_not_in_future` 只是警告，所以这些行进入了 silver 和 gold。结果是空的，说明这次的数据里没有未来日期的成交。
-- MAGIC
-- MAGIC **为什么重要：** 只警告的规则让你看到问题，却不拦住数据：报表里可能已经有这些行了。
-- MAGIC
-- MAGIC **想一想：** 哪些规则应该删除行 (DROP)，哪些只警告 (WARN)，哪些应该让管道失败 (FAIL)？实验 03c 会让你亲手试这三种。
-- MAGIC
-- MAGIC **What this shows:** deals in gold dated after today. `deal_time_not_in_future` only warns, so these rows went into silver and gold. An empty result means this data has no future-dated deals.
-- MAGIC
-- MAGIC **Why it matters:** a warn-only rule shows you the problem without stopping the data: the rows may already be in a report.
-- MAGIC
-- MAGIC **Think about it:** which rules should drop rows (DROP), which should only warn (WARN), and which should fail the pipeline (FAIL)? Lab 03c lets you try all three.

-- COMMAND ----------

SELECT deal_date, sum(deals) AS deals FROM gold_daily_symbol_volume WHERE deal_date > current_date() GROUP BY deal_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · SCD Type 2 — 客户变更历史 (client change history)
-- MAGIC
-- MAGIC **这一步看什么：** 3 个有多个版本的客户，按时间排列。每一行是一个版本：`__START_AT` 是它开始生效的时间，`__END_AT` 是被下一个版本取代的时间；当前版本的 `__END_AT` 是空的。
-- MAGIC
-- MAGIC **注意：** 只有 `TRACK HISTORY ON` 里的列（组别、账户类型、杠杆、IB、状态）变化才会产生新版本。`last_access_at` 不在里面：它变化时只会就地更新当前行，不会产生新版本，否则每次登录都会多一个版本。
-- MAGIC
-- MAGIC **为什么重要：** 报表可以按任何一天客户当时的状态来算，例如那天的杠杆或所属 IB。这就是 AUTO CDC 的 `STORED AS SCD TYPE 2` 帮你做的事，你不用自己写 MERGE。
-- MAGIC
-- MAGIC **What this shows:** 3 clients that have more than one version, in time order. Each row is one version: `__START_AT` is when it took effect and `__END_AT` is when the next version replaced it; the current version has an empty `__END_AT`.
-- MAGIC
-- MAGIC **Notice:** only changes to the columns in `TRACK HISTORY ON` (group, account type, leverage, IB, status) create a new version. `last_access_at` isn't one of them: a change to it updates the current row in place instead of adding a version, otherwise every login would add one.
-- MAGIC
-- MAGIC **Why it matters:** reports can use what a client looked like on any given day, for example their leverage or IB on that day. AUTO CDC's `STORED AS SCD TYPE 2` does this for you, with no MERGE to write.

-- COMMAND ----------

WITH changed AS (
  SELECT server_id, login FROM silver_mt5_users GROUP BY ALL HAVING count(*) > 1 LIMIT 3
)
SELECT u.server_id, u.login, u.mt_group, u.leverage, u.ib_login, u.status, u.last_access_at, u.__START_AT, u.__END_AT
FROM silver_mt5_users u JOIN changed c USING (server_id, login)
ORDER BY u.login, u.__START_AT;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 · 交易修正与删除 (Dealer corrections and deletions)
-- MAGIC
-- MAGIC **这一步看什么：** 最近 10 条做市商修正 (U) 或删除 (D)：原始成交的盈亏和修正后的盈亏并排显示。
-- MAGIC
-- MAGIC **注意：** 原始成交仍然在 `silver_mt5_deals` 里：这张表只追加，从不改写。修正单独放在小表 `silver_mt5_deal_corrections` 里，`silver_mt5_deals_current` 再把两者合起来，gold 只读它。
-- MAGIC
-- MAGIC **为什么重要：** 成交约 99.9% 是插入，修正很少。把修正分开存放，大的成交表就可以一直只追加。第 5 节看这对写入意味着什么。
-- MAGIC
-- MAGIC **What this shows:** the 10 most recent dealer corrections (U) or deletions (D), with the deal's original and corrected profit side by side.
-- MAGIC
-- MAGIC **Notice:** the original deal is still in `silver_mt5_deals`: that table is append-only and never rewritten. The corrections sit in the small table `silver_mt5_deal_corrections`, and `silver_mt5_deals_current` combines the two; gold reads only that.
-- MAGIC
-- MAGIC **Why it matters:** about 99.9% of deals are inserts and corrections are rare. Keeping the corrections separate lets the big deals table stay append-only. Section 5 shows what that means for writes.

-- COMMAND ----------

SELECT c.change_op, d.deal_id, d.deal_time, d.symbol, d.profit_usd AS original_profit, c.profit_usd AS corrected_profit, c.comment
FROM silver_mt5_deal_corrections c
JOIN silver_mt5_deals d USING (server_id, deal_id)
ORDER BY c.cdc_ts DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · 追加 vs MERGE (Append vs MERGE) — 写入成本为什么不同？
-- MAGIC
-- MAGIC **这一步看什么：** `silver_mt5_deals`（只追加）最近 5 次写入的 Delta 历史。看 `operation` 和 `operationMetrics`：每次写入只新增文件（`numAddedFiles`、`numOutputRows`），从不删除或重写已有文件。如果看到 `OPTIMIZE`，那是 Databricks 自动合并小文件，不是你的管道写入。
-- MAGIC
-- MAGIC **为什么重要：** 追加的成本与**新增**的数据量成正比，表再大也不变慢。
-- MAGIC
-- MAGIC **What this shows:** the Delta history of the last 5 writes to `silver_mt5_deals` (append-only). Look at `operation` and `operationMetrics`: each write only adds files (`numAddedFiles`, `numOutputRows`) and never removes or rewrites existing ones. An `OPTIMIZE` entry is Databricks compacting small files automatically, not a write by your pipeline.
-- MAGIC
-- MAGIC **Why it matters:** an append costs in proportion to the data **added**, so it doesn't get slower as the table grows.

-- COMMAND ----------

DESCRIBE HISTORY silver_mt5_deals LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **这一步看什么：** `silver_mt5_deal_corrections`（AUTO CDC）的 Delta 历史。这里的操作是 **MERGE**：它先读取目标表，找到要改的行，再重写包含这些行的文件；`operationMetrics` 里有 `numTargetRowsUpdated`、`numTargetFilesRemoved` 等指标。
-- MAGIC
-- MAGIC **为什么重要：** MERGE 的成本与**已存储**的数据量有关，因为每个微批都要扫描目标表。修正表很小，所以 MERGE 很便宜；如果对整张成交表做 MERGE，每个微批都要扫描几十万行。这就是成交表用只追加、修正用一张小 AUTO CDC 表的原因。
-- MAGIC
-- MAGIC **What this shows:** the Delta history of `silver_mt5_deal_corrections` (AUTO CDC). Here the operation is **MERGE**: it reads the target table, finds the rows to change, and rewrites the files that hold them; `operationMetrics` shows `numTargetRowsUpdated`, `numTargetFilesRemoved` and similar.
-- MAGIC
-- MAGIC **Why it matters:** a MERGE costs in proportion to the data **stored**, because every micro-batch scans the target table. The corrections table is tiny, so its MERGE is cheap; a MERGE into the whole deals table would scan hundreds of thousands of rows on every micro-batch. That's why deals are append-only and corrections go to a small AUTO CDC table.

-- COMMAND ----------

DESCRIBE HISTORY silver_mt5_deal_corrections LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · 新鲜度 (Freshness) — DMS 提交 → bronze 入湖延迟
-- MAGIC
-- MAGIC **这一步看什么：** 每台服务器最近两小时的变更：最后一次 DMS 提交 (`cdc_ts`)、最后一次进入 bronze (`ingested_at`)，以及两者之间延迟的中位数（分钟）。全量加载文件不算，因为它们的 `cdc_ts` 都是加载时间。
-- MAGIC
-- MAGIC **注意：** 在这个工作坊里，延迟就是你运行 `00b_Live_Data` 到运行管道之间的时间。结果是空的？先运行一次 `00b_Live_Data`，再运行管道，然后重新运行这个单元格。
-- MAGIC
-- MAGIC **为什么重要：** 这是“报表有多新”的直接度量。在生产中，可以在它超过约定的时间时发出告警。
-- MAGIC
-- MAGIC **What this shows:** each server's changes from the last two hours: the latest DMS commit (`cdc_ts`), the latest arrival in bronze (`ingested_at`), and the median lag between the two, in minutes. Full-load files are left out, because their `cdc_ts` is the load time.
-- MAGIC
-- MAGIC **Notice:** in this workshop, the lag is the time between running `00b_Live_Data` and running the pipeline. Empty result? Run `00b_Live_Data` once, run the pipeline, then run this cell again.
-- MAGIC
-- MAGIC **Why it matters:** this is a direct measure of how fresh the reports are. In production you can raise an alert when it goes over an agreed limit.

-- COMMAND ----------

-- 仅来自最近 CDC 文件（你的 00b 运行）的行；全量加载行的 cdc_ts 都是加载时间
-- Only rows from recent CDC files (your 00b runs); full-load rows all carry cdc_ts = load time
SELECT server_id,
       max(cdc_ts)                                                                  AS last_dms_commit,
       max(ingested_at)                                                             AS last_ingested,
       round(percentile(timestampdiff(SECOND, cdc_ts, ingested_at), 0.5) / 60, 1)   AS p50_lag_minutes
FROM bronze_mt5_deals
WHERE source_file NOT LIKE '%/LOAD%' AND cdc_ts >= current_timestamp() - INTERVAL 2 HOURS
GROUP BY server_id
ORDER BY server_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7 · JSON：救回的数据 (rescued data recovered in silver)
-- MAGIC
-- MAGIC **这一步看什么：** 每种 App 事件：事件数、带有救援数据的事件数、有 `campaign_id` 的事件数（全部来自救援数据），以及有金额 `amount_usd` 的事件数（其中一部分是从救援数据中恢复的）。
-- MAGIC
-- MAGIC **注意：** bronze 按固定的契约 schema 读取 JSON（和实验 02 第 D 节一样）。契约之外的新字段（`campaign_id`）和类型不符的值（以字符串 `"1,234.50"` 到达的 `amount`）不会让管道失败，也不会丢失，而是进入 `_rescued_data`；silver 再把它们解析成 `campaign_id` 和 `amount_usd`。
-- MAGIC
-- MAGIC **为什么重要：** 上游 App 改了埋点也不会打断管道，你还能看到具体改了什么，再决定是否把新字段加进契约。
-- MAGIC
-- MAGIC **What this shows:** for each kind of app event: the number of events, how many carry rescued data, how many have a `campaign_id` (all of them recovered from the rescued data), and how many have an amount `amount_usd` (some of them recovered from the rescued data).
-- MAGIC
-- MAGIC **Notice:** bronze reads the JSON with a fixed contract schema (as in section D of lab 02). New fields outside the contract (`campaign_id`) and values of the wrong type (`amount` arriving as the string `"1,234.50"`) neither fail the pipeline nor get lost: they go into `_rescued_data`, and silver parses them out into `campaign_id` and `amount_usd`.
-- MAGIC
-- MAGIC **Why it matters:** a change in the app's tracking doesn't break the pipeline, and you can see exactly what changed before deciding whether to add the new field to the contract.

-- COMMAND ----------

SELECT event, count(*) AS events, count_if(has_rescued_data) AS with_rescued, count(campaign_id) AS campaign_ids,
       count_if(amount_usd IS NOT NULL) AS amounts
FROM silver_app_events GROUP BY event ORDER BY events DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8 · Gold 一瞥 (A look at gold)
-- MAGIC
-- MAGIC **这一步看什么：** 最近 30 天返佣最多的 10 个介绍经纪人 (IB)：客户手数、应付返佣和客户净入金（美元）。
-- MAGIC
-- MAGIC **为什么重要：** gold 是物化视图，成交数字都来自 `silver_mt5_deals_current`（这张 IB 表经由 `gold_client_daily_pnl`），所以已经应用了修正、去掉了删除。业务看到的数字和第 4 节的修正一致。
-- MAGIC
-- MAGIC **What this shows:** the 10 introducing brokers (IBs) with the most rebates over the last 30 days: their clients' lots, the rebates owed, and the clients' net funding (USD).
-- MAGIC
-- MAGIC **Why it matters:** gold tables are materialized views, and their deal figures all come from `silver_mt5_deals_current` (this IB table through `gold_client_daily_pnl`), so corrections are already applied and deletions removed. The numbers the business sees match section 4's corrections.

-- COMMAND ----------

SELECT ib_name, tier, region, round(sum(lots)) AS lots, round(sum(rebate_usd)) AS rebate_usd, round(sum(net_funding_usd)) AS net_funding_usd
FROM gold_ib_daily_performance
WHERE deal_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL ORDER BY rebate_usd DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **这一步看什么：** 按美元名义金额排序的每个品种的客户净敞口：客户当前未平仓的多头和空头相抵之后剩下的部分，也就是经纪商要对冲或自己承担的风险，以及浮动盈亏。它来自 `silver_mt5_positions`（只有未平仓的持仓）。
-- MAGIC
-- MAGIC **为什么重要：** 这是风险团队每天要看的数字。实验 04 的 AI 日报、实验 09 的仪表盘和实验 10 的 Genie Agent 都读这些 gold 表。
-- MAGIC
-- MAGIC **What this shows:** the net client exposure per symbol, ordered by USD notional: what's left after clients' open long and short positions offset each other, which is the risk the broker has to hedge or carry, plus the floating P&L. It comes from `silver_mt5_positions` (open positions only).
-- MAGIC
-- MAGIC **Why it matters:** this is a number the risk team checks every day. Lab 04's AI summary, lab 09's dashboard and lab 10's Genie Agent all read these gold tables.

-- COMMAND ----------

SELECT * FROM gold_net_exposure_by_symbol ORDER BY abs(net_notional_usd) DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 小结 (Recap)
-- MAGIC
-- MAGIC - 各层回答不同的问题：bronze 是到了什么，silver 是哪些对、现在是什么样，gold 是业务要看什么。
-- MAGIC - 数据质量的结果在事件日志里；DROP 拦住坏行，WARN 只记录。
-- MAGIC - AUTO CDC 处理可变的实体（客户用 SCD2，持仓用 SCD1）；不可变的成交只追加，修正放在小表里。
-- MAGIC - 下一步：`03c_Data_Quality_and_Monitoring`，亲手试 WARN / DROP / FAIL。
-- MAGIC
-- MAGIC - Each layer answers a different question: bronze is what arrived, silver is what is valid and current, gold is what the business looks at.
-- MAGIC - Data-quality results live in the event log; DROP stops bad rows, WARN only records them.
-- MAGIC - AUTO CDC handles the entities that change (SCD2 for clients, SCD1 for positions); immutable deals are append-only, with corrections in a small table.
-- MAGIC - Next: `03c_Data_Quality_and_Monitoring`, where you try WARN / DROP / FAIL yourself.
