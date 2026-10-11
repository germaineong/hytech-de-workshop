-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03b · 探索你的管道结果 (Explore your pipeline)
-- MAGIC
-- MAGIC 在 lab 03 的管道成功运行之后再执行。所有查询都在你自己的 schema 里。
-- MAGIC
-- MAGIC Execute after the pipeline from lab 03 has run successfully. All queries run in your own schema.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE my_schema STRING DEFAULT
  'u_' || regexp_replace(lower(split_part(current_user(), '@', 1)), '[^a-z0-9]', '_');
USE CATALOG hytech_ws_catalog;
USE SCHEMA IDENTIFIER(my_schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · 各层行数 (Rows per layer)

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
-- MAGIC Prerequisite: *Publish event log* is enabled in pipeline settings, table name is `pipeline_event_log`.

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
-- MAGIC `deal_time_not_in_future` 只是 **warn**（没有 `ON VIOLATION`）——这些行进入了 silver 和 gold。
-- MAGIC 看一看 gold 里”未来日期”的成交 → 讨论：哪些规则应该 drop，哪些应该 warn，哪些应该 fail？
-- MAGIC
-- MAGIC `deal_time_not_in_future` is only a **warn** (no `ON VIOLATION`) — these rows enter silver and gold.
-- MAGIC Look at “future-dated” deals in gold → discuss: which rules should drop, which should warn, which should fail?

-- COMMAND ----------

SELECT deal_date, sum(deals) AS deals FROM gold_daily_symbol_volume WHERE deal_date > current_date() GROUP BY deal_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · SCD Type 2 — 客户变更历史 (client change history)

-- COMMAND ----------

WITH changed AS (
  SELECT server_id, login FROM silver_mt5_users GROUP BY ALL HAVING count(*) > 1 LIMIT 3
)
SELECT u.server_id, u.login, u.mt_group, u.leverage, u.ib_login, u.status, u.last_access_at, u.__START_AT, u.__END_AT
FROM silver_mt5_users u JOIN changed c USING (server_id, login)
ORDER BY u.login, u.__START_AT;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 注意：`last_access_at` 变化**不会**产生新版本（不在 `TRACK HISTORY ON` 里），只会就地更新当前行。
-- MAGIC
-- MAGIC Note: changes to `last_access_at` **do not** create a new version (not in `TRACK HISTORY ON`), they just update the current row in place.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 · 交易修正与删除 (Dealer corrections and deletions)

-- COMMAND ----------

SELECT c.change_op, d.deal_id, d.deal_time, d.symbol, d.profit_usd AS original_profit, c.profit_usd AS corrected_profit, c.comment
FROM silver_mt5_deal_corrections c
JOIN silver_mt5_deals d USING (server_id, deal_id)
ORDER BY c.cdc_ts DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · 追加 vs MERGE (Append vs MERGE) — 成本为什么不同？
-- MAGIC
-- MAGIC `silver_mt5_deals` 是**追加**写入；`silver_mt5_deal_corrections` 是 **MERGE**。看 Delta 历史中的操作和指标：
-- MAGIC MERGE 需要读取目标表并重写文件。如果把 80 万行的 deals 全表 MERGE，每个微批都要扫描整张表 ——
-- MAGIC 这正是 Hytech 实时 POC 中成本从每天约 480 美元降到 40 美元以下的原因。
-- MAGIC
-- MAGIC `silver_mt5_deals` is **append** writes; `silver_mt5_deal_corrections` is **MERGE**. See operations and metrics in Delta history:
-- MAGIC MERGE reads the target table and rewrites files. If you MERGE all 800k deals rows, each micro-batch scans the entire table —
-- MAGIC this is why costs dropped from ~$480/day to below $40/day in Hytech's real-time POC.

-- COMMAND ----------

DESCRIBE HISTORY silver_mt5_deals LIMIT 5;

-- COMMAND ----------

DESCRIBE HISTORY silver_mt5_deal_corrections LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · 新鲜度 (Freshness) — DMS 提交 → bronze 入湖延迟

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

-- COMMAND ----------

SELECT event, count(*) AS events, count_if(has_rescued_data) AS with_rescued, count(campaign_id) AS campaign_ids,
       count_if(amount_usd IS NOT NULL) AS amounts
FROM silver_app_events GROUP BY event ORDER BY events DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8 · Gold 一瞥 (A look at gold)

-- COMMAND ----------

SELECT ib_name, tier, region, round(sum(lots)) AS lots, round(sum(rebate_usd)) AS rebate_usd, round(sum(net_funding_usd)) AS net_funding_usd
FROM gold_ib_daily_performance
WHERE deal_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL ORDER BY rebate_usd DESC LIMIT 10;

-- COMMAND ----------

SELECT * FROM gold_net_exposure_by_symbol ORDER BY abs(net_notional_usd) DESC LIMIT 10;
