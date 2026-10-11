-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · 系统表 (System tables) — 成本、运行情况、血缘 (cost, runs, lineage) · LAB
-- MAGIC
-- MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/06_System_Tables` 复制原始版本。
-- MAGIC
-- MAGIC 通过 `hytech_ws_catalog.ops` 下的**治理视图**访问系统表：只包含本工作区的数据，SQL 文本已移除，其他人的身份被掩码。
-- MAGIC 这本身就是一个治理实践：分析师不需要直接访问整个账户的系统表。
-- MAGIC
-- MAGIC ⚠️ 账单数据（billing）有数小时延迟：今天的运行可能明天早上才完整出现。
-- MAGIC
-- MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/06_System_Tables`.
-- MAGIC
-- MAGIC Access system tables through **governance views** under `hytech_ws_catalog.ops`: only your workspace's data, SQL text removed, others' identities masked.
-- MAGIC This is a governance practice: analysts don't need direct access to the entire account's system tables.
-- MAGIC
-- MAGIC ⚠️ Billing data has a few hours lag: today's runs may not appear completely until tomorrow morning.

-- COMMAND ----------

-- 定义变量：我的 schema、管道和作业 (Define variables: my schema, pipeline, job)
DECLARE OR REPLACE VARIABLE my_schema STRING DEFAULT
  'u_' || regexp_replace(lower(split_part(current_user(), '@', 1)), '[^a-z0-9]', '_');
DECLARE OR REPLACE VARIABLE my_pipeline_id STRING;
DECLARE OR REPLACE VARIABLE my_job_id STRING;

USE CATALOG hytech_ws_catalog;
USE SCHEMA ops;

-- 查找你的管道（Lab 03）和作业（Lab 04），按所有者和名称查找 (Find your pipeline (lab 03) and job (lab 04) by owner + name)
SET VAR my_pipeline_id = (
  SELECT max_by(pipeline_id, create_time) FROM pipelines
  WHERE created_by = current_user() AND name LIKE 'trade_lakehouse%' AND delete_time IS NULL);
SET VAR my_job_id = (
  SELECT max_by(job_id, change_time) FROM jobs
  WHERE creator_user_name = current_user() AND name LIKE '%daily_trading_reporting%' AND delete_time IS NULL);

SELECT my_schema, my_pipeline_id, my_job_id;

-- COMMAND ----------

-- MAGIC %md ## 1 · 管道每次更新 (Pipeline updates)

-- COMMAND ----------

SELECT update_id,
       max(update_type)                                        AS update_type,
       min(period_start_time)                                  AS started_at,
       max(period_end_time)                                    AS ended_at,
       timestampdiff(SECOND, min(period_start_time), max(period_end_time)) AS seconds,
       max_by(result_state, period_end_time)                   AS result_state
FROM pipeline_update_timeline
WHERE pipeline_id = my_pipeline_id
GROUP BY update_id
ORDER BY started_at DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 2 · 作业运行与任务耗时 (Job runs and task durations)

-- COMMAND ----------

SELECT run_id,
       min(period_start_time)                 AS started_at,
       -- run_duration_seconds can be 0 for serverless runs, so derive it from the period timestamps
       timestampdiff(SECOND, min(period_start_time), max(period_end_time)) AS run_seconds,
       max_by(result_state, period_end_time)  AS result_state,
       max(trigger_type)                      AS trigger_type
FROM job_run_timeline
WHERE job_id = my_job_id
GROUP BY run_id
ORDER BY started_at DESC
LIMIT 10;

-- COMMAND ----------

SELECT task_key,
       count(*)                                         AS task_runs,
       count_if(result_state = 'FAILED')                AS failures,
       round(avg(execution_duration_seconds))           AS avg_exec_seconds,
       max(execution_duration_seconds)                  AS max_exec_seconds
FROM job_task_run_timeline
WHERE job_id = my_job_id
GROUP BY task_key
ORDER BY avg_exec_seconds DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · 成本 (Cost) — DBU × 标价 (list price)
-- MAGIC `billing_usage.usage_metadata` 记录了每条用量来自哪个管道 / 作业 / 作业运行。
-- MAGIC 实际价格 = 标价 × 合同折扣（Hytech POC 中按 DLT 24% 折扣计算）。

-- COMMAND ----------

SELECT u.usage_date,
       u.billing_origin_product,
       round(sum(u.usage_quantity), 3)                        AS dbus,
       -- 要点 1 (Key point 1) · 成本 = DBU × 标价 (cost = DBUs × list price (p.pricing.default))
       round(sum(u.usage_quantity * p.pricing.default), 2)    AS list_cost_usd
FROM billing_usage u
JOIN list_prices p
  -- 要点 2 (Key point 2) · 按 SKU 名称和价格有效期联接用量与价格 (join usage to its price by SKU name, within the price's validity window)
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_metadata.dlt_pipeline_id = my_pipeline_id
   OR u.usage_metadata.job_id = my_job_id
GROUP BY ALL
ORDER BY u.usage_date, u.billing_origin_product;

-- COMMAND ----------

-- 每次作业运行的成本（哪次运行最贵？） (Cost per job run (which run was the most expensive?))
SELECT u.usage_metadata.job_run_id                          AS job_run_id,
       round(sum(u.usage_quantity), 3)                      AS dbus,
       round(sum(u.usage_quantity * p.pricing.default), 3)  AS list_cost_usd
FROM billing_usage u
JOIN list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_metadata.job_id = my_job_id
GROUP BY ALL
ORDER BY list_cost_usd DESC;

-- COMMAND ----------

-- MAGIC %md ## 4 · 血缘 (Lineage) — 你的表从哪里来，被谁使用

-- COMMAND ----------

SELECT DISTINCT
       coalesce(source_table_full_name, source_path) AS source,
       target_table_full_name                       AS target,
       entity_type
FROM table_lineage
-- 要点 3 (Key point 3) · 只保留你的 schema 是源或目标的血缘边 (keep only lineage edges where your schema is the source or the target)
WHERE target_table_schema = my_schema OR source_table_schema = my_schema
ORDER BY target, source;

-- COMMAND ----------

-- MAGIC %md ## 5 · 查询历史 (Query history) — 你最近的 SQL

-- COMMAND ----------

SELECT start_time, statement_type, execution_status, total_duration_ms, read_rows, produced_rows, client_application
FROM query_history
WHERE executed_by = current_user()
ORDER BY start_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · 思考 (Discuss)
-- MAGIC
-- MAGIC * Hytech 每周的成本复盘需要哪些指标？（团队 / 管道 / 作业 / 失败运行浪费 / 全量 vs 增量）
-- MAGIC * 用 Genie Code 把上面的成本查询做成一个 AI/BI 仪表盘页面（Lab 05 的最后一题）。
-- MAGIC
-- MAGIC * What metrics does Hytech's weekly cost review need? (team / pipeline / job / wasted failed runs / full vs incremental)
-- MAGIC * Use Genie Code to build a dashboard from the cost queries above (the final task from Lab 05).
