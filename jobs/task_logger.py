# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · `task_logger` — 记录"这个任务运行了" (record that this task ran)
# MAGIC
# MAGIC 用于实验 03d 的 Run if 作业：只有在 Run if 条件满足时它才运行。它打印任务名称和条件，并在 `ops_run_if_log` 中追加一行，方便你在作业结束后查询哪些任务真正运行了。
# MAGIC
# MAGIC Used by the lab 03d Run if job: it only runs when its Run if condition is met. It prints the task name and the condition, and appends a row to `ops_run_if_log`, so you can query afterwards which tasks really ran.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("task_key", "")
dbutils.widgets.text("run_if", "")
dbutils.widgets.text("run_id", "")

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
task_key, run_if, run_id = dbutils.widgets.get("task_key"), dbutils.widgets.get("run_if"), dbutils.widgets.get("run_id")
cs = f"{catalog}.{schema}"

# COMMAND ----------

print("=" * 60)
print(f"task_key : {task_key}")
print(f"run_if   : {run_if}")
print(f"run_id   : {run_id}")
print("✅ 条件满足，所以这个任务运行了 (the condition was met, so this task ran)")
print("=" * 60)

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {cs}.ops_run_if_log (
    logged_at TIMESTAMP, run_id STRING, task_key STRING, run_if STRING)
  COMMENT 'Lab 03d: one row per task that ran in the Run if job'
""")
spark.sql(f"INSERT INTO {cs}.ops_run_if_log VALUES (current_timestamp(), :r, :t, :c)",
          args={"r": run_id, "t": task_key, "c": run_if})
