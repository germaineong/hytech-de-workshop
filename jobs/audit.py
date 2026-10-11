# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · `audit` — 运行审计 (run audit, **Run if = All done**)
# MAGIC
# MAGIC 总是最后运行，无论上游发生了什么，并通过 Jobs API 将此运行的每个任务的状态记录在 `ops_job_audit` 中 — 方便进行操作评审的证据。
# MAGIC
# MAGIC Always runs last, whatever happened upstream, and records the state of every task of this run in
# MAGIC `ops_job_audit` (via the Jobs API) — handy evidence for operations reviews.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")

import json

from databricks.sdk import WorkspaceClient

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
job_id, run_id = dbutils.widgets.get("job_id"), dbutils.widgets.get("run_id")
cs = f"{catalog}.{schema}"

# COMMAND ----------

tasks = []
if run_id:
    run = WorkspaceClient().jobs.get_run(int(run_id))
    for t in run.tasks or []:
        state = t.state
        tasks.append({
            "task_key": t.task_key,
            "life_cycle": state.life_cycle_state.value if state and state.life_cycle_state else None,
            "result": state.result_state.value if state and state.result_state else None,
        })
print(json.dumps(tasks, indent=1))

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {cs}.ops_job_audit (
    audited_at TIMESTAMP, job_id STRING, run_id STRING, tasks STRING,
    failed_tasks INT, excluded_tasks INT)
""")
failed = sum(1 for t in tasks if t["result"] in ("FAILED", "TIMEDOUT", "CANCELED"))
excluded = sum(1 for t in tasks if t["result"] in ("EXCLUDED", "UPSTREAM_FAILED", "UPSTREAM_CANCELED"))
spark.sql(f"INSERT INTO {cs}.ops_job_audit VALUES (current_timestamp(), :j, :r, :t, {failed}, {excluded})",
          args={"j": job_id, "r": run_id, "t": json.dumps(tasks)})
