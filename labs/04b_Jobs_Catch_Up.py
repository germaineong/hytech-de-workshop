# Databricks notebook source
# MAGIC %md
# MAGIC # 04b · 作业补课 (Jobs catch-up)
# MAGIC
# MAGIC 按照 `04_Lakeflow_Jobs.md` 的描述，创建（或更新）`daily_trading_reporting_<your_name>`，
# MAGIC 连接到 **你的** 管道和 schema。如果你落后了，或想对比你搭建的作业，可以运行这个。
# MAGIC
# MAGIC Creates (or updates) `daily_trading_reporting_<your_name>` exactly as described in `04_Lakeflow_Jobs.md`,
# MAGIC wired to **your** pipeline and schema. Use it if you fell behind, or to compare with what you built.

# COMMAND ----------

# 输入参数 (Input parameters)
dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("pipeline_name", "", "Default: trade_lakehouse_<your_name>")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5")

from databricks.sdk import WorkspaceClient

# 获取当前用户信息和路径 (Get current user info and paths)
w = WorkspaceClient()
me = w.current_user.me().user_name
name = "".join(ch if ch.isalnum() else "_" for ch in me.split("@")[0].lower()).strip("_")
catalog = dbutils.widgets.get("catalog")
schema = f"u_{name}"
pipeline_name = dbutils.widgets.get("pipeline_name").strip() or f"trade_lakehouse_{name}"
jobs_dir = f"/Users/{me}/hytech_de_lab/jobs"

# 查找管道 (Find pipeline)
matches = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")]
if not matches:
    raise ValueError(f"Pipeline '{pipeline_name}' not found — create it in lab 03 first (or set pipeline_name).")
pipeline_id = matches[0].pipeline_id
print("pipeline:", pipeline_name, pipeline_id, "| schema:", f"{catalog}.{schema}", "| notebooks:", jobs_dir)

# COMMAND ----------


def nb(task_key, notebook, deps=(), params=None, run_if=None, outcome=None):
    # 创建 notebook 任务 (Create notebook task)
    t = {"task_key": task_key, "notebook_task": {"notebook_path": f"{jobs_dir}/{notebook}", "base_parameters": params or {}}}
    if deps:
        t["depends_on"] = [{"task_key": d} if outcome is None else {"task_key": d, "outcome": outcome} for d in deps]
    if run_if:
        t["run_if"] = run_if
    return t


# 作业参数和设置 (Job parameters and settings)
ids = {"job_id": "{{job.id}}", "run_id": "{{job.run_id}}"}
settings = {
    "name": f"daily_trading_reporting_{name}",
    "max_concurrent_runs": 1,
    "parameters": [
        {"name": "catalog", "default": catalog},
        {"name": "schema", "default": schema},
        {"name": "dq_max_drop_pct", "default": "1.0"},
        {"name": "servers", "default": '["mt5-sg-01","mt5-sg-02","mt5-uk-01","mt5-cy-01"]'},
        {"name": "llm_endpoint", "default": dbutils.widgets.get("llm_endpoint")},
        {"name": "fail_server", "default": "mt5-uk-01"},
        {"name": "report_date", "default": ""},
    ],
    "tasks": [
        {"task_key": "run_pipeline", "pipeline_task": {"pipeline_id": pipeline_id}, "max_retries": 1},
        nb("dq_gate", "dq_gate", ["run_pipeline"]),
        {"task_key": "dq_ok", "depends_on": [{"task_key": "dq_gate"}],
         "condition_task": {"op": "LESS_THAN", "left": "{{tasks.dq_gate.values.dq_drop_pct}}",
                            "right": "{{job.parameters.dq_max_drop_pct}}"}},
        nb("publish_daily_summary", "publish_daily_summary", ["dq_ok"], outcome="true"),
        nb("notify_dq_owner", "notify", ["dq_ok"], outcome="false",
           params={"reason": "dq_gate_failed", "detail": "dq_drop_pct={{tasks.dq_gate.values.dq_drop_pct}}%", **ids}),
        {"task_key": "reconcile_servers", "depends_on": [{"task_key": "run_pipeline"}],
         "for_each_task": {"inputs": "{{job.parameters.servers}}", "concurrency": 4,
                           "task": nb("reconcile_server", "reconcile_server", params={"server_id": "{{input}}"})}},
        nb("alert_on_failure", "notify", ["run_pipeline", "dq_gate", "reconcile_servers", "publish_daily_summary"],
           params={"reason": "job_task_failed", **ids}, run_if="AT_LEAST_ONE_FAILED"),
        nb("write_audit_row", "audit", ["alert_on_failure", "notify_dq_owner", "publish_daily_summary", "reconcile_servers"],
           params=ids, run_if="ALL_DONE"),
    ],
}

# 创建或更新作业 (Create or update job)
existing = [j for j in w.jobs.list(name=settings["name"])]
if existing:
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": existing[0].job_id, "new_settings": settings})
    job_id = existing[0].job_id
    print("updated job", job_id)
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    print("created job", job_id)

# 显示作业链接 (Display job link)
displayHTML(f'<a href="/jobs/{job_id}" target="_blank">Open daily_trading_reporting_{name}</a>')
