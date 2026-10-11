# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · 系统表治理视图 (Governed views over system tables)
# MAGIC
# MAGIC 学员不应需要直接访问账户范围的系统表。这些视图（由运行此笔记本的管理员拥有）**仅暴露此工作区**并删除敏感列，如 SQL 文本；学员在 `ops` schema 上获得 `SELECT` 权限（在 `01` 中授予）。视图使用所有者的权限运行。
# MAGIC
# MAGIC Participants should not need direct access to account-wide system tables. These views (owned by the
# MAGIC admin who runs this notebook) expose **only this workspace** and drop sensitive columns such as SQL text;
# MAGIC participants get `SELECT` on the `ops` schema (granted in `01`). Views run with the owner's rights.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
catalog = dbutils.widgets.get("catalog")

from databricks.sdk import WorkspaceClient

ws_id = str(WorkspaceClient().get_workspace_id())
print("workspace_id =", ws_id)

# COMMAND ----------

ws = f"workspace_id = '{ws_id}'"
views = {
    "billing_usage": ("Billable usage (DBUs) for this workspace", f"SELECT * FROM system.billing.usage WHERE {ws}"),
    "list_prices": ("List prices per SKU (USD)", "SELECT * FROM system.billing.list_prices"),
    "jobs": ("Jobs in this workspace (latest definition per job)", f"SELECT * FROM system.lakeflow.jobs WHERE {ws}"),
    "job_run_timeline": ("Job run timeline", f"SELECT * FROM system.lakeflow.job_run_timeline WHERE {ws}"),
    "job_task_run_timeline": ("Job task run timeline", f"SELECT * FROM system.lakeflow.job_task_run_timeline WHERE {ws}"),
    "pipelines": ("Pipelines in this workspace", f"SELECT * FROM system.lakeflow.pipelines WHERE {ws}"),
    "pipeline_update_timeline": ("Pipeline update timeline", f"SELECT * FROM system.lakeflow.pipeline_update_timeline WHERE {ws}"),
    "table_lineage": (
        f"Table lineage touching the {catalog} catalog",
        f"SELECT * FROM system.access.table_lineage WHERE {ws} "
        f"AND (source_table_catalog = '{catalog}' OR target_table_catalog = '{catalog}')",
    ),
    "query_history": (
        "SQL query history (no SQL text; identities of other users masked)",
        "SELECT statement_id, CASE WHEN executed_by = current_user() THEN executed_by ELSE '***' END AS executed_by, "
        "compute, execution_status, statement_type, client_application, start_time, end_time, total_duration_ms, "
        "read_rows, read_bytes, produced_rows, written_bytes "
        f"FROM system.query.history WHERE {ws}",
    ),
}
created = []
for name, (comment, body) in views.items():
    try:
        safe_comment = comment.replace("'", "")
        spark.sql(f"CREATE OR REPLACE VIEW {catalog}.ops.{name} COMMENT '{safe_comment}' AS {body}")
        spark.sql(f"SELECT * FROM {catalog}.ops.{name} LIMIT 1").collect()
        created.append((name, "ok"))
    except Exception as e:  # noqa: BLE001 - some system schemas may not be enabled
        created.append((name, str(e).splitlines()[0][:200]))
display(spark.createDataFrame(created, "view string, status string"))
