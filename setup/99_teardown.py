# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · 清理 (Teardown)
# MAGIC
# MAGIC 删除工作坊 catalog（所有 schema、表、volume 和文件）、Genie Code 技能和成本仪表盘，以及 `00_master_setup` 创建的作业和管道。由 bundle 部署的作业和管道请用 `databricks bundle destroy` 删除；学员自己创建的管道和作业需要在 UI 中删除（或由其所有者删除）。
# MAGIC
# MAGIC Drops the workshop catalog (all schemas, tables, volumes and files), the Genie Code skill, the cost
# MAGIC dashboard, and the jobs and pipeline created by `00_master_setup`.
# MAGIC Jobs and pipelines deployed by the bundle are removed with `databricks bundle destroy`;
# MAGIC participant-created pipelines and jobs must be deleted in the UI (or by their owners).

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("confirm", "", "Type the catalog name to confirm")

catalog = dbutils.widgets.get("catalog")
if dbutils.widgets.get("confirm") != catalog:
    dbutils.notebook.exit(f"Not confirmed — type '{catalog}' in the confirm widget to drop it.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# 删除 00_master_setup 创建的作业和管道（按标签和配置识别，不会碰 bundle 部署的对象）
# Delete the jobs and pipeline created by 00_master_setup (found by tag / configuration, never bundle-deployed ones)
for name in ("hytech_ws_setup", "hytech_ws_drip_producer", "hytech_daily_trading_reporting_solution"):
    for job in w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": name}).get("jobs", []):
        settings = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": job["job_id"]})["settings"]
        if (settings.get("tags") or {}).get("managed_by") == "master_setup":
            w.api_client.do("POST", "/api/2.2/jobs/delete", body={"job_id": job["job_id"]})
            print("deleted job", name, job["job_id"])
listed = w.api_client.do("GET", "/api/2.0/pipelines", query={"filter": "name LIKE 'hytech_trade_lakehouse_solution'"})
for p in listed.get("statuses", []):
    spec = w.api_client.do("GET", f"/api/2.0/pipelines/{p['pipeline_id']}").get("spec", {})
    if (spec.get("configuration") or {}).get("hytech.managed_by") == "master_setup":
        w.api_client.do("DELETE", f"/api/2.0/pipelines/{p['pipeline_id']}")
        print("deleted pipeline", p["name"], p["pipeline_id"])

# 删除 08 创建的两个 SQL 仓库（按名称和标签识别）(delete the two SQL warehouses 08 created, found by name and tag)
for wh in w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", []):
    tags = {t.get("key"): t.get("value") for t in (wh.get("tags") or {}).get("custom_tags", [])}
    if wh["name"] in ("hytech_workshop_sql", "hytech_workshop_rt") and tags.get("managed_by") == "master_setup":
        w.api_client.do("DELETE", f"/api/2.0/sql/warehouses/{wh['id']}")
        print("deleted warehouse", wh["name"], wh["id"])

spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
print("dropped catalog", catalog)

for label, path in [("Genie Code skill", "/Workspace/.assistant/skills/hytech-de-conventions"),
                    ("cost dashboard folder", f"/Workspace/Users/{w.current_user.me().user_name}/hytech_de_workshop")]:
    try:
        w.workspace.delete(path, recursive=True)
        print("removed", label)
    except Exception as e:  # noqa: BLE001
        print(label, "not removed:", str(e).splitlines()[0][:200])
