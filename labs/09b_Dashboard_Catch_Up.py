# Databricks notebook source
# MAGIC %md
# MAGIC # 09b · 仪表盘补课 (Dashboard catch-up)
# MAGIC
# MAGIC 按实验 09 的设计创建（或更新）参考仪表盘 `MT5 交易概览 · <你的名字> · 参考 (reference)`，读取你自己的 gold 表；如果有 Lakehouse RT 仓库，就用它作为计算资源，否则用 serverless 仓库。落后了，或想对比你自己搭建的仪表盘时运行：它有自己的标题，不会改动你手动搭建的仪表盘。
# MAGIC
# MAGIC Creates (or updates) the reference dashboard `MT5 交易概览 · <your_name> · 参考 (reference)` exactly as lab 09 describes, reading your own gold tables, on the Lakehouse RT warehouse if there is one and a serverless warehouse otherwise. Use it if you fell behind, or to compare with the dashboard you built: it has its own title, so it never changes the dashboard you built by hand.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("source_schema", "", "留空 = 你自己的 schema；管道坏了可填 solutions (empty = your own schema; solutions if your pipeline is broken)")

# COMMAND ----------

# MAGIC %run ./dashboard_spec

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
name = "".join(ch if ch.isalnum() else "_" for ch in me.split("@")[0].lower()).strip("_")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("source_schema").strip() or f"u_{name}"
title = DASHBOARD_TITLE.format(name=name)
parent = f"/Workspace/Users/{me}"

# 有 Real-Time 仓库就用它，否则用 serverless 仓库 (use a Real-Time warehouse if there is one, else a serverless one)
whs = w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
realtime = sorted((x for x in whs if x.get("warehouse_type") == "REALTIME"), key=lambda x: x["name"] != "hytech_workshop_rt")
serverless = sorted((x for x in whs if x.get("enable_serverless_compute") and x.get("warehouse_type") != "REALTIME"),
                    key=lambda x: (x["name"] != "hytech_workshop_sql", x.get("state") != "RUNNING", x["name"]))
warehouse = (realtime or serverless or [None])[0]
if warehouse is None:
    raise ValueError("没有可用的 SQL 仓库：请管理员授予 CAN USE (no SQL warehouse you can use: ask the admin for CAN USE)")
print("source:", f"{catalog}.{schema}", "| warehouse:", warehouse["name"], warehouse.get("warehouse_type"))

# COMMAND ----------

body = {"display_name": title, "warehouse_id": warehouse["id"],
        "serialized_dashboard": json.dumps(trading_overview(catalog, schema), ensure_ascii=False)}
try:
    dashboard_id = w.api_client.do("GET", "/api/2.0/workspace/get-status",
                                   query={"path": f"{parent}/{title}.lvdash.json"})["resource_id"]
except Exception:  # noqa: BLE001 - first run: the dashboard does not exist yet
    dashboard_id = None
if dashboard_id:
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)
    print("✅ updated", dashboard_id)
else:
    dashboard_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={**body, "parent_path": parent})["dashboard_id"]
    print("✅ created", dashboard_id)
w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                body={"embed_credentials": True, "warehouse_id": warehouse["id"]})
url = f"{w.config.host.rstrip('/')}/dashboardsv3/{dashboard_id}/published"
print("✅ published:", url)
displayHTML(f'<a href="{url}" target="_blank">{title}</a>')
