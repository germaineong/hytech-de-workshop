# Databricks notebook source
# MAGIC %md
# MAGIC # 10b · Genie Agent 补课 (Genie Agent catch-up)
# MAGIC
# MAGIC 按实验 10 的设计创建（或更新）参考 Genie Agent `MT5 交易分析助手 · <你的名字> · 参考 (reference)`（它有自己的标题，不会改动你手动配置的 Agent）：表、说明、中文同义词、一个关联、一个度量、一个筛选、一个示例 SQL、常见问题和一个基准问题。然后用 Conversation API 问一个问题来验证。Genie 使用 serverless 仓库（它不能在 Real-Time 仓库上运行）。
# MAGIC
# MAGIC Creates (or updates) the reference Genie Agent `MT5 交易分析助手 · <your_name> · 参考 (reference)` (it has its own title, so it never changes the agent you configure by hand) exactly as lab 10 describes: tables, instructions, Chinese synonyms, one join, one measure, one filter, one example SQL, common questions and one benchmark. Then asks one question through the Conversation API to check it works. Genie uses a serverless warehouse (it can't run on a Real-Time warehouse).

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("source_schema", "", "留空 = 你自己的 schema；管道坏了可填 solutions (empty = your own schema; solutions if your pipeline is broken)")

# COMMAND ----------

# MAGIC %run ./genie_spec

# COMMAND ----------

import json
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
name = "".join(ch if ch.isalnum() else "_" for ch in me.split("@")[0].lower()).strip("_")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("source_schema").strip() or f"u_{name}"
title = GENIE_TITLE.format(name=name)

whs = w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
serverless = sorted((x for x in whs if x.get("enable_serverless_compute") and x.get("warehouse_type") != "REALTIME"),
                    key=lambda x: (x["name"] != "hytech_workshop_sql", x.get("state") != "RUNNING", x["name"]))
if not serverless:
    raise ValueError("没有可用的 serverless SQL 仓库：请管理员授予 CAN USE (no serverless SQL warehouse you can use: ask the admin for CAN USE)")
warehouse_id = serverless[0]["id"]
print("source:", f"{catalog}.{schema}", "| warehouse:", serverless[0]["name"])

# COMMAND ----------


def find_space(title):
    """按标题找到你的 Genie Agent（列表每页约 20 个）(find your Genie Agent by title; the list pages ~20 at a time)."""
    token = None
    while True:
        page = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_token": token} if token else None)
        for s in page.get("spaces", []):
            if s.get("title") == title:
                return s["space_id"]
        token = page.get("next_page_token")
        if not token:
            return None


body = {"title": title, "description": GENIE_DESCRIPTION, "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(genie_space(catalog, schema), ensure_ascii=False)}
space_id = find_space(title)
if space_id:
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print("✅ updated", space_id)
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**body, "parent_path": f"/Workspace/Users/{me}"})["space_id"]
    print("✅ created", space_id)
url = f"{w.config.host.rstrip('/')}/genie/rooms/{space_id}"
displayHTML(f'<a href="{url}" target="_blank">{title}</a>')

# COMMAND ----------

# 验证：问一个问题（Conversation API = chat 模式）(check: ask one question — the Conversation API is chat mode)
question = SAMPLE_QUESTIONS[0]
started = w.api_client.do("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", body={"content": question})
conversation_id, message_id = started["conversation_id"], started["message_id"]
deadline = time.time() + 300
while True:
    msg = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}")
    if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED") or time.time() > deadline:
        break
    time.sleep(5)
queries = [a["query"] for a in msg.get("attachments") or [] if a.get("query")]
print("status:", msg.get("status"))
print("question:", question)
for q in queries:
    print("SQL:", q.get("query", "")[:400])
print(f"{w.config.host.rstrip('/')}/genie/rooms/{space_id}/chats/{conversation_id}")
assert msg.get("status") == "COMPLETED" and queries, \
    f"Genie 没有用查询回答：打开上面的链接查看 (Genie did not answer with a query: open the link above) -> {msg.get('status')} {msg.get('error')}"
