# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · 学员 Schema (Participant schemas)
# MAGIC
# MAGIC 为每个学员创建 `<catalog>.u_<name>` 和一个 `checkpoints` volume，并让学员成为**所有者**：他们可以在自己的 schema 中创建管道、表、列掩码和行过滤器，无需额外授权。将 `participants` 留空即可跳过（学员可以在 `labs/00_Start_Here` 中自行创建，如果他们在 catalog 上有 `CREATE SCHEMA` 权限）。
# MAGIC
# MAGIC Creates `<catalog>.u_<name>` plus a `checkpoints` volume for every participant and makes the participant
# MAGIC the **owner**: they can then create pipelines, tables, masks and row filters in their own schema without
# MAGIC extra grants. Leave `participants` empty to skip (participants can self-create in `labs/00_Start_Here`
# MAGIC if they have `CREATE SCHEMA` on the catalog).

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("participants", "", "Emails (comma or newline separated)")

catalog = dbutils.widgets.get("catalog")
emails = [e.strip() for e in dbutils.widgets.get("participants").replace("\n", ",").split(",") if e.strip()]

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from hytech_workshop.config import participant_schema  # noqa: E402

rows = []
for email in emails:
    schema = participant_schema(email)
    status = "ok"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Workshop schema for {email}'")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints COMMENT 'Auto Loader checkpoints (lab 02)'")
        spark.sql(f"ALTER VOLUME {catalog}.{schema}.checkpoints OWNER TO `{email}`")
        spark.sql(f"ALTER SCHEMA {catalog}.{schema} OWNER TO `{email}`")
    except Exception as e:  # noqa: BLE001
        status = str(e).splitlines()[0][:200]
    rows.append((email, f"{catalog}.{schema}", status))
    print(email, "->", f"{catalog}.{schema}", status)

if rows:
    display(spark.createDataFrame(rows, "participant string, schema string, status string"))
else:
    print("No participants given — skipped.")
