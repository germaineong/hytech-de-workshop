# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · 实时数据滴灌 (Drip producer) — 可选，只供讲师的测试工作区 (optional, an instructor's own test workspace)
# MAGIC
# MAGIC 在实验期间保持数据接入区"活跃"：每 `interval_seconds`，为每个 MT5 服务器的每个表写入一个 DMS 风格的变更数据 (CDC) 文件（新成交、平仓、头寸标记、入金/出金、客户更新）和一个应用事件 JSON 文件。
# MAGIC
# MAGIC Keeps the landing zone "live" during labs: every `interval_seconds` it writes one DMS-style CDC file per
# MAGIC MT5 table per server (new trades, closes, position marks, deposits/withdrawals, client updates) and one
# MAGIC app-events JSON file.
# MAGIC
# MAGIC **不要在 Hytech 的课堂上运行。** 每位学员用 `labs/00b_Live_Data` 向自己的落地区写入新数据；这个作业只写共享落地区（只有参考答案管道读取）。学员重新运行实验 00 时，会把它写的文件复制进自己的落地区，成交编号会和他们自己的 tick 重复。
# MAGIC
# MAGIC **Don't run it in the Hytech class.** Each participant writes new data into their own landing zone with `labs/00b_Live_Data`; this job writes only to the shared landing zone (which only the solution pipeline reads). A participant who re-runs lab 00 would copy its files into their own landing zone, and its deal numbers would repeat their own ticks'.
# MAGIC
# MAGIC | 参数 (Parameter) | 在讲师的测试工作区中 (On an instructor's test workspace) |
# MAGIC |---|---|
# MAGIC | `duration_minutes` / `interval_seconds` | 让共享落地区持续有新文件（例如 60 分钟，每 30 秒）<br>Keep new files arriving in the shared landing zone (for example 60 minutes, every 30 s) |
# MAGIC | `new_server=mt5-hk-01` | 上线新的 MT5 服务器（DMS 全量，然后 CDC）<br>Onboard a new MT5 server (DMS full load, then CDC) |
# MAGIC | `bad_batch_pct=30` | 30% 的新成交无效 → 期望将其删除 → 作业走 `false` 分支<br>30% of new trades are invalid → expectations drop them → the job takes the `false` branch |

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("duration_minutes", "60")
dbutils.widgets.text("interval_seconds", "30")
dbutils.widgets.text("servers", "")
dbutils.widgets.text("new_server", "")
dbutils.widgets.text("bad_batch_pct", "0")
dbutils.widgets.text("deals_per_tick", "24")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from hytech_workshop.config import WorkshopConfig  # noqa: E402
from hytech_workshop.drip import run_drip  # noqa: E402

cfg = WorkshopConfig(catalog=dbutils.widgets.get("catalog"))
servers = [s.strip() for s in dbutils.widgets.get("servers").split(",") if s.strip()] or None
result = run_drip(
    cfg,
    duration_minutes=float(dbutils.widgets.get("duration_minutes")),
    interval_seconds=float(dbutils.widgets.get("interval_seconds")),
    servers=servers,
    new_server=dbutils.widgets.get("new_server").strip() or None,
    bad_batch_pct=float(dbutils.widgets.get("bad_batch_pct")) / 100.0,
    deals_per_tick=int(dbutils.widgets.get("deals_per_tick")),
    log=print,
)
print(result)
