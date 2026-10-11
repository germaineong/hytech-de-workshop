# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · 你自己的实时数据 (Your own live data)
# MAGIC
# MAGIC 这个笔记本运行工作坊的数据生成器，向**你自己的**落地区 `/Volumes/<catalog>/u_<你的名字>/landing` 写入新的 DMS 文件：每个 MT5 服务器的每个表一个 CDC 文件（新成交、平仓、持仓标记、入金/出金、客户更新），再加一个应用事件 JSON 文件。只有你的管道会读到它们，其他学员不受影响。先运行 `00_Start_Here` 的第 1b 步。
# MAGIC
# MAGIC This notebook runs the workshop's data generator and writes new DMS files into **your own** landing zone `/Volumes/<catalog>/u_<your_name>/landing`: one CDC file per MT5 table per server (new trades, closes, position marks, deposits/withdrawals, client updates), plus one app-events JSON file. Only your pipeline reads them; other participants are not affected. Run step 1b of `00_Start_Here` first.
# MAGIC
# MAGIC | 什么时候 (When) | 设置 (Widgets) |
# MAGIC |---|---|
# MAGIC | 实验 02：再次运行 Auto Loader 之前<br>Lab 02: before you run Auto Loader again | `ticks` = 1 |
# MAGIC | 实验 03：再次运行管道之前<br>Lab 03: before you run the pipeline again | `ticks` = 5 |
# MAGIC | 实验 04 §4：文件到达触发器<br>Lab 04 §4: the file-arrival trigger | `ticks` = 1 |
# MAGIC | 实验 04 §6：上线新服务器（在 03d 之后）<br>Lab 04 §6: onboard a new server (after 03d) | `new_server` = `mt5-hk-01` |
# MAGIC | 实验 04 §6：坏批次 → DQ 闸门走 false 分支<br>Lab 04 §6: a bad batch → the DQ gate takes the false branch | `bad_batch_pct` = 60 |
# MAGIC
# MAGIC **注意 (Notes)**
# MAGIC - 一次只运行一个：同时运行两个会互相覆盖生成器的状态，成交编号会重复。<br>Run one at a time: two runs at once overwrite each other's generator state and reuse deal numbers.
# MAGIC - 第一次运行写得比较多：它会把共享历史结束以后到期的持仓全部平仓。<br>The first run writes more: it closes every position that came due after the shared history ends.
# MAGIC - 完成 03d **之后**才上线 `mt5-hk-01`：03d 预期你的落地区里还没有这台服务器。<br>Onboard `mt5-hk-01` only **after** 03d: 03d expects your landing zone to have no files from that server yet.
# MAGIC - 写入坏批次之后，马上运行一次你的作业（或管道），让坏行在 04c 之前被处理。<br>After a bad batch, run your job (or pipeline) once straight away, so the bad rows are processed before 04c.
# MAGIC - 每次运行写完就结束（每批几秒钟）；需要更多数据时再运行一次，或把 `ticks` 调大（最多 20）。<br>Each run writes and finishes (a few seconds per batch); run it again when you need more data, or raise `ticks` (up to 20).

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("ticks", "1")
dbutils.widgets.text("new_server", "")
dbutils.widgets.text("bad_batch_pct", "0")

# COMMAND ----------

# MAGIC %run ./live_feed

# COMMAND ----------

import json
import os
import sys
import time

catalog = dbutils.widgets.get("catalog")
me = spark.sql("SELECT current_user()").first()[0]
roots = personal_roots(catalog, me)

# 数据生成器在仓库的 src/ 里；在你自己的副本中，实验 00 第 3 步把它复制到了 ./src
# The data generator lives in the repo's src/; in your own copy, step 3 of lab 00 copied it to ./src
for candidate in (os.path.join(os.getcwd(), "src"), os.path.join(os.getcwd(), "..", "src")):
    if os.path.isdir(os.path.join(candidate, "hytech_workshop")):
        sys.path.insert(0, os.path.abspath(candidate))
        break
else:
    dbutils.notebook.exit("❌ 找不到数据生成器 src/hytech_workshop：请重新运行 00_Start_Here 的第 3 步。"
                          " Can't find the data generator src/hytech_workshop: re-run step 3 of 00_Start_Here.")

from hytech_workshop.config import SERVER_META, WorkshopConfig  # noqa: E402

if not os.path.exists(f"{roots['producer']}/state.json") or not os.path.isdir(f"{roots['landing']}/mt5"):
    dbutils.notebook.exit(f"❌ 还没有你自己的落地区（{roots['volume_root']}）：请先运行 00_Start_Here 的第 1b 步。"
                          " Your own landing zone isn't there yet: run step 1b of 00_Start_Here first.")

with open(f"{roots['producer']}/state.json", encoding="utf-8") as fh:
    onboarded = sorted(json.load(fh)["servers"])
new_server = dbutils.widgets.get("new_server").strip()
if new_server in onboarded:
    print(f"ℹ️ {new_server} 已经上线，这次只写入普通的 CDC 文件"
          f" ({new_server} is already onboarded: this run writes ordinary CDC files only)")
    new_server = ""
problems = check_widgets(dbutils.widgets.get("ticks"), new_server, dbutils.widgets.get("bad_batch_pct"), SERVER_META)
if problems:
    dbutils.notebook.exit("❌ " + " · ".join(problems))
print(f"你的落地区 (your landing zone): {roots['landing']}")
print(f"已上线的服务器 (servers onboarded): {', '.join(onboarded)}")

# COMMAND ----------

ticks = int(dbutils.widgets.get("ticks"))
bad_pct = float(dbutils.widgets.get("bad_batch_pct")) / 100.0

# 同一个生成器，只是 volume_root 指向你的 schema：landing_root、producer_root 和 mt5_dir() 都随之指向你的 volume
# The same generator, with volume_root pointing at your schema: landing_root, producer_root and mt5_dir() follow it into your volumes
cfg = WorkshopConfig(catalog=catalog, volume_root=roots["volume_root"])
started = time.time() - 2   # 留出 2 秒的时钟误差 (allow 2 seconds of clock skew)
results = run_ticks(cfg, ticks, new_server=new_server, bad_pct=bad_pct)
summary = summarize(roots["landing"], files_since(roots["landing"], started))
display(spark.createDataFrame(summary, "table string, server string, file string, row_count long"))

# COMMAND ----------

servers = results[-1]["servers"]
missing = missing_files(summary, servers, new_server)
if missing:
    print("❌ 没有看到这些新文件 (these new files did not appear):", ", ".join(missing))
else:
    print(f"✅ {ticks} 批新文件已写入你的落地区，服务器：{', '.join(servers)}"
          f" ({ticks} batch(es) of new files written to your landing zone; servers: {', '.join(servers)})")
    print("   下一步：回到你的实验，再次运行 Auto Loader 单元格、管道或作业。"
          " Next: go back to your lab and run the Auto Loader cell, the pipeline or the job again.")
