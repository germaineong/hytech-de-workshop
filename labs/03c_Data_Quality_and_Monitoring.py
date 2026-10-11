# Databricks notebook source
# MAGIC %md
# MAGIC # 03c · 数据质量、依赖与监控 (Data quality, dependencies and monitoring) · LAB
# MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/03c_Data_Quality_and_Monitoring` 复制原始版本。
# MAGIC
# MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/03c_Data_Quality_and_Monitoring`.
# MAGIC
# MAGIC **场景 (Scenario).** Hytech 正在上线新的 MT5 服务器 `mt5-hk-01`。它的 DMS 文件先进入你自己 schema 中的**预发布通道**，通过规则之前不进入生产。你将：
# MAGIC
# MAGIC 1. 把预发布通道加入你的管道（4 张新表）
# MAGIC 2. 看到三种规则行为：WARN（保留并计数）、DROP（丢弃并隔离）、FAIL（整个更新失败）
# MAGIC 3. 从管道图、事件日志和血缘读懂上游失败对下游的影响
# MAGIC 4. 用两种方法恢复：修改规则（策略修复）或修改数据（源头修复）
# MAGIC 5. 用事件日志监控管道，并打开失败邮件通知
# MAGIC
# MAGIC Hytech is onboarding a new MT5 server, `mt5-hk-01`. Its DMS files land first in a **staging lane** in your own schema, and stay out of production until they pass the rules. You will:
# MAGIC
# MAGIC 1. Add the staging lane to your pipeline (4 new tables)
# MAGIC 2. See the three rule behaviours: WARN (keep and count), DROP (drop and quarantine), FAIL (the whole update fails)
# MAGIC 3. Read the impact of an upstream failure downstream, from the pipeline graph, the event log and lineage
# MAGIC 4. Recover in two ways: change the rule (policy fix) or change the data (source fix)
# MAGIC 5. Monitor the pipeline with the event log, and switch on failure emails
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/u_<you>/staging/mt5/mt5-hk-01/mt5_deals/*.parquet
# MAGIC   └─► bronze_staging_mt5_deals ─┬─► silver_staging_mt5_deals (WARN · DROP · FAIL) ─► gold_staging_daily_volume
# MAGIC                                 └─► silver_staging_mt5_deals_quarantine (违反规则的行 / rows that break a rule)
# MAGIC ```
# MAGIC
# MAGIC 前提：实验 03 的管道已成功运行，并把事件日志发布为 `pipeline_event_log`（README 第 4 步）。<br>
# MAGIC Before you start: your lab 03 pipeline ran green and publishes its event log as `pipeline_event_log` (README step 4).
# MAGIC
# MAGIC ⚠️ 第 4 步会让你的管道失败，直到第 6 步才恢复。第 2 天的实验 04 需要一个绿色的管道：请至少完成第 6 步。<br>
# MAGIC ⚠️ Step 4 makes your pipeline fail until step 6 fixes it. Day 2's lab 04 needs a green pipeline: finish at least step 6.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.dropdown("auto", "false", ["false", "true"], "自动完成手动步骤 (auto: do the hand steps for me)")
dbutils.widgets.dropdown("start_over", "false", ["false", "true"], "从头再来：清空预发布通道 (start over: empty the staging lane)")

# COMMAND ----------

# MAGIC %run ./staging_feed

# COMMAND ----------

from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
catalog = dbutils.widgets.get("catalog")
AUTO = dbutils.widgets.get("auto") == "true"
START_OVER = dbutils.widgets.get("start_over") == "true"
me = spark.sql("SELECT current_user()").first()[0]
names = my_names(me)
my_schema = names["schema"]
cs = f"{catalog}.{my_schema}"
spark.sql(f"USE {cs}")
checks = {}   # 检查 → 是否通过；重新运行一个单元格会覆盖它的结果 (check → passed; re-running a cell replaces its results)
last_request = None   # 最近一次请求运行管道的时间 (when a pipeline run was last requested)


def check(label, ok, detail=""):
    """打印 ✅ / ❌ 并记录结果 (print ✅ / ❌ and record the result)."""
    print(("✅ " if ok else "❌ ") + label + (f" — {detail}" if detail else ""))
    checks[label] = bool(ok)
    return ok


def pipeline_run(full_refresh_selection=None):
    """auto=true 时代你运行管道；否则提醒你在管道编辑器中运行。
    With auto=true, runs the pipeline for you; otherwise reminds you to run it in the pipeline editor."""
    global last_request
    last_request = datetime.now(timezone.utc)
    if AUTO:
        return run_pipeline(w, find_pipeline_id(w, names["pipeline"]), full_refresh_selection)
    what = "Full refresh: " + ", ".join(full_refresh_selection) if full_refresh_selection else "Run pipeline"
    print(f"👉 现在在管道编辑器中运行（{what}），结束后再运行下一个单元格。")
    print(f"👉 Now run it in the Lakeflow Pipelines Editor ({what}), then run the next cell when it has finished.")


tables = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {cs}").collect()}
if not {"bronze_mt5_deals", "pipeline_event_log"} <= tables:
    raise RuntimeError("先完成实验 03：运行你的管道，并按 README 第 4 步发布事件日志 pipeline_event_log。"
                       " (Finish lab 03 first: run your pipeline and publish the event log as pipeline_event_log, README step 4.)")
print("schema:", cs, "| pipeline:", names["pipeline"], "| staging_root:", staging_root(catalog, my_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · 建立预发布通道 (Set up the staging lane)
# MAGIC 创建 volume `staging`，然后写入新服务器的第一个 DMS 文件。和真实的第一批数据一样，它夹着几行问题数据：3 笔 Volume = 0、2 笔缺少品种、1 笔未来日期。
# MAGIC
# MAGIC Create the volume `staging`, then write the new server's first DMS file. Like a real first batch, it carries a few problem rows: 3 zero-volume trades, 2 trades without a symbol and 1 future-dated trade.
# MAGIC
# MAGIC 做到一半想从头再来？把顶部的 `start_over` 设为 `true`：这个单元格会清空预发布通道，第 3 步会对预发布的四张表做完全刷新。<br>
# MAGIC Stopped halfway and want to start again? Set `start_over` at the top to `true`: this cell empties the staging lane, and step 3 fully refreshes the four staging tables.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {cs}.staging COMMENT 'Labs 03c/04c staging lane: DMS files of the new server mt5-hk-01'")
if START_OVER:
    try:
        old_files = dbutils.fs.ls(staging_dir(catalog, my_schema))
    except Exception:  # noqa: BLE001 - the folder does not exist yet
        old_files = []
    for f in old_files:
        dbutils.fs.rm(f.path)
    print("start over: removed", len(old_files), "staging file(s)")
# 要点 1 (Key point 1) · 文件写进你自己的 volume，而不是共享的落地区 (the file goes into your own volume, not the shared landing zone)
first_batch = write_batch(spark, catalog, my_schema, "junk")
display(dbutils.fs.ls(staging_dir(catalog, my_schema)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 把预发布通道加入你的管道 (Add the staging lane to your pipeline)
# MAGIC
# MAGIC 1. 打开你的管道 `trade_lakehouse_<你的名字>` → **Settings → Configuration** → 添加键 `staging_root`，值为上面打印的 staging_root。
# MAGIC 2. 在管道编辑器左侧的文件列表中，把 `03_pipeline/staging/06_staging_mt5_hk01.sql` 移到 `03_pipeline/transformations/`（拖动，或右键 → Move）。
# MAGIC 3. 打开这个文件，读一读其中的要点注释：WARN、DROP、FAIL 三种规则，以及隔离表。
# MAGIC
# MAGIC 1. Open your pipeline `trade_lakehouse_<your_name>` → **Settings → Configuration** → add the key `staging_root` with the staging_root value printed above.
# MAGIC 2. In the file list on the left of the pipeline editor, move `03_pipeline/staging/06_staging_mt5_hk01.sql` into `03_pipeline/transformations/` (drag it, or right-click → Move).
# MAGIC 3. Open the file and read its key-point comments: the WARN, DROP and FAIL rules, and the quarantine table.
# MAGIC
# MAGIC 不想手动操作？把顶部的 `auto` 设为 `true`，再运行下一个单元格。<br>
# MAGIC Rather not do it by hand? Set `auto` at the top to `true` and run the next cell.

# COMMAND ----------

if AUTO:
    # API 启动的更新默认会自动重试；这里关闭重试，让失败立即可见（实验 04c 会讲）
    # API-started updates retry automatically by default; turn that off so failures show at once (lab 04c explains)
    wire_staging_lane(w, catalog, me, no_retries=True)
    set_price_rule(w, me, "FAIL")
else:
    print("👉 完成上面的第 1–3 步后，运行下一个单元格检查 (do steps 1–3 above, then run the next cell to check them)")

# COMMAND ----------

# 检查第 2 步；有 ❌ 就按提示修改，再运行这个单元格 (check step 2; for any ❌, fix it as shown and run this cell again)
lane = lane_status(w, catalog, me)
check("管道配置 staging_root 正确 (the pipeline setting staging_root is right)", lane["staging_root"],
      "" if lane["staging_root"] else f"Settings → Configuration: staging_root = {staging_root(catalog, my_schema)}")
check("06_staging_mt5_hk01.sql 在 transformations 中 (the file is in transformations)", lane["file"],
      "" if lane["file"] else "把它从 03_pipeline/staging 移到 03_pipeline/transformations (move it from 03_pipeline/staging to 03_pipeline/transformations)")
check("valid_trade_price 是 FAIL UPDATE (valid_trade_price is FAIL UPDATE)", lane["price_rule"] == "FAIL",
      "" if lane["price_rule"] == "FAIL" else f"现在是 (now) {lane['price_rule']}：改回 ON VIOLATION FAIL UPDATE 并保存 (change it back to ON VIOLATION FAIL UPDATE and save)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · 第一次运行：WARN 和 DROP (First run: WARN and DROP)
# MAGIC 运行管道，图中会出现 4 张新表。打开 `silver_staging_mt5_deals` 的 **Data quality** 选项卡：哪些规则丢弃了行？哪一条只是警告？
# MAGIC
# MAGIC Run the pipeline: four new tables appear in the graph. Open the **Data quality** tab of `silver_staging_mt5_deals`: which rules dropped rows, and which one only warned?

# COMMAND ----------

# start_over 且预发布表已存在时，先完全刷新它们 (with start_over and existing staging tables, fully refresh them first)
pipeline_run(full_refresh_selection=list(STAGING_TABLES) if START_OVER and set(STAGING_TABLES) <= tables else None)

# COMMAND ----------

wait_for_latest_update(spark, cs, after=last_request)
c = batch_counts(spark, cs, first_batch)
print(c)
check("bronze 收到了整个文件 (bronze got the whole file)", c["bronze"] == c["file"], str(c["bronze"]))
check("silver 丢弃了 5 行 (silver dropped 5 rows)", c["silver"] == c["file"] - 5, str(c["silver"]))
check("隔离表保存了这 5 行 (the quarantine kept those 5 rows)", c["quarantine"] == 5, str(c["quarantine"]))
future = spark.sql(
    "SELECT count(*) AS n FROM silver_staging_mt5_deals WHERE deal_time > current_timestamp() + INTERVAL 1 HOUR AND source_file LIKE :f",
    args={"f": "%" + first_batch.rsplit("/", 1)[-1]}).first()["n"]
check("未来日期的成交被保留了，只记一次警告 (the future-dated deal was kept: WARN)", future == 1, str(future))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 隔离表：每一行都写明了违反的规则 (the quarantine: every row says which rules it broke)
# MAGIC SELECT deal_id, failed_rules, volume_raw, symbol, price, source_file
# MAGIC FROM silver_staging_mt5_deals_quarantine ORDER BY ingested_at DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · 一个坏批次：FAIL (A bad batch: FAIL)
# MAGIC 下一个 DMS 文件中有 5 笔负价格的成交：价格源或复制出了问题。`valid_trade_price` 是 FAIL UPDATE 规则，所以**整个更新会失败**。写入文件，再运行管道。
# MAGIC
# MAGIC The next DMS file has 5 trades with negative prices: the price feed or the replication is broken. `valid_trade_price` is a FAIL UPDATE rule, so **the whole update fails**. Write the file, then run the pipeline.
# MAGIC
# MAGIC ⚠️ 从这里开始，你的管道每次更新都会失败，直到第 6 步修复它。第 2 天的实验 04 需要一个绿色的管道。<br>
# MAGIC ⚠️ From here on, every update of your pipeline fails until step 6 fixes it. Day 2's lab 04 needs a green pipeline.

# COMMAND ----------

bad_batch = write_batch(spark, catalog, my_schema, "bad_prices")

# COMMAND ----------

pipeline_run()   # 预期：更新失败 (expected: the update fails)

# COMMAND ----------

# 要点 2 (Key point 2) · 事件日志记录了这次更新中每张表的状态 (the event log records the status of every table in this update)
wait_for_latest_update(spark, cs, after=last_request)
flows = latest_update_flows(spark, cs)
display(spark.createDataFrame(sorted(flows.items()), "table string, status string"))
check("silver_staging_mt5_deals 失败了 (FAILED)", flows.get("silver_staging_mt5_deals") == "FAILED", flows.get("silver_staging_mt5_deals"))
check("下游 gold_staging_daily_volume 被跳过 (SKIPPED)", flows.get("gold_staging_daily_volume") == "SKIPPED", flows.get("gold_staging_daily_volume"))
check("同级的隔离表完成了 (the sibling quarantine COMPLETED)",
      flows.get("silver_staging_mt5_deals_quarantine") == "COMPLETED", flows.get("silver_staging_mt5_deals_quarantine"))
others = sorted(t for t, s in flows.items() if s == "FAILED" and t != "silver_staging_mt5_deals")
check("其他表都没有失败 (no other table failed)", not others, ", ".join(others))
c = batch_counts(spark, cs, bad_batch)
check("坏文件进入了 bronze，但没有一行进入 silver (the bad file reached bronze, but no row reached silver)",
      c["bronze"] == c["file"] and c["silver"] == 0, str(c))
check("隔离表里能看到这 5 笔负价格 (the quarantine shows the 5 negative prices)", c["quarantine"] == 5, str(c["quarantine"]))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 要点 3 (Key point 3) · 错误信息写明了是哪条规则、哪一行 (the error names the rule and the record)
# MAGIC SELECT timestamp, origin.flow_name AS flow, error.exceptions[0].class_name AS error_class,
# MAGIC        left(error.exceptions[0].message, 500) AS message
# MAGIC FROM pipeline_event_log
# MAGIC WHERE level = 'ERROR' AND error IS NOT NULL
# MAGIC ORDER BY timestamp DESC LIMIT 5;

# COMMAND ----------

err = spark.sql("SELECT error.exceptions[0].message AS m FROM pipeline_event_log "
                "WHERE level = 'ERROR' AND error IS NOT NULL ORDER BY timestamp DESC LIMIT 1").first()
check("错误信息中有规则名 valid_trade_price (the error names valid_trade_price)", bool(err and err["m"] and "valid_trade_price" in err["m"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · 依赖与血缘 (Dependencies and lineage)
# MAGIC
# MAGIC * **管道图：** 失败的 silver 是红色，下游的 gold 被跳过，其他表照常完成。依赖关系是根据每个查询读取的表**自动推断**的：SQL 管道里不需要写 depends on。作业里的依赖和 Run if 见实验 03d。
# MAGIC * **Catalog Explorer** → `gold_staging_daily_volume` → **Lineage**：上游一直追溯到 volume 中的文件。
# MAGIC * 下面的查询从血缘系统表中找出 `silver_staging_mt5_deals` 和 `silver_mt5_deals` 的全部下游表：如果它们坏了，哪些报表会受影响？血缘数据有几分钟的延迟。
# MAGIC
# MAGIC * **The pipeline graph:** the failed silver table is red, the gold table downstream was skipped, and every other table completed as usual. Dependencies are **inferred** from which tables each query reads: a SQL pipeline needs no depends on. Dependencies and Run if in a job are in lab 03d. (Python pipelines can also order flows with `depends_on`, in Public Preview; 中文：Python 管道也可以用 `depends_on` 为流排序，目前是公开预览版。)
# MAGIC * **Catalog Explorer** → `gold_staging_daily_volume` → **Lineage**: upstream all the way to the files in the volume.
# MAGIC * The query below uses the lineage system table to find every table downstream of `silver_staging_mt5_deals` and `silver_mt5_deals`: if they break, which reports are affected? Lineage arrives a few minutes late.

# COMMAND ----------

# 要点 4 (Key point 4) · 血缘系统表（通过受治理视图 ops.table_lineage）记录了哪些表写入了哪些表 (the lineage system table, through the governed view ops.table_lineage, records which tables feed which)
try:
    edges = spark.sql(f"""
      SELECT DISTINCT source_table_full_name AS src, target_table_full_name AS dst
      FROM {catalog}.ops.table_lineage
      WHERE source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
        AND (source_table_schema = '{my_schema}' OR target_table_schema = '{my_schema}')
    """).collect()
except Exception as e:  # noqa: BLE001 - e.g. the system lineage schema is not enabled in this workspace
    edges = None
    print("⚠️ 血缘视图不可用：请在 Catalog Explorer 中查看血缘 (the lineage view is not available: look at lineage in Catalog Explorer) ->",
          str(e).splitlines()[0][:160])
children = {}
for e in edges or []:
    children.setdefault(e["src"], set()).add(e["dst"])


def downstream(table):
    """沿血缘向下找出所有下游表 (walk lineage down to every downstream table)."""
    seen, todo = [], [table]
    while todo:
        for child in sorted(children.get(todo.pop(0), ())):
            if child not in seen:
                seen.append(child)
                todo.append(child)
    return seen


for start in ("silver_staging_mt5_deals", "silver_mt5_deals") if edges is not None else ():
    down = downstream(f"{cs}.{start}")
    print(f"{start} → {len(down)} downstream:",
          ", ".join(t.split(".")[-1] for t in down) or "(血缘还没到，几分钟后再试 / lineage not there yet: retry in a few minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · 恢复方法一：策略修复 (Recovery 1: the policy fix)
# MAGIC 决定：负价格不应该停掉整个通道，而应该进入隔离表等待复查。在 `transformations/06_staging_mt5_hk01.sql` 中，把 `valid_trade_price` 的 `ON VIOLATION FAIL UPDATE` 改为 `ON VIOLATION DROP ROW`，保存后再运行管道。
# MAGIC
# MAGIC 被卡住的批次会被重新处理：流式表从上次成功的位置继续，所以不会丢数据，也不需要手动回填。
# MAGIC
# MAGIC Decision: negative prices should not stop the whole lane; they should go to the quarantine for review. In `transformations/06_staging_mt5_hk01.sql`, change `valid_trade_price` from `ON VIOLATION FAIL UPDATE` to `ON VIOLATION DROP ROW`, save, and run the pipeline again.
# MAGIC
# MAGIC The stuck batch is processed again: a streaming table continues from where it last succeeded, so no data is lost and nothing needs a manual backfill.

# COMMAND ----------

if AUTO:
    set_price_rule(w, me, "DROP")
pipeline_run()

# COMMAND ----------

wait_for_latest_update(spark, cs, after=last_request)
c = batch_counts(spark, cs, bad_batch)
check("坏批次中的好行进入了 silver (the good rows of the bad batch reached silver)", c["silver"] == c["file"] - 5, str(c))
flows = latest_update_flows(spark, cs)
check("这次更新中所有表都完成了 (every table completed in this update)",
      all(s != "FAILED" for s in flows.values()) and flows.get("gold_staging_daily_volume") == "COMPLETED",
      flows.get("gold_staging_daily_volume"))
dropped = spark.sql("""
  WITH latest AS (
    SELECT origin.update_id AS update_id FROM pipeline_event_log
    WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1)
  SELECT coalesce(sum(CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)), 0) AS n
  FROM pipeline_event_log JOIN latest ON origin.update_id = latest.update_id
  WHERE event_type = 'flow_progress' AND origin.flow_name LIKE '%silver_staging_mt5_deals'""").first()["n"]
check("这次更新丢弃的行数 = 隔离表中这个批次的行数 (rows dropped in this update = this batch's rows in the quarantine)",
      dropped == c["quarantine"], f"{dropped} vs {c['quarantine']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 ·（可选）恢复方法二：源头修复 (Optional — Recovery 2: the source fix)
# MAGIC 另一种决定：价格规则必须保持 FAIL，问题要在源头修复。先把 `valid_trade_price` 改回 `ON VIOLATION FAIL UPDATE` 并保存；再写一个坏批次并运行（失败）。然后 DMS 团队重新发送了正确的数据：删除所有带负价格的文件，并只对预发布的四张表做**完全刷新**，它们会清空并重新读取 volume 中现有的全部文件。
# MAGIC
# MAGIC 完全刷新会重新读取源中的全部数据；只有当源仍保留全部数据时才安全（这里的 volume 保留了每个文件）。
# MAGIC
# MAGIC Another decision: the price rule must stay FAIL and the problem must be fixed at the source. First change `valid_trade_price` back to `ON VIOLATION FAIL UPDATE` and save; then write another bad batch and run (it fails). Then the DMS team re-sends correct data: delete every file with negative prices, and **fully refresh** only the four staging tables, which empties them and re-reads every file still in the volume.
# MAGIC
# MAGIC A full refresh re-reads all the data in the source; it is only safe while the source still holds all of it (this volume keeps every file).

# COMMAND ----------

if AUTO:
    set_price_rule(w, me, "FAIL")
else:
    print("👉 先把 valid_trade_price 改回 ON VIOLATION FAIL UPDATE 并保存 (change valid_trade_price back to FAIL UPDATE and save first)")
second_bad = write_batch(spark, catalog, my_schema, "bad_prices")

# COMMAND ----------

pipeline_run()   # 预期：再次失败 (expected: it fails again)

# COMMAND ----------

wait_for_latest_update(spark, cs, after=last_request)
flows = latest_update_flows(spark, cs)
check("又失败了：规则是 FAIL (it failed again: the rule is FAIL)", flows.get("silver_staging_mt5_deals") == "FAILED", flows.get("silver_staging_mt5_deals"))
# 要点 5 (Key point 5) · 修复源头：删除所有带负价格的文件 (fix the source: delete every file with negative prices)
bad_files = [f.path for f in dbutils.fs.ls(staging_dir(catalog, my_schema))
             if spark.read.parquet(f.path).where("Action <> 2 AND Price <= 0").limit(1).count()]
for path in bad_files:
    dbutils.fs.rm(path)
print("deleted", len(bad_files), "bad file(s)")
pipeline_run(full_refresh_selection=list(STAGING_TABLES))

# COMMAND ----------

wait_for_latest_update(spark, cs, after=last_request)
neg = spark.sql("SELECT count(*) AS n FROM silver_staging_mt5_deals WHERE deal_type <> 'BALANCE' AND price <= 0").first()["n"]
check("完全刷新后 silver 中没有负价格 (no negative prices in silver after the full refresh)", neg == 0, str(neg))
q = spark.sql("SELECT count(*) AS n FROM silver_staging_mt5_deals_quarantine WHERE array_contains(failed_rules, 'valid_trade_price')").first()["n"]
check("隔离表中也没有了：坏文件已不在源中 (none in the quarantine either: the bad files are gone)", q == 0, str(q))
flows = latest_update_flows(spark, cs)
check("更新完成 (the update completed)", flows.get("silver_staging_mt5_deals") == "COMPLETED", flows.get("silver_staging_mt5_deals"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · 用事件日志监控管道 (Monitor the pipeline with the event log)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 要点 6 (Key point 6) · 每次更新：开始、结束、耗时和最终状态 (every update: start, end, duration and final state)
# MAGIC SELECT origin.update_id AS update_id,
# MAGIC        min(timestamp) AS started_at,
# MAGIC        max(timestamp) AS ended_at,
# MAGIC        timestampdiff(SECOND, min(timestamp), max(timestamp)) AS seconds,
# MAGIC        max_by(details:update_progress.state, timestamp) FILTER (WHERE event_type = 'update_progress') AS final_state
# MAGIC FROM pipeline_event_log
# MAGIC WHERE event_type IN ('create_update', 'update_progress')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY started_at DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 最近一次更新中每张表的状态和写入行数 (each table's status and rows written in the latest update)
# MAGIC WITH latest AS (
# MAGIC   SELECT origin.update_id AS update_id FROM pipeline_event_log
# MAGIC   WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1)
# MAGIC SELECT origin.flow_name AS flow,
# MAGIC        max_by(details:flow_progress.status, timestamp) FILTER (WHERE details:flow_progress.status IS NOT NULL) AS status,
# MAGIC        sum(CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)) AS rows_written
# MAGIC FROM pipeline_event_log JOIN latest ON origin.update_id = latest.update_id
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY status, flow;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 每次更新中每条规则通过和失败的行数 (rows that passed and failed each rule, per update)
# MAGIC SELECT update_id, min(at) AS at, rule, sum(passed) AS passed, sum(failed) AS failed
# MAGIC FROM (
# MAGIC   SELECT origin.update_id AS update_id, timestamp AS at, e.name AS rule, e.dataset AS dataset,
# MAGIC          e.passed_records AS passed, e.failed_records AS failed
# MAGIC   FROM pipeline_event_log
# MAGIC   LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
# MAGIC     'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
# MAGIC   WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL)
# MAGIC WHERE dataset LIKE '%silver_staging_mt5_deals'
# MAGIC GROUP BY update_id, rule
# MAGIC ORDER BY at DESC, rule;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 失败邮件通知 (Failure emails)
# MAGIC 管道 → **Settings → Notifications → Add notification**：填写你的邮箱，勾选更新失败 (update failure) 和致命失败 (fatal failure)。之后每次更新失败，你会收到一封邮件。
# MAGIC
# MAGIC Pipeline → **Settings → Notifications → Add notification**: your email, with update failure and fatal failure ticked. From then on, every failed update sends you one email.
# MAGIC
# MAGIC ## 9 · 小结 (Recap)
# MAGIC
# MAGIC | 行为 (Behaviour) | 写法 (Syntax) | 结果 (Result) | 适合 (Use for) |
# MAGIC |---|---|---|---|
# MAGIC | WARN | `CONSTRAINT … EXPECT (…)` | 保留行并计数<br>keep the row, count it | 软规则<br>soft rules |
# MAGIC | DROP | `… ON VIOLATION DROP ROW` | 丢弃并计数（+ 隔离表）<br>drop and count (+ quarantine) | 单行的垃圾数据<br>row-level junk |
# MAGIC | FAIL | `… ON VIOLATION FAIL UPDATE` | 更新失败，下游跳过<br>update fails, downstream skipped | 合同破坏<br>contract breaks |
# MAGIC
# MAGIC * 管道中的依赖是自动推断的；作业中的依赖用 Depends on + Run if 显式控制 → 实验 03d。<br>Pipelines infer dependencies; jobs control them explicitly with Depends on + Run if → lab 03d.
# MAGIC * 恢复：修改规则后重新运行（从断点继续，无需回填）；或者修复源头，再只对受影响的表做完全刷新。<br>Recovery: change the rule and re-run (it continues from where it stopped; no backfill needed), or fix the source and fully refresh only the affected tables.
# MAGIC * 监控：事件日志 + 失败邮件。第 2 天：作业中的重试、修复运行和回填（实验 04c）。<br>Monitoring: the event log + failure emails. Day 2: retries, repair runs and backfills in a job (lab 04c).

# COMMAND ----------

# 第 2 天的实验 04 需要一个绿色的管道 (Day 2's lab 04 needs a green pipeline)
lane = lane_status(w, catalog, me)
check("管道最近一次更新没有失败：可以进入第 2 天 (the pipeline's latest update did not fail: ready for Day 2)",
      lane["latest_update"] != "FAILED",
      "" if lane["latest_update"] != "FAILED" else
      "完成第 6 步（或第 7 步），或者设置 start_over = true 后从头运行 (finish step 6 or step 7, or set start_over = true and run from the top)")
print(f"{sum(checks.values())}/{len(checks)} checks passed")
if AUTO:
    failed = [label for label, ok in checks.items() if not ok]
    assert not failed, f"failed checks: {failed}"
    dbutils.notebook.exit(f"{len(checks)}/{len(checks)} checks passed")
