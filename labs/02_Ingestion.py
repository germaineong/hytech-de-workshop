# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · 数据接入 (Data ingestion) — CTAS · COPY INTO · Auto Loader · JSON & rescued data · LAB
# MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/02_Ingestion` 复制原始版本。
# MAGIC
# MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/02_Ingestion`.
# MAGIC
# MAGIC | 方式 (Method) | 适合 (Use case) | 增量? (Incremental?) |
# MAGIC |---|---|---|
# MAGIC | `CREATE TABLE AS SELECT` + `read_files` | 一次性 / 小文件全量重建 (one-off / small file full rebuild) | 否：每次全量 (No: full each time) |
# MAGIC | `COPY INTO` | 批量、幂等地加载新文件（SQL）(Batch, idempotent load of new files — SQL) | 是：已加载的文件会被跳过 (Yes: loaded files skipped) |
# MAGIC | **Auto Loader** (`cloudFiles`) | 持续到达的大量文件（DMS CDC!）(Continuous high-volume files — DMS CDC!) | 是：checkpoint 记录已处理的文件 (Yes: checkpoint tracks processed files) |
# MAGIC | Lakeflow Connect 托管连接器 | SaaS / 数据库（NetSuite GA，MySQL CDC 预览）(SaaS / database — NetSuite GA, MySQL CDC preview) | 是：托管 CDC (Yes: managed CDC) |

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "hytech_ws_catalog"
me = spark.sql("SELECT current_user()").first()[0]
my_schema = "u_" + "".join(ch if ch.isalnum() else "_" for ch in me.split("@")[0].lower()).strip("_")
landing = f"/Volumes/{catalog}/{my_schema}/landing"   # 你自己的落地区（实验 00 第 1b 步）(your own landing zone, lab 00 step 1b)
ref = f"/Volumes/{catalog}/raw/ref"
checkpoints = f"/Volumes/{catalog}/{my_schema}/checkpoints"
spark.sql(f"USE {catalog}.{my_schema}")
print("schema:", f"{catalog}.{my_schema}", "| landing:", landing, "| checkpoints:", checkpoints)

# COMMAND ----------

# MAGIC %md ## A · CTAS — 一次性建表 (one-off load of a small reference file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ref_fx_rates
# MAGIC COMMENT 'Daily USD value of each currency (CTAS from CSV)'
# MAGIC AS SELECT * FROM read_files('/Volumes/hytech_ws_catalog/raw/ref/fx_rates/', format => 'csv', header => true);
# MAGIC
# MAGIC SELECT ccy, count(*) AS days, min(rate_date) AS first_day, max(rate_date) AS last_day, round(avg(usd_per_ccy), 6) AS avg_usd
# MAGIC FROM ref_fx_rates GROUP BY ccy ORDER BY ccy;

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · COPY INTO — 幂等的批量加载 (idempotent batch load)
# MAGIC **运行两次！** 第二次 `num_affected_rows = 0`：COPY INTO 记得哪些文件已经加载过。
# MAGIC
# MAGIC **Run it twice!** The second time `num_affected_rows = 0`: COPY INTO remembers which files have been loaded.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ref_symbols_copy COMMENT 'Symbols loaded with COPY INTO';
# MAGIC
# MAGIC COPY INTO ref_symbols_copy
# MAGIC FROM '/Volumes/hytech_ws_catalog/raw/ref/symbols/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT asset_class, count(*) AS symbols, collect_list(symbol) AS examples FROM ref_symbols_copy GROUP BY asset_class;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Auto Loader — DMS CDC Parquet → bronze
# MAGIC * `cloudFiles.format` = 文件格式；`schemaLocation` = 推断出的 schema 存放处（并支持 schema 演进）
# MAGIC * `cloudFiles.format` = file format; `schemaLocation` = inferred schema location (supports schema evolution)
# MAGIC * `checkpointLocation` = 已处理文件的记录 → **只处理新文件**
# MAGIC * `checkpointLocation` = record of processed files → **only process new files**
# MAGIC * `trigger(availableNow=True)` = 处理完现有新文件就停止（批式调度的流）
# MAGIC * `trigger(availableNow=True)` = stop after processing existing new files (streaming batch for scheduled runs)
# MAGIC * 路径通配符 `mt5/*/mt5_deals/`：以后新增的服务器会自动被接入
# MAGIC * Path wildcard `mt5/*/mt5_deals/`: new servers added later will be automatically onboarded

# COMMAND ----------

deals_stream = (
    spark.readStream.format("cloudFiles")
    # 要点 1 (Key point 1) · DMS 写入 Parquet，所以 Auto Loader 的文件格式是 parquet (DMS writes Parquet, so the Auto Loader file format is parquet)
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoints}/bronze_deals_al/_schema")
    .load(f"{landing}/mt5/*/mt5_deals/")
    .select(
        "*",
        F.regexp_extract("_metadata.file_path", r"/mt5/([^/]+)/", 1).alias("server_id"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("file_modified_at"),
        F.current_timestamp().alias("ingested_at"),
    )
)
query = (
    deals_stream.writeStream
    # 要点 2 (Key point 2) · Auto Loader 在 checkpoint 中记录已处理过的文件，重新运行时只处理新文件 (the checkpoint is where Auto Loader remembers which files it has processed, so a re-run picks up only new files)
    .option("checkpointLocation", f"{checkpoints}/bronze_deals_al")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{my_schema}.bronze_deals_al")
)
query.awaitTermination()
print("rows in this run:", sum(p["numInputRows"] for p in query.recentProgress))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT server_id, Op, count(*) AS rows, count(DISTINCT source_file) AS files
# MAGIC FROM bronze_deals_al GROUP BY ALL ORDER BY server_id, Op;

# COMMAND ----------

# MAGIC %md
# MAGIC **再运行一次上面的 Auto Loader 单元格 (run the Auto Loader cell again):** 如果没有新文件，`rows in this run: 0`。打开 `00b_Live_Data`，用 `ticks` = 1 运行一次：它向你的落地区写入新的 CDC 文件 → 再运行 Auto Loader 单元格，就只会读到那些新文件。
# MAGIC
# MAGIC If there are no new files, you see `rows in this run: 0`. Open `00b_Live_Data` and run it once with `ticks` = 1: it writes new CDC files into your landing zone → run the Auto Loader cell again, and it reads only those new files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · JSON + rescued data — 数据契约 (a data contract for app events)
# MAGIC 我们给 Sensors 风格的事件定义一个**契约 schema**。契约外的新字段（如 `campaign_id`）和类型不符的值（`amount` 以字符串 `"1,234.50"` 到达）不会丢失，而是进入 `_rescued_data`。
# MAGIC
# MAGIC We define a **contract schema** for Sensors-style events. New fields outside the contract (e.g., `campaign_id`) and type mismatches (e.g., `amount` arrives as string `"1,234.50"`) are not lost; they go into `_rescued_data`.

# COMMAND ----------

events_schema = """
  event_id STRING, event STRING, distinct_id STRING, time BIGINT,
  lib STRUCT<`$lib`: STRING, `$lib_version`: STRING>,
  properties STRUCT<`$os`: STRING, `$app_version`: STRING, `$country`: STRING, `$ip`: STRING, brand: STRING,
                    server_id: STRING, login: BIGINT, symbol: STRING, screen: STRING, method: STRING,
                    amount: DOUBLE, currency: STRING, feedback_text: STRING, rating: INT>
"""
events_query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    # 要点 3 (Key point 3) · 将未知字段 / 类型不匹配的值保存在名为 _rescued_data 的列中 (keep unknown fields / type mismatches in a column called _rescued_data)
    .option("rescuedDataColumn", "_rescued_data")
    .schema(events_schema)
    .load(f"{landing}/app_events/")
    .select("*", F.col("_metadata.file_path").alias("source_file"), F.current_timestamp().alias("ingested_at"))
    .writeStream
    .option("checkpointLocation", f"{checkpoints}/bronze_app_events_al")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{my_schema}.bronze_app_events_al")
)
events_query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event, properties.amount AS amount_parsed, _rescued_data
# MAGIC FROM bronze_app_events_al
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 有多少事件带有被"救回"的数据？哪些字段？
# MAGIC SELECT
# MAGIC   count(*)                                                                    AS events,
# MAGIC   count_if(_rescued_data IS NOT NULL)                                         AS with_rescued_data,
# MAGIC   count_if(get_json_object(_rescued_data, '$.properties.campaign_id') IS NOT NULL) AS new_field_campaign_id,
# MAGIC   count_if(get_json_object(_rescued_data, '$.properties.amount') IS NOT NULL)      AS amount_type_mismatch
# MAGIC FROM bronze_app_events_al;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · 小结 (Recap)
# MAGIC * bronze 保留**原始**数据 + 元数据列（`server_id`、`source_file`、`ingested_at`）→ 可追溯、可重放<br>bronze keeps **raw** data + metadata columns (`server_id`, `source_file`, `ingested_at`) → traceable, replayable
# MAGIC * Auto Loader 的 checkpoint 让流式接入既增量又可靠；COPY INTO 适合 SQL 用户的批量增量加载<br>Auto Loader checkpoints make streaming incremental and reliable; COPY INTO suits SQL users for batch incremental load
# MAGIC * 下一步 (next): 在 **Spark 声明式管道** 中，用更少的代码声明同样的 bronze 表，并加上 silver / gold 与数据质量规则。<br>Next: in **Spark Declarative Pipelines**, declare the same bronze table with less code and add silver / gold layers plus data quality rules.
