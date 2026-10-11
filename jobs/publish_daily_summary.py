# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · `publish_daily_summary` — AI 每日交易简报 (AI daily trading commentary)
# MAGIC
# MAGIC 仅在数据质量闸门通过时运行（`if/else` → `true`）。从黄金数据集收集该天的 KPI，并通过 AI Functions（`ai_query`）要求基础模型为交易台提供一份简体中文的短评。输出：`gold_daily_commentary`。
# MAGIC
# MAGIC Runs only when the DQ gate passes (`if/else` → `true`). Collects the day's KPIs from the gold marts and
# MAGIC asks a Foundation Model (via `ai_query`) for a short commentary in Simplified Chinese for the dealing desk.
# MAGIC Output: `gold_daily_commentary`.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5")
dbutils.widgets.text("report_date", "", "报表日期，留空 = 最新一天 (report date; empty = the latest day)")

import json
import re
from datetime import date

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
endpoint = dbutils.widgets.get("llm_endpoint")
if not re.fullmatch(r"[A-Za-z0-9._-]+", endpoint):
    raise ValueError(f"invalid endpoint name: {endpoint}")
cs = f"{catalog}.{schema}"

# COMMAND ----------

latest = spark.sql(f"SELECT max(deal_date) AS d FROM {cs}.gold_daily_symbol_volume WHERE deal_date <= current_date()").first()["d"]
requested = dbutils.widgets.get("report_date").strip()
# 回填时作业传入 {{backfill.iso_date}}；平时留空 = 最新一天 (a backfill passes {{backfill.iso_date}}; otherwise empty = the latest day)
day = date.fromisoformat(requested[:10]) if requested else latest   # iso_date 或 iso_datetime 都可以 (iso_date or iso_datetime)
if not spark.sql(f"SELECT count(*) AS n FROM {cs}.gold_daily_symbol_volume WHERE deal_date = '{day}'").first()["n"]:
    dbutils.notebook.exit(f"no trades on {day}: nothing to summarise")
totals = spark.sql(f"""
  SELECT round(sum(notional_usd), 0) AS notional_usd, round(sum(lots), 1) AS lots, sum(deals) AS deals,
         round(sum(client_pnl_usd), 0) AS client_pnl_usd, round(sum(commission_usd), 0) AS commission_usd
  FROM {cs}.gold_daily_symbol_volume WHERE deal_date = '{day}'
""").first().asDict()
top = [r.asDict() for r in spark.sql(f"""
  SELECT symbol, round(sum(notional_usd), 0) AS notional_usd, round(sum(client_pnl_usd), 0) AS client_pnl_usd
  FROM {cs}.gold_daily_symbol_volume WHERE deal_date = '{day}'
  GROUP BY symbol ORDER BY notional_usd DESC LIMIT 5
""").collect()]
funding = [r.asDict() for r in spark.sql(f"""
  SELECT method, round(sum(deposits_usd), 0) AS deposits_usd, round(sum(withdrawals_usd), 0) AS withdrawals_usd
  FROM {cs}.gold_funding_daily WHERE deal_date = '{day}' GROUP BY method ORDER BY deposits_usd DESC
""").collect()]
# 风险敞口是"现在"的持仓快照，只属于最新一天的日报 (exposure is a snapshot of open positions now, so it only belongs in the latest day's summary)
exposure = [r.asDict() for r in spark.sql(f"""
  SELECT symbol, net_lots, round(net_notional_usd, 0) AS net_notional_usd
  FROM {cs}.gold_net_exposure_by_symbol ORDER BY abs(net_notional_usd) DESC LIMIT 5
""").collect()] if day == latest else []
# 预计算模型应引用的每个数字：LLM 在算术和单位转换中不可靠。
# Pre-compute every number the model should quote: LLMs are unreliable at arithmetic and unit conversion.
totals["deposits_usd"] = round(sum(f["deposits_usd"] or 0 for f in funding))
totals["withdrawals_usd"] = round(sum(f["withdrawals_usd"] or 0 for f in funding))
payload = {"date": str(day), "totals": totals, "top_symbols": top, "funding_by_method": funding, "net_exposure": exposure}
print(json.dumps(payload, ensure_ascii=False, default=str, indent=1))

# COMMAND ----------

prompt = (
    "你是 Hytech 交易数据团队的分析师。请根据下面的 JSON 数据，用简体中文给交易主管写一段每日简报，"
    "3–4 句话、不超过 150 字：总成交名义金额与最活跃品种、客户盈亏、出入金、最需要关注的净风险敞口。"
    "所有金额直接引用 JSON 中的美元数值并加千分位（例如 29,161 美元），不要自行换算成万或百万，也不要自己计算新数字。"
    "如果 net_exposure 为空，就不写风险敞口。"
    "只输出简报正文。\n数据：" + json.dumps(payload, ensure_ascii=False, default=str)
)
commentary = spark.sql(f"SELECT ai_query('{endpoint}', :prompt) AS t", args={"prompt": prompt}).first()["t"]
print(commentary)

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {cs}.gold_daily_commentary (
    report_date DATE, language STRING, commentary STRING, model STRING, created_at TIMESTAMP)
  COMMENT 'AI-written daily trading commentary (ai_query), one row per report date and language'
""")
# 幂等：同一天重跑（修复运行、回填）会替换这一行，而不是再追加一行 (idempotent: re-running a day — a repair, a backfill — replaces its row instead of appending another)
spark.sql(f"""
  MERGE INTO {cs}.gold_daily_commentary t
  USING (SELECT DATE'{day}' AS report_date, 'zh-CN' AS language, :c AS commentary,
                '{endpoint}' AS model, current_timestamp() AS created_at) s
  ON t.report_date = s.report_date AND t.language = s.language
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""", args={"c": commentary})
dbutils.jobs.taskValues.set(key="report_date", value=str(day))
