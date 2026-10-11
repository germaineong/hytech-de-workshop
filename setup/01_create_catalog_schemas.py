# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · 创建 Catalog / Schema / Volume 与授权 (Create catalog, schemas, volumes, grants)
# MAGIC
# MAGIC 幂等操作 — 可以安全地重新运行。需要以工作区管理员或 catalog 所有者（例如 Triones）身份运行。
# MAGIC
# MAGIC Idempotent — safe to re-run. Run as a workspace admin or the catalog owner (e.g. Triones).
# MAGIC
# MAGIC | 对象 (Object) | 用途 (Purpose) |
# MAGIC |---|---|
# MAGIC | `<catalog>.raw` · volumes `landing`, `ref`, `producer` | DMS 风格 MT5 变更数据 (CDC) 文件、应用事件、参考 CSV、滴灌程序 (drip producer) 状态 |
# MAGIC | `<catalog>.solutions` | 讲师参考答案管道（追进度、AI Functions 实验） |
# MAGIC | `<catalog>.ops` | 系统表的治理视图（可观测性实验） |
# MAGIC | `<catalog>.u_<name>` | 每个学员的 schema（由 `04_participant_schemas` 或学员在 `labs/00_Start_Here` 中创建） |

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.text("participant_group", "de_workshop_sz")
dbutils.widgets.dropdown("create_catalog", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
group = dbutils.widgets.get("participant_group").strip()
create_catalog = dbutils.widgets.get("create_catalog") == "true"

# COMMAND ----------


def run(stmt: str, optional: bool = False) -> bool:
    try:
        spark.sql(stmt)
        print("✅", " ".join(stmt.split())[:140])
        return True
    except Exception as e:  # noqa: BLE001 - surface any SQL error, optionally continue
        if not optional:
            raise
        print("⚠️ ", " ".join(stmt.split())[:140], "\n    ->", str(e).splitlines()[0][:220])
        return False


if create_catalog:
    run(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Hytech DE workshop — MT5 trade lakehouse'")

schemas = {
    "raw": "Landing zone: DMS-style MT5 CDC Parquet, Sensors-style app events, reference CSVs",
    "solutions": "Instructor solution pipeline output (catch-up, Genie, AI Functions labs)",
    "ops": "Governed views over system tables for the observability lab",
}
for name, comment in schemas.items():
    run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{name} COMMENT '{comment}'")

volumes = {
    "landing": "MT5 CDC files (landing/mt5/<server>/<table>/) and app events (landing/app_events/<date>/)",
    "ref": "Reference CSVs: symbols, IB hierarchy, servers, daily FX rates",
    "producer": "Drip producer state (instructor only)",
}
for name, comment in volumes.items():
    run(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.{name} COMMENT '{comment}'")

run(f"ALTER SCHEMA {catalog}.raw SET TAGS ('workshop' = 'hytech_de_2026', 'layer' = 'landing')", optional=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 学员组的授权 (Grants for the participant group)
# MAGIC 学员可以**读取**数据接入区、数据生成器的状态（`raw.producer`：实验 00 把它复制到学员自己的 schema，供 `00b_Live_Data` 使用）、解决方案和 ops 视图，并**创建自己的 schema**。授权需要一个**账户级别**的组；如果还不存在，会跳过并显示警告。
# MAGIC
# MAGIC Participants can **read** the landing zone, the data generator's state (`raw.producer`: lab 00 copies it into their own schema for `00b_Live_Data`), solutions and ops views, and **create their own schema**.
# MAGIC Grants need an **account-level** group; if it does not exist yet, they are skipped with a warning.

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG, CREATE SCHEMA ON CATALOG {catalog} TO `{group}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalog}.raw TO `{group}`",
    f"GRANT READ VOLUME ON VOLUME {catalog}.raw.landing TO `{group}`",
    f"GRANT READ VOLUME ON VOLUME {catalog}.raw.ref TO `{group}`",
    f"GRANT READ VOLUME ON VOLUME {catalog}.raw.producer TO `{group}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.solutions TO `{group}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.ops TO `{group}`",
]
results = [run(g, optional=True) for g in grants] if group else []
if group and not all(results):
    print(f"\nSome grants were skipped. Create the account-level group `{group}`, add the participants, and re-run.")

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
