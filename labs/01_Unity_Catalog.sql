-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Unity Catalog — 数据治理 (Data governance) · LAB
-- MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/01_Unity_Catalog` 复制原始版本。
-- MAGIC
-- MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/01_Unity_Catalog`.
-- MAGIC
-- MAGIC **目标 (Goals):** 三级命名空间 · 所有权 · 授权 · 视图 · 标签 · 列掩码 · 行过滤 · 血缘
-- MAGIC
-- MAGIC Three-level namespace · ownership · grants · views · tags · column mask · row filter · lineage
-- MAGIC
-- MAGIC `catalog.schema.table` — 例如 `hytech_ws_catalog.u_zhang_san.users_snapshot`
-- MAGIC
-- MAGIC `catalog.schema.table` — e.g. `hytech_ws_catalog.u_zhang_san.users_snapshot`
-- MAGIC
-- MAGIC 本 notebook 的所有对象都建在**你自己的 schema** 里（你是 owner）。
-- MAGIC
-- MAGIC All objects in this notebook are created in **your own schema** (you are the owner).

-- COMMAND ----------

-- 你的 schema = u_<email name>；SQL 会话变量让每个人的 notebook 保持一致
-- Your schema = u_<email name>; a SQL session variable keeps the notebook identical for everyone
DECLARE OR REPLACE VARIABLE my_schema STRING DEFAULT
  'u_' || regexp_replace(lower(split_part(current_user(), '@', 1)), '[^a-z0-9]', '_');

USE CATALOG hytech_ws_catalog;
USE SCHEMA IDENTIFIER(my_schema);
SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS me;

-- COMMAND ----------

-- MAGIC %md ## 1 · 浏览三级命名空间 (Explore the namespace)

-- COMMAND ----------

SHOW SCHEMAS IN hytech_ws_catalog;

-- COMMAND ----------

SHOW VOLUMES IN hytech_ws_catalog.raw;

-- COMMAND ----------

LIST '/Volumes/hytech_ws_catalog/raw/landing/mt5/mt5-sg-01/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · 建一张受管表 (Create a managed table)
-- MAGIC 从 4 台服务器的 DMS 全量文件读取客户账户。`_metadata.file_path` 告诉我们每行来自哪台服务器。
-- MAGIC
-- MAGIC Read client accounts from the full load files of 4 MT5 servers via DMS. `_metadata.file_path` tells us which server each row came from.

-- COMMAND ----------

CREATE OR REPLACE TABLE users_snapshot
COMMENT 'MT5 client accounts from the DMS full load (all servers) — lab 01'
AS SELECT
  regexp_extract(_metadata.file_path, '/mt5/([^/]+)/', 1) AS server_id,
  Login                                                   AS login,
  split_part(`Group`, '\\', 2)                            AS brand,
  FirstName                                               AS first_name,
  LastName                                                AS last_name,
  Country                                                 AS country,
  Email                                                   AS email,
  Phone                                                   AS phone,
  Status                                                  AS status,
  Leverage                                                AS leverage
FROM read_files('/Volumes/hytech_ws_catalog/raw/landing/mt5/*/mt5_users/LOAD00000001.parquet', format => 'parquet');

SELECT * FROM users_snapshot LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## 3 · 所有权与授权 (Ownership and grants)

-- COMMAND ----------

-- "Owner" 行：你创建的东西你拥有 (you own what you create)
DESCRIBE TABLE EXTENDED users_snapshot;

-- COMMAND ----------

-- 授权给一个 GROUP（不要授权给个人）。de_workshop_sz = 所有工作坊参与者 (Grant to a GROUP, never to individuals. de_workshop_sz = all workshop participants.)
GRANT SELECT ON TABLE users_snapshot TO `de_workshop_sz`;
SHOW GRANTS ON TABLE users_snapshot;

-- COMMAND ----------

REVOKE SELECT ON TABLE users_snapshot FROM `de_workshop_sz`;
SHOW GRANTS ON TABLE users_snapshot;

-- COMMAND ----------

-- MAGIC %md ## 4 · 视图 (Views) — share an aggregate, not the PII

-- COMMAND ----------

CREATE OR REPLACE VIEW v_clients_by_country
COMMENT 'Client counts and average leverage per brand and country (no PII)'
-- 要点 1 (Key point 1) · 每个品牌和国家的客户数量和平均杠杆（四舍五入）(number of clients and average leverage (rounded) per brand and country)
AS SELECT brand, country, count(*) AS clients, round(avg(leverage)) AS avg_leverage
FROM users_snapshot
GROUP BY ALL;

SELECT * FROM v_clients_by_country ORDER BY clients DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · 标签 (Tags) — classify sensitive columns
-- MAGIC Hytech 已有”列标签 + 下游检查”框架；Unity Catalog 原生支持列标签，并可驱动 ABAC 策略。如果某个标签键是**治理标签 (governed tag)**，只能使用账户管理员允许的值，否则报错 `UC_TAG_POLICY_VALUE_NOT_ALLOWED` —— 这就是公司级标签规范的强制力。
-- MAGIC
-- MAGIC Hytech already has a “column tags + downstream checks” framework. Unity Catalog natively supports column tags and can drive ABAC policies. If a tag key is a **governed tag**, only account admin-allowed values are permitted; otherwise you get error `UC_TAG_POLICY_VALUE_NOT_ALLOWED` — this is how company-level tag standards are enforced.

-- COMMAND ----------

ALTER TABLE users_snapshot ALTER COLUMN email SET TAGS ('hytech_pii' = 'email');
-- 要点 2 (Key point 2) · 给 phone 列打上 hytech_pii = phone 标签（和上面一行的模式一样）(tag the phone column as hytech_pii = phone, same pattern as the line above)
ALTER TABLE users_snapshot ALTER COLUMN phone SET TAGS ('hytech_pii' = 'phone');
ALTER TABLE users_snapshot SET TAGS ('hytech_sensitivity' = 'L3', 'hytech_domain' = 'client');

SELECT column_name, tag_name, tag_value
FROM information_schema.column_tags
WHERE schema_name = my_schema AND table_name = 'users_snapshot';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · 列掩码 (Column mask)
-- MAGIC 只有 `hytech_pii_readers` 组的成员能看到完整邮箱；其他人（包括你）看到的是掩码后的值。
-- MAGIC
-- MAGIC Only members of `hytech_pii_readers` group can see full email addresses; others (including you) see masked values.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('hytech_pii_readers') THEN email
  ELSE regexp_replace(email, '^(.)[^@]*', '$1***')
END;

-- 要点 3 (Key point 3) · 将掩码函数附加到 email 列 (attach the mask function to the email column)
ALTER TABLE users_snapshot ALTER COLUMN email SET MASK mask_email;

SELECT login, first_name, email FROM users_snapshot LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7 · 行过滤 (Row filter)
-- MAGIC 例如：Apex 品牌的分析师只能看到 Apex 的客户。
-- MAGIC
-- MAGIC For example: Apex brand analysts can only see Apex clients.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION brand_filter(brand STRING)
RETURN is_account_group_member('hytech_all_brands') OR brand = 'Apex';

-- 要点 4 (Key point 4) · 附加 brand_filter，传递 brand 列 (attach brand_filter, passing the brand column)
ALTER TABLE users_snapshot SET ROW FILTER brand_filter ON (brand);

SELECT brand, count(*) AS clients FROM users_snapshot GROUP BY brand;

-- COMMAND ----------

-- Remove the filter again (the rest of the workshop needs all brands)
ALTER TABLE users_snapshot DROP ROW FILTER;
SELECT brand, count(*) AS clients FROM users_snapshot GROUP BY brand;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8 · 血缘 (Lineage)
-- MAGIC 打开 **Catalog Explorer → `v_clients_by_country` → Lineage**：可以看到它来自 `users_snapshot`，而后者来自 landing volume 的文件。
-- MAGIC
-- MAGIC Open **Catalog Explorer → `v_clients_by_country` → Lineage**: you can see it comes from `users_snapshot`, which in turn comes from files in the landing volume.
-- MAGIC
-- MAGIC ## 9 · 讲师演示 (Instructor demo) — ABAC with governed tags
-- MAGIC 治理标签（governed tags）由账户管理员定义允许的值；一条 ABAC 策略就能让**所有**打了 `pii` 标签的列自动被掩码，新建的表也会被自动保护——不需要逐表 `SET MASK`。这正是 Hytech 公司级数据安全策略需要的扩展方式。
-- MAGIC
-- MAGIC Governed tags are defined by the account admin with allowed values; a single ABAC policy can automatically mask **all** columns tagged with `pii`, and new tables are also auto-protected — no need to `SET MASK` per table. This is how Hytech's enterprise-level data security strategy scales.
