-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · AI Functions — 在 SQL 里批量调用大模型 (batch LLM calls in SQL)
-- MAGIC
-- MAGIC | 函数<br>Function | 用途<br>Purpose |
-- MAGIC |---|---|
-- MAGIC | `ai_query(endpoint, prompt)` | 任意提示词 → 文本（选择你的模型端点）<br>Any prompt → text (choose your model endpoint) |
-- MAGIC | `ai_classify(text, labels)` | 分类到给定标签<br>Classify to given labels |
-- MAGIC | `ai_extract(text, labels)` | 抽取实体<br>Extract entities |
-- MAGIC | `ai_mask(text, labels)` | 掩码敏感信息<br>Mask sensitive information |
-- MAGIC | `ai_analyze_sentiment(text)` | 情感分析<br>Analyze sentiment |
-- MAGIC
-- MAGIC ⚠️ `ai_translate` 目前**不支持**中文目标语言 → 中文请用 `ai_query`。
-- MAGIC 本 lab 使用讲师的 `solutions` schema，保证大家的数据一样。
-- MAGIC
-- MAGIC ⚠️ `ai_translate` currently does **not** support Chinese as a target language → use `ai_query` for Chinese.
-- MAGIC This lab uses the instructor's `solutions` schema to ensure everyone has the same data.

-- COMMAND ----------

USE CATALOG hytech_ws_catalog;
USE SCHEMA solutions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · `ai_query` — 每日交易简报 (daily trading commentary)
-- MAGIC **教训 (lesson learned while building this lab):** 模型把 29,161 美元写成了“2,916 万美元”。
-- MAGIC 规则：**数字由 SQL 先算好**，在提示词里要求“直接引用、不要换算单位”，并抽查结果。

-- COMMAND ----------

WITH d AS (
  SELECT max(deal_date) AS day FROM gold_daily_symbol_volume WHERE deal_date <= current_date()
),
kpi AS (
  SELECT to_json(named_struct(
           'date', d.day,
           'notional_usd', round(sum(g.notional_usd)),
           'client_pnl_usd', round(sum(g.client_pnl_usd)),
           'deals', sum(g.deals),
           'top_symbol_by_notional', max_by(g.symbol, g.notional_usd))) AS facts
  FROM gold_daily_symbol_volume g JOIN d ON g.deal_date = d.day
  GROUP BY d.day
)
SELECT facts,
       ai_query(
         'databricks-claude-sonnet-4-5',
         '你是交易数据分析师。用简体中文写两句话总结当天交易情况。金额直接引用数据中的美元数值并加千分位，'
         || '不要换算成万或百万，不要自己计算新数字。数据：' || facts
       ) AS commentary
FROM kpi;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · `ai_classify` — 出入金方式：规则 vs AI (funding method: rules vs AI)
-- MAGIC
-- MAGIC `gold_funding_daily` 用正则从 MT5 备注里解析支付方式。看看规则解析不了的（`unknown`）和中文备注，AI 怎么分类：
-- MAGIC
-- MAGIC `gold_funding_daily` uses regex to parse payment methods from MT5 comments. See how AI classifies the comments that regex can't parse (`unknown`) and Chinese remarks:

-- COMMAND ----------

-- 比较规则和 AI 分类 (Compare rule-based vs AI classification)
WITH c AS (
  SELECT DISTINCT comment,
    CASE
      WHEN comment RLIKE '(?i)usdt|btc|crypto|比特币'              THEN 'crypto'
      WHEN comment RLIKE '(?i)visa|mastercard|unionpay|银联|card'  THEN 'card'
      WHEN comment RLIKE '(?i)skrill|neteller|fasapay|wallet|钱包' THEN 'e_wallet'
      WHEN comment RLIKE '(?i)wire|bank|电汇|银行'                 THEN 'bank_transfer'
      WHEN comment RLIKE '(?i)transfer|内部转账'                   THEN 'internal_transfer'
      ELSE 'unknown'
    END AS rule_method
  FROM silver_mt5_deals_current
  WHERE deal_type = 'BALANCE'
)
SELECT comment, rule_method,
       ai_classify(comment, ARRAY('crypto', 'card', 'bank_transfer', 'e_wallet', 'internal_transfer', 'unknown')) AS ai_method
FROM c
WHERE rule_method = 'unknown' OR comment RLIKE '\\p{IsHan}'
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 讨论 (Discuss)
-- MAGIC
-- MAGIC 只有”入金”两个字的备注，AI 也无法知道支付方式——AI 不是魔法，源系统数据质量依然重要。
-- MAGIC 规则 + AI 结合：规则覆盖 95%，AI 处理长尾，并把 AI 结果写回表里（批量推理），而不是每次查询都调用。
-- MAGIC
-- MAGIC For a comment with just “deposit” in Chinese, AI also cannot know the payment method — AI is not magic; source system data quality still matters.
-- MAGIC Hybrid approach: rules cover 95%, AI handles the tail, and materialize AI results in a table (batch inference) rather than calling on every query.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · `ai_mask` — 反馈文本里的个人信息 (PII in free-text feedback)

-- COMMAND ----------

-- 掩码个人信息 (Mask PII)
SELECT feedback_text,
       ai_mask(feedback_text, ARRAY('email', 'phone number', 'person name')) AS masked_text
FROM (SELECT DISTINCT feedback_text FROM silver_app_events WHERE event = 'app_feedback')
WHERE feedback_text RLIKE '@|\\+[0-9]'
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## 4 · 反馈主题与情感 (feedback topics and sentiment) — 中英文混合

-- COMMAND ----------

SELECT feedback_text, rating,
       ai_classify(feedback_text, ARRAY('withdrawal_delay', 'spread_or_pricing', 'app_stability',
                                        'kyc_onboarding', 'deposit_issue', 'praise')) AS topic,
       ai_analyze_sentiment(feedback_text) AS sentiment
FROM (SELECT DISTINCT feedback_text, rating FROM silver_app_events WHERE event = 'app_feedback')
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · 批量推理写入你的 schema (Batch inference into a table)
-- MAGIC
-- MAGIC 生产中：AI 结果**物化**到表里（一次计算、多次查询），通过 Unity AI Gateway 控制模型、速率与成本。
-- MAGIC
-- MAGIC In production: AI results are **materialized** into a table (compute once, query many times), with model, rate, and cost control via Unity AI Gateway.

-- COMMAND ----------

-- 定义 schema (Define my schema)
DECLARE OR REPLACE VARIABLE my_schema STRING DEFAULT
  'u_' || regexp_replace(lower(split_part(current_user(), '@', 1)), '[^a-z0-9]', '_');

-- 创建富化反馈表 (Create enriched feedback table)
CREATE OR REPLACE TABLE IDENTIFIER('hytech_ws_catalog.' || my_schema || '.feedback_enriched')
COMMENT 'App feedback with AI topic, sentiment and masked text (batch inference, lab 07)'
AS SELECT feedback_text, rating,
          ai_classify(feedback_text, ARRAY('withdrawal_delay', 'spread_or_pricing', 'app_stability',
                                           'kyc_onboarding', 'deposit_issue', 'praise')) AS topic,
          ai_analyze_sentiment(feedback_text) AS sentiment,
          ai_mask(feedback_text, ARRAY('email', 'phone number')) AS feedback_masked
   FROM (SELECT DISTINCT feedback_text, rating FROM silver_app_events WHERE event = 'app_feedback' LIMIT 200);  -- 保持 lab 运行快速且便宜 (keep the lab fast/cheap)

SELECT topic, sentiment, count(*) AS n
FROM IDENTIFIER('hytech_ws_catalog.' || my_schema || '.feedback_enriched')
GROUP BY ALL ORDER BY n DESC;
