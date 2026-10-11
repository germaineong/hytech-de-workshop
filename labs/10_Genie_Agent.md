# Lab 10 · Genie Agent + Genie One

**目标 (Goal)：** 在你自己的 gold 表上创建并配置一个 **Genie Agent**，用中文提问；再通过 **Genie One** 用同样的问题提问，看它如何找到你的 Agent。

Create and configure a **Genie Agent** on your own gold tables and ask it questions in Chinese; then ask the same question through **Genie One** and see how it finds your agent.

> 你需要 (You need)：Databricks SQL 权限，以及对工作坊 SQL 仓库 `hytech_workshop_sql` 的 *Can use*（工作坊设置已授予）。Genie 不能在 Lakehouse RT 仓库上运行，所以这里用 serverless 仓库。如果工作区在新加坡区域而看不到 Agent 模式或 Genie One 对话，请管理员开启跨地域处理 (cross-Geo processing)。<br>
> The Databricks SQL entitlement and *Can use* on the workshop SQL warehouse `hytech_workshop_sql` (the workshop setup grants it). Genie can't run on a Lakehouse RT warehouse, so use a serverless one. If the workspace is in Singapore and you don't see Agent mode or Genie One chat, ask the admin to enable cross-Geo processing.
>
> 落后了，或管道坏了？运行 `10b_Genie_Catch_Up`（管道坏了就把 `source_schema` 设为 `solutions`）：它会创建一个单独的参考 Agent `MT5 交易分析助手 · <你的名字> · 参考 (reference)`，不会改动你自己配置的 Agent。<br>
> Fell behind, or your pipeline is broken? Run `10b_Genie_Catch_Up` (set `source_schema` to `solutions` if your pipeline is broken): it creates a separate reference agent, `MT5 交易分析助手 · <your_name> · 参考 (reference)`, and never changes the one you configure.

## 1 · 创建 (Create) — 3 分钟

1. 左侧边栏 **Genie Agents → New**。选择你 schema 中的这些表：`gold_client_daily_pnl`、`gold_daily_symbol_volume`、`gold_funding_daily`、`gold_ib_daily_performance`、`gold_net_exposure_by_symbol`、`ref_ib_hierarchy` → **Create**。
2. **Genie Code** 会自动打开，读取数据，并建议表的说明和示例查询：逐条看一看，接受合理的建议。
3. **Configure → Settings**：标题 `MT5 交易分析助手 · <你的名字>`；仓库选 `hytech_workshop_sql`（或其他 serverless 仓库，不要选 Real-Time 仓库）。

1. In the sidebar, **Genie Agents → New**. Choose these tables from your schema: `gold_client_daily_pnl`, `gold_daily_symbol_volume`, `gold_funding_daily`, `gold_ib_daily_performance`, `gold_net_exposure_by_symbol`, `ref_ib_hierarchy` → **Create**.
2. **Genie Code** opens by itself, reads the data and suggests table descriptions and example queries: review them and accept the sensible ones.
3. **Configure → Settings**: title `MT5 交易分析助手 · <your_name>`; choose `hytech_workshop_sql` (or another serverless warehouse, not a Real-Time one).

## 2 · 配置 (Configure) — 10 分钟

每种设置做一个例子，理解它的作用。<br>
One example of each kind of setting, so you learn what each one is for.

**Configure → Instructions（常规说明 / general instructions）** — 粘贴 (paste)：

```
用简体中文回答；表名、列名和 SQL 保持英文。
金额都是美元：直接引用数值并加千分位，不要换算成万或亿。
client_pnl_usd 和 trading_pnl_usd 是客户视角：负数表示客户亏钱（经纪商的收入）。
lots 是标准手数；notional_usd 是美元名义金额。
品种别名：黄金 = XAUUSD，白银 = XAGUSD，原油 = USOUSD 或 UKOUSD，比特币 = BTCUSD，以太坊 = ETHUSD，纳指 = NAS100，道指 = US30。
'上周' = 最近 7 天：deal_date >= date_sub(current_date(), 7)；'昨天' = 数据中最近的一个 deal_date。
风险敞口用 gold_net_exposure_by_symbol：它是当前持仓的快照，没有日期。
```

**Configure → Data（列的同义词 / column synonyms）** — 打开每张表，给这些列添加中文同义词 (open each table and add Chinese synonyms to these columns)：

| 表 (Table) | 列 (Column) | 同义词 (Synonyms) |
|---|---|---|
| `gold_daily_symbol_volume` | `notional_usd` · `client_pnl_usd` · `lots` · `symbol` | 名义金额、成交额 · 客户盈亏 · 手数 · 品种 |
| `gold_funding_daily` | `deposits_usd` · `withdrawals_usd` · `net_funding_usd` · `method` | 入金 · 出金 · 净入金 · 支付方式 |
| `gold_ib_daily_performance` | `ib_name` · `rebate_usd` | 代理、IB · 返佣 |

**Configure → Examples → Add → Joins（关联 / a join）**：`gold_client_daily_pnl.ib_login` = `ref_ib_hierarchy.ib_login`（多对一 many-to-one）。

**Configure → Examples → Add → SQL expressions（一个度量 + 一个筛选 / one measure + one filter）：**

- Measure `每笔名义金额 (Notional per deal)`：`SUM(gold_daily_symbol_volume.notional_usd) / SUM(gold_daily_symbol_volume.deals)`
- Filter `加密货币品种 (Crypto symbols)`：`gold_daily_symbol_volume.asset_class = 'CRYPTO'`，同义词 (synonyms)：加密货币

**Configure → Examples → Add → Example SQL（一个示例 SQL / one example SQL）** — 问题 (question)：`上周每个品牌的客户盈亏是多少？`（把 `<你的名字>` 换成你的 schema 名中 `u_` 后面的部分，例如 `u_zhang_san` → `zhang_san` / replace `<你的名字>` with the part of your schema name after `u_`, for example `u_zhang_san` → `zhang_san`）

```sql
SELECT brand, round(sum(trading_pnl_usd), 2) AS client_pnl_usd
FROM hytech_ws_catalog.u_<你的名字>.gold_client_daily_pnl
WHERE deal_date >= date_sub(current_date(), 7)
GROUP BY brand ORDER BY client_pnl_usd
```

**Configure → Settings → Common questions（常见问题，显示为起始问题 / shown as starter questions）：** `昨天名义金额最大的 5 个品种是什么？` · `上周每个品牌的客户盈亏是多少？` · `哪个代理 (IB) 上周的返佣最多？`

## 3 · 测试 (Test) — 5 分钟

1. 点一个常见问题，然后自己用中文提问，例如 `黄金最近 30 天每天的名义金额是多少？`、`加密货币品种的每笔名义金额是多少？`。每次都点开 **Show code** 读一读生成的 SQL：它用了你的同义词、度量和筛选吗？
2. 切换到 **Agent 模式**，问一个需要多步分析的问题：`分析过去 30 天客户盈亏的变化，主要是哪些品种和品牌造成的？`
3. 对答案点 👍 / 👎；把一个答对的问题 **Add as benchmark**，在 **Monitor → Benchmarks** 中运行。

1. Click a common question, then ask your own in Chinese, e.g. `黄金最近 30 天每天的名义金额是多少？` or `加密货币品种的每笔名义金额是多少？`. Each time, open **Show code** and read the generated SQL: did it use your synonyms, measure and filter?
2. Switch to **Agent mode** and ask a question that needs several steps: `分析过去 30 天客户盈亏的变化，主要是哪些品种和品牌造成的？`
3. Give answers 👍 / 👎; **Add as benchmark** one correctly answered question and run it under **Monitor → Benchmarks**.

## 4 · Genie One — 5 分钟

1. 打开 `https://<工作区地址>/one`（或应用切换器 → Genie One）。问：`上周每个品牌的客户盈亏是多少？`。Genie One 会先查找匹配的 Genie Agent，找到你的 Agent 后用它来回答。
2. 在搜索栏用 **Ask** → 选择你的 Agent，再问一次：直接指定 Agent 可以跳过查找，通常快 30–60 秒。
3. 讨论：业务用户在 Genie One 里同时看到仪表盘、Genie Agent 和应用；Agent 的质量来自你在第 2 步中写的说明、同义词和示例。

1. Open `https://<workspace>/one` (or the app switcher → Genie One) and ask `上周每个品牌的客户盈亏是多少？`. Genie One first looks for a matching Genie Agent, finds yours and answers with it.
2. In the search bar, use **Ask** → choose your agent and ask again: naming the agent skips the search, which is usually 30–60 seconds faster.
3. Discuss: in Genie One, business users see dashboards, Genie Agents and apps in one place; an agent's quality comes from the instructions, synonyms and examples you wrote in step 2.
