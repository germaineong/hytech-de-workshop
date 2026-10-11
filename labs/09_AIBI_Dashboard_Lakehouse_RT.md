# Lab 09 · AI/BI 仪表盘 + Lakehouse RT (AI/BI dashboard + Lakehouse RT)

**目标 (Goal)：** 在你自己的 gold 表上搭建一个仪表盘，然后把它的计算资源切换到 **Lakehouse RT**（Real-Time）仓库：同样的仪表盘，为成百上千个并发用户提供亚秒级的响应。

Build a dashboard on your own gold tables, then switch its compute to a **Lakehouse RT** (Real-Time) warehouse: the same dashboard, with sub-second responses for hundreds or thousands of concurrent users.

> **开始之前 (Before you start).** Lakehouse RT 是 Beta 功能。管理员需要先：① 请 Databricks 客户团队为账户开启 Lakehouse//RT Beta；② 在工作区菜单 → **Previews** 中开启 **Lakehouse RT**；③ 创建一个 Real-Time 仓库并授予你 *Can use*（工作坊的设置步骤会自动创建 `hytech_workshop_rt`）。如果计算资源菜单里没有 Real-Time 仓库，就用工作坊的 SQL 仓库 `hytech_workshop_sql`（或 Serverless Starter Warehouse）完成本实验，其余步骤完全相同。
>
> Lakehouse RT is in Beta. An admin must first ① ask the Databricks account team to enable the Lakehouse//RT Beta for the account, ② turn on **Lakehouse RT** in the workspace menu → **Previews**, and ③ create a Real-Time warehouse and grant you *Can use* (the workshop setup creates `hytech_workshop_rt` for you). If no Real-Time warehouse appears in the compute menu, do this lab on the workshop SQL warehouse `hytech_workshop_sql` (or the Serverless Starter Warehouse); every other step is the same.
>
> 落后了？运行 `09b_Dashboard_Catch_Up`：它会创建一个单独的参考仪表盘 `MT5 交易概览 · <你的名字> · 参考 (reference)`，不会改动你自己搭建的仪表盘。管道坏了、gold 表不存在？把 09b 的 `source_schema` 设为 `solutions`，用参考答案的表。<br>
> Fell behind? Run `09b_Dashboard_Catch_Up`: it creates a separate reference dashboard, `MT5 交易概览 · <your_name> · 参考 (reference)`, and never changes the one you build. Pipeline broken, no gold tables? Set 09b's `source_schema` to `solutions` to use the solution tables.

## 1 · 创建仪表盘和数据集 (Create the dashboard and its datasets)

1. 左侧边栏 **New → Dashboard**。双击标题，改为 `MT5 交易概览 · <你的名字>`。
2. 打开 **Data** 选项卡 → **Add SQL dataset**，粘贴下面的查询（把 `<你的名字>` 换成你的 schema 名中 `u_` 后面的部分，例如 `u_zhang_san` → `zhang_san`），点 **Run**，然后把数据集重命名为 `daily_volume`：

   ```sql
   SELECT deal_date, server_id, symbol, asset_class, deals, lots, notional_usd, client_pnl_usd
   FROM hytech_ws_catalog.u_<你的名字>.gold_daily_symbol_volume
   ```

3. 再添加一个 SQL 数据集 `funding`：

   ```sql
   SELECT deal_date, brand, method, deposits_usd, withdrawals_usd, net_funding_usd
   FROM hytech_ws_catalog.u_<你的名字>.gold_funding_daily
   ```

4. 第三个 SQL 数据集 `top_symbols`：名义金额最大的 10 个品种（34 个品种画成柱状图太挤）。保留 `deal_date`，日期筛选器才能作用于它：

   ```sql
   SELECT deal_date, symbol, asset_class, notional_usd
   FROM hytech_ws_catalog.u_<你的名字>.gold_daily_symbol_volume
   WHERE symbol IN (SELECT symbol FROM hytech_ws_catalog.u_<你的名字>.gold_daily_symbol_volume
                    GROUP BY symbol ORDER BY sum(notional_usd) DESC LIMIT 10)
   ```

1. In the sidebar, **New → Dashboard**. Double-click the title and rename it `MT5 交易概览 · <your_name>`.
2. Open the **Data** tab → **Add SQL dataset**, paste the first query above (replace `<你的名字>` with the part of your schema name after `u_`, for example `u_zhang_san` → `zhang_san`), click **Run**, and rename the dataset `daily_volume`.
3. Add a second SQL dataset, `funding`, with the second query.
4. Add a third SQL dataset, `top_symbols`, with the third query: the 10 symbols with the most notional (34 symbols are too many bars). It keeps `deal_date`, so the date filter applies to it.

## 2 · 添加可视化 (Add visualizations)

回到画布页面，点 **Add a visualization**，在右侧配置面板中设置：<br>
Back on the canvas page, click **Add a visualization** and configure it in the panel on the right:

| 组件 (Widget) | 类型 (Type) | 设置 (Settings) |
|---|---|---|
| 名义金额 (Notional) | Counter | `daily_volume` · Value: `SUM(notional_usd)` |
| 客户盈亏 (Client P&L) | Counter | `daily_volume` · Value: `SUM(client_pnl_usd)` |
| 每日名义金额 (Daily notional) | Line | `daily_volume` · X: `deal_date`（DAILY）· Y: `SUM(notional_usd)` · Color: `asset_class` |
| 名义金额最大的 10 个品种 (Top 10 symbols) | Bar | `top_symbols` · X: `symbol`（按 Y 排序 / sort by Y）· Y: `SUM(notional_usd)` |

**用 Genie Code 再添加一个 (Add one more with Genie Code)：** 打开 **Genie Code** 侧边栏，在右下角选择 **Agent**，输入：<br>
Open the **Genie Code** side panel, select **Agent** in the bottom right corner, and type:

> 用 funding 数据集做一个柱状图：每种支付方式 (method) 的净入金 net_funding_usd 合计
>
> A bar chart from the funding dataset: total net_funding_usd for each payment method

## 3 · 添加日期筛选器 (Add a date filter)

**Add a filter (field/parameter)** → 标题 `日期 (Date range)` → Filter 选 **Date range picker** → Fields 中添加三个数据集的 `deal_date`，这样一个筛选器同时作用于所有数据集。<br>
**Add a filter (field/parameter)** → title `日期 (Date range)` → choose **Date range picker** as the filter → under Fields, add `deal_date` from all three datasets, so one filter drives every widget.

## 4 · 选择计算资源：Lakehouse RT (Choose the compute: Lakehouse RT)

1. 在仪表盘顶部的计算资源（仓库）菜单中，从 `hytech_workshop_sql`（或 Serverless Starter Warehouse）切换到 **`hytech_workshop_rt`**（类型 Real-Time）。所有组件会在新的仓库上重新运行。
2. 打开左侧边栏的 **Query History**，按计算资源 (Compute) 筛选：先看 `hytech_workshop_rt`，再看之前的仓库，对比同一组仪表盘查询的耗时。
3. 讨论：Lakehouse RT 适合**选择性强、读少量数据**的查询，为大量并发用户服务。它只支持 SELECT；不支持 Genie、AI 函数，也不支持带行过滤器或列掩码的表（实验 01 的 `users_snapshot` 就不行）；有 2 分钟的单查询上限。数据量大的聚合先用物化视图预聚合，就像我们的 gold 表。

1. In the compute (warehouse) menu at the top of the dashboard, switch from `hytech_workshop_sql` (or the Serverless Starter Warehouse) to **`hytech_workshop_rt`** (type Real-Time). Every widget re-runs on the new warehouse.
2. Open **Query History** in the sidebar and filter by compute: first `hytech_workshop_rt`, then the warehouse you used before, and compare how long the same dashboard queries take.
3. Discuss: Lakehouse RT suits **selective queries that read little data**, served to many concurrent users. It is SELECT-only; it doesn't support Genie, AI functions, or tables with row filters or column masks (lab 01's `users_snapshot` won't work); a single query is capped at 2 minutes. Pre-aggregate big aggregations into materialized views first, like our gold tables.

## 5 · 发布 (Publish)

**Publish** → 保持 **Share with data permissions (default)** → **Publish**。发布后的仪表盘使用发布者的权限运行查询，所以查看者不需要直接访问你的表。<br>
**Publish** → keep **Share with data permissions (default)** → **Publish**. The published dashboard runs its queries with the publisher's permissions, so viewers don't need direct access to your tables.

> 注意：这个仪表盘没有 **Ask Genie** 按钮，因为 Genie 不能在 Lakehouse RT 仓库上运行。实验 10 会在普通的 serverless 仓库上创建 Genie Agent。<br>
> Note: this dashboard has no **Ask Genie** button, because Genie doesn't run on a Lakehouse RT warehouse. Lab 10 creates a Genie Agent on a regular serverless warehouse.
