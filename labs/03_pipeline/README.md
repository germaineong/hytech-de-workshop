# Lab 03 · 交易湖仓管道 (Trade lakehouse pipeline) — Lakeflow Spark Declarative Pipelines

**目标 (Goal)：** 在你自己的 schema `u_<name>` 中为 MT5 数据构建 bronze → silver → gold 数据分层。

Build bronze → silver → gold for the MT5 data in **your own schema** `u_<name>`.

## 1 · 创建管道 (Create the pipeline) — Lakeflow Pipelines Editor

1. 首先将此文件夹复制到你的主目录（`labs/00_Start_Here` → 第 3 步会为你完成）：`/Users/<you>/hytech_de_lab/03_pipeline/`

   Copy this folder to your home first (`labs/00_Start_Here` → step 3 does it for you):
   `/Users/<you>/hytech_de_lab/03_pipeline/`

2. **New → ETL pipeline**（Lakeflow 管道编辑器）。选择 **Add existing assets** 并设置：

   **New → ETL pipeline** (Lakeflow Pipelines Editor). Choose **Add existing assets** and set:

   | 设置 (Setting) | 值 (Value) |
   |---|---|
   | 管道名称 (Pipeline name) | `trade_lakehouse_<your_name>` |
   | 根文件夹 (Root folder) | `/Users/<you>/hytech_de_lab/03_pipeline` |
   | 源代码 (Source code) | `/Users/<you>/hytech_de_lab/03_pipeline/transformations` |
   | 默认 catalog / schema (Default catalog / schema) | `hytech_ws_catalog` / `u_<your_name>` |

3. **Settings → Configuration** — 添加两个键值对：

   **Settings → Configuration** — add two key/value pairs:
   | 键 (Key) | 值 (Value) |
   |---|---|
   | `landing_root` | `/Volumes/hytech_ws_catalog/u_<your_name>/landing`（你自己的落地区，实验 00 第 1b 步打印了这个值）<br>(your own landing zone; step 1b of lab 00 printed this value) |
   | `ref_root` | `/Volumes/hytech_ws_catalog/raw/ref` |

   > 你在这次更新之前已经用共享落地区 `/Volumes/hytech_ws_catalog/raw/landing` 建好了管道？把 `landing_root` 改成你自己的落地区之后，运行一次 **Full refresh all**（完全刷新）；否则 bronze 会从新路径把全部历史再读一遍，silver 里会出现重复。
   >
   > Built the pipeline on the shared landing zone `/Volumes/hytech_ws_catalog/raw/landing` before this update? After you switch `landing_root` to your own landing zone, run **Full refresh all** once; otherwise bronze reads the whole history again from the new path and silver gets duplicates.

4. **Settings → Advanced → Publish event log to metastore**：打开，表名 `pipeline_event_log`（catalog `hytech_ws_catalog`，schema `u_<your_name>`）。Lab 04 中的作业会读取它。

   **Settings → Advanced → Publish event log to metastore**: on, table name `pipeline_event_log`
   (catalog `hytech_ws_catalog`, schema `u_<your_name>`). The job in lab 04 reads it.

5. 计算资源（Compute）：**Serverless**。管道模式（Pipeline mode）：**Triggered**。

   Compute: **Serverless**. Pipeline mode: **Triggered**.

## 2 · 读懂代码中的要点 (Read the key points in the code)

代码已经完整。运行之前，先在各个文件中找到下面这些「要点」注释，看懂它们在做什么：

The code is complete. Before you run it, find these "Key point" comments in the files and make sure you understand what they do:

| 要点 (Key point) | 文件 (File) | 内容 (What) |
|---|---|---|
| 1a, 1b | `01_bronze_mt5.py` | Auto Loader 格式；从文件路径中提取 `server_id`<br>Auto Loader format; extract `server_id` from the file path |
| 2 | `03_silver_mt5.sql` | AUTO CDC 的键 / 删除 / 序列 / SCD2 / 用户的追踪列<br>AUTO CDC keys / delete / sequence / SCD2 / tracked columns for users |
| 3a, 3b | `03_silver_mt5.sql` | 两个无效交易的期望<br>Two expectations for invalid trades |
| 4 | `03_silver_mt5.sql` | 交易的只追加过滤器<br>Append-only filter for deals |
| 5 | `05_gold_reporting.sql` | USD 名义本金和客户盈亏<br>USD notional and client P&L |

使用 **Dry run** 验证，然后 **Run pipeline**。改坏了代码？从 `solutions/pipeline/transformations/` 复制原始文件。

Use **Dry run** to validate, then **Run pipeline**. Broke the code? Copy the original file from
`solutions/pipeline/transformations/`.

## 3 · 观察 (Observe)

- 图（Graph）：哪些表是流式表，哪些是物化视图？为什么？期望（Expectations）选项卡 `silver_mt5_deals`：有多少行被删除，按哪条规则删除？
- 运行 `00b_Live_Data`（`ticks` = 5）写入新文件，再次运行管道：只处理新文件（增量）；有些客户的杠杆可能会改变，在 `03b` 第 3 节里多出一个 SCD2 版本。然后打开 `labs/03b_Explore_Pipeline`。

- Graph: which tables are streaming tables, which are materialized views? Why?
- Expectations tab of `silver_mt5_deals`: how many rows were dropped, and by which rule?
- Run `00b_Live_Data` (`ticks` = 5) to write new files, then run the pipeline again: only the new
  files are processed (incremental); some clients' leverage may change, adding an SCD2 version you can
  see in section 3 of `03b`. Then open `labs/03b_Explore_Pipeline`.
