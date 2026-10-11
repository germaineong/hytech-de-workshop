# 08 · 知识测验 (Knowledge check)

*M6c · 15 道单选题 + 1 道加分题 · 约 15 分钟*

题目覆盖两天的内容，并对应 **Databricks Certified Data Engineer Associate** 考试大纲（2026 年 5 月版）。
讲师会用举手、纸笔或飞书问卷来答题，然后逐题讲解。
*(15 single-choice questions plus a bonus. They cover both days and map to the Data Engineer Associate exam guide, May 2026 version. The instructor runs it and then goes through each answer.)*

---

**1 · 平台 (Platform)**
路径 `/Volumes/hytech_ws_catalog/raw/landing/` 中，`hytech_ws_catalog`、`raw`、`landing` 分别是什么？
*(In this path, what are `hytech_ws_catalog`, `raw` and `landing`?)*

- A. 工作区 / 文件夹 / 文件 (workspace / folder / file)
- B. metastore / catalog / schema
- C. catalog / schema / volume
- D. S3 存储桶 / 前缀 / 对象 (S3 bucket / prefix / object)

**2 · 治理 (Governance)**
要让组 `de_workshop_sz` 能查询 `hytech_ws_catalog.solutions.gold_daily_symbol_volume`，最少需要授予哪些权限？
*(What is the minimum set of privileges the group needs to query this table?)*

- A. 只需要表上的 `SELECT` (only `SELECT` on the table)
- B. catalog 上的 `ALL PRIVILEGES`
- C. 表上的 `MODIFY`
- D. catalog 上的 `USE CATALOG` + schema 上的 `USE SCHEMA` + 表上的 `SELECT`

**3 · 治理 (Governance)**
要求：合规组以外的人看到的 `email` 必须被掩码；Apex 品牌的分析师只能看到 Apex 的客户。最合适的做法是？
*(Everyone outside compliance must see a masked `email`, and Apex analysts may only see Apex clients. Best approach?)*

- A. 在 `email` 列上设置列掩码 (column mask)，在表上设置行过滤器 (row filter)
- B. 给 `email` 列打上 `hytech_pii` 标签就够了 (tagging the column is enough)
- C. 每个品牌复制一张表，分别授权 (one copy of the table per brand)
- D. 只把 `SELECT` 授予合规组 (grant `SELECT` to compliance only)

**4 · 治理 (Governance)**
关于 Unity Catalog 的血缘 (lineage)，哪一项正确？
*(Which statement about lineage in Unity Catalog is true?)*

- A. 需要在每个作业里手动登记血缘 (it must be registered manually in each job)
- B. 只记录表级血缘，不记录列级 (table level only, no column level)
- C. 在 Unity Catalog 计算上运行的查询、管道和作业会被自动记录，包括列级血缘 (captured automatically, including column level)
- D. 只有物化视图才有血缘 (only materialized views have lineage)

**5 · 数据接入 (Ingestion)**
关于 `COPY INTO` 和 Auto Loader，哪一项正确？
*(Which statement is true?)*

- A. 两者重复运行时都会重新加载所有文件 (both reload every file when re-run)
- B. `COPY INTO` 重复运行会跳过已加载的文件；Auto Loader 在 checkpoint 中记录已发现的文件，适合持续到达的大量文件
  *(re-running `COPY INTO` skips files already loaded; Auto Loader tracks discovered files in its checkpoint and suits large numbers of continuously arriving files)*
- C. Auto Loader 只能读取 JSON (Auto Loader only reads JSON)
- D. `COPY INTO` 必须指定 checkpoint 路径 (`COPY INTO` needs a checkpoint path)

**6 · 数据接入 (Ingestion)**
用显式 schema 读取 App 埋点 JSON，并开启了 `rescuedDataColumn`。某条记录的 `amount` 是字符串 `"1,234.50"`，而 schema 中是 `DOUBLE`。结果是？
*(The schema says `amount` is `DOUBLE`, one record has the string `"1,234.50"`, and `rescuedDataColumn` is on. What happens?)*

- A. 整条记录被丢弃 (the record is dropped)
- B. 管道更新失败 (the pipeline update fails)
- C. schema 自动改成 STRING (the schema changes to STRING)
- D. `amount` 为 NULL，原始值保存在 `_rescued_data` 列中 (`amount` is NULL and the original value is kept in `_rescued_data`)

**7 · 转换与建模 (Transformation)**
下列哪一个最适合定义为流式表 (streaming table)，而不是物化视图 (materialized view)？
*(Which one is best defined as a streaming table rather than a materialized view?)*

- A. 从 Volume 增量读取只追加的 DMS Parquet 文件（bronze）(incrementally reading append-only DMS Parquet files from a volume)
- B. 按日、按品种汇总的交易量 (daily volume by symbol)
- C. 需要反映历史更正的 IB 业绩汇总 (IB performance that must reflect past corrections)
- D. 与经常更新的参考表 join 的报表 (a report joined to a frequently updated reference table)

**8 · 转换与建模 (Transformation)**
在 `AUTO CDC INTO silver_mt5_users ... KEYS (server_id, login) SEQUENCE BY cdc_ts` 中，`SEQUENCE BY` 的作用是？
*(What does `SEQUENCE BY` do here?)*

- A. 决定输出表的排序 (sorts the output table)
- B. 处理乱序到达的变更：按 `cdc_ts` 判断每个键的哪条变更最新 (orders the changes for each key, so late or out-of-order events are applied correctly)
- C. 定义分区列 (defines the partition column)
- D. 定义主键 (defines the primary key)

**9 · 转换与建模 (Transformation)**
`CONSTRAINT valid_trade_price EXPECT (price > 0) ON VIOLATION DROP ROW` 会怎样？
*(What does this expectation do?)*

- A. 违规行仍写入目标表，只是被标记 (violating rows are kept but flagged)
- B. 整个管道更新失败 (the whole update fails)
- C. 违规行被丢弃，丢弃数量记录在管道事件日志 (event log) 的数据质量指标中 (violating rows are dropped and the counts are recorded in the event log)
- D. 违规行自动写入隔离表 (violating rows go to a quarantine table automatically)

**10 · 优化 (Optimization)**
`mt5_deals` 约 99.9% 是插入。为什么用"只追加的流式表 + 很小的 corrections AUTO CDC 表"，而不是对整张 deals 表做 AUTO CDC？
*(About 99.9% of `mt5_deals` changes are inserts. Why use an append-only table plus a tiny corrections table instead of AUTO CDC on the whole deals table?)*

- A. AUTO CDC 不支持删除 (AUTO CDC cannot apply deletes)
- B. 流式表不能定义期望 (streaming tables cannot have expectations)
- C. 物化视图不能 join (materialized views cannot join)
- D. MERGE 每批都要扫描目标表来匹配键，并重写包含匹配行的文件，表越大越贵；追加只写入新数据
  *(MERGE scans the target to match keys and rewrites the files holding matched rows, so it costs more as the table grows; an append only writes new data)*

**11 · Lakeflow 作业 (Jobs)**
希望 `alert_on_failure` 只在上游有任务失败时运行，而 `write_audit_row` 无论成功失败都运行。它们的 **Run if** 分别是？
*(`alert_on_failure` should run only when an upstream task failed; `write_audit_row` should always run. Which Run if settings?)*

- A. `At least one failed` / `All done`
- B. `All succeeded` / `All done`
- C. `None failed` / `All succeeded`
- D. `All failed` / `At least one succeeded`

**12 · Lakeflow 作业 (Jobs)**
`dq_gate` 任务设置了任务值 (task value) `dq_drop_pct`。If/else 条件任务里如何引用它？
*(How does the If/else condition task reference this task value?)*

- A. `${dq_gate.dq_drop_pct}`
- B. `dbutils.widgets.get("dq_drop_pct")`
- C. `{{tasks.dq_gate.values.dq_drop_pct}}`
- D. `{{job.parameters.dq_drop_pct}}`

**13 · Lakeflow 作业 (Jobs)**
一次运行中，`reconcile_servers`（For each）有一个迭代失败了。点击 **Repair run（修复运行）** 会怎样？
*(One iteration of the for-each task failed. What does a repair run do?)*

- A. 从头重跑所有任务 (re-runs every task from the start)
- B. 只重跑未成功的任务和依赖它们的下游任务；已成功的任务不重跑，仍是同一个运行
  *(re-runs only the unsuccessful tasks and the tasks that depend on them, within the same run)*
- C. 生成新的 run_id，并删除原来的运行记录 (creates a new run ID and deletes the original run)
- D. 修复时可以把 For each 的输入列表改成任意长度 (you can change the for-each input list to any length)

**14 · CI/CD**
用 `databricks bundle deploy -t dev`（`mode: development`）部署后，会发生什么？
*(What happens when you deploy to a target with `mode: development`?)*

- A. 资源名称加上 `[dev 你的用户名]` 前缀，计划和触发器默认暂停 (resource names get a `[dev <your name>]` prefix, and schedules and triggers are paused)
- B. 直接覆盖生产环境的作业 (it overwrites the production jobs)
- C. 作业以服务主体身份运行 (jobs run as a service principal)
- D. 只做校验，不上传文件 (it only validates and uploads nothing)

**15 · 监控与成本 (Monitoring)**
想知道你的管道每天花了多少美元，需要组合哪两张系统表？
*(Which two system tables give your pipeline's cost in USD per day?)*

- A. `system.access.audit` + `system.compute.clusters`
- B. `system.billing.usage` + `system.billing.list_prices`
- C. `system.lakeflow.jobs` + `system.query.history`
- D. `system.information_schema.tables` + `system.billing.usage`

---

**加分题 (Bonus, after Lab 07)**
用 `ai_query` 生成中文日报时，哪种做法最可靠？
*(What is the most reliable way to produce the Chinese daily summary with `ai_query`?)*

- A. 把所有成交明细交给模型，让它自己汇总 (send every deal and let the model aggregate)
- B. 让模型把美元换算成"万美元"方便阅读 (ask the model to convert dollars to 万美元)
- C. 调高 temperature，让文字更生动 (raise the temperature for livelier text)
- D. 先在 SQL 里算好所有数字，让模型只负责措辞、原样引用数字，再抽查结果 (compute every number in SQL, have the model quote them as-is, then spot-check)
