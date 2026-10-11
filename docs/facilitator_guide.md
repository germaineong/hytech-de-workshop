# 讲师指南：Hytech DE 工作坊 (Facilitator guide — Hytech DE workshop) · Zhi Han & Germaine

一位讲师主讲，另一位巡场答疑，每个模块轮换。面对 20 名新学员，巡场讲师直接决定是"多数人完成"还是"多数人卡住"。分工建议见 `docs/workshop_plan.md` §4。

One instructor presents while the other floats; swap per module. With 20 newcomers the floater is the difference between "most finished" and "most stuck". Split proposal in `docs/workshop_plan.md` §4.

## 开课前 (Before the day)

- [ ] Triones 已完成 `docs/setup_guide_triones.md`，包括**在会场完成的连通性测试**
- [ ] 参考答案作业已运行一次 → `hytech_ws_catalog.solutions` 已有数据（实验 07 和追进度都依赖它）
- [ ] `llm_endpoint` 已设为 Hytech 所在区域可用的模型（在 SQL 编辑器中测试 `ai_query`）
- [ ] Genie Code 技能在 `/Workspace/.assistant/skills/hytech-de-conventions/` 可见
- [ ] 成本仪表盘 *Hytech DE Workshop - Cost and Health* 已发布（setup 任务 `cost_dashboard`）
- [ ] CI/CD 演示：服务主体 `hytech-ws-cicd`、相关授权，以及一次成功的 `[cicd]` 运行（已于 10 月 3 日在 FEVM 完成）。当天由 `cicd/deploy_as_service_principal.sh` 自动创建并删除临时密钥（`docs/cicd_demo.md`）
- [ ] 幻灯片以 **PDF/PPTX 存在借用的笔记本电脑上**（中国大陆不使用 VPN 时无法访问 Google Workspace）
- [ ] 10 月 12 日（周一）晚上预演，最好使用一个 Hytech 学员账号
- [ ] Lakehouse RT：客户团队已为账户开启 Beta，Triones 已在 Previews 中开启，`setup/08` 已创建 `hytech_workshop_rt`（否则实验 09 用 `hytech_workshop_sql`）
- [ ] Genie：学员有 Databricks SQL 权限；在 ap-southeast-1 用学员账号确认 Agent 模式和 Genie One 对话可用（由 Triones 或一位学员操作，讲师在旁指导；可能需要账户管理员开启跨地域处理）
- [ ] 预演时用一个新的学员账号走一遍：00 → 03 → 03c（故意填错一次 `staging_root`）→ 03d 第 3 步（改 Run if → Repair run）→ 04 → 04c（含 Run backfill 对话框）→ 09（计算资源菜单）→ 10 + Genie One（由 Triones 或一位学员操作，讲师在旁指导）
- [ ] 在代码更新前运行过实验 00 的账号：删除 `hytech_de_lab/jobs/`，然后在共享文件夹中打开 `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here`，重新运行复制单元格（在自己的副本中运行时不会复制任何文件；复制不会覆盖已有文件，旧的作业笔记本不认识 `report_date`）

- [ ] Triones has completed `docs/setup_guide_triones.md`, including the **connectivity test from the venue**
- [ ] Solution job ran once → `hytech_ws_catalog.solutions` is populated (lab 07 and catch-up depend on it)
- [ ] `llm_endpoint` set to a model served in Hytech's region (test `ai_query` in the SQL editor)
- [ ] Genie Code skill visible at `/Workspace/.assistant/skills/hytech-de-conventions/`
- [ ] Cost dashboard *Hytech DE Workshop - Cost and Health* published (setup task `cost_dashboard`)
- [ ] CI/CD demo: service principal `hytech-ws-cicd`, its grants and a green `[cicd]` run (done on FEVM, 3 Oct). On the day, `cicd/deploy_as_service_principal.sh` creates and deletes its own temporary secret (`docs/cicd_demo.md`)
- [ ] Slides as **PDF/PPTX on the loaner laptop** (Google Workspace is blocked in mainland China without VPN)
- [ ] Dry run on Mon 12 Oct evening, ideally with one Hytech participant account
- [ ] Lakehouse RT: the account team enabled the Beta, Triones turned it on in Previews, and `setup/08` created `hytech_workshop_rt` (otherwise lab 09 uses `hytech_workshop_sql`)
- [ ] Genie: participants have the Databricks SQL entitlement; on ap-southeast-1, confirm Agent mode and Genie One chat with a participant account (Triones or a participant drives, the instructors guide; an account admin may need to turn on cross-Geo processing)
- [ ] At the dry run, walk one fresh participant account through 00 → 03 → 03c (mistype `staging_root` once) → 03d step 3 (edit a Run if → Repair run) → 04 (including the §4 file-arrival trigger: run 00b once, and the job should start within a few minutes) → 04c (including the Run backfill dialog) → 09 (compute menu) → 10 + Genie One (Triones or a participant drives; the instructors guide)
- [ ] Accounts that ran lab 00 before the code refresh (for example the dry-run accounts): delete their whole `hytech_de_lab/` folder, then open `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here` in the shared folder and run all of it (run from your own copy, it copies nothing; the copy never overwrites existing files, so old copies keep the shared landing path in labs 02–04 and the old job notebooks, which don't know `report_date` or the participant's own landing zone)

## 实时数据：每位学员运行 00b (Live data: each participant runs 00b)

课堂上不需要运行任何集中作业，也不需要 Triones 操作。每位学员在实验 00 第 1b 步把共享历史复制到自己的落地区（约 760 个文件、约 90 MB，只复制一次），之后需要新数据时自己运行 `00b_Live_Data`：

| 何时 (When) | 学员运行 (Participant runs) | 效果 (Effect) |
|---|---|---|
| 实验 02，第二次运行 Auto Loader 之前 | `00b_Live_Data`，`ticks` = 1 | 新的 CDC 文件 → Auto Loader 只处理新文件 |
| 实验 03，第二次运行管道之前 | `ticks` = 5 | 管道只处理新文件；可能有客户出现新的 SCD2 版本 |
| 实验 04 §4（文件到达触发器）| `ticks` = 1 | 自己的作业自动启动 |
| 实验 04 §6（可选，在 03d 之后）| `new_server=mt5-hk-01` | 上线一台新的 MT5 服务器（全量 + CDC），无需改代码即可接入 |
| 实验 04 §6（可选）| `bad_batch_pct=60`，然后运行作业 | DQ 闸门走 **false** 分支，`notify_dq_owner` 运行 |

提醒学员：一次只运行一个 00b；完成 03d 之后才上线 `mt5-hk-01`；写入坏批次（`bad_batch_pct`）之后马上运行一次自己的作业，否则 04c 的第一次管道更新会处理这些坏行，DQ 闸门走 false 分支。

作业 `hytech_ws_drip_producer` 仍然保留，但是可选的：它只写共享落地区（只有参考答案管道读取），用于讲师在自己的测试工作区里演示。不要在 Hytech 的课堂上运行它：学员重新运行实验 00 时，会把它写的文件复制进自己的落地区，成交编号会和他们自己的 tick 重复。

Nothing runs centrally during class, and Triones has nothing to operate. In step 1b of lab 00 each participant copies the shared history into their own landing zone (about 760 files, about 90 MB, once); afterwards they run `00b_Live_Data` whenever a lab needs new data:

| When | Participant runs | Effect |
|---|---|---|
| Lab 02, before the second Auto Loader run | `00b_Live_Data` with `ticks` = 1 | New CDC files → Auto Loader processes only the new files |
| Lab 03, before the second pipeline run | `ticks` = 5 | The pipeline processes only the new files; some clients may get a new SCD2 version |
| Lab 04 §4 (file-arrival trigger) | `ticks` = 1 | Their own job starts by itself |
| Lab 04 §6 (optional, after 03d) | `new_server=mt5-hk-01` | Onboards a new MT5 server (full load + CDC); ingested with no code change |
| Lab 04 §6 (optional) | `bad_batch_pct=60`, then run the job | The DQ gate takes the **false** branch and `notify_dq_owner` runs |

Remind people: one 00b run at a time; onboard `mt5-hk-01` only after 03d; after a bad batch (`bad_batch_pct`), run their own job once straight away, otherwise the first pipeline update in 04c processes the bad rows and the DQ gate takes the false branch.

The job `hytech_ws_drip_producer` stays but is optional: it writes only to the shared landing zone (which only the solution pipeline reads), for an instructor's demo on their own test workspace. Don't run it in the Hytech class: a participant who re-runs lab 00 would copy its files into their own landing zone, and its deal numbers would repeat their own ticks'.

## 第 1 天流程 (Day 1 — run of show)

| 时间 (Time) | 环节 (Segment) | 备注 (Notes) |
|---|---|---|
| 09:30 | **M1** 平台概览（30）| 数据湖仓、Unity Catalog、无服务器、声明式管道、Genie。展示 Hytech 的架构（MySQL → DMS → S3 → Auto Loader → SDP → Power BI/Genie），对照 `docs/workshop_plan.md` §2。**实验 00** 放在最后 10 分钟：每人创建自己的 schema 并复制实验 |
| 10:00 | **M2** Unity Catalog（30）| 实验 01。讲解要点：标签 + 掩码就是 Robin 那套列标签检查框架的 UC 原生版本；受治理标签会强制限定允许的取值。演示：Catalog Explorer 血缘、访问申请 |
| 10:30 | **M3** 数据接入（30）| 实验 02。学员运行一次 `00b_Live_Data`（`ticks` = 1）后重新运行 Auto Loader 单元格，指出它只处理新文件。演示：Lakeflow Connect NetSuite（GA）和 MySQL CDC（预览版），作为不依赖 DMS 的方案。缩短为 30 分钟：Lakeflow Connect 只做 5 分钟演示 |
| 11:00 | **M4** 声明式管道（90）| 实验 03：按 README 创建管道，讲解代码中的 5 个要点，运行。**要点：追加 vs MERGE**：mt5_deals 约 99.9% 是插入 → 只追加 + 很小的更正表。在 Hytech 的 POC 中，这次重新设计把成本从约 $480/天降到 $40/天以下（数字仅供讲师掌握）。演示：用 Lakeflow Designer 制作 IB 周报。然后实验 03b（10）→ **03c（20）**：每人在自己的预发布通道写坏批次。强调 FAIL 只让 silver 失败，下游跳过，其他表照常完成。**提醒：第 4 步之后管道会一直失败，直到第 6 步**；落后的学员把 `auto` 设为 `true`。→ **03d（15）**：6 种 Run if，看作业图的颜色（概念 15 · 实验 03 25 · 03b 10 · 03c 20 · 03d 15 · Lakeflow Designer 演示 5）|
| 12:30 | 问答（30）| 追进度：落后的学员直接运行自己的管道（代码已完整）。离开前确认每个人 03c 的最后一个检查是 ✅（管道是绿色的，第 2 天的实验 04 需要它）；落后的学员把 03c/03d 的 `auto` 设为 `true` |

| Time | Segment | Notes |
|---|---|---|
| 09:30 | **M1** Platform overview (30) | Lakehouse, UC, serverless, Lakeflow, Genie. Show Hytech's architecture (MySQL → DMS → S3 → Auto Loader → SDP → Power BI/Genie) next to `docs/workshop_plan.md` §2. **Lab 00** last 10 min: everyone creates their schema and copies the labs |
| 10:00 | **M2** Unity Catalog (30) | Lab 01. Talk track: tags + masks are the UC-native version of Robin's column-tag check framework; governed tags enforce allowed values. Demo: Catalog Explorer lineage, access requests |
| 10:30 | **M3** Ingestion (30) | Lab 02. People run `00b_Live_Data` once (`ticks` = 1), then re-run the Auto Loader cell: point out the new files only. Demo: Lakeflow Connect NetSuite (GA) and MySQL CDC (preview) as the DMS-free path. Shortened to 30 min: Lakeflow Connect is a 5-min demo |
| 11:00 | **M4** Declarative Pipelines (90) | Lab 03: create the pipeline (README), walk through the 5 key points in the code, run. **Lesson: append vs MERGE**: mt5_deals is about 99.9% inserts → append-only + tiny corrections table. In Hytech's POC this redesign cut cost from about $480/day to under $40/day (facilitator-only number). Demo: Lakeflow Designer builds the IB weekly report. Then lab 03b (10) → **03c (20)**: everyone writes a bad batch into their own staging lane. Stress that FAIL fails only the silver table, downstream is skipped and every other table completes. **Remind them: after step 4 the pipeline keeps failing until step 6**; anyone behind sets `auto` to `true`. → **03d (15)**: the six Run if conditions; read the colours in the job graph (concept 15 · lab 03 25 · 03b 10 · 03c 20 · 03d 15 · Lakeflow Designer demo 5) |
| 12:30 | Q&A (30) | Catch-up: anyone behind just runs their own pipeline (the code is complete). Before people leave, check that everyone's last 03c check is ✅ (a green pipeline, which Day 2's lab 04 needs); anyone behind sets `auto` to `true` in 03c/03d |

## 第 2 天流程 (Day 2 — run of show)

| 时间 (Time) | 环节 (Segment) | 备注 (Notes) |
|---|---|---|
| 09:30 | **M5** Lakeflow 作业（60）| 实验 04（UI）。第一次运行会按设计在 `mt5-uk-01` 上失败 → 修复运行（清空 `fail_server`，保留 `servers`）。讲解 *Succeeded with failures*（成功但有失败）。可选（实验 04 §4、§6，学员用 `00b_Live_Data` 自己完成）：文件到达触发器、上线 `mt5-hk-01`、坏批次 → DQ false 分支。演示：**以服务主体身份执行 `bundle deploy -t cicd`**（`docs/cicd_demo.md`，对应 Hytech "禁止直接访问生产环境"的优先事项）。追进度：`04b_Jobs_Catch_Up`。实验 04 之后做 **04c（25）**，时间很紧：第 2 步的运行（约 5 分钟）期间让学员读第 3–4 步；修复运行约 4 分钟；回填两天约 7 分钟：时间不够时照常对两天启动 Run backfill，然后进入下一个模块，回填在后台完成，课后再运行检查单元格。本次议程没有时间做上面的可选部分和演示：坏批次和文件到达触发器由 04c 在每个人自己的通道里演示（文件到达是 04c 的加分题）；CI/CD 演示放到第 2 天问答，或跳过。检查单元格会等学员点 Run now / Repair run / Run backfill |
| 10:30 | **M6** Genie Code + 系统表（60）| **Genie Code（30）：**实验 05 提示词阶梯。先打开技能（把团队约定写成代码，是 Ray 的优先事项）。适合现场演示：用中文输入 L1 **系统表（30）：**打开 *Hytech DE Workshop - Cost and Health* 仪表盘（链接由 setup 任务 `cost_dashboard` 打印），然后通过受治理的 `ops` 视图做实验 06。账单有数小时延迟，所以看第 1 天的运行。呼应 Hytech 每周的成本复盘（成本报表由 Triones 的团队负责）。预告：Genie ZeroOps（私有预览版）。回顾放到第 2 天问答开始时（5 分钟回顾）；测验 08 作为课后作业 |
| 11:30 | **M7** AI/BI 仪表盘、Lakehouse RT 与 Genie（60）| 实验 09（15）：每人搭建仪表盘，把计算资源切换到 `hytech_workshop_rt`，对比查询历史。Lakehouse RT 讲解（5）。实验 10（25）：创建并配置 Genie Agent，然后用 Genie One；先提问让它自动找 Agent，再用 Ask → 选择 Agent。Lark 演示（10，讲师自己的环境）。缓冲（5） |
| 12:30 | 问答（30）| 先做 5 分钟回顾（两天的内容对应考试各部分）；反馈表；实验 07（AI 函数）和测验 08 作为可选的课后练习 |

| Time | Segment | Notes |
|---|---|---|
| 09:30 | **M5** Lakeflow Jobs (60) | Lab 04 (UI). First run fails on `mt5-uk-01` by design → Repair run (clear `fail_server`, keep `servers`). Explain *Succeeded with failures*. Optional (lab 04 §4 and §6, each person with `00b_Live_Data`): the file-arrival trigger, onboarding `mt5-hk-01`, a bad batch → DQ false branch. Demo: **bundle deploy `-t cicd` as a service principal** (`docs/cicd_demo.md`; Hytech's "no direct prod access" priority). Catch-up: `04b_Jobs_Catch_Up`. After lab 04, **04c (25)** is tight: have people read steps 3–4 while the step 2 run goes (about 5 min); the repair takes about 4 min and the two-day backfill about 7: if time is short, start Run backfill for both days as usual and move on; the runs finish in the background and people re-run the check cell later. This agenda has no time for the optional parts and the demo above: 04c covers the bad batch and the file-arrival trigger in each person's own lane (file arrival is 04c's bonus); move the CI/CD demo to the Day 2 Q&A, or skip it. The check cells wait for people to click Run now / Repair run / Run backfill |
| 10:30 | **M6** Genie Code + system tables (60) | **Genie Code (30):** Lab 05 ladder. Open the skill first (team conventions as code, Ray's priority). Good live prompt: L1 in Chinese **System tables (30):** Open the *Hytech DE Workshop - Cost and Health* dashboard (link printed by setup task `cost_dashboard`), then Lab 06 via governed `ops` views. Billing lags by hours, so look at Day-1 runs. Tie back to Hytech's weekly cost review (Triones' team owns cost reporting). Teaser: Genie ZeroOps (private preview). The recap moves to the start of the Day 2 Q&A (a 5-minute recap); quiz 08 becomes homework |
| 11:30 | **M7** AI/BI dashboards, Lakehouse RT and Genie (60) | Lab 09 (15): everyone builds a dashboard, switches its compute to `hytech_workshop_rt` and compares query history. Lakehouse RT talk (5). Lab 10 (25): create and configure a Genie Agent, then Genie One: ask once routed, then Ask → pick the agent. Lark demo (10, the instructor's own environment); buffer (5) |
| 12:30 | Q&A (30) | A 5-minute recap first (map the two days to the exam sections); feedback form; lab 07 (AI Functions) and quiz 08 as optional homework |

## 精彩时刻：提前安排 (Wow moments — stage them)

1. 一个 CDC 文件到达 → 下一次管道更新只处理这个文件；某个客户出现 SCD2 历史记录。
2. 更正表上的 `DESCRIBE HISTORY` 显示 MERGE 在重写文件，对比成交表上的纯追加。
3. 现场上线新服务器 → 下一次运行直接成功，无需改代码。
4. 坏批次 → 期望丢弃这些行 → 作业自动走告警分支。
5. Genie Code 根据中文提示词写出 gold 物化视图，并遵守团队技能。
6. 每位学员在成本仪表盘和实验 06 中看到自己第 1 天管道的成本。
7. 学员自己的坏批次让 silver 失败，而隔离表照常完成，显示出这 5 行（03c）。
8. 同一次运行修复后变绿，坏着时到达的文件一行不丢（04c）。
9. 同一个仪表盘切换到 Lakehouse RT。
10. 用中文问 Genie Agent，看它用上你写的同义词和说明。

**Wow moments (stage them)**

1. A CDC file lands → the next pipeline update processes only that file; SCD2 history appears for a client.
2. `DESCRIBE HISTORY` on the corrections table shows MERGE rewriting files, vs plain appends on deals.
3. New server onboarded live → green on the next run with zero code change.
4. Bad batch → expectations drop rows → the job takes the alert branch on its own.
5. Genie Code writes a gold MV from a Chinese prompt and follows the team skill.
6. Each participant sees the cost of their own Day-1 pipeline, on the cost dashboard and in Lab 06.
7. A participant's own bad batch fails the silver table, while the quarantine completes and shows the 5 rows (03c).
8. The same run turns green after the repair, and the files that arrived while it was broken lose no row (04c).
9. The same dashboard switched to Lakehouse RT.
10. Ask the Genie Agent in Chinese and watch it use the synonyms and instructions you wrote.

## 故障恢复 (Recovery playbook)

详见 `docs/troubleshooting.md`。最常见的情况：

- **改代码后出现语法错误** → 从 `solutions/` 复制原始文件。
- **事件日志未发布** → `dq_gate` 失败。修改管道设置后重新运行。
- **修复运行被拒** → For each 的迭代次数变了；保持 `servers` 不变。
- **网络问题** → 找 Triones 检查 IP 允许列表，或改用热点。
- **03c 第 4 步之后管道一直失败** → 完成第 6 步（策略修复），或把 `start_over` 设为 `true` 从头运行。
- **03c 第 2 步有 ❌** → 按 ❌ 后面的提示修改配置或移动文件，再运行检查单元格。
- **04c 检查单元格一直在等** → 学员还没点 Run now / Repair run / Run backfill。最多等 10 分钟（回填 5 分钟），之后单元格报错：点击后再运行一次。第 5 步要在失败的那次运行页面点 Repair run，不是 Run now。
- **实验 09 的计算资源菜单里没有 Real-Time 仓库** → Lakehouse RT 没开启或没有 Can use：用 `hytech_workshop_sql`（或 Serverless Starter）完成。
- **实验 10 看不到 Agent 模式或 Genie One 对话** → 跨地域处理（账户管理员）。

**Recovery playbook**

See `docs/troubleshooting.md`. Most common:

- **Syntax errors after editing the code** → copy the original file from `solutions/`.
- **Event log not published** → `dq_gate` fails. Fix the pipeline setting and re-run.
- **Repair rejected** → the for-each iteration count changed; keep `servers` the same.
- **Network** → Triones, the IP allow list, or a hotspot.
- **Pipeline keeps failing after 03c step 4** → finish step 6 (the policy fix), or set `start_over` to `true` and run from the top.
- **❌ in 03c step 2** → fix the setting or move the file as the ❌ says, then run the check cell again.
- **A 04c check cell keeps waiting** → the participant hasn't clicked Run now / Repair run / Run backfill yet. It waits up to 10 min (5 for the backfill), then errors: click, then run the cell again. In step 5, click Repair run on the failed run's page, not Run now.
- **No Real-Time warehouse in lab 09's compute menu** → Lakehouse RT is off or there's no Can use: finish on `hytech_workshop_sql` (or Serverless Starter).
- **Lab 10: no Agent mode or Genie One chat** → cross-Geo processing (an account admin).

## 在 FEVM 上的验证结果 (Verified on FEVM, `fe-vm-zh-serverless-ws`, 3 Oct 2026)

| 检查 (Check) | 结果 (Result) |
|---|---|
| setup 作业 | ✅ 4 台服务器共约 78.5 万笔成交（每台 1 个 LOAD + 48 个 CDC 文件），14.1 万条 App 事件 |
| 参考答案管道 | ✅ 丢弃 1,238 笔无效成交，标记 299 笔未来日期成交；SCD2 历史正确；应用 151 条更正/删除；去重后 14 万条事件；恢复 1.2 万个救援数据值 |
| 参考答案作业 — 正常路径 | ✅ DQ 门为 true → 中文 AI 日报；For each 4/4 成功；告警任务被跳过；审计已运行 |
| For each 失败 | ✅ 运行条件告警 + 审计都已运行；运行状态 = *Succeeded with failures* |
| 上线 `mt5-hk-01` + 修复运行 | ✅（修复运行必须保持相同的迭代次数） |
| 坏批次 (60%) | ✅ 丢弃 162/3,229 (5%) → false 分支 → DQ 告警；日报被跳过 |
| 学员流程 | ✅ 实验 00 → 自己的管道（用追进度文件）→ 04b 作业 → 模拟失败 → 修复运行 → 成功 |
| 实验笔记本 | ✅ 01、02、03b、06、07 已无界面执行；`ai_mask` 遮蔽了电话和邮箱 |
| 成本仪表盘 | ✅ 由 setup 任务 `cost_dashboard` 发布；两页均正常显示；学员组只读，仅管理员可管理；统计有任务失败的运行 |
| 实验 06 作业时长 | ✅ 用时间段的起止时间计算（无服务器运行的 `run_duration_seconds` 为 0） |
| CI/CD（`cicd` 目标）| ✅ 服务主体 `hytech-ws-cicd` 通过 OAuth M2M 部署并运行：作业和管道归它所有并以它的身份运行，`users` 只读，`solutions_cicd` 中 23 张表归它所有，约 3.5 分钟成功完成，临时密钥已删除 |
| 一键安装 (`00_master_setup`) | ✅ 创建 3 个作业和 1 个管道，依次运行 setup 作业和参考答案作业（约 6 分钟，solutions 中 78.7 万笔成交和中文 AI 日报）；重复运行时原地更新，不重复创建；学员组只读 |

| Check | Result |
|---|---|
| Setup job | ✅ ~785k deals across 4 servers (1 LOAD + 48 CDC files each), 141k app events |
| Solution pipeline | ✅ 1,238 invalid trades dropped, 299 future-dated flagged; SCD2 history; 151 corrections/deletes applied; 140k de-duplicated events; 12k rescued values recovered |
| Solution job — happy path | ✅ DQ gate true → Chinese AI summary; for-each 4/4 OK; alert excluded; audit ran |
| for-each failure | ✅ Run-if alert + audit ran; run = *Succeeded with failures* |
| Onboard `mt5-hk-01` + repair | ✅ (repair must keep the same iteration count) |
| Bad batch (60%) | ✅ 162/3,229 dropped (5%) → false branch → DQ alert; summary excluded |
| Participant flow | ✅ Lab 00 → own pipeline (catch-up files) → 04b job → simulated failure → repair → green |
| Lab notebooks | ✅ 01, 02, 03b, 06, 07 executed headless; `ai_mask` masked phones/emails |
| Cost dashboard | ✅ Published by setup task `cost_dashboard`; both pages render; view-only for the group, only admins manage it; counts runs with failed tasks |
| Lab 06 job durations | ✅ Computed from the period timestamps (`run_duration_seconds` is 0 for serverless runs) |
| CI/CD (`cicd` target) | ✅ Deployed and run by service principal `hytech-ws-cicd` over OAuth M2M: job and pipeline owned by and run as it, `users` CAN VIEW, 23 tables in `solutions_cicd` owned by it, green in ~3.5 min, temporary secret deleted |
| Master setup (`00_master_setup`) | ✅ Created 3 jobs and the pipeline, ran the setup job and the solution job (~6 min; 787k deals and the Chinese AI summary in the solutions schema); a re-run updated everything in place with no duplicates; view-only for the participant group |
