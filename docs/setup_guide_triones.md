# 环境准备指南 · Workspace setup guide (for Triones / workshop admin)

*Hytech "Data Engineering with Databricks" workshop · Shenzhen · 13–14 Oct 2026*

**请在 10 月 9 日（周五）前完成**，这样我们可以在 10 月 12 日（周一）做预演。预计需要约 30 分钟，大部分是等待时间。

**Please finish by Fri 9 Oct** so we can do a dry run on Mon 12 Oct. Expect about 30 minutes, most of it waiting.

## 0 · 你需要 (You need)

| 要求 (Requirement) | 原因 (Why) |
|---|---|
| 工作区管理员（或 catalog 所有者 + `CREATE CATALOG`）| 创建 catalog、volume、授权和 Genie Code skill |
| 无服务器计算（notebook、作业、管道）和一个无服务器 SQL 仓库（Small，最多 3 个集群）| 所有实验都使用无服务器 |
| 基础模型 API 端点可用，例如 `databricks-claude-sonnet-4-5`（或其他对话模型）| 在第 04 和 07 个实验中使用 `ai_query`。在 ap-southeast-1 区域可能需要启用*跨地域处理* |
| 启用 Genie Code | 第 05 个实验 |
| **Lakehouse RT（Beta）**：先请 Databricks 客户团队为账户开启，然后在工作区 Previews 中开启（见 §2b）<br>**Lakehouse RT (Beta)**: first the Databricks account team enables it for the account, then turn it on in the workspace Previews (see §2b) | 实验 09 的计算资源选择；没有时用 `hytech_workshop_sql`（或 Serverless Starter）<br>Lab 09's compute choice; without it, `hytech_workshop_sql` (or Serverless Starter) |
| Genie：学员有 Databricks SQL 权限；ap-southeast-1 可能需要账户管理员开启跨地域处理<br>Genie: participants have the Databricks SQL entitlement; ap-southeast-1 may need an account admin to turn on cross-Geo processing | 实验 10（Genie Agent、Genie One）<br>Lab 10 (Genie Agent, Genie One) |
| **账户级**组 `de_workshop_sz` 包含约 20 位学员（通过 IdP/SCIM）| Unity Catalog 授权只适用于账户级组。在工作区内创建的组**不**工作 |
| 深圳办公室出站 IP 在工作区的 **IP 访问列表**中| 学员必须从会场网络访问工作区 |

## 1 · 导入代码 (Get the code into the workspace)

**选项 A — Git 文件夹（推荐）:** *Workspace → Create → Git folder* → 输入仓库 URL（向 Zhi Han 请求访问权限）→ 克隆到 `/Workspace/Shared/hytech-de-workshop`。

**Option A — Git folder (recommended):** *Workspace → Create → Git folder* → URL of the repo (ask Zhi Han for
access) → clone into `/Workspace/Shared/hytech-de-workshop`.

**选项 B — zip 文件:** *Workspace → Import* 将仓库归档导入 `/Workspace/Shared/hytech-de-workshop`。

**Option B — zip:** *Workspace → Import* the repo archive into `/Workspace/Shared/hytech-de-workshop`.

## 2 · 安装：一键安装笔记本，推荐 (Set up: the master setup notebook, recommended)

打开 `/Workspace/Shared/hytech-de-workshop/setup/00_master_setup` → 右上角选择 **Serverless** → 填写顶部参数 → **Run all**（约 15 分钟）。它会创建下表中的作业和管道，再依次运行 setup 作业（第 3 节）和参考答案作业（第 4 节）。不需要 Databricks CLI，也不需要修改 `databricks.yml`。可以重复运行：同名的作业和管道会原地更新。

Open `/Workspace/Shared/hytech-de-workshop/setup/00_master_setup` → attach **Serverless** → fill in the widgets at the top → **Run all** (about 15 minutes). It creates the jobs and the pipeline below, then runs the setup job (section 3) and the solution job (section 4) in order. No Databricks CLI needed, and no change to `databricks.yml`. Safe to re-run: jobs and the pipeline with the same name are updated in place.

| 参数 (Widget) | 值 (Value) |
|---|---|
| `catalog` | `hytech_ws_catalog` |
| `participant_group` | `de_workshop_sz` |
| `participants` | 学员邮箱，逗号分隔<br>Participant emails, comma-separated |
| `instructors` | Zhi Han 和 Germaine 的邮箱：他们会获得作业、管道和成本仪表盘的管理权限<br>Zhi Han's and Germaine's emails: they get CAN MANAGE on the jobs, the pipeline and the cost dashboard |
| `llm_endpoint` | 你的模型端点，例如 `databricks-claude-sonnet-4-5`<br>Your model endpoint, e.g. `databricks-claude-sonnet-4-5` |
| `warehouse_id` | 可选：成本仪表盘使用的 SQL 仓库<br>Optional: SQL warehouse for the cost dashboard |
| 其他 (Others) | 保持默认：`scale=full`、`reset=false`，两个运行选项为 `true`<br>Keep the defaults: `scale=full`, `reset=false`, both run options `true` |

**方式 B：bundle (Option B: the bundle).** 如果更想用 CLI：从 Git 文件夹打开 `databricks.yml` → 点击 **Deploy**（bundle UI），或使用 CLI，然后按第 3、4 节手动运行两个作业：

If you prefer the CLI: from the Git folder, open `databricks.yml` → **Deploy** (bundle UI), or use the CLI, then run the two jobs yourself as in sections 3 and 4:

```bash
databricks bundle deploy -t prod --var="catalog=hytech_ws_catalog" --var="llm_endpoint=<your endpoint>"
```

先在 `databricks.yml` 中设置 `workspace.host` 为你的工作区 URL。（`cicd` 目标是讲师的 CI/CD 演示，详见 `docs/cicd_demo.md`；你不需要部署它。）这会创建：

Set `workspace.host` in `databricks.yml` to your workspace URL first. (The `cicd` target is the instructors'
CI/CD demo, see `docs/cicd_demo.md`; you don't need to deploy it.) This creates:

| 资源 (Resource) | 用途 (Purpose) |
|---|---|
| Job `hytech_ws_setup` | Catalog、schema、volume、授权、综合数据、学员 schema、ops 视图、成本仪表盘、Genie Code skill |
| Job `hytech_ws_drip_producer` | 可选：向共享落地区写入新 CDC 文件，供讲师在自己的测试工作区演示。课堂上不要运行：学员用 `labs/00b_Live_Data` 生成自己的数据 |
| Pipeline `hytech_trade_lakehouse_solution` | 讲师参考答案（追进度、AI 实验数据）|
| Job `hytech_daily_trading_reporting_solution` | 第 04 个实验的讲师参考答案 |

## 2b · Lakehouse RT 与 Genie（实验 09 和 10）(Lakehouse RT and Genie, labs 09 and 10)

0. 先更新工作区中的代码：Git 文件夹 → **Pull**（选项 A），或重新导入 zip（选项 B）。旧代码中没有 `setup/08_workshop_warehouses` 和新的实验。
1. 先检查 Lakehouse RT 的限制：没有 serverless egress control、出站 Private Link 或合规安全配置；workshop catalog 不在 Unity Catalog 默认存储中（在 Catalog Explorer 中查看 catalog 的存储位置）。有任何一项，Lakehouse RT 就不能用：跳过第 2–3 步，实验 09 用 `hytech_workshop_sql` 完成。
2. 请 Databricks 客户团队为账户开启 **Lakehouse//RT Beta**（需要提前几天）。
3. 工作区菜单 → **Previews** → 搜索 "Lakehouse RT" → 开启。
4. 重新运行 `setup/00_master_setup`（可以重复运行）。设置作业的 `workshop_warehouses` 任务（`setup/08_workshop_warehouses`）会创建工作坊 SQL 仓库 `hytech_workshop_sql`（serverless，Small，1–3 个集群，空闲 10 分钟自动停止），Lakehouse RT 开启时再创建 `hytech_workshop_rt`（Real-Time，Small），并授予 `de_workshop_sz` *Can use*。想用已有的仓库代替 `hytech_workshop_sql`：在 `00_master_setup` 中设置 `warehouse_id`。
5. Genie：学员需要 **Databricks SQL** 权限。在 ap-southeast-1，如果看不到 Agent 模式或 Genie One 对话，请账户管理员在账户控制台中开启跨地域处理 (cross-Geo processing)；这也需要提前安排。
6. 用一个**非管理员**学员账号测试（由你或一位学员操作）：创建管道和作业、实验 03c 的预发布通道、实验 09 的计算资源菜单中能看到 Real-Time 仓库、实验 10 能创建 Genie Agent。

0. First refresh the code in the workspace: Git folder → **Pull** (option A), or re-import the zip (option B). The old
   code has no `setup/08_workshop_warehouses` and none of the new labs.
1. Check Lakehouse RT's limits first: no serverless egress control, outbound Private Link or compliance security
   profile, and the workshop catalog is not in Unity Catalog default storage (check the catalog's storage location in
   Catalog Explorer). If any applies, Lakehouse RT won't work: skip steps 2–3; lab 09 runs on `hytech_workshop_sql`.
2. Ask the Databricks account team to enable the **Lakehouse//RT Beta** for the account (allow a few days).
3. Workspace menu → **Previews** → search "Lakehouse RT" → turn it on.
4. Re-run `setup/00_master_setup` (it is safe to re-run). The setup job's `workshop_warehouses` task
   (`setup/08_workshop_warehouses`) creates the workshop SQL warehouse `hytech_workshop_sql` (serverless, Small, 1–3
   clusters, stops after 10 idle minutes) and, when Lakehouse RT is on, `hytech_workshop_rt` (Real-Time, Small), and
   grants `de_workshop_sz` *Can use*. To use an existing warehouse instead of `hytech_workshop_sql`, set `warehouse_id`
   in `00_master_setup`.
5. Genie: participants need the **Databricks SQL** entitlement. On ap-southeast-1, if Agent mode or Genie One chat is
   missing, an account admin turns on cross-Geo processing in the account console; plan for that too.
6. Test with one **non-admin** participant account (you or a participant drives it): creating a pipeline and a job,
   lab 03c's staging lane, the Real-Time warehouse in lab 09's compute menu, and creating a Genie Agent in lab 10.

## 3 · 运行环境搭建作业 (Run the setup job)

一键安装会自动运行这一步；下面的参数用于方式 B 或以后重新运行。<br>
The master setup runs this step for you; the parameters below are for option B or later re-runs.

使用以下作业参数运行 **`hytech_ws_setup`**：

Run **`hytech_ws_setup`** with these job parameters:

| 参数 (Parameter) | 值 (Value) |
|---|---|
| `catalog` | `hytech_ws_catalog`（或选择你喜欢的名称：然后在所有地方使用它）|
| `participant_group` | `de_workshop_sz` |
| `participants` | 学员的邮箱地址，用逗号分隔（创建并转移 `u_<name>` schema）|
| `scale` | `full` |
| `reset` | `false`（首次运行生成数据。仅在工作坊**前**重新生成时使用 `true`）|
| `warehouse_id` | 可选：用于成本仪表盘的 SQL 仓库（留空 = 选择无服务器仓库）|

耗时约 10 分钟。检查所有任务是否都是绿色的。`cost_dashboard` 任务会打印仪表盘链接：它位于你的主文件夹（`/Workspace/Users/<你>/hytech_de_workshop`），并与 `de_workshop_sz` 只读共享。请也将其与 Zhi Han 和 Germaine 共享（*Share* → Can Manage）；使用一键安装并填写 `instructors` 时会自动共享。在 `generate_data` 中，摘要应显示 4 台服务器 × 3 张表，每张表有 1 个 LOAD 文件 + 48 个 CDC 文件。

Takes about 10 minutes. Check that every task is green. The `cost_dashboard` task prints the dashboard link: it is
in your home folder (`/Workspace/Users/<you>/hytech_de_workshop`) and shared read-only with `de_workshop_sz`.
Please also share it with Zhi Han and Germaine (*Share* → Can Manage); the master setup does this for you when `instructors` is filled in. In `generate_data`, the summary should show 4 servers ×
3 tables, each with 1 LOAD file + 48 CDC files.

## 4 · 运行参考答案作业 (Run the solution once)

一键安装同样会自动运行这一步。<br>
The master setup runs this step for you too.

运行作业 **`hytech_daily_trading_reporting_solution`**（约 4 分钟）。它运行参考答案管道并填充 `hytech_ws_catalog.solutions` — 学员在第 07 个实验和追进度时使用此 schema。

Run job **`hytech_daily_trading_reporting_solution`** (about 4 minutes). It runs the solution pipeline and fills
`hytech_ws_catalog.solutions` — participants use this schema in lab 07 and to catch up.

## 5 · 连通性测试 (Connectivity test from the Shenzhen office) — important

从**会场网络**中的笔记本电脑进行测试：

1. 打开工作区 URL → 登录。
2. 打开 `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here` → 选择 **Serverless** → 运行全部单元格。所有单元格应该都成功。第 1b 步把共享历史复制到这个账户自己的 volume（约 760 个文件、约 90 MB），记下用时；用非管理员的学员账户运行时，landing 和 producer 两行都应该是 ✅（如果出现 ⚠️ raw.producer，说明更新代码后还没有重新运行 setup）。
3. 打开 SQL 编辑器 → `SELECT ai_query('<endpoint>', '用一句话介绍 Databricks')` → 你应该会得到一个答案。

如果任何步骤失败，请向 Zhi Han 和 Germaine 发送截图（Lark/微信）。

From a laptop **on the venue network**:

1. Open the workspace URL → log in.
2. Open `/Workspace/Shared/hytech-de-workshop/labs/00_Start_Here` → attach **Serverless** → run all.
   All cells should succeed. Step 1b copies the shared history into the account's own volumes (about 760
   files, about 90 MB): note the time. With a non-admin participant account, both the landing and the
   producer lines must show ✅ (a ⚠️ raw.producer line means setup hasn't been re-run since the code refresh).
3. Open the SQL editor → `SELECT ai_query('<endpoint>', '用一句话介绍 Databricks')` → you should get an answer.

If any step fails, send Zhi Han and Germaine a screenshot (Lark/WeChat).

## 6 · 培训期间 (During the workshop)

- 作为工作坊管理员，需要处理权限和网络问题。
- 课堂上不需要运行任何作业：学员用 `labs/00b_Live_Data` 生成自己的实时数据。
- 学员只需要对 `raw` 的读权限，绝不需要直接访问系统表（他们使用 `ops.*` 视图）。
- 实验 09 和 10 用的 `hytech_workshop_sql` 和 `hytech_workshop_rt` 的权限由 `setup/08_workshop_warehouses` 授予；有学员看不到仓库时，检查 `de_workshop_sz` 是否有 *Can use*。

- Be available as workshop admin for permissions and network issues.
- Nothing needs to run during class: participants generate their own live data with `labs/00b_Live_Data`.
- Participants never need access to `raw` beyond read, and never to system tables directly (they use `ops.*` views).
- Labs 09 and 10 get access to `hytech_workshop_sql` and `hytech_workshop_rt` from `setup/08_workshop_warehouses`; if a participant can't see a warehouse, check that `de_workshop_sz` has *Can use*.

## 7 · 培训之后 (After the workshop)

将 catalog 保留约 2 周，以便学员继续练习。然后运行 `setup/99_teardown`（输入 catalog 名称确认），它也会删除一键安装创建的作业和管道。如果用的是方式 B（bundle），再运行 `databricks bundle destroy -t prod`。`setup/99_teardown` 也会删除 `hytech_workshop_sql` 和 `hytech_workshop_rt`（只删除带 `managed_by = master_setup` 标签的仓库；手动创建的仓库请自己删除）。

Keep the catalog for about 2 weeks so people can practise. Then run `setup/99_teardown` (type the catalog name
to confirm); it also deletes the jobs and the pipeline the master setup created. If you used option B (the bundle),
also run `databricks bundle destroy -t prod`. `setup/99_teardown` also deletes `hytech_workshop_sql` and `hytech_workshop_rt` (only warehouses tagged `managed_by = master_setup`; delete a hand-made one yourself).
