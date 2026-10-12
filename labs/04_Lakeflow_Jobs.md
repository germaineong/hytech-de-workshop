# Lab 04 · Lakeflow Jobs — 像生产一样调度 (orchestrate like production)

**目标 (Goal):** 在 UI 中搭建作业 `daily_trading_reporting_<your_name>`：运行你的管道 → 数据质量闸门（if/else）→ AI 每日简报或告警；按服务器 for-each 对账；失败告警（Run if）；审计（Run if）；然后制造一次失败并用 **Repair run** 修复。

Build the job `daily_trading_reporting_<your_name>` in the UI: run your pipeline → data quality gate (if/else) → AI daily summary or alert; reconcile by server (for-each); alert on failure (Run if); audit (Run if); then trigger a failure and repair it with **Repair run**.

```
run_pipeline ─┬─► dq_gate ─► dq_ok (if/else) ─┬─ true  ─► publish_daily_summary
              │                               └─ false ─► notify_dq_owner
              └─► reconcile_servers (for each server) ─► reconcile_server
alert_on_failure  Run if: At least one failed     (deps: run_pipeline, dq_gate, reconcile_servers, publish_daily_summary)
write_audit_row   Run if: All done                (deps: alert_on_failure, notify_dq_owner, publish_daily_summary, reconcile_servers)
```

> 任务用到的 notebook 已经在 `/Users/<you>/hytech_de_lab/jobs/`（Lab 00 复制过来的）。
> 落后了？运行 `04b_Jobs_Catch_Up` 自动创建同样的作业。
>
> The notebooks are already in `/Users/<you>/hytech_de_lab/jobs/` (copied from Lab 00).
> Fell behind? Run `04b_Jobs_Catch_Up` to auto-create the same job.

## 1 · 创建作业与参数 (Create the job and its parameters)

**Jobs & Pipelines → Create → Job**, name `daily_trading_reporting_<your_name>`. Then add **Job parameters**:

| Key | Value |
|---|---|
| `catalog` | `hytech_ws_catalog` |
| `schema` | `u_<your_name>` |
| `dq_max_drop_pct` | `1.0` |
| `servers` | `["mt5-sg-01","mt5-sg-02","mt5-uk-01","mt5-cy-01"]` |
| `llm_endpoint` | `databricks-claude-sonnet-4-5`（或讲师提供的端点）<br>`databricks-claude-sonnet-4-5` (or the endpoint the instructor gives you) |
| `fail_server` | `mt5-uk-01` ← 故意制造失败 (to practise repair) |

作业参数会自动传递给每个 notebook 任务（作为 widget）。

Job parameters are passed automatically to every notebook task (as widgets).

## 2 · 添加任务 (Add the tasks)

| # | 任务名称<br>Task name | 类型<br>Type | 配置<br>Settings |
|---|---|---|---|
| 1 | `run_pipeline` | **管道**<br>**Pipeline** | 你的管道 `trade_lakehouse_<your_name>`；重试次数: 1<br>your pipeline `trade_lakehouse_<your_name>`; *Retries*: 1 |
| 2 | `dq_gate` | Notebook<br>Notebook | `.../hytech_de_lab/jobs/dq_gate`；依赖于 `run_pipeline`<br>`.../hytech_de_lab/jobs/dq_gate`; depends on `run_pipeline` |
| 3 | `dq_ok` | **条件任务**<br>**If/else condition** | `{{tasks.dq_gate.values.dq_drop_pct}}` **<** `{{job.parameters.dq_max_drop_pct}}`；依赖于 `dq_gate`<br>`{{tasks.dq_gate.values.dq_drop_pct}}` **<** `{{job.parameters.dq_max_drop_pct}}`; depends on `dq_gate` |
| 4 | `publish_daily_summary` | Notebook<br>Notebook | `.../jobs/publish_daily_summary`；依赖于 `dq_ok` = **true**<br>`.../jobs/publish_daily_summary`; depends on `dq_ok` = **true** |
| 5 | `notify_dq_owner` | Notebook<br>Notebook | `.../jobs/notify`；依赖于 `dq_ok` = **false**；参数 `reason` = `dq_gate_failed`、`detail` = `{{tasks.dq_gate.values.dq_drop_pct}}`、`job_id` = `{{job.id}}`、`run_id` = `{{job.run_id}}`<br>`.../jobs/notify`; depends on `dq_ok` = **false**; parameters `reason` = `dq_gate_failed`, `detail` = `{{tasks.dq_gate.values.dq_drop_pct}}`, `job_id` = `{{job.id}}`, `run_id` = `{{job.run_id}}` |
| 6 | `reconcile_servers` | **循环任务**<br>**For each** | 输入 `{{job.parameters.servers}}`，并发数 4；嵌套任务：Notebook `.../jobs/reconcile_server`，参数 `server_id` = `{{input}}`；依赖于 `run_pipeline`<br>Inputs `{{job.parameters.servers}}`, concurrency 4; nested task: Notebook `.../jobs/reconcile_server` with parameter `server_id` = `{{input}}`; depends on `run_pipeline` |
| 7 | `alert_on_failure` | Notebook<br>Notebook | `.../jobs/notify`；依赖于 `run_pipeline`、`dq_gate`、`reconcile_servers`、`publish_daily_summary`；**运行条件：至少一个失败**；参数 `reason` = `job_task_failed`、`job_id` = `{{job.id}}`、`run_id` = `{{job.run_id}}`<br>`.../jobs/notify`; depends on `run_pipeline`, `dq_gate`, `reconcile_servers`, `publish_daily_summary`; **Run if: At least one failed**; parameters `reason` = `job_task_failed`, `job_id` = `{{job.id}}`, `run_id` = `{{job.run_id}}` |
| 8 | `write_audit_row` | Notebook<br>Notebook | `.../jobs/audit`；依赖于 `alert_on_failure`、`notify_dq_owner`、`publish_daily_summary`、`reconcile_servers`；**运行条件：全部完成**；参数 `job_id` = `{{job.id}}`、`run_id` = `{{job.run_id}}`<br>`.../jobs/audit`; depends on `alert_on_failure`, `notify_dq_owner`, `publish_daily_summary`, `reconcile_servers`; **Run if: All done**; parameters `job_id` = `{{job.id}}`, `run_id` = `{{job.run_id}}` |

计算资源：对每个任务使用 **无服务器 (Serverless)**。

Compute: **Serverless** for every task.

## 3 · 运行、失败、修复 (Run → fail → repair)

1. **现在运行。** 观察图表：`reconcile_servers` → `mt5-uk-01` 迭代失败（模拟）；`alert_on_failure` 运行（至少一个失败）；`write_audit_row` 仍然运行（全部完成）。注意运行状态 **成功但有失败**：叶子任务成功，但中间的任务失败了。

   **Run now.** Watch the graph: `reconcile_servers` → the `mt5-uk-01` iteration fails (simulated); `alert_on_failure` runs (*At least one failed*); `write_audit_row` still runs (*All done*). Note the run status **Succeeded with failures**: the leaf tasks succeeded, but a task in the middle failed.

2. 打开失败的迭代 → 读错误信息。（Genie Code：问 *”为什么失败？如何只重跑失败的任务？”*）

   Open the failed iteration → read the error. (Genie Code: ask *”为什么失败？如何只重跑失败的任务？”*)

3. **修复运行 (Repair run)** → 编辑作业参数 `fail_server` 改为空 → 修复。
   只重跑失败的任务及其下游；管道 **不会** 重跑。
   ⚠️ 修复时 for-each 必须解析为与原运行 **相同数量的迭代**：保持 `servers` 列表不变。

   **Repair run** → edit the job parameter `fail_server` to empty → repair. Only the failed tasks and their dependants re-run; the pipeline is **not** re-run. ⚠️ A repair must resolve the for-each to the **same number of iterations** as the original run: keep the `servers` list unchanged.

4. 检查你的 schema 中的输出表：`ops_dq_results`、`ops_reconciliation`、`ops_alerts`、`ops_job_audit`、`gold_daily_commentary`。

   Check the output tables in your schema: `ops_dq_results`, `ops_reconciliation`, `ops_alerts`, `ops_job_audit`, `gold_daily_commentary`.

## 4 · 触发器与通知 (Triggers and notifications)

- **添加触发器 → 文件到达** 在你自己的落地区 `/Volumes/hytech_ws_catalog/u_<your_name>/landing/mt5/`（触发器之间最少 60 秒）。用 `ticks` = 1 运行 `00b_Live_Data` → 新文件到达 → 你的作业自动启动（大约一分钟内）。演示后 **暂停** 触发器，以后运行 00b 时作业就不会再自动启动。几分钟后作业还没有启动？在作业的 **Schedules & Triggers** 中查看触发器的状态，或者直接 **Run now**，继续后面的步骤。

  **Add trigger → File arrival** on your own landing zone `/Volumes/hytech_ws_catalog/u_<your_name>/landing/mt5/` (minimum 60 s between triggers). Run `00b_Live_Data` with `ticks` = 1 → new files arrive → your job starts by itself (within about a minute). **Pause** the trigger again after the demo, so your later 00b runs don't start the job. No run after a few minutes? Check the trigger's status under the job's **Schedules & Triggers**, or just click **Run now** and carry on.

- 其他选项：**定时** (e.g. 每小时)、**表更新** (当上游表更新时启动，用于团队间管道依赖)、**持续**。

  Alternatives: **Scheduled** (e.g. hourly), **Table update** (start when an upstream table changes: useful for dependencies between teams' pipelines), **Continuous**.

- **作业通知 → 失败时 → 发送邮件至你的邮箱。**

  **Job notifications → On failure → your email.**

## 5 · 代码即作业 (Jobs as code → CI/CD)

打开你的作业 → **⋮ → 以代码形式查看 (YAML)**，与 repo 中的 `resources/solution.job.yml` 对比。
该 YAML 通过 **声明式管道包 (Declarative Automation Bundles)**（`databricks bundle deploy -t prod`）由 GitLab CI 部署，以服务主体身份运行。
这样 Hytech 就能 **移除对生产的直接访问**：所有改动都经过审查和 CI/CD，没人手工编辑生产。

Open your job → **⋮ → View as code (YAML)** and compare with `resources/solution.job.yml` in the repo. That YAML is deployed with **Declarative Automation Bundles** (`databricks bundle deploy -t prod`), from GitLab CI, running as a service principal. This is how Hytech can **remove direct access to production**: changes go through review and CI/CD, and nobody edits prod by hand.

## 6 · 你自己的演示（可选，有时间再做）(Your own demos — optional, if time allows)

用 `00b_Live_Data` 自己完成，只影响你自己的落地区、管道和作业。

Do these yourself with `00b_Live_Data`; they only touch your own landing zone, pipeline and job.

- **实时上线新服务器**（完成 03d **之后**再做：03d 预期你的落地区里还没有这台服务器）：用 `new_server=mt5-hk-01` 运行 `00b_Live_Data`（DMS 全量加载 + CDC）。然后在作业 → **Job parameters** 中清空 `fail_server`，给 `servers` 加上 `"mt5-hk-01"`，再 **Run now**。bronze glob `mt5/*/<table>/` 在这次管道运行中自动拾取它，无需改代码；`reconcile_servers` 多了一次迭代。

  **Onboard a new server live** (only **after** 03d: 03d expects your landing zone to have no files from that server yet): run `00b_Live_Data` with `new_server=mt5-hk-01` (DMS full load + CDC). Then in the job → **Job parameters**, clear `fail_server` and add `"mt5-hk-01"` to `servers`, and click **Run now**. The bronze glob `mt5/*/<table>/` picks it up in this pipeline run, with no code change, and `reconcile_servers` gets one more iteration.

- **坏数据批次**：用 `bad_batch_pct=60` 运行 `00b_Live_Data`，然后马上 **Run now** 你的作业（`fail_server` 已清空）。期望丢弃坏行，`dq_drop_pct` 超过 1%，条件走 **false** 分支，`notify_dq_owner` 发出告警。这次运行处理了坏行，所以之后的 04c 不受影响。

  **Bad batch:** run `00b_Live_Data` with `bad_batch_pct=60`, then **Run now** your job straight away (with `fail_server` cleared). Expectations drop the bad rows, `dq_drop_pct` goes above 1%, the condition takes the **false** branch, and `notify_dq_owner` raises the alert. This run processes the bad rows, so 04c later isn't affected.
