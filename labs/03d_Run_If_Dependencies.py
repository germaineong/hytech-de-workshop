# Databricks notebook source
# MAGIC %md
# MAGIC # 03d · 任务依赖与 Run if (Task dependencies and Run if) · LAB
# MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/03d_Run_If_Dependencies` 复制原始版本。
# MAGIC
# MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/03d_Run_If_Dependencies`.
# MAGIC
# MAGIC 在管道里，依赖关系由每个查询读取哪些表自动推断（实验 03c：silver 失败 → 下游 gold 被跳过）。在**作业**里，你用 **Depends on** 声明依赖，用 **Run if** 决定上游成功或失败时下游任务是否运行。本实验创建一个作业，把 6 种 Run if 都跑一遍。
# MAGIC
# MAGIC In a pipeline, dependencies are inferred from which tables each query reads (lab 03c: silver fails → the gold table downstream is skipped). In a **job** you declare them with **Depends on**, and **Run if** decides whether a downstream task runs when its upstream tasks succeed or fail. This lab builds a job that exercises all six Run if conditions.
# MAGIC
# MAGIC | Run if | 何时运行 (Runs when) | 否则 (Otherwise) |
# MAGIC |---|---|---|
# MAGIC | All succeeded（默认）<br>All succeeded (default) | 所有上游都成功<br>every upstream task succeeded | Upstream failed |
# MAGIC | At least one succeeded | 至少一个上游成功<br>at least one upstream task succeeded | Upstream failed |
# MAGIC | None failed | 没有上游失败<br>no upstream task failed | Upstream failed |
# MAGIC | All done | 所有上游都已结束，无论成败<br>every upstream task finished, whatever the result | — |
# MAGIC | At least one failed | 至少一个上游失败<br>at least one upstream task failed | Excluded |
# MAGIC | All failed | 所有上游都失败<br>every upstream task failed | Excluded |

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.dropdown("auto", "false", ["false", "true"], "自动运行作业 (auto: run the job for me)")

# COMMAND ----------

# MAGIC %run ./run_if_spec

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
name = "".join(ch if ch.isalnum() else "_" for ch in me.split("@")[0].lower()).strip("_")
catalog = dbutils.widgets.get("catalog")
schema = f"u_{name}"
AUTO = dbutils.widgets.get("auto") == "true"
jobs_dir = f"/Users/{me}/hytech_de_lab/jobs"
job_name = run_if_job_name(name)
spark.sql(f"USE {catalog}.{schema}")

missing = []
for notebook in RUN_IF_JOB_NOTEBOOKS:
    try:
        w.workspace.get_status(f"{jobs_dir}/{notebook}")
    except Exception:  # noqa: BLE001 - not copied yet
        missing.append(notebook)
if missing:
    raise FileNotFoundError(f"{jobs_dir} 中缺少 {missing}：在共享文件夹中重新运行 /Workspace/Shared/hytech-de-workshop/labs/00_Start_Here 的第 3 步（在自己的副本中运行时不会复制）"
                            f"({missing} not in {jobs_dir}: re-run step 3 of /Workspace/Shared/hytech-de-workshop/labs/00_Start_Here in the shared folder; run from your own copy, it copies nothing)")
print("schema:", f"{catalog}.{schema}", "| job notebooks:", jobs_dir, "| job:", job_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · 作业的形状 (The shape of the job)
# MAGIC
# MAGIC ```
# MAGIC check_mt5_sg01 ─┐
# MAGIC check_mt5_uk01 ─┼─► 7 个条件任务，每个依赖两台服务器 (7 condition tasks, each depending on two servers) ─► all_done_AUDIT (All done)
# MAGIC check_mt5_hk01 ─┘
# MAGIC ```
# MAGIC
# MAGIC 三个起点任务运行 `jobs/reconcile_server`：`mt5-sg-01` 和 `mt5-uk-01` 对账通过；`mt5-hk-01` 还在预发布通道（实验 03c），生产落地区里没有它的 DMS 文件，所以这个任务会失败。**先预测**下面每个任务的结果，再运行作业。
# MAGIC
# MAGIC The three starting tasks run `jobs/reconcile_server`: `mt5-sg-01` and `mt5-uk-01` reconcile; `mt5-hk-01` is still in the staging lane (lab 03c), with no DMS files in the production landing zone, so that task fails. **Predict** the result of every task below first, then run the job.
# MAGIC
# MAGIC | 任务 (Task) | 依赖 (Depends on) | Run if | 你的预测 (Your prediction) |
# MAGIC |---|---|---|---|
# MAGIC | `all_success_RUNS` | sg01, uk01 | All succeeded | ? |
# MAGIC | `all_success_SKIPPED` | uk01, hk01 | All succeeded | ? |
# MAGIC | `at_least_one_success_RUNS` | uk01, hk01 | At least one succeeded | ? |
# MAGIC | `none_failed_RUNS` | sg01, uk01 | None failed | ? |
# MAGIC | `none_failed_SKIPPED` | uk01, hk01 | None failed | ? |
# MAGIC | `at_least_one_failed_ALERT` | uk01, hk01 | At least one failed | ? |
# MAGIC | `all_failed_EXCLUDED` | uk01, hk01 | All failed | ? |
# MAGIC | `all_done_AUDIT` | 上面 7 个 (the 7 above) | All done | ? |

# COMMAND ----------

settings = run_if_job_settings(job_name, jobs_dir, catalog, schema)
# 要点 1 (Key point 1) · 每个下游任务都有 depends_on（等待哪些任务）和 run_if（在什么条件下运行）(every downstream task has depends_on — which tasks it waits for — and run_if — when it runs)
for t in settings["tasks"]:
    if t.get("depends_on"):
        print(f"{t['task_key']:28} depends_on={[d['task_key'] for d in t['depends_on']]}  run_if={t['run_if']}")

existing = [j for j in w.jobs.list(name=job_name)]
if existing:
    job_id = existing[0].job_id
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
    print("updated job", job_id)
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    print("created job", job_id)
displayHTML(f'<a href="/jobs/{job_id}" target="_blank">Open {job_name}</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 运行并核对 (Run and check)
# MAGIC 打开作业 → **Run now**，在图中看每个任务的颜色：绿色运行了，红色失败，灰色是 Upstream failed 或 Excluded。运行结束后再运行下面的检查单元格。
# MAGIC
# MAGIC Open the job → **Run now**, and watch the colours in the graph: green ran, red failed, grey is Upstream failed or Excluded. When the run has finished, run the check cell below.

# COMMAND ----------

if AUTO:
    started = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})["run_id"]
    print("started run", started)
else:
    print("👉 在作业页面点 Run now，然后运行下一个单元格 (click Run now on the job page, then run the next cell)")

# COMMAND ----------


def latest_run(job_id, timeout_minutes=30):
    """等待作业最近一次运行结束并返回它 (wait for the job's latest run to finish and return it)."""
    deadline = time.time() + timeout_minutes * 60
    while True:
        runs = w.api_client.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "limit": 1}).get("runs", [])
        if not runs:
            raise RuntimeError("这个作业还没有运行过：先点 Run now (the job has not run yet: click Run now first)")
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": runs[0]["run_id"]})
        if run["state"].get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run
        if time.time() > deadline:
            raise TimeoutError(f"运行 {run['run_id']} 还没结束：结束后再运行这个单元格 (run {run['run_id']} is still running: run this cell again when it has finished)")
        print("运行中 (still running) ...")
        time.sleep(20)


run = latest_run(job_id)
actual = {t["task_key"]: (t.get("state") or {}).get("result_state") for t in run.get("tasks", [])}
rows = compare_states(actual, expected_states())
display(spark.createDataFrame([(k, e, a or "", "✅" if ok else "❌") for k, e, a, ok in rows],
                              "task string, expected string, actual string, ok string"))
result = run["state"].get("result_state")
# 要点 2 (Key point 2) · 运行状态由叶子任务决定：all_done_AUDIT 成功 → "Succeeded with failures" (the run status comes from the leaf tasks: all_done_AUDIT succeeded → "Succeeded with failures")
print(f"run {run['run_id']}: {result} (expected {RUN_RESULT_EXPECTED})")
all_ok = all(ok for *_, ok in rows) and result == RUN_RESULT_EXPECTED
print("✅ 与预期一致 (matches the expected results)" if all_ok else
      "❌ 有差异：如果你修改过作业（第 3 步），这是正常的 (differences: expected if you edited the job in step 3)")
if AUTO:
    assert all_ok, rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 审计行：all_done_AUDIT 记录了这次运行中每个任务的结果 (the audit row: every task's result in this run)
# MAGIC SELECT audited_at, run_id, failed_tasks, excluded_tasks, tasks FROM ops_job_audit ORDER BY audited_at DESC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- task_logger 写的日志：只有满足条件的任务才会出现 (the task_logger rows: only the tasks whose condition was met appear)
# MAGIC SELECT run_id, task_key, run_if, logged_at FROM ops_run_if_log ORDER BY logged_at DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- at_least_one_failed_ALERT 用 jobs/notify 写了一条告警 (at_least_one_failed_ALERT wrote an alert with jobs/notify)
# MAGIC SELECT alert_at, reason, message FROM ops_alerts WHERE reason = 'server_check_failed' ORDER BY alert_at DESC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · 动手修改 (Change it yourself)
# MAGIC
# MAGIC 1. 打开作业 → 任务 `all_success_SKIPPED` → **Run if** 改为 **All done** → 保存。回到这次运行的页面，点 **Repair run**：只重跑没有成功的任务（以及依赖它们的任务），这次它会运行。
# MAGIC 2. 任务 `all_failed_EXCLUDED` → **Depends on** 去掉 `check_mt5_uk01`，只保留 `check_mt5_hk01` → 保存 → **Run now**：它唯一的上游失败了，满足 All failed，所以会运行。
# MAGIC 3. 再运行上面的检查单元格：你改过的两个任务会显示 ❌，这正是 Run if 和 Depends on 的作用。
# MAGIC
# MAGIC 1. Open the job → task `all_success_SKIPPED` → change **Run if** to **All done** → save. Back on the page of this run, click **Repair run**: only the unsuccessful tasks (and the tasks that depend on them) re-run, and this time it runs.
# MAGIC 2. Task `all_failed_EXCLUDED` → in **Depends on**, remove `check_mt5_uk01` and keep only `check_mt5_hk01` → save → **Run now**: its only upstream task failed, which meets All failed, so it runs.
# MAGIC 3. Run the check cell above again: the two tasks you changed show ❌, which is exactly the effect of Run if and Depends on.
# MAGIC
# MAGIC ## 4 · 小结 (Recap)
# MAGIC
# MAGIC * 管道里依赖是自动推断的；作业里由你用 Depends on + Run if 控制。<br>Pipelines infer dependencies; in a job you control them with Depends on + Run if.
# MAGIC * 实验 04 的生产作业里：`alert_on_failure` = At least one failed，`write_audit_row` = All done。<br>In lab 04's production job: `alert_on_failure` = At least one failed, `write_audit_row` = All done.
# MAGIC * 运行状态由叶子任务决定，所以有任务失败时运行仍可能显示 *Succeeded with failures*，而这在通知里算作成功（实验 04c）。<br>The run status comes from the leaf tasks, so a run with failed tasks can still show *Succeeded with failures*, which counts as success for notifications (lab 04c).
# MAGIC * 没有运行的任务不产生计算成本。<br>Tasks that don't run don't cost compute.

# COMMAND ----------

if AUTO:
    dbutils.notebook.exit(f"run {run['run_id']}: {result}, {sum(ok for *_, ok in rows)}/{len(rows)} tasks as expected")
