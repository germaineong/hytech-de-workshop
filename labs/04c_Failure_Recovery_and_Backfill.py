# Databricks notebook source
# MAGIC %md
# MAGIC # 04c · 管道失败之后：重试、修复运行与回填 (When the pipeline fails: retries, repair runs and backfills) · LAB
# MAGIC 本实验包含完整代码：逐个运行单元格，结合说明和代码中的「要点」注释理解每一步。改坏了代码？从 `solutions/04c_Failure_Recovery_and_Backfill` 复制原始版本。
# MAGIC
# MAGIC This lab has the complete code: run the cells one by one, and use the notes and the "Key point" comments to follow each step. Broke the code? Copy the original from `solutions/04c_Failure_Recovery_and_Backfill`.
# MAGIC
# MAGIC **场景 (Scenario).** 预发布通道 `mt5-hk-01` 送来一个价格损坏的批次，你的生产作业 `daily_trading_reporting_<你的名字>` 中的管道任务失败了。DMS 照常写入新文件，日报却停了。你将：
# MAGIC
# MAGIC 1. 让作业为真实的失败做好准备：重试和失败通知
# MAGIC 2. 重现失败，读懂作业图中的连锁反应
# MAGIC 3. 修复后用 **Repair run** 让作业恢复绿色，并证明没有丢失数据
# MAGIC 4. 用 **Run backfill** 补上错过的日报，并确认重跑不会产生重复
# MAGIC
# MAGIC The staging lane `mt5-hk-01` sends a batch with corrupted prices, and the pipeline task of your production job `daily_trading_reporting_<your_name>` fails. DMS keeps writing new files, but the daily reports stop. You will:
# MAGIC
# MAGIC 1. Prepare the job for real failures: retries and failure notifications
# MAGIC 2. Reproduce the failure and read the knock-on effects in the job graph
# MAGIC 3. Fix it, bring the job back to green with a **Repair run**, and prove that no data was lost
# MAGIC 4. Fill in the missed daily reports with **Run backfill**, and confirm that re-running creates no duplicates
# MAGIC
# MAGIC 前提：已完成实验 03c（你的管道中有预发布通道），并已用实验 04 或 04b 创建了作业。<br>
# MAGIC Before you start: lab 03c is done (your pipeline has the staging lane), and lab 04 or 04b created your job.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog")
dbutils.widgets.dropdown("auto", "false", ["false", "true"], "自动完成手动步骤 (auto: do the hand steps for me)")

# COMMAND ----------

# MAGIC %run ./staging_feed

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
catalog = dbutils.widgets.get("catalog")
AUTO = dbutils.widgets.get("auto") == "true"
me = spark.sql("SELECT current_user()").first()[0]
names = my_names(me)
my_schema = names["schema"]
cs = f"{catalog}.{my_schema}"
spark.sql(f"USE {cs}")
pipeline_id = find_pipeline_id(w, names["pipeline"])
found = [j for j in w.jobs.list(name=names["job"])]
if not found:
    raise ValueError(f"作业 {names['job']} 不存在：先完成实验 04 或运行 04b_Jobs_Catch_Up。 (Job {names['job']} not found: do lab 04 or run 04b first.)")
job_id = found[0].job_id
staging_file = f"{transformations_dir(w, pipeline_id, names['lab_root'])}/{STAGING_FILE}"
try:
    w.workspace.get_status(staging_file)
except Exception:  # noqa: BLE001 - the staging lane is not wired yet
    if not AUTO:
        raise RuntimeError("预发布通道还不在你的管道里：先完成实验 03c 第 1–2 步，或把 auto 设为 true。"
                           " (The staging lane is not in your pipeline yet: do lab 03c steps 1–2 first, or set auto to true.)")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cs}.staging")
    write_batch(spark, catalog, my_schema, "clean")
    wire_staging_lane(w, catalog, me)
checks = []


def check(label, ok, detail=""):
    """打印 ✅ / ❌ 并记录结果 (print ✅ / ❌ and record the result)."""
    print(("✅ " if ok else "❌ ") + label + (f" — {detail}" if detail else ""))
    checks.append((label, bool(ok)))
    return ok


def job_settings():
    return w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]


def run_now(params=None):
    body = {"job_id": job_id, **({"job_parameters": params} if params else {})}
    return w.api_client.do("POST", "/api/2.2/jobs/run-now", body=body)["run_id"]


def wait_run(run_id, timeout_minutes=45):
    """等待一次作业运行（包括它的修复）结束 (wait for a job run, including its repair, to finish)."""
    deadline = time.time() + timeout_minutes * 60
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        if run["state"].get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run
        if time.time() > deadline:
            raise TimeoutError(f"运行 {run_id} 还没结束：结束后再运行这个单元格 (run {run_id} is still running: run this cell again when it has finished)")
        print("运行中 (still running) ...")
        time.sleep(20)


def latest_run(since_ms=0, wait_minutes=10):
    """等你点 Run now：取这个作业在 since_ms 之后启动的最近一次运行，并等它结束。
    Waits for you to click Run now: takes the job's latest run started after since_ms, and waits for it to finish."""
    deadline = time.time() + wait_minutes * 60
    while True:
        runs = w.api_client.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "limit": 1}).get("runs", [])
        if runs and runs[0]["start_time"] >= since_ms:
            return wait_run(runs[0]["run_id"])
        if time.time() > deadline:
            raise RuntimeError("没有找到新的运行：先在作业页面点 Run now，再运行这个单元格 (no new run: click Run now on the job page, then run this cell again)")
        print("👉 等你在作业页面点 Run now (waiting for you to click Run now on the job page) ...")
        time.sleep(15)


def wait_repaired(run_id, wait_minutes=10):
    """等你点 Repair run：等这次运行有了修复记录，再等修复结束。
    Waits for you to click Repair run: waits until the run has a repair in its history, then for the repair to finish."""
    deadline = time.time() + wait_minutes * 60
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id, "include_history": "true"})
        if any(h.get("type") == "REPAIR" for h in run.get("repair_history", [])):
            return wait_run(run_id)
        if time.time() > deadline:
            raise RuntimeError("这次运行还没有修复：先点 Repair run，再运行这个单元格 (the run has not been repaired: click Repair run, then run this cell again)")
        print("👉 等你点 Repair run (waiting for you to click Repair run) ...")
        time.sleep(15)


def backfill_runs(dates):
    """这个作业中，每个回填日期 (report_date) 最近的一次运行 (this job's latest run for each backfill date, report_date)."""
    found = {}
    for r in w.api_client.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "limit": 25}).get("runs", []):
        d = {p["name"]: p.get("value") or "" for p in r.get("job_parameters", [])}.get("report_date", "")[:10]
        if d in dates and d not in found:   # 列表从新到旧 (the list is newest first)
            found[d] = r["run_id"]
    return found


def task_states(run):
    """每个任务最后一次尝试的结果，以及尝试次数 (each task's result on its latest attempt, and how many attempts it had)."""
    latest = {}
    for t in run.get("tasks", []):
        n = t.get("attempt_number", 0)
        if t["task_key"] not in latest or n >= latest[t["task_key"]][0]:
            latest[t["task_key"]] = (n, (t.get("state") or {}).get("result_state"))
    return {k: v[1] for k, v in latest.items()}, {k: v[0] + 1 for k, v in latest.items()}


print("作业 (job):", names["job"], job_id, "| 管道 (pipeline):", names["pipeline"], "| 预发布文件 (staging file):", staging_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · 为真实的失败做准备 (Prepare for real failures)
# MAGIC
# MAGIC | 重试的层次 (Retry layer) | 默认 (Default) | 本实验 (This lab) |
# MAGIC |---|---|---|
# MAGIC | 管道更新 `pipelines.numUpdateRetryAttempts`<br>Pipeline update | 由作业或 API 启动时自动重试 5 次；在编辑器中运行不重试<br>5 automatic retries when a job or the API starts it; none when run from the editor | 0 |
# MAGIC | 管道中的流 `pipelines.maxFlowRetryAttempts`<br>A flow inside the pipeline | 2 | 默认 (default) |
# MAGIC | 作业任务 Retries<br>Job task | 0（无服务器 notebook 任务会自动重试）<br>0 (serverless notebook tasks retry automatically) | `run_pipeline`: 1 |
# MAGIC
# MAGIC 重试只对**暂时性**错误有用（网络、资源）；数据错误重试多少次都一样会失败，只会推迟告警。<br>
# MAGIC Retries only help with **transient** errors (network, capacity); a data error fails on every retry and only delays the alert.
# MAGIC
# MAGIC 1. 在 `transformations/06_staging_mt5_hk01.sql` 中确认 `valid_trade_price` 是 `ON VIOLATION FAIL UPDATE`（03c 可能停在了 DROP）。
# MAGIC 2. 管道 → **Settings → Configuration**：添加 `pipelines.numUpdateRetryAttempts` = `0`。
# MAGIC 3. 作业 → **Job parameters**：添加 `report_date`（值留空），并把 `fail_server` 清空。
# MAGIC 4. 作业 → 任务 `run_pipeline` → **Retries** = 1；**Notifications → On failure**：你的邮箱。为什么用任务级通知？这个作业的叶子任务 `write_audit_row` 总会运行，所以失败后的运行状态是 *Succeeded with failures*，在作业级通知里算作**成功**。
# MAGIC
# MAGIC 1. In `transformations/06_staging_mt5_hk01.sql`, make sure `valid_trade_price` is `ON VIOLATION FAIL UPDATE` (lab 03c may have ended on DROP).
# MAGIC 2. Pipeline → **Settings → Configuration**: add `pipelines.numUpdateRetryAttempts` = `0`.
# MAGIC 3. Job → **Job parameters**: add `report_date` (leave the value empty), and clear `fail_server`.
# MAGIC 4. Job → task `run_pipeline` → **Retries** = 1; **Notifications → On failure**: your email. Why a task notification? The job's leaf task `write_audit_row` always runs, so after a failure the run ends *Succeeded with failures*, which job-level notifications count as **success**.

# COMMAND ----------

if AUTO:
    set_price_rule(w, me, "FAIL")
    spec = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}")["spec"]
    conf = {**(spec.get("configuration") or {}), "pipelines.numUpdateRetryAttempts": "0"}
    w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**spec, "configuration": conf, "id": pipeline_id})
    s = job_settings()
    params = {p["name"]: p for p in s.get("parameters", [])}
    params["report_date"] = {"name": "report_date", "default": ""}
    if "fail_server" in params:
        params["fail_server"]["default"] = ""
    s["parameters"] = list(params.values())
    for t in s["tasks"]:
        if t["task_key"] == "run_pipeline":
            t["max_retries"] = 1
            t["email_notifications"] = {"on_failure": [me]}
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": s})
    print("✅ 作业和管道已准备好 (job and pipeline prepared)")

s = job_settings()
params = {p["name"]: p.get("default") for p in s.get("parameters", [])}
rp = next((t for t in s["tasks"] if t["task_key"] == "run_pipeline"), None)
check("任务 run_pipeline 存在 (the job has a task named run_pipeline)", rp is not None,
      "" if rp else "作业中的任务 (tasks in the job): " + ", ".join(sorted(t["task_key"] for t in s["tasks"])))
rp = rp or {}
conf = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}")["spec"].get("configuration") or {}
text = w.workspace.download(staging_file).read().decode("utf-8")
check("valid_trade_price 是 FAIL UPDATE (valid_trade_price is FAIL UPDATE)", switch_rule(text, "valid_trade_price", "FAIL") == text)
check("作业参数 report_date 存在且为空 (job parameter report_date exists and is empty)", params.get("report_date") == "", repr(params.get("report_date")))
check("fail_server 已清空 (fail_server is cleared)", not params.get("fail_server"), repr(params.get("fail_server")))
check("管道自动重试已关闭 (pipeline automatic retries are off)", conf.get("pipelines.numUpdateRetryAttempts") == "0", repr(conf.get("pipelines.numUpdateRetryAttempts")))
check("run_pipeline 重试 1 次 (run_pipeline retries once)", rp.get("max_retries") == 1, repr(rp.get("max_retries")))
check("run_pipeline 失败时发邮件 (run_pipeline emails on failure)", bool((rp.get("email_notifications") or {}).get("on_failure")), repr(rp.get("email_notifications")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 让它失败 (Break it)
# MAGIC 写入一个价格损坏的批次，然后在作业页面点 **Run now**。看作业图：`run_pipeline` 失败、重试一次、再次失败；之后的任务都没有运行；`alert_on_failure`（At least one failed）和 `write_audit_row`（All done）照常运行。
# MAGIC
# MAGIC Write a batch with corrupted prices, then click **Run now** on the job page. Watch the graph: `run_pipeline` fails, retries once and fails again; the tasks after it don't run; `alert_on_failure` (At least one failed) and `write_audit_row` (All done) run as usual.

# COMMAND ----------

bad_batch = write_batch(spark, catalog, my_schema, "bad_prices")
bad_batch_at = int(time.time() * 1000) - 30_000   # 只接受这之后启动的运行；30 秒余量 (only runs started after this count; 30 s slack)
if AUTO:
    print("已启动运行 (started run)", run_now())
else:
    print("👉 在作业页面点 Run now，然后运行下一个单元格 (click Run now on the job page, then run the next cell)")

# COMMAND ----------

run = latest_run(since_ms=bad_batch_at)
broken_run_id = run["run_id"]
states, attempts = task_states(run)
display(spark.createDataFrame(sorted((k, v or "", attempts[k]) for k, v in states.items()), "task string, result string, attempts int"))
check("run_pipeline 失败了 (run_pipeline FAILED)", states.get("run_pipeline") == "FAILED", states.get("run_pipeline"))
check("run_pipeline 重试了一次，数据错误重试也没用 (it was retried once: a retry doesn't fix a data error)", attempts.get("run_pipeline") == 2, str(attempts.get("run_pipeline")))
blocked = {k: states.get(k) for k in ("dq_gate", "publish_daily_summary", "reconcile_servers")}
check("下游任务没有运行 (the downstream tasks did not run)", all(v in ("UPSTREAM_FAILED", "EXCLUDED") for v in blocked.values()), str(blocked))
check("alert_on_failure 运行了 (At least one failed)", states.get("alert_on_failure") == "SUCCESS", states.get("alert_on_failure"))
check("write_audit_row 运行了 (All done)", states.get("write_audit_row") == "SUCCESS", states.get("write_audit_row"))
# 要点 1 (Key point 1) · 叶子任务成功 → Succeeded with failures：作业级通知把它当作成功 (a successful leaf task → Succeeded with failures, which job-level notifications treat as success)
check("运行状态 = Succeeded with failures (run status)", run["state"].get("result_state") == "SUCCESS_WITH_FAILURES", run["state"].get("result_state"))
alerts = spark.sql("SELECT count(*) AS n FROM ops_alerts WHERE reason = 'job_task_failed' AND run_id = :r",
                   args={"r": str(broken_run_id)}).first()["n"]
check("ops_alerts 中有这次运行的告警 (ops_alerts has this run's alert)", alerts >= 1, str(alerts))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · 数据继续到达 (Data keeps arriving)
# MAGIC 值班工程师还在睡觉，DMS 又送来了两个正常的文件。作业坏着的时候，它们会怎样？<br>
# MAGIC The on-call engineer is still asleep, and DMS sends two more normal files. What happens to them while the job is broken?

# COMMAND ----------

late_batches = [write_batch(spark, catalog, my_schema, "clean") for _ in range(2)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · 诊断 (Diagnose)
# MAGIC 在运行页面打开失败的 `run_pipeline` → 错误信息和管道更新的链接。下面也用 API 和事件日志读出同样的信息。<br>
# MAGIC On the run page, open the failed `run_pipeline` → the error and the link to the pipeline update. Below, the same information from the API and the event log.

# COMMAND ----------

failed_task = max((t for t in run["tasks"] if t["task_key"] == "run_pipeline"), key=lambda t: t.get("attempt_number", 0), default=None)
if failed_task:
    output = w.api_client.do("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": failed_task["run_id"]})
    print((output.get("error") or "")[:800])
else:
    print("这次运行中没有 run_pipeline 任务 (this run has no run_pipeline task)")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, origin.flow_name AS flow, error.exceptions[0].class_name AS error_class,
# MAGIC        left(error.exceptions[0].message, 500) AS message
# MAGIC FROM pipeline_event_log
# MAGIC WHERE level = 'ERROR' AND error IS NOT NULL
# MAGIC ORDER BY timestamp DESC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · 修复并用 Repair run 恢复 (Fix it and recover with a Repair run)
# MAGIC
# MAGIC 1. **修复原因（策略修复）：** 把 `valid_trade_price` 改为 `ON VIOLATION DROP ROW` 并保存：坏行进入隔离表，好行继续处理。另一种方法是源头修复：删除**所有**带负价格的文件，再对预发布表做完全刷新，见 03c 第 7 步。本实验用策略修复，下面的检查也按它验证。
# MAGIC 2. **Repair run：** 在这次失败运行的页面点 **Repair run**。它只重跑没有成功的任务和依赖它们的任务，仍然是同一次运行，并保留修复历史。
# MAGIC 3. 管道从上次成功的位置继续：卡住的坏批次和坏着时到达的两个文件都会被处理。**流式表不需要手动回填数据。**
# MAGIC
# MAGIC 1. **Fix the cause (the policy fix):** change `valid_trade_price` to `ON VIOLATION DROP ROW` and save: the bad rows go to the quarantine and the good rows carry on. The other way is a source fix: delete **every** file with negative prices, then fully refresh the staging tables, as in lab 03c step 7. This lab uses the policy fix, and the checks below verify it.
# MAGIC 2. **Repair run:** on the page of the failed run, click **Repair run**. It re-runs only the unsuccessful tasks and the tasks that depend on them, inside the same run, with a repair history.
# MAGIC 3. The pipeline continues from where it last succeeded: the stuck bad batch and the two files that arrived meanwhile are all processed. **Streaming tables need no manual data backfill.**

# COMMAND ----------

if AUTO:
    set_price_rule(w, me, "DROP")
    # 要点 2 (Key point 2) · 修复运行：只重跑失败的任务及其下游，仍是同一次运行 (a repair re-runs only the failed tasks and their dependants, inside the same run)
    repair = w.api_client.do("POST", "/api/2.2/jobs/runs/repair",
                             body={"run_id": broken_run_id, "rerun_all_failed_tasks": True, "rerun_dependent_tasks": True})
    print("修复 (repair)", repair.get("repair_id"))
else:
    print("👉 把 valid_trade_price 改为 ON VIOLATION DROP ROW 并保存，然后点 Repair run，再运行下一个单元格 "
          "(change valid_trade_price to ON VIOLATION DROP ROW and save, click Repair run, then run the next cell)")

# COMMAND ----------

run = wait_repaired(broken_run_id)
states, _ = task_states(run)
check("valid_trade_price 是 DROP ROW：策略修复 (valid_trade_price is DROP ROW: the policy fix)",
      price_rule_action(w.workspace.download(staging_file).read().decode("utf-8")) == "DROP")
check("修复后运行成功 (after the repair the run SUCCEEDED)", run["state"].get("result_state") == "SUCCESS", run["state"].get("result_state"))
check("日报生成了 (the daily summary ran)", states.get("publish_daily_summary") == "SUCCESS", states.get("publish_daily_summary"))
for path in [bad_batch] + late_batches:
    if not os.path.exists(path):
        check(f"批次 {os.path.basename(path)} 已完整处理 (batch fully processed)", False,
              "文件已被删除：本实验用策略修复，见第 5 步 (the file was deleted: this lab uses the policy fix, see step 5)")
        continue
    c = batch_counts(spark, cs, path)
    want = c["file"] - (5 if path == bad_batch else 0)
    check(f"批次 {os.path.basename(path)} 已完整处理 (batch fully processed)", c["silver"] == want, str(c))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · 回填错过的日报 (Backfill the missed daily reports)
# MAGIC 管道坏了两天，这两天没有日报。管道数据会自己追上，但**按日期产生的输出**（日报）必须按日期补跑：
# MAGIC
# MAGIC 1. 作业页面 → **Run now** 旁边的箭头 → **Run backfill**。
# MAGIC 2. 日期范围用下面打印的两天，间隔 **1 天**。
# MAGIC 3. 参数 `report_date` = `{{backfill.iso_date}}`，然后运行。作业中有管道任务，所以回填的运行会一个接一个地执行。
# MAGIC
# MAGIC The pipeline was broken for two days, so those two days have no daily report. The pipeline's data catches up by itself, but **date-based outputs** (the daily report) must be re-run per date:
# MAGIC
# MAGIC 1. Job page → the arrow next to **Run now** → **Run backfill**.
# MAGIC 2. Use the two dates printed below as the range, every **1 day**.
# MAGIC 3. Set the parameter `report_date` = `{{backfill.iso_date}}` and run. The job has a pipeline task, so the backfill runs execute one after another.

# COMMAND ----------

days = [r["d"] for r in spark.sql(
    "SELECT DISTINCT deal_date AS d FROM gold_daily_symbol_volume WHERE deal_date <= current_date() ORDER BY d DESC LIMIT 3").collect()]
latest_day, missed = days[0], sorted(days[1:])
print(f"最新一天 (latest day): {latest_day} | 回填范围 (backfill range): {missed[0]} → {missed[-1]} "
      f"| 间隔 1 天 (every 1 day) | report_date = {{{{backfill.iso_date}}}}")
if AUTO:
    for d in missed:
        r = wait_run(run_now({"report_date": str(d)}))
        print(d, "回填运行 (backfill run):", r["state"].get("result_state"))

# COMMAND ----------

# 要点 3 (Key point 3) · 日报按 (report_date, language) 合并，所以修复和回填都不会产生重复 (the summary merges on report_date + language, so repairs and backfills never duplicate)
want = {str(d) for d in missed}
found, deadline = backfill_runs(want), time.time() + 5 * 60
while len(found) < len(want) and time.time() < deadline:   # 等 Run backfill 为每个日期启动运行 (wait for Run backfill to start a run per date)
    print("👉 等待回填运行 (waiting for the backfill runs):", ", ".join(sorted(want - set(found))))
    time.sleep(20)
    found = backfill_runs(want)
if len(found) < len(want):
    print("⚠️ 没有找到这些日期的回填运行：先启动 Run backfill，再运行这个单元格 "
          "(no backfill run for these dates: start Run backfill, then run this cell again):", ", ".join(sorted(want - set(found))))
for d, run_id in sorted(found.items()):
    print(d, "回填运行 (backfill run):", wait_run(run_id)["state"].get("result_state"))
per_day = {r["report_date"]: r["n"] for r in spark.sql(
    "SELECT report_date, count(*) AS n FROM gold_daily_commentary GROUP BY report_date").collect()}
for d in [latest_day] + missed:
    check(f"{d} 正好有一份日报 (exactly one summary for {d})", per_day.get(d) == 1, str(per_day.get(d)))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT report_date, language, model, created_at, left(commentary, 120) AS commentary
# MAGIC FROM gold_daily_commentary ORDER BY report_date DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · 小结：什么数据用什么方法回填 (Recap: what to backfill, and how)
# MAGIC
# MAGIC | 情况 (Situation) | 方法 (How) |
# MAGIC |---|---|
# MAGIC | 管道失败，修复后重跑<br>The pipeline failed and is fixed | Repair run（或重新运行）：流式表从断点继续，不丢、不重<br>Repair run (or run again): streaming tables continue from where they stopped, nothing lost or duplicated |
# MAGIC | 错误数据已经写进了表<br>Wrong data was already written | 修复源头，再对受影响的表做完全刷新（源中必须仍有全部数据）<br>Fix the source, then fully refresh only the affected tables (the source must still hold all the data) |
# MAGIC | 一次性导入历史数据<br>A one-off historical load | 一次性流 `INSERT INTO ONCE`（不在本实验中）<br>A one-time flow `INSERT INTO ONCE` (not in this lab) |
# MAGIC | 按日期产生的输出漏了几天<br>Date-based outputs missed some days | Run backfill + 日期参数（如 `report_date`）<br>Run backfill + a date parameter (such as `report_date`) |
# MAGIC
# MAGIC **让一切安全的规则：任务必须幂等。** 修复运行会从头重跑任务；用 MERGE 或覆盖写入结果，而不是追加。日志类的表（告警、审计）可以按次追加。<br>
# MAGIC **The rule that makes all of this safe: tasks must be idempotent.** A repair re-runs a task from the beginning; write results with MERGE or overwrite, not append. Log tables (alerts, audit) may append one row per attempt.
# MAGIC
# MAGIC **加分 (Bonus):** 作业 → **Add trigger → File arrival**，路径 `/Volumes/<catalog>/u_<你的名字>/staging/`。再运行一次第 3 步的单元格写入新文件，作业会自己启动（触发器之间至少 60 秒）。演示后记得暂停触发器。<br>
# MAGIC Job → **Add trigger → File arrival** on `/Volumes/<catalog>/u_<your_name>/staging/`. Run the step 3 cell again to write a new file, and the job starts by itself (at least 60 s between triggers). Pause the trigger afterwards.

# COMMAND ----------

print(f"{sum(ok for _, ok in checks)}/{len(checks)} checks passed")
if AUTO:
    failed = [label for label, ok in checks if not ok]
    assert not failed, f"failed checks: {failed}"
    dbutils.notebook.exit(f"{len(checks)}/{len(checks)} checks passed")
