# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · 一键安装 (Master setup)
# MAGIC
# MAGIC 不需要 Databricks CLI，也不需要 bundle：在工作区里运行这个笔记本，就会创建工作坊所需的全部作业和管道，并依次运行 setup 作业和参考答案作业。可以重复运行：同名的作业和管道会原地更新，不会重复创建。
# MAGIC
# MAGIC No Databricks CLI and no bundle needed: run this notebook in the workspace to create every job and pipeline the workshop needs, then run the setup job and the solution job in order. Safe to re-run: jobs and the pipeline with the same name are updated in place, never duplicated.
# MAGIC
# MAGIC | 资源 (Resource) | 用途 (Purpose) |
# MAGIC |---|---|
# MAGIC | 作业 (Job) `hytech_ws_setup` | catalog、schema、volume、授权、合成数据、学员 schema、`ops` 视图、成本仪表盘、Genie Code 技能<br>Catalog, schemas, volumes, grants, synthetic data, participant schemas, `ops` views, cost dashboard, Genie Code skill |
# MAGIC | 作业 (Job) `hytech_ws_drip_producer` | 可选，只供讲师在自己的测试工作区使用：向共享落地区写入新的 CDC 文件（这里不运行）。不要在 Hytech 的课堂上运行：学员用 `labs/00b_Live_Data` 生成自己的数据<br>Optional, for an instructor's own test workspace only: writes new CDC files into the shared landing zone (not run here). Don't run it in the Hytech class: participants generate their own data with `labs/00b_Live_Data` |
# MAGIC | 管道 (Pipeline) `hytech_trade_lakehouse_solution` | 参考答案管道，输出到参考答案 schema<br>Reference solution pipeline, writes the solutions schema |
# MAGIC | 作业 (Job) `hytech_daily_trading_reporting_solution` | 实验 04 的参考答案作业<br>Reference solution job for lab 04 |
# MAGIC
# MAGIC **步骤：** 右上角选择 **Serverless** → 填写顶部的参数 → **Run all**。全部完成约需 15 分钟。<br>
# MAGIC **Steps:** attach **Serverless** → fill in the widgets at the top → **Run all**. About 15 minutes in total.
# MAGIC
# MAGIC 需要工作区管理员权限（或 catalog 所有者 + `CREATE CATALOG`）。作业归运行此笔记本的用户所有，并以该用户身份运行；`instructors` 中的讲师会获得这些作业和管道的管理权限。<br>
# MAGIC Needs workspace-admin rights (or catalog owner + `CREATE CATALOG`). The jobs are owned by and run as the user who runs this notebook; the people in `instructors` get CAN MANAGE on the jobs and the pipeline.
# MAGIC
# MAGIC ⚠️ 如果这个工作区已经用 bundle 部署过参考答案管道（例如讲师的测试工作区），请把 `solutions_schema` 设为新的 schema（如 `solutions_master`）：一个 schema 里的管道表只能属于一个管道。笔记本会先检查这一点。<br>
# MAGIC ⚠️ If this workspace already has the solution pipeline from a bundle deployment (for example the instructors' test workspace), set `solutions_schema` to a new schema such as `solutions_master`: pipeline tables in a schema can belong to one pipeline only. The notebook checks this first.

# COMMAND ----------

dbutils.widgets.text("catalog", "hytech_ws_catalog", "Catalog")
dbutils.widgets.text("participant_group", "de_workshop_sz", "学员组 (Participant group)")
dbutils.widgets.text("participants", "", "学员邮箱，逗号分隔 (Participant emails, comma-separated)")
dbutils.widgets.text("instructors", "", "讲师邮箱，逗号分隔 (Instructor emails, comma-separated)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5", "模型端点 (LLM endpoint)")
dbutils.widgets.text("warehouse_id", "", "仪表盘 SQL 仓库，留空自动选择 (Dashboard SQL warehouse, empty = auto)")
dbutils.widgets.text("solutions_schema", "solutions", "参考答案 schema (Solutions schema)")
dbutils.widgets.dropdown("scale", "full", ["full", "demo"], "数据规模 (Data scale)")
dbutils.widgets.dropdown("reset", "false", ["false", "true"], "重新生成数据 (Regenerate data)")
dbutils.widgets.dropdown("run_setup_job", "true", ["true", "false"], "运行 setup 作业 (Run setup job)")
dbutils.widgets.dropdown("run_solution_job", "true", ["true", "false"], "运行参考答案作业 (Run solution job)")

P = {k: dbutils.widgets.get(k).strip() for k in (
    "catalog", "participant_group", "participants", "instructors", "llm_endpoint", "warehouse_id",
    "solutions_schema", "scale", "reset", "run_setup_job", "run_solution_job")}
catalog, schema = P["catalog"], P["solutions_schema"]
instructors = [e.strip() for e in P["instructors"].replace("\n", ",").split(",") if e.strip()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · 辅助函数 (Helpers)
# MAGIC
# MAGIC 用 REST API 创建作业和管道，与实验 04b 的写法一致。同名对象原地更新；如果同名对象由 bundle 管理，则停止，避免两套部署互相覆盖。
# MAGIC
# MAGIC Jobs and the pipeline are created through the REST API, the same way lab 04b does it. Objects with the same name are updated in place; if one is managed by a bundle, the notebook stops so the two deployments never overwrite each other.

# COMMAND ----------

import os
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")
# 仓库根目录：本笔记本位于 <root>/setup (Repo root: this notebook lives in <root>/setup)
ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
# 用于识别和清理本笔记本创建的作业 (Marks the jobs this notebook created, for teardown)
TAGS = {"workshop": "hytech_de_2026", "managed_by": "master_setup"}
print("repo root =", ROOT)


def notebook(rel):
    """仓库内笔记本的工作区路径，并确认它存在。
    Workspace path of a notebook in this repo, checked to exist."""
    path = f"{ROOT}/{rel}"
    try:
        info = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": path})
    except Exception as e:  # noqa: BLE001 - give a clear hint instead of a raw 404
        raise FileNotFoundError(f"{path} not found: run this notebook from inside the workshop repo folder") from e
    if info.get("object_type") != "NOTEBOOK":
        raise FileNotFoundError(f"{path} is a {info.get('object_type')}, expected a notebook")
    return path


def task(key, rel, deps=(), params=None, **extra):
    """一个 notebook 任务；deps 可以是任务名，也可以是带 outcome 的字典。
    A notebook task; deps are task keys or dicts with an outcome."""
    t = {"task_key": key, "notebook_task": {"notebook_path": notebook(rel), "base_parameters": params or {}}}
    if deps:
        t["depends_on"] = [d if isinstance(d, dict) else {"task_key": d} for d in deps]
    t.update(extra)
    return t


def upsert_job(settings):
    """按名称创建或原地更新作业；绝不改动由 bundle 管理的同名作业。
    Create the job, or update it in place by name; never touch a bundle-managed job of the same name."""
    found = w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": settings["name"]}).get("jobs", [])
    if not found:
        job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
        print(f"✅ created job {settings['name']} ({job_id})")
        return job_id
    job_id = found[0]["job_id"]
    current = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})
    if (current["settings"].get("deployment") or {}).get("kind") == "BUNDLE":
        raise RuntimeError(f"Job {settings['name']} ({job_id}) is managed by a bundle: use the bundle, or delete it first")
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
    print(f"✅ updated job {settings['name']} ({job_id})")
    return job_id


def find_pipeline(name):
    """按名称查找管道，返回 pipeline_id；没有则返回 None。
    Look up a pipeline by name; return its pipeline_id, or None."""
    listed = w.api_client.do("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{name}'"})
    found = [p for p in listed.get("statuses", []) if p["name"] == name]
    return found[0]["pipeline_id"] if found else None


def pipelines_owning(catalog, schema):
    """该 schema 中各表所属的管道 ID；catalog 或 schema 还不存在时返回空集合。
    IDs of the pipelines that own tables in this schema; empty if the catalog or schema does not exist yet."""
    try:
        tables = [r.table_name for r in spark.sql(
            f"SELECT table_name FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name NOT LIKE '__materialization%'").collect()]
    except Exception:  # noqa: BLE001 - the catalog does not exist yet, so nothing can conflict
        return set()
    owners = set()
    for table in tables[:50]:
        try:
            props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {catalog}.{schema}.`{table}`").collect()}
        except Exception:  # noqa: BLE001 - skip a table we cannot read
            continue
        if props.get("pipelines.pipelineId"):
            owners.add(props["pipelines.pipelineId"])
    return owners


def upsert_pipeline(spec):
    """按名称创建或原地更新管道；同样绝不改动由 bundle 管理的管道。
    Create the pipeline, or update it in place by name; again never a bundle-managed one."""
    pipeline_id = find_pipeline(spec["name"])
    if not pipeline_id:
        pipeline_id = w.api_client.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
        print(f"✅ created pipeline {spec['name']} ({pipeline_id})")
        return pipeline_id
    current = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}")
    if (current.get("spec", {}).get("deployment") or {}).get("kind") == "BUNDLE":
        raise RuntimeError(f"Pipeline {spec['name']} ({pipeline_id}) is managed by a bundle: use the bundle, or delete it first")
    w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**spec, "id": pipeline_id})
    print(f"✅ updated pipeline {spec['name']} ({pipeline_id})")
    return pipeline_id


def share(kind, object_id, acl):
    """给作业或管道追加权限；主体不存在时只提示，不中断。
    Add permissions on a job or pipeline; warn instead of failing if a principal does not exist."""
    for entry in acl:
        try:
            w.api_client.do("PATCH", f"/api/2.0/permissions/{kind}/{object_id}", body={"access_control_list": [entry]})
        except Exception as e:  # noqa: BLE001 - e.g. the account-level group is not created yet
            who = entry.get("user_name") or entry.get("group_name")
            print(f"⚠️  could not grant {entry['permission_level']} to {who} ->", str(e).splitlines()[0][:160])


def run_and_wait(job_id, params, label, timeout_minutes):
    """运行作业并等待结束；失败时抛出异常并给出运行链接。
    Run a job and wait for it to finish; raise with the run link if it does not succeed."""
    run_id = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id, "job_parameters": params})["run_id"]
    url = f"{HOST}/jobs/{job_id}/runs/{run_id}"
    print(f"▶️  {label}: {url}")
    deadline = time.time() + timeout_minutes * 60
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = run.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"{label} is still running after {timeout_minutes} min: {url}")
        time.sleep(20)
    result = state.get("result_state")
    tasks = ", ".join(f"{t['task_key']}={(t.get('state') or {}).get('result_state')}" for t in run.get("tasks", []))
    print(f"   {label}: {result} ({tasks})")
    if result != "SUCCESS":
        raise RuntimeError(f"{label} finished with {result}: {url}")


manage = [{"user_name": e, "permission_level": "CAN_MANAGE"} for e in instructors]
view = [{"group_name": P["participant_group"], "permission_level": "CAN_VIEW"}] if P["participant_group"] else []
PIPELINE_NAME = "hytech_trade_lakehouse_solution"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b · 预检：参考答案 schema 是否空闲 (Pre-flight: is the solutions schema free?)
# MAGIC
# MAGIC 如果参考答案 schema 里已有另一个管道的表（例如测试工作区里 bundle 部署的管道），这里会立即停止，而不是等 setup 作业跑完后在管道里失败。
# MAGIC
# MAGIC If the solutions schema already holds another pipeline's tables (for example the bundle-deployed pipeline on a test workspace), the notebook stops here instead of failing inside the pipeline after the setup job.

# COMMAND ----------

foreign = pipelines_owning(catalog, schema) - {find_pipeline(PIPELINE_NAME)}
if foreign:
    raise RuntimeError(
        f"{catalog}.{schema} already holds tables of another pipeline ({', '.join(sorted(foreign))}). "
        f"Set solutions_schema to a new schema, for example {schema}_master, or delete that pipeline first.")
print(f"✅ {catalog}.{schema} is free for {PIPELINE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · setup 作业和滴灌程序 (Setup job and drip producer)
# MAGIC
# MAGIC 与 `resources/setup.job.yml` 和 `resources/producer.job.yml` 的定义相同。
# MAGIC
# MAGIC Same definitions as `resources/setup.job.yml` and `resources/producer.job.yml`.

# COMMAND ----------

setup_job = {
    "name": "hytech_ws_setup",
    "description": "一次性环境搭建：catalog/schema/volume + 授权、合成 MT5 落地区（DMS 风格 CDC）、学员 schema、"
                   "系统表治理视图、成本仪表盘、Genie Code 技能。 / One-time workshop setup: catalog/schemas/volumes + "
                   "grants, synthetic MT5 landing zone (DMS-style CDC), participant schemas, governed system-table "
                   "views, cost dashboard, Genie Code skill.",
    "tags": TAGS,
    "max_concurrent_runs": 1,
    "parameters": [
        {"name": "catalog", "default": catalog},
        {"name": "participant_group", "default": P["participant_group"]},
        {"name": "scale", "default": P["scale"]},
        {"name": "reset", "default": "false"},
        {"name": "participants", "default": P["participants"]},
        {"name": "warehouse_id", "default": P["warehouse_id"]},
    ],
    "tasks": [
        task("create_catalog_schemas", "setup/01_create_catalog_schemas"),
        task("generate_data", "setup/02_generate_data", ["create_catalog_schemas"], timeout_seconds=3600),
        task("participant_schemas", "setup/04_participant_schemas", ["create_catalog_schemas"]),
        task("ops_views", "setup/05_ops_views", ["create_catalog_schemas"]),
        task("cost_dashboard", "setup/07_cost_dashboard", ["ops_views"]),
        task("workshop_warehouses", "setup/08_workshop_warehouses"),
        task("install_genie_code_skill", "setup/06_install_genie_code_skill"),
    ],
}

drip_job = {
    "name": "hytech_ws_drip_producer",
    "description": "可选，只供讲师在自己的测试工作区使用（课堂上学员用 labs/00b_Live_Data）：按间隔向共享落地区写入新的 "
                   "DMS 风格 CDC 文件和应用事件。new_server=mt5-hk-01 上线新 MT5 服务器；bad_batch_pct 注入无效成交。 / "
                   "Optional, for an instructor's own test workspace only (in class, participants use labs/00b_Live_Data): "
                   "writes new DMS-style CDC files and app events into the shared landing zone every interval. "
                   "new_server=mt5-hk-01 onboards a new MT5 server; bad_batch_pct injects invalid deals.",
    "tags": TAGS,
    "max_concurrent_runs": 1,
    "parameters": [
        {"name": "catalog", "default": catalog},
        {"name": "duration_minutes", "default": "60"},
        {"name": "interval_seconds", "default": "30"},
        {"name": "servers", "default": ""},
        {"name": "new_server", "default": ""},
        {"name": "bad_batch_pct", "default": "0"},
        {"name": "deals_per_tick", "default": "24"},
    ],
    "tasks": [task("drip", "setup/03_drip_producer", timeout_seconds=14400)],
}

setup_id = upsert_job(setup_job)
drip_id = upsert_job(drip_job)
for job_id in (setup_id, drip_id):
    share("jobs", job_id, manage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · 运行 setup 作业 (Run the setup job)
# MAGIC
# MAGIC 约 10 分钟。如果落地区已有数据且 `reset=false`，数据生成步骤会跳过，其他步骤照常刷新。
# MAGIC
# MAGIC About 10 minutes. If the landing zone already has data and `reset=false`, the data step is skipped and the other steps refresh as usual.

# COMMAND ----------

if P["run_setup_job"] == "true":
    run_and_wait(setup_id, {
        "catalog": catalog, "participant_group": P["participant_group"], "participants": P["participants"],
        "scale": P["scale"], "reset": P["reset"], "warehouse_id": P["warehouse_id"],
    }, "hytech_ws_setup", timeout_minutes=90)
else:
    print("⏭️  setup job not run (run_setup_job=false)")

# 成本仪表盘由 setup 作业创建在本用户的主目录中；同样共享给讲师
# The setup job creates the cost dashboard in this user's home folder; share it with the instructors too
if manage:
    dash = f"/Workspace/Users/{w.current_user.me().user_name}/hytech_de_workshop/Hytech DE Workshop - Cost and Health.lvdash.json"
    try:
        share("dashboards", w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": dash})["resource_id"], manage)
    except Exception as e:  # noqa: BLE001 - the dashboard only exists after the setup job has run
        print("⚠️  cost dashboard not shared ->", str(e).splitlines()[0][:160])

# 08 创建的 SQL 仓库也共享给讲师 (share the SQL warehouses 08 created with the instructors too)
if manage:
    for x in w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", []):
        if x["name"] in ("hytech_workshop_sql", "hytech_workshop_rt"):
            share("warehouses", x["id"], manage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · 参考答案管道和作业 (Solution pipeline and job)
# MAGIC
# MAGIC 与 `resources/solution.pipeline.yml` 和 `resources/solution.job.yml` 的定义相同。管道需要 catalog 已存在，所以放在 setup 作业之后创建。学员组获得只读权限，可以在 M5 中查看参考答案。
# MAGIC
# MAGIC Same definitions as `resources/solution.pipeline.yml` and `resources/solution.job.yml`. The pipeline needs the catalog to exist, so it is created after the setup job. The participant group gets view rights so people can look at the reference solution in M5.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} "
          "COMMENT 'Instructor solution pipeline output (catch-up, Genie, AI Functions labs)'")

pipeline_id = upsert_pipeline({
    "name": PIPELINE_NAME,
    "catalog": catalog,
    "schema": schema,
    "serverless": True,
    "channel": "CURRENT",
    "continuous": False,
    "root_path": f"{ROOT}/solutions/pipeline",
    "libraries": [{"glob": {"include": f"{ROOT}/solutions/pipeline/transformations/**"}}],
    "configuration": {
        "landing_root": f"/Volumes/{catalog}/raw/landing",
        "ref_root": f"/Volumes/{catalog}/raw/ref",
        "hytech.managed_by": "master_setup",  # 供 99_teardown 识别 (lets 99_teardown find it)
    },
    "event_log": {"catalog": catalog, "schema": schema, "name": "pipeline_event_log"},
})

IDS = {"job_id": "{{job.id}}", "run_id": "{{job.run_id}}"}
solution_job = {
    "name": "hytech_daily_trading_reporting_solution",
    "description": "实验 04 的参考答案：管道 -> 数据质量门（if/else）-> AI 日报 | 数据质量告警；服务器循环任务对账；"
                   "运行条件告警和审计。 / Reference solution for lab 04: pipeline -> DQ gate (if/else) -> AI daily "
                   "summary | DQ alert; for-each server reconciliation; Run-if alerting and audit.",
    "tags": TAGS,
    "max_concurrent_runs": 1,
    "parameters": [
        {"name": "catalog", "default": catalog},
        {"name": "schema", "default": schema},
        {"name": "dq_max_drop_pct", "default": "1.0"},
        {"name": "servers", "default": '["mt5-sg-01","mt5-sg-02","mt5-uk-01","mt5-cy-01"]'},
        {"name": "llm_endpoint", "default": P["llm_endpoint"]},
        {"name": "webhook_url", "default": ""},
        {"name": "fail_server", "default": ""},
        {"name": "report_date", "default": ""},
    ],
    "tasks": [
        {"task_key": "run_pipeline", "pipeline_task": {"pipeline_id": pipeline_id},
         "max_retries": 1, "min_retry_interval_millis": 60000},
        task("dq_gate", "jobs/dq_gate", ["run_pipeline"]),
        {"task_key": "dq_ok", "depends_on": [{"task_key": "dq_gate"}],
         "condition_task": {"op": "LESS_THAN", "left": "{{tasks.dq_gate.values.dq_drop_pct}}",
                            "right": "{{job.parameters.dq_max_drop_pct}}"}},
        task("publish_daily_summary", "jobs/publish_daily_summary", [{"task_key": "dq_ok", "outcome": "true"}]),
        task("notify_dq_owner", "jobs/notify", [{"task_key": "dq_ok", "outcome": "false"}], params={
            "reason": "dq_gate_failed",
            "detail": "dq_drop_pct={{tasks.dq_gate.values.dq_drop_pct}}% > {{job.parameters.dq_max_drop_pct}}%", **IDS}),
        {"task_key": "reconcile_servers", "depends_on": [{"task_key": "run_pipeline"}],
         "for_each_task": {"inputs": "{{job.parameters.servers}}", "concurrency": 4,
                           "task": task("reconcile_server", "jobs/reconcile_server", params={"server_id": "{{input}}"})}},
        task("alert_on_failure", "jobs/notify", ["run_pipeline", "dq_gate", "reconcile_servers", "publish_daily_summary"],
             params={"reason": "job_task_failed", **IDS}, run_if="AT_LEAST_ONE_FAILED"),
        task("write_audit_row", "jobs/audit", ["alert_on_failure", "notify_dq_owner", "publish_daily_summary",
                                               "reconcile_servers"], params=IDS, run_if="ALL_DONE"),
    ],
}

solution_id = upsert_job(solution_job)
share("pipelines", pipeline_id, manage + view)
share("jobs", solution_id, manage + view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · 运行参考答案作业 (Run the solution job)
# MAGIC
# MAGIC 约 4 分钟：运行管道、数据质量门、AI 日报和对账，填充参考答案 schema（实验 07 和追进度需要它）。
# MAGIC
# MAGIC About 4 minutes: runs the pipeline, the DQ gate, the AI summary and the reconciliation, and fills the solutions schema (lab 07 and catch-up need it).

# COMMAND ----------

if P["run_solution_job"] == "true":
    run_and_wait(solution_id, {}, "hytech_daily_trading_reporting_solution", timeout_minutes=60)
else:
    print("⏭️  solution job not run (run_solution_job=false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · 结果 (Result)
# MAGIC
# MAGIC 下面是创建的资源链接。课堂上不需要运行任何作业：学员需要新数据时自己运行 `labs/00b_Live_Data`；滴灌程序作业是可选的，不要在课堂上运行（见讲师指南）。
# MAGIC
# MAGIC Links to everything that was created. Nothing needs to run in class: participants run `labs/00b_Live_Data` when they need new data; the drip producer job is optional, so don't run it in class (see the facilitator guide).

# COMMAND ----------

resources = [
    ("作业 (Job)", "hytech_ws_setup", f"{HOST}/jobs/{setup_id}"),
    ("作业 (Job)", "hytech_ws_drip_producer", f"{HOST}/jobs/{drip_id}"),
    ("管道 (Pipeline)", "hytech_trade_lakehouse_solution", f"{HOST}/pipelines/{pipeline_id}"),
    ("作业 (Job)", "hytech_daily_trading_reporting_solution", f"{HOST}/jobs/{solution_id}"),
]
for kind, name, url in resources:
    print(f"{kind:16} {name:42} {url}")
rows = "".join(f"<tr><td>{k}</td><td><a href='{u}' target='_blank'>{n}</a></td></tr>" for k, n, u in resources)
displayHTML(f"<table><tr><th>类型 (Type)</th><th>名称 (Name)</th></tr>{rows}</table>")
