"""作业定义和作业任务的结构检查 (structural checks for the job definitions and job tasks)."""

import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..")


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def test_every_job_definition_passes_report_date():
    for path in ("labs/04b_Jobs_Catch_Up.py", "setup/00_master_setup.py"):
        assert '{"name": "report_date", "default": ""}' in read(path), path
    assert re.search(r'- name: report_date\n\s+default: ""', read("resources/solution.job.yml"))


def test_publish_daily_summary_is_date_aware_and_idempotent():
    text = read("jobs/publish_daily_summary.py")
    assert 'dbutils.widgets.text("report_date", ""' in text
    assert "MERGE INTO {cs}.gold_daily_commentary" in text
    assert "INSERT INTO {cs}.gold_daily_commentary" not in text


def test_dq_gate_merges_per_update():
    text = read("jobs/dq_gate.py")
    assert "MERGE INTO {cs}.ops_dq_results" in text
    assert 'mode("append")' not in text


def test_publish_daily_summary_accepts_a_backfill_date_or_datetime():
    # Run backfill 的参数可以选 {{backfill.iso_date}} 或 {{backfill.iso_datetime}}：两种都要能用
    # Run backfill can pass {{backfill.iso_date}} or {{backfill.iso_datetime}}: both must work
    assert "date.fromisoformat(requested[:10])" in read("jobs/publish_daily_summary.py")


def solution_job_dependencies():
    deps = {}
    for block in read("resources/solution.job.yml").split("\n        - task_key: ")[1:]:
        deps[block.split()[0]] = set(re.findall(r"^            - task_key: (\w+)", block, re.M))
    return deps


def test_lab_04_names_each_dependency_as_in_the_solution_job():
    # 学员在 Depends on 下拉框中选的是任务名称，不是表格里的行号 (participants pick task names in the Depends on dropdown, not the table's row numbers)
    expected = solution_job_dependencies()
    rows = re.findall(r"^\| \d+ \| `(\w+)` \|[^|]*\|(.*)\|$", read("labs/04_Lakeflow_Jobs.md"), re.M)
    assert [task for task, _ in rows] == list(expected)
    for task, settings in rows:
        zh, en = settings.split("<br>")
        for half, marker, end in ((zh, "依赖于 ", "；"), (en, "depends on ", ";")):
            listed = half.split(marker, 1)[1].split(end, 1)[0] if marker in half else ""
            assert set(re.findall(r"`(\w+)`", listed)) == expected[task], (task, listed)
