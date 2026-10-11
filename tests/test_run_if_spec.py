"""实验 03d 作业定义的离线检查 (offline checks for the lab 03d job definition)."""

import importlib.util
import os

HERE = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location("run_if_spec", os.path.join(HERE, "..", "labs", "run_if_spec.py"))
rif = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rif)

SETTINGS = rif.run_if_job_settings("run_if_lab_x", "/Users/x@example.com/hytech_de_lab/jobs", "hytech_ws_catalog", "u_x")
TASKS = {t["task_key"]: t for t in SETTINGS["tasks"]}


def test_eleven_unique_tasks():
    assert len(SETTINGS["tasks"]) == 11 == len(TASKS)


def test_every_dependency_exists():
    for t in SETTINGS["tasks"]:
        for d in t.get("depends_on", []):
            assert d["task_key"] in TASKS, d


def test_all_six_run_if_values_are_used():
    used = {t["run_if"] for t in SETTINGS["tasks"] if t.get("depends_on")}
    assert used == {"ALL_SUCCESS", "AT_LEAST_ONE_SUCCESS", "NONE_FAILED", "ALL_DONE", "AT_LEAST_ONE_FAILED", "ALL_FAILED"}


def test_the_failing_check_does_not_retry():
    t = TASKS["check_mt5_hk01"]
    assert t["max_retries"] == 0 and t["disable_auto_optimization"] is True
    assert t["notebook_task"]["base_parameters"]["server_id"] == "mt5-hk-01"


def test_audit_depends_on_every_condition_task():
    assert {d["task_key"] for d in TASKS["all_done_AUDIT"]["depends_on"]} == {t[0] for t in rif.CONDITION_TASKS}


def test_job_parameters_and_notebook_paths():
    assert SETTINGS["parameters"] == [{"name": "catalog", "default": "hytech_ws_catalog"}, {"name": "schema", "default": "u_x"}]
    paths = {t["notebook_task"]["notebook_path"].rsplit("/", 1)[-1] for t in SETTINGS["tasks"]}
    assert paths == set(rif.RUN_IF_JOB_NOTEBOOKS)
    assert rif.run_if_job_name("zhang_san") == "run_if_lab_zhang_san"


def _evaluate(run_if, dep_states):
    """Run if 规则（Databricks 文档）(Run if rules, from the Databricks docs)."""
    failed = [s in ("FAILED", "UPSTREAM_FAILED") for s in dep_states]
    succeeded = [s in ("SUCCESS", "EXCLUDED") for s in dep_states]   # Excluded counts as success
    return {
        "ALL_SUCCESS": "SUCCESS" if all(succeeded) else "UPSTREAM_FAILED",
        "AT_LEAST_ONE_SUCCESS": "SUCCESS" if any(succeeded) else "UPSTREAM_FAILED",
        "NONE_FAILED": "SUCCESS" if not any(failed) else "UPSTREAM_FAILED",
        "ALL_DONE": "SUCCESS",
        "AT_LEAST_ONE_FAILED": "SUCCESS" if any(failed) else "EXCLUDED",
        "ALL_FAILED": "SUCCESS" if all(failed) else "EXCLUDED",
    }[run_if]


def test_expected_states_follow_the_run_if_rules():
    actual = {key: state for key, _, state in rif.CHECK_TASKS}
    for key, _, deps, run_if, _ in rif.CONDITION_TASKS:
        actual[key] = _evaluate(run_if, [actual[d] for d in deps])
    audit_key, _, audit_if, _ = rif.AUDIT_TASK
    actual[audit_key] = _evaluate(audit_if, [actual[t[0]] for t in rif.CONDITION_TASKS])
    assert actual == rif.expected_states()


def test_compare_states_flags_differences():
    rows = rif.compare_states({"a": "SUCCESS", "b": "FAILED"}, {"a": "SUCCESS", "b": "SUCCESS"})
    assert rows == [("a", "SUCCESS", "SUCCESS", True), ("b", "SUCCESS", "FAILED", False)]
