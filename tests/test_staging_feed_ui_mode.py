"""实验 03c 手动模式（UI）用到的辅助函数的离线检查，用假的客户端和 Spark。
Offline checks for the helpers lab 03c's hand (UI) mode relies on, with a fake client and a fake Spark."""

import ast
import importlib.util
import io
import os
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

HERE = os.path.dirname(__file__)
ROOT = os.path.join(HERE, "..")
_spec = importlib.util.spec_from_file_location("staging_feed", os.path.join(ROOT, "labs", "staging_feed.py"))
sf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sf)

USER = "zhang.wei@hytech.example"
CATALOG = "hytech_ws_catalog"
CJK = re.compile(r"[一-鿿]")
with open(os.path.join(ROOT, "labs", "03_pipeline", "staging", sf.STAGING_FILE), encoding="utf-8") as fh:
    STAGING_SQL = fh.read()


class FakeSpark:
    """每次 sql() 返回下一个 n；记录 SQL 和参数 (each sql() returns the next n; records the SQL and its args)."""

    def __init__(self, answers, tables=("bronze_staging_mt5_deals",)):
        self.answers, self.seen = list(answers), []
        self.catalog = SimpleNamespace(tableExists=lambda name: name.split(".")[-1] in tables)

    def sql(self, text, args=None):
        self.seen.append((text, args))
        n = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        return SimpleNamespace(first=lambda: {"n": n})


def fake_w(configuration, files, latest_state="COMPLETED"):
    """一个只有管道、配置和工作区文件的假 WorkspaceClient (a fake WorkspaceClient with one pipeline, its settings and workspace files)."""
    names = sf.my_names(USER)
    pipeline = {"spec": {"configuration": configuration,
                         "libraries": [{"glob": {"include": f"/Workspace{names['lab_root']}/03_pipeline/transformations/**"}}]},
                "latest_updates": [{"update_id": "u1", "state": latest_state}]}

    def download(path):
        if path not in files:
            raise FileNotFoundError(path)
        return io.BytesIO(files[path].encode("utf-8"))

    return SimpleNamespace(
        pipelines=SimpleNamespace(list_pipelines=lambda filter=None: [SimpleNamespace(name=names["pipeline"], pipeline_id="p1")]),
        api_client=SimpleNamespace(do=lambda method, path, body=None, query=None: pipeline),
        workspace=SimpleNamespace(download=download),
    )


def wired_files(sql_text=STAGING_SQL):
    names = sf.my_names(USER)
    return {f"{names['lab_root']}/03_pipeline/transformations/{sf.STAGING_FILE}": sql_text}


def test_price_rule_action_reads_the_valid_trade_price_clause():
    assert sf.price_rule_action(STAGING_SQL) == "FAIL"
    assert sf.price_rule_action(sf.switch_rule(STAGING_SQL, "valid_trade_price", "DROP")) == "DROP"
    assert sf.price_rule_action("SELECT 1") is None


def test_lane_status_reports_every_missing_piece():
    status = sf.lane_status(fake_w({}, {}, latest_state="FAILED"), CATALOG, USER)
    assert status == {"staging_root": False, "file": False, "price_rule": None, "latest_update": "FAILED"}


def test_lane_status_is_green_when_the_lane_is_wired():
    conf = {"staging_root": sf.staging_root(CATALOG, sf.my_names(USER)["schema"])}
    status = sf.lane_status(fake_w(conf, wired_files()), CATALOG, USER)
    assert status == {"staging_root": True, "file": True, "price_rule": "FAIL", "latest_update": "COMPLETED"}


def test_lane_status_sees_a_rule_left_at_drop():
    conf = {"staging_root": sf.staging_root(CATALOG, sf.my_names(USER)["schema"])}
    dropped = sf.switch_rule(STAGING_SQL, "valid_trade_price", "DROP")
    assert sf.lane_status(fake_w(conf, wired_files(dropped)), CATALOG, USER)["price_rule"] == "DROP"


def test_wait_for_latest_update_polls_until_the_update_has_finished(monkeypatch, capsys):
    monkeypatch.setattr(sf.time, "sleep", lambda s: None)
    spark = FakeSpark([0, 0, 1])
    assert sf.wait_for_latest_update(spark, "c.s", wait_seconds=600) is True
    assert len(spark.seen) == 3
    assert CJK.search(capsys.readouterr().out)


def test_wait_for_latest_update_warns_in_both_languages_when_it_gives_up(monkeypatch, capsys):
    monkeypatch.setattr(sf.time, "sleep", lambda s: None)
    assert sf.wait_for_latest_update(FakeSpark([0]), "c.s", wait_seconds=0) is False
    out = capsys.readouterr().out
    assert "⚠️" in out and CJK.search(out) and "run this cell again" in out


def test_wait_for_latest_update_ignores_updates_created_before_the_run_was_requested():
    spark = FakeSpark([1])
    after = datetime(2026, 10, 13, 3, 0, tzinfo=timezone.utc)
    sf.wait_for_latest_update(spark, "c.s", after=after, wait_seconds=0)
    text, args = spark.seen[0]
    assert ":after" in text and args["after"] <= after


def test_batch_counts_explains_missing_staging_tables():
    with pytest.raises(RuntimeError, match=CJK):
        sf.batch_counts(FakeSpark([0], tables=()), "c.s", "/Volumes/c/s/staging/x.parquet")


def raised_messages(path):
    """文件中每个 raise 的消息文字 (the literal text of every raise message in a file)."""
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call) and node.exc.args:
            arg = node.exc.args[0]
            parts = [arg] if isinstance(arg, ast.Constant) else [v for v in ast.walk(arg) if isinstance(v, ast.Constant)]
            yield node.lineno, "".join(str(p.value) for p in parts if isinstance(p.value, str))


@pytest.mark.parametrize("path", ["labs/staging_feed.py", "labs/03d_Run_If_Dependencies.py", "labs/03c_Data_Quality_and_Monitoring.py",
                                  "labs/04c_Failure_Recovery_and_Backfill.py"])
def test_participant_facing_errors_are_chinese_first_and_english(path):
    for line, text in raised_messages(path):
        assert CJK.search(text) and re.search(r"[A-Za-z]{3,}", text), f"{path}:{line}: {text!r}"
