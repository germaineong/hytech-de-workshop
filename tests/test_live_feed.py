"""实验 00 和 00b 的个人实时数据辅助函数的离线检查 (offline checks for the personal live-data helpers of labs 00 and 00b)."""

import glob
import importlib.util
import json
import os
import re
import sys
import time

import pandas as pd
import pyarrow.parquet as pq
import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
from hytech_workshop.config import SERVER_META, WorkshopConfig, participant_schema  # noqa: E402
from hytech_workshop.history import build_all  # noqa: E402

_spec = importlib.util.spec_from_file_location("live_feed", os.path.join(ROOT, "labs", "live_feed.py"))
lf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lf)
CJK = re.compile(r"[一-鿿]")


def quiet(*_):
    pass


@pytest.fixture(scope="module")
def shared(tmp_path_factory):
    """和 setup 一样由 build_all 生成的小型共享历史 (a small shared history, built by build_all as setup does)."""
    root = str(tmp_path_factory.mktemp("raw"))
    cfg = WorkshopConfig(volume_root=root, users_per_server=200, positions_per_server_per_day=150,
                         history_days=5, events_per_day=500, event_days=2)
    build_all(cfg, log=quiet)
    return cfg


def snapshot(root):
    return {p: (os.path.getsize(p), os.path.getmtime(p))
            for p in glob.glob(f"{root}/**/*", recursive=True) if os.path.isfile(p)}


def personal_copy(shared, tmp_path):
    """和实验 00 第 1b 步一样复制 (copy the way step 1b of lab 00 does)."""
    personal = str(tmp_path / "u_zhang_san")
    lf.copy_missing(f"{shared.raw_root}/landing", f"{personal}/landing")
    lf.seed_producer(f"{shared.raw_root}/producer", f"{personal}/producer")
    return WorkshopConfig(volume_root=personal)


def test_personal_roots_follow_the_lab_00_naming_rule():
    assert lf.personal_roots("hytech_ws_catalog", "Zhang.San@hytech.example") == {
        "schema": "u_zhang_san", "volume_root": "/Volumes/hytech_ws_catalog/u_zhang_san",
        "landing": "/Volumes/hytech_ws_catalog/u_zhang_san/landing",
        "producer": "/Volumes/hytech_ws_catalog/u_zhang_san/producer"}
    for user in ("Zhang.San@hytech.example", "li-si+test@example.cn", "WANG_wu@example.com"):
        assert lf.personal_roots("c", user)["schema"] == participant_schema(user), user


def test_copy_missing_copies_skips_resumes_and_never_deletes(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "a" / "b").mkdir(parents=True)
    for i in range(5):
        (src / "a" / "b" / f"f{i}.parquet").write_bytes(b"x" * (i + 1))
    (src / "top.json").write_text("{}")
    assert lf.copy_missing(str(src), str(dst)) == (6, 0)
    assert lf.copy_missing(str(src), str(dst)) == (0, 6)
    (dst / "a" / "b" / "f3.parquet").write_bytes(b"x")              # 半个文件：大小不对 (a half-copied file: wrong size)
    (dst / "a" / "b" / "f4.parquet").unlink()                         # 中断的复制：文件缺失 (an interrupted copy: file missing)
    (dst / "mine.parquet").write_bytes(b"the participant's own file")
    assert lf.copy_missing(str(src), str(dst)) == (2, 4)
    assert (dst / "a" / "b" / "f3.parquet").read_bytes() == b"x" * 4
    assert (dst / "mine.parquet").exists()                            # 从不删除 (never deletes)
    assert not [p for p in glob.glob(f"{dst}/**/*", recursive=True) if p.endswith((".tmp", ".copying"))]


def test_copy_missing_fails_loudly_when_the_source_cannot_be_read(tmp_path):
    with pytest.raises(FileNotFoundError):
        lf.copy_missing(str(tmp_path / "missing"), str(tmp_path / "dst"))
    locked = tmp_path / "locked"
    locked.mkdir()
    (locked / "f.parquet").write_bytes(b"x")
    os.chmod(locked, 0)
    try:
        if os.access(locked, os.R_OK):
            pytest.skip("running as a user that ignores permissions")
        with pytest.raises(PermissionError):
            lf.copy_missing(str(locked), str(tmp_path / "dst2"))
    finally:
        os.chmod(locked, 0o755)


def test_seed_producer_copies_once_with_state_json_last(shared, tmp_path, monkeypatch):
    order = []
    real_copy = lf.shutil.copyfile
    monkeypatch.setattr(lf.shutil, "copyfile", lambda s, d: (order.append(os.path.basename(d)), real_copy(s, d))[1])
    dst = str(tmp_path / "producer")
    assert lf.seed_producer(f"{shared.raw_root}/producer", dst) is True
    assert order[-1] == "state.json"                                  # 最后复制：有它就说明复制完整 (copied last: its presence means a complete copy)
    assert sorted(order) == sorted(os.listdir(f"{shared.raw_root}/producer"))
    assert lf.seed_producer(f"{shared.raw_root}/producer", dst) is False
    with pytest.raises(FileNotFoundError):
        lf.seed_producer(str(tmp_path / "missing"), str(tmp_path / "other"))


def test_rerunning_lab_00_after_00b_keeps_your_generator_state(shared, tmp_path):
    cfg = personal_copy(shared, tmp_path)
    lf.run_ticks(cfg, 1, log=quiet)
    with open(f"{cfg.producer_root}/state.json", encoding="utf-8") as fh:
        after_tick = fh.read()
    personal_copy(shared, tmp_path)                                   # 实验 00 再运行一次 (lab 00 run again)
    with open(f"{cfg.producer_root}/state.json", encoding="utf-8") as fh:
        assert fh.read() == after_tick                                # 计数器没有回滚 (the counters are not rolled back)


def test_check_widgets_accepts_good_values_and_explains_bad_ones_in_both_languages():
    assert lf.check_widgets("1", "", "0", SERVER_META) == []
    assert lf.check_widgets("20", "mt5-hk-01", "60", SERVER_META) == []
    for ticks in ("0", "21", "x", "", "1.5"):
        assert len(lf.check_widgets(ticks, "", "0", SERVER_META)) == 1, ticks
    assert len(lf.check_widgets("1", "mt5-xx-99", "0", SERVER_META)) == 1
    for pct in ("-1", "101", "abc", "nan"):
        assert len(lf.check_widgets("1", "", pct, SERVER_META)) == 1, pct
    problems = lf.check_widgets("0", "nope", "999", SERVER_META)
    assert len(problems) == 3 and all(CJK.search(p) and re.search(r"[A-Za-z]{4,}", p) for p in problems)


def test_run_ticks_writes_new_files_into_the_personal_landing_only(shared, tmp_path):
    cfg = personal_copy(shared, tmp_path)
    before = snapshot(shared.raw_root)
    t0 = time.time()                                                  # 复制之后：复制来的文件不算新文件 (after the copy: copied files don't count as new)
    results = lf.run_ticks(cfg, 2, log=quiet)
    assert [r["ticks"] for r in results] == [1, 1]
    new = [p for p, _ in lf.files_since(cfg.landing_root, t0)]
    for server in shared.servers:
        assert any(f"/mt5/{server}/mt5_deals/" in p for p in new), server
    assert snapshot(shared.raw_root) == before                        # 共享历史不变 (the shared history is untouched)
    with open(f"{cfg.producer_root}/state.json", encoding="utf-8") as fh:
        assert json.load(fh)["last_tick_us"] >= int(t0 * 1_000_000)


def test_run_ticks_gives_every_tick_its_own_events_file(shared, tmp_path):
    cfg = personal_copy(shared, tmp_path)
    t0 = time.time()
    lf.run_ticks(cfg, 3, log=quiet)
    events = [p for p, _ in lf.files_since(cfg.events_dir, t0)]
    assert len(events) == 3, events   # 事件文件名精确到秒：同一秒的两个 tick 会互相覆盖 (event file names are per second: two ticks in one second overwrite each other)


def test_a_new_server_is_onboarded_into_the_personal_landing_only(shared, tmp_path):
    cfg = personal_copy(shared, tmp_path)
    t0 = time.time()
    results = lf.run_ticks(cfg, 1, new_server="mt5-hk-01", log=quiet)
    assert "mt5-hk-01" in results[-1]["servers"]
    summary = lf.summarize(cfg.landing_root, lf.files_since(cfg.landing_root, t0))
    assert {"mt5_deals", "mt5_positions", "app_events"} <= {table for table, *_ in summary}
    assert all(rows > 0 for *_, rows in summary)
    assert lf.missing_files(summary, results[-1]["servers"], "mt5-hk-01") == []
    assert not os.path.exists(shared.mt5_dir("mt5-hk-01", "mt5_deals"))


def test_missing_files_names_what_did_not_arrive():
    assert lf.missing_files([], ["mt5-sg-01"], "mt5-hk-01") == [
        "mt5-sg-01/mt5_deals", "mt5-hk-01/mt5_users/LOAD00000001.parquet",
        "mt5-hk-01/mt5_deals/LOAD00000001.parquet", "mt5-hk-01/mt5_positions/LOAD00000001.parquet"]
    seen = [("mt5_deals", "mt5-sg-01", "20261011-120000123.parquet", 12)]
    assert lf.missing_files(seen, ["mt5-sg-01"]) == []


def test_bad_batch_pct_makes_that_share_of_new_trades_invalid(shared, tmp_path):
    cfg = personal_copy(shared, tmp_path)
    t0 = time.time()
    lf.run_ticks(cfg, 1, bad_pct=0.6, log=quiet)
    files = [p for p, _ in lf.files_since(cfg.landing_root, t0) if "/mt5_deals/" in p]
    deals = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    trades = deals[deals["Action"] != 2]
    bad = trades[(trades["Volume"] <= 0) | (trades["Price"] <= 0) | trades["Symbol"].isna()]
    assert 0.5 <= len(bad) / len(trades) <= 0.7
