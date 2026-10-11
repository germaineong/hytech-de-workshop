"""工作坊的 catalog 名称在每个地方都一致 (the workshop's catalog name is the same everywhere)."""

import os
import re
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
from hytech_workshop.config import WorkshopConfig  # noqa: E402

CATALOG = "hytech_ws_catalog"
OLD = "hytech_de" + "_workshop"   # 分开写，这个测试才不会找到它自己 (split, so this test doesn't find itself)
# 成本仪表盘所在的工作区文件夹，不是 catalog (the cost dashboard's workspace folder, not the catalog)
DASHBOARD_FOLDER = re.compile(r"Users/[^/`]+/" + OLD + r"\b")


def tracked_files():
    out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return out.splitlines()


def test_the_old_catalog_name_is_gone_except_the_dashboard_folder():
    stale = []
    for path in tracked_files():
        try:
            with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
                text = fh.read()
        except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
            continue
        for n, line in enumerate(text.splitlines(), 1):
            if OLD in DASHBOARD_FOLDER.sub("", line):
                stale.append(f"{path}:{n}")
    assert not stale, (len(stale), stale[:20])


def test_every_default_is_the_new_catalog():
    assert WorkshopConfig().catalog == CATALOG
    for path in ("setup/00_master_setup.py", "labs/00_Start_Here.py", "labs/00b_Live_Data.py",
                 "labs/03c_Data_Quality_and_Monitoring.py", "jobs/reconcile_server.py"):
        with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
            assert f'dbutils.widgets.text("catalog", "{CATALOG}"' in fh.read(), path
    with open(os.path.join(ROOT, "databricks.yml"), encoding="utf-8") as fh:
        assert f"default: {CATALOG}" in fh.read()
