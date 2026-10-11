"""设置步骤的结构检查 (structural checks for the setup steps)."""

import os

ROOT = os.path.join(os.path.dirname(__file__), "..")


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def test_the_warehouse_step_is_in_both_setup_job_definitions():
    assert "../setup/08_workshop_warehouses.py" in read("resources/setup.job.yml")
    assert 'task("workshop_warehouses", "setup/08_workshop_warehouses")' in read("setup/00_master_setup.py")


def test_the_warehouse_step_grants_can_use_and_creates_a_small_realtime_warehouse():
    text = read("setup/08_workshop_warehouses.py")
    assert '"permission_level": "CAN_USE"' in text
    assert '"warehouse_type": "REALTIME"' in text and '"cluster_size": "Small"' in text and '"auto_stop_mins": 10' in text


def test_teardown_removes_the_realtime_warehouse_setup_created():
    assert "hytech_workshop_rt" in read("setup/99_teardown.py")


def test_cost_dashboard_never_picks_a_realtime_warehouse():
    # SDK 把 warehouse_type = REALTIME 解析成 None（databricks-sdk 0.151），所以要读原始 JSON
    # The SDK parses warehouse_type = REALTIME as None (databricks-sdk 0.151), so the raw JSON must be read
    text = read("setup/07_cost_dashboard.py")
    assert '"/api/2.0/sql/warehouses"' in text and 'x.get("warehouse_type") != "REALTIME"' in text


def test_the_realtime_warehouse_is_created_with_a_cluster_range():
    # 在测试工作区实测：不写 max_num_clusters 时 API 拒绝（0 is not a valid value for max_num_clusters）
    # Live on a test workspace: without max_num_clusters the API rejects the create (0 is not a valid value for max_num_clusters)
    text = read("setup/08_workshop_warehouses.py")
    assert '"min_num_clusters": 1' in text and '"max_num_clusters": 1' in text
    # 创建失败不一定是因为预览没开：先看错误 (a failed create is not always the preview being off: read the error first)
    assert "见上面的错误" in text and "see the error above" in text


def test_the_warehouse_step_only_grants_a_warehouse_the_workshop_owns():
    # 不能把学员加到任意一个正在运行的仓库上：它可能是生产仓库 (never grant participants whichever warehouse is running: it may be production)
    text = read("setup/08_workshop_warehouses.py")
    assert '"hytech_workshop_sql"' in text and '{"key": "managed_by", "value": "master_setup"}' in text
    assert 'x.get("state") != "RUNNING"' not in text


def test_teardown_removes_both_workshop_warehouses_and_tolerates_tags_without_values():
    text = read("setup/99_teardown.py")
    assert "hytech_workshop_sql" in text and "hytech_workshop_rt" in text
    assert 't["value"]' not in text


def test_the_catch_ups_prefer_the_workshop_sql_warehouse():
    for path in ("labs/09b_Dashboard_Catch_Up.py", "labs/10b_Genie_Catch_Up.py"):
        assert '"hytech_workshop_sql"' in read(path), path


NEW_FILES = [
    "setup/08_workshop_warehouses.py", "jobs/task_logger.py", "labs/staging_feed.py", "labs/run_if_spec.py",
    "labs/dashboard_spec.py", "labs/genie_spec.py", "labs/03c_Data_Quality_and_Monitoring.py",
    "labs/03d_Run_If_Dependencies.py", "labs/04c_Failure_Recovery_and_Backfill.py", "labs/09b_Dashboard_Catch_Up.py",
    "labs/10b_Genie_Catch_Up.py", "labs/09_AIBI_Dashboard_Lakehouse_RT.md", "labs/10_Genie_Agent.md",
    "labs/03_pipeline/staging/06_staging_mt5_hk01.sql", "tests/test_setup_steps.py", "tests/test_dashboard_spec.py",
    "tests/test_genie_spec.py", "tests/test_lab_04c.py", "tests/test_lab_notebooks.py", "tests/test_staging_feed.py",
    "tests/test_staging_feed_ui_mode.py", "tests/test_run_if_spec.py", "tests/test_docs.py",
]


def test_new_files_name_no_internal_workspace():
    # 公开代码库：新文件中不出现内部工作区的名称 (public repo: no internal workspace names in the new files)
    marker = "fe" + "vm"   # 拆开写，这一行本身不触发检查 (split so this line doesn't trip the check)
    for path in NEW_FILES:
        assert marker not in read(path).lower().replace("-", ""), path


def test_setup_creates_the_catalog_only_when_it_is_missing():
    # 在测试工作区实测：catalog 已经存在时，CREATE CATALOG IF NOT EXISTS 仍然要求 metastore 上的 CREATE CATALOG 权限
    # Live on a test workspace: CREATE CATALOG IF NOT EXISTS still needs CREATE CATALOG on the metastore when the catalog exists
    text = read("setup/01_create_catalog_schemas.py")
    check = text.index("SHOW CATALOGS LIKE")
    assert check < text.index("CREATE CATALOG IF NOT EXISTS")
    assert "if create_catalog and not exists:" in text and "already exists" in text
