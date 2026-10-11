"""文档一致性检查：新实验在每个入口都能找到，文档中的名称与代码一致。
Docs consistency: the new labs can be found from every entry point, and the names in the docs match the code."""

import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..")
NEW_LABS = ("03c_Data_Quality_and_Monitoring", "03d_Run_If_Dependencies", "04c_Failure_Recovery_and_Backfill",
            "09_AIBI_Dashboard_Lakehouse_RT", "10_Genie_Agent")
ENTRY_POINTS = ("labs/00_Start_Here.py", "README.md", "docs/participant_guide_zh.md")


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def test_every_new_lab_is_listed_in_teaching_order_at_every_entry_point():
    for path in ENTRY_POINTS:
        text = read(path)
        positions = [text.find(lab) for lab in NEW_LABS]
        assert all(p >= 0 for p in positions), (path, dict(zip(NEW_LABS, positions)))
        assert positions == sorted(positions), path


def test_every_lab_file_is_in_the_readme():
    readme = read("README.md")
    for name in sorted(os.listdir(os.path.join(ROOT, "labs"))):
        stem = re.sub(r"\.(py|sql|md)$", "", name)
        if re.match(r"\d\d[a-z]?_", stem):
            assert stem in readme, name


def test_ai_functions_is_marked_optional_everywhere():
    for path in ENTRY_POINTS:
        row = next(line for line in read(path).splitlines() if "07_AI_Functions" in line)
        assert "可选" in row, (path, row)


def test_the_setup_guide_has_the_lakehouse_rt_and_genie_pre_steps():
    text = read("docs/setup_guide_triones.md")
    for needle in ("Lakehouse RT", "Previews", "hytech_workshop_rt", "hytech_workshop_sql", "08_workshop_warehouses",
                   "cross-Geo", "Databricks SQL", "warehouse_id"):
        assert needle in text, needle
    # §2b：先更新代码、先查限制，再找客户团队、开 Previews、重新运行设置 (refresh the code and check the limits first,
    # then the account team, Previews, and the setup re-run)
    section = text[text.index("## 2b ·"):text.index("## 3 ·")]
    steps = ("Pull", "egress", "客户团队", "Previews", "00_master_setup")
    positions = [section.find(word) for word in steps]
    assert all(p >= 0 for p in positions) and positions == sorted(positions), dict(zip(steps, positions))


def test_troubleshooting_covers_the_new_labs():
    text = read("docs/troubleshooting.md")
    for needle in ("start_over", "03c", "04c", "Run backfill", "hytech_workshop_rt", "10b", "source_schema", "hytech_de_lab/jobs"):
        assert needle in text, needle


def test_the_run_of_show_follows_the_customers_agenda():
    for path in ("docs/workshop_plan.md", "docs/facilitator_guide.md"):
        text = read(path)
        for stale in ("| 11:15 |", "| 12:15 |", "| 12:00 | **AI"):
            assert stale not in text, (path, stale)
        m4 = next(line for line in text.splitlines() if line.startswith("| 11:00 | **M4"))
        assert "(90)" in m4 or "（90）" in m4, path
        assert any(line.startswith("| 11:30 | **M7") for line in text.splitlines()), path
    facilitator = read("docs/facilitator_guide.md")
    m4 = next(line for line in facilitator.splitlines() if line.startswith("| 11:00 | **M4** 声明式管道"))
    assert all(f"{part}" in m4 for part in ("15", "25", "10", "20", "5"))   # 各段的分钟 (the minutes of each segment)
    assert "缓冲（5）" in facilitator and "buffer (5)" in facilitator
    for path in ("docs/workshop_plan.md", "docs/facilitator_guide.md"):
        assert "5 分钟回顾" in read(path), path   # 回顾留在课内 (the recap stays in class, spec §1)


def test_the_docs_name_the_workshop_sql_warehouse_and_one_term_for_cross_geo():
    for path in ("README.md", "docs/setup_guide_triones.md", "docs/facilitator_guide.md", "docs/troubleshooting.md",
                 "docs/workshop_plan.md"):
        text = read(path)
        assert "hytech_workshop_sql" in text, path
        assert "跨区域" not in text, path
    assert "跨区域" not in read("labs/10_Genie_Agent.md")


def test_the_jobs_copy_reset_runs_lab_00_from_the_shared_folder():
    for path in ("docs/facilitator_guide.md", "docs/troubleshooting.md", "labs/03d_Run_If_Dependencies.py"):
        assert "Shared/hytech-de-workshop/labs/00_Start_Here" in read(path), path


def test_no_short_path_or_instructor_action_that_the_labs_contradict():
    facilitator, plan = read("docs/facilitator_guide.md"), read("docs/workshop_plan.md")
    assert "只回填一天" not in facilitator and "backfill one day" not in facilitator
    assert "讲师上线它" not in plan and "instructor onboards it" not in plan
    assert "bad_batch_pct" in facilitator and "04c" in facilitator[facilitator.index("bad_batch_pct"):]


def test_troubleshooting_explains_the_04c_waits():
    text = read("docs/troubleshooting.md")
    row = next(line for line in text.splitlines() if "Run now / Repair run" in line)
    assert "10 分钟" in row and "Repair run" in row and "不是 Run now" in row
    assert "labs/03_pipeline/README.md" in text


STALE_DRIP = ("启动滴灌", "starts the drip", "start the drip", "讲师的”滴灌”", "drip producer” continuously",
              "drive the live data", "由 Triones 运行", "Triones runs the drip", "讲师的 drip producer",
              "instructor's drip producer", "讲师的滴灌程序", "请讲师启动滴灌")


def test_no_doc_or_lab_waits_for_a_centrally_run_drip():
    for path in ("README.md", "docs/facilitator_guide.md", "docs/participant_guide_zh.md", "docs/setup_guide_triones.md",
                 "docs/troubleshooting.md", "docs/workshop_plan.md", "labs/02_Ingestion.py", "labs/03_pipeline/README.md",
                 "labs/03b_Explore_Pipeline.sql", "labs/04_Lakeflow_Jobs.md"):
        text = read(path)
        for phrase in STALE_DRIP:
            assert phrase not in text, (path, phrase)


def test_00b_is_findable_from_every_entry_point_and_guide():
    for path in ENTRY_POINTS + ("docs/facilitator_guide.md", "docs/workshop_plan.md", "docs/setup_guide_triones.md",
                                "docs/troubleshooting.md"):
        assert "00b_Live_Data" in read(path), path


def test_troubleshooting_covers_your_own_live_data():
    text = read("docs/troubleshooting.md")
    for needle in ("00b_Live_Data", "raw.producer", "Full refresh all", "bad_batch_pct"):
        assert needle in text, needle


def test_setup_and_the_drip_job_say_it_is_optional_and_not_for_class():
    # Triones 每次运行设置都会读到这些文字 (Triones reads these words on every setup run)
    for path in ("setup/00_master_setup.py", "setup/03_drip_producer.py", "resources/producer.job.yml"):
        text = read(path)
        for stale in ("during class", "课堂上由讲师启动", "课堂上持续写入", "在 M3–M5 期间运行", "Use in class",
                      "Instructor-only", "讲师专用", "instructor only"):
            assert stale not in text, (path, stale)
        assert "00b_Live_Data" in text, path


def test_troubleshooting_covers_a_lab_copy_made_before_this_update():
    text = read("docs/troubleshooting.md")
    row = next(line for line in text.splitlines() if "reconcile" in line and "raw/landing" in line)
    assert "hytech_de_lab/" in row and "00_Start_Here" in row and "Full refresh all" in row
    reset = text[text.index("## 重置学员 (Reset a participant)"):text.index("## 重新生成数据")]
    assert "hytech_de_lab/" in reset


def test_the_setup_guide_starts_with_trioness_steps_in_order():
    text = read("docs/setup_guide_triones.md")
    order = text[text.index("## 按顺序完成 (In order)"):text.index("## 0 ·")]
    positions = [order.find(f"| {n} · ") for n in range(1, 11)]
    assert all(p >= 0 for p in positions) and positions == sorted(positions), positions
    assert "labs/00_Start_Here" in order and "CREATE CATALOG" in order
    get_code = text[text.index("## 1 ·"):text.index("## 2 ·")]
    assert "https://github.com/germaineong/hytech-de-workshop" in get_code and "branch `main`" in get_code
    assert "向 Zhi Han 请求访问权限" not in text and "ask Zhi Han for" not in text


def test_no_doc_or_lab_quotes_the_pocs_cost_figures():
    paths = ["README.md"] + [f"docs/{n}" for n in os.listdir(os.path.join(ROOT, "docs")) if n.endswith(".md")]
    paths += [f"labs/{n}" for n in os.listdir(os.path.join(ROOT, "labs")) if n.endswith((".py", ".sql", ".md"))]
    for path in paths:
        text = read(path)
        for figure in ("$480", "$40/", "480 美元", "40 美元"):
            assert figure not in text, (path, figure)


def test_the_setup_guide_leaves_the_dry_run_to_the_instructors():
    text = read("docs/setup_guide_triones.md")
    assert "预演" not in text and "dry run" not in text.lower()
    assert "第 1–3 步要提前几天安排" in text and "steps 1–3 below need a few days' lead time" in text


def test_everyone_clones_one_repo_with_no_merge_to_wait_for():
    # Triones 和学员直接克隆同一个代码库 (Triones and the participants clone the same repo directly)
    import subprocess
    files = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    for path in files:
        try:
            text = read(path)
        except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
            continue
        for stale in ("zhihan" + "tan/hytech-de-workshop", "PR " + "#1"):
            assert stale not in text, (path, stale)
