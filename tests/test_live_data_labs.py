"""实验 00、00b 和使用个人落地区的实验的结构检查 (structural checks for labs 00, 00b and the labs that use the personal landing zone)."""

import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..")


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def test_00b_takes_the_widgets_loads_the_helpers_and_finds_the_generator_in_either_layout():
    text = read("labs/00b_Live_Data.py")
    for widget in ('"catalog"', '"ticks"', '"new_server"', '"bad_batch_pct"'):
        assert f"dbutils.widgets.text({widget}" in text, widget
    assert "# MAGIC %run ./live_feed" in text
    assert 'os.path.join(os.getcwd(), "src")' in text and 'os.path.join(os.getcwd(), "..", "src")' in text
    assert 'WorkshopConfig(catalog=catalog, volume_root=roots["volume_root"])' in text
    for call in ("check_widgets(", "run_ticks(", "files_since(", "summarize(", "missing_files("):
        assert call in text, call


def test_00b_stops_with_a_bilingual_message_before_writing_anything():
    text = read("labs/00b_Live_Data.py")
    exits = [m.start() for m in re.finditer(r"dbutils\.notebook\.exit\(", text)]
    assert len(exits) == 3 and all(e < text.index("run_ticks(cfg") for e in exits)
    assert "run step 1b of 00_Start_Here first" in text and "re-run step 3 of 00_Start_Here" in text


def test_lab_00_copies_the_history_into_the_participants_own_volumes():
    text = read("labs/00_Start_Here.py")
    assert "# MAGIC %run ./live_feed" in text and "## 1b ·" in text
    assert text.index("## 1b ·") < text.index("## 2 ·")
    assert 'copy_missing(f"/Volumes/{catalog}/raw/landing", roots["landing"])' in text
    assert 'seed_producer(f"/Volumes/{catalog}/raw/producer", roots["producer"])' in text
    assert "except OSError" in text
    assert 'landing = f"/Volumes/{catalog}/{my_schema}/landing"' in text
    assert 'copy_tree(f"{src_repo}/src", f"{dst_labs}/src")' in text
    assert "`00b_Live_Data`" in text


PERSONAL = "/Volumes/hytech_ws_catalog/u_<your_name>/landing"


def test_lab_02_reads_your_own_landing_and_points_at_00b():
    text = read("labs/02_Ingestion.py")
    assert 'landing = f"/Volumes/{catalog}/{my_schema}/landing"' in text and "raw/landing" not in text
    assert "00b_Live_Data" in text


def test_lab_03_readme_sets_your_own_landing_root_and_says_when_to_full_refresh():
    text = read("labs/03_pipeline/README.md")
    assert f"| `landing_root` | `{PERSONAL}`" in text
    assert "00b_Live_Data" in text and "Full refresh all" in text
    assert "第 3 步" in text and "step 3" in text and "第 4 步" not in text


def test_lab_04_trigger_and_demos_use_your_own_landing_and_00b():
    text = read("labs/04_Lakeflow_Jobs.md")
    assert f"`{PERSONAL}/mt5/`" in text and "raw/landing" not in text and "讲师演示" not in text
    section = text[text.index("## 6 ·"):]
    for needle in ("00b_Live_Data", "new_server=mt5-hk-01", "bad_batch_pct=60", "03d", "fail_server"):
        assert needle in section, needle


def test_03b_freshness_comment_names_00b():
    assert "（你的 00b 运行）" in read("labs/03b_Explore_Pipeline.sql")


def test_reconcile_checks_the_participants_own_landing_before_the_shared_one():
    text = read("jobs/reconcile_server.py")
    personal = text.index('landing_root = f"/Volumes/{catalog}/{schema}/landing"')
    assert personal < text.index('landing_root = f"/Volumes/{catalog}/raw/landing"')
    assert "if not os.path.isdir(landing_root):" in text


def test_setup_grants_read_on_the_generator_state():
    assert "GRANT READ VOLUME ON VOLUME {catalog}.raw.producer TO `{group}`" in read("setup/01_create_catalog_schemas.py")


def test_lab_04_trigger_step_says_what_to_do_when_the_job_does_not_start():
    text = read("labs/04_Lakeflow_Jobs.md")
    section = text[text.index("## 4 ·"):text.index("## 5 ·")]
    assert section.count("Schedules & Triggers") == 2 and section.count("Run now") == 2   # 中文和英文各一次 (once in each language)


def test_the_dry_run_tries_the_file_arrival_trigger():
    line = next(l for l in read("docs/facilitator_guide.md").splitlines() if l.startswith("- [ ] At the dry run"))
    assert "file-arrival trigger" in line and "00b" in line


def test_lab_02_counts_a_progress_entry_without_numinputrows_as_zero():
    # 在交互式无服务器会话中实测：recentProgress 中有的条目 numInputRows 是 None，sum() 会报 int + NoneType
    # Live in an interactive serverless session: some recentProgress entries have numInputRows None, and sum() fails on int + NoneType
    assert 'sum((p["numInputRows"] or 0) for p in query.recentProgress)' in read("labs/02_Ingestion.py")


def test_03b_and_the_lab_map_point_to_the_lab_03_readme():
    # 实验 03 不是笔记本：学员要先找到 03_pipeline/README.md (lab 03 isn't a notebook: people must find 03_pipeline/README.md first)
    intro = read("labs/03b_Explore_Pipeline.sql").split("-- COMMAND ----------")[0]
    assert intro.count("`03_pipeline/README.md`") == 2 and "trade_lakehouse_<你的名字>" in intro and "trade_lakehouse_<your_name>" in intro
    row = next(line for line in read("labs/00_Start_Here.py").splitlines() if "`03b_Explore_Pipeline`" in line)
    assert "README.md" in row
