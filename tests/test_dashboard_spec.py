"""实验 09 仪表盘 JSON 的离线检查（规则来自 databricks-aibi-dashboards）(offline checks for the lab 09 dashboard JSON)."""

import importlib.util
import json
import os

HERE = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location("dashboard_spec", os.path.join(HERE, "..", "labs", "dashboard_spec.py"))
ds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ds)

D = ds.trading_overview("hytech_ws_catalog", "u_x")
WIDGETS = [item["widget"] for page in D["pages"] for item in page["layout"]]
VERSIONS = {"counter": 2, "table": 2, "line": 3, "bar": 3, "filter-date-range-picker": 2}


def test_serialisable_and_datasets_read_the_participants_own_gold_tables():
    json.dumps(D)
    queries = {d["name"]: "".join(d["queryLines"]) for d in D["datasets"]}
    assert set(queries) == {"daily_volume", "funding", "top_symbols"}
    assert "`hytech_ws_catalog`.`u_x`.gold_daily_symbol_volume" in queries["daily_volume"]
    assert "`hytech_ws_catalog`.`u_x`.gold_funding_daily" in queries["funding"]
    # 34 个品种画成柱状图太挤：只取名义金额最大的 10 个，但保留 deal_date，日期筛选器仍然有效
    # 34 symbols are too many bars: keep the 10 biggest by notional, but keep deal_date so the date filter still applies
    assert "LIMIT 10" in queries["top_symbols"] and "deal_date" in queries["top_symbols"]
    for d in D["datasets"]:
        assert all(line.endswith((" ", "\n")) for line in d["queryLines"][:-1]), d["name"]


def test_pages_use_the_grid_layout_and_rows_fill_twelve_columns_without_overlap():
    for page in D["pages"]:
        assert page["pageType"] == "PAGE_TYPE_CANVAS" and page["layoutVersion"] == "GRID_V1"
        cells = set()
        for item in page["layout"]:
            p = item["position"]
            assert p["x"] + p["width"] <= 12
            for x in range(p["x"], p["x"] + p["width"]):
                for y in range(p["y"], p["y"] + p["height"]):
                    assert (x, y) not in cells, item["widget"]["name"]
                    cells.add((x, y))
        for y in {y for _, y in cells}:
            assert sum(1 for x, yy in cells if yy == y) == 12, f"row {y} has a gap"


def test_every_encoding_matches_a_query_field_and_every_widget_has_the_right_version():
    datasets = {d["name"] for d in D["datasets"]}
    for w in WIDGETS:
        if "multilineTextboxSpec" in w:
            continue
        spec = w["spec"]
        assert spec["version"] == VERSIONS[spec["widgetType"]], w["name"]
        assert spec["frame"]["showTitle"] is True
        fields = {}
        for q in w["queries"]:
            assert q["query"]["datasetName"] in datasets
            fields[q["name"]] = {f["name"] for f in q["query"]["fields"]}
        enc = spec["encodings"]
        if spec["widgetType"].startswith("filter"):
            for f in enc["fields"]:
                assert f["fieldName"] in fields[f["queryName"]]
            assert {q["query"]["datasetName"] for q in w["queries"]} == datasets   # the date filter drives both datasets
        else:
            names = fields["main_query"]
            for key in ("x", "y", "color", "value", "period"):
                if key in enc:
                    assert enc[key]["fieldName"] in names, (w["name"], key)


def test_widget_names_are_simple():
    for w in WIDGETS:
        assert w["name"].replace("_", "").replace("-", "").isalnum() and len(w["name"]) <= 60


def test_the_catch_up_never_overwrites_the_dashboard_built_by_hand():
    # 实验 09 让学员把自己的仪表盘命名为 "MT5 交易概览 · <你的名字>"；09b 按标题更新，所以必须用另一个标题
    # Lab 09 has participants title their own dashboard "MT5 交易概览 · <your_name>"; 09b updates by title, so it needs another title
    assert ds.DASHBOARD_TITLE.format(name="zhang_wei") != "MT5 交易概览 · zhang_wei"
    assert "参考" in ds.DASHBOARD_TITLE and "reference" in ds.DASHBOARD_TITLE


def test_lab_09_builds_the_same_datasets_as_the_catch_up():
    # 手动搭建（实验 09）和补课（09b）必须一致 (the hand-built dashboard, lab 09, and the catch-up, 09b, must match)
    with open(os.path.join(HERE, "..", "labs", "09_AIBI_Dashboard_Lakehouse_RT.md"), encoding="utf-8") as fh:
        lab = fh.read()
    for d in D["datasets"]:
        assert d["queryLines"][0].strip() in lab, d["name"]
        assert f"`{d['name']}`" in lab
    assert "LIMIT 10" in lab and "参考 (reference)" in lab


def test_lab_09_hints_the_name_the_fallback_and_where_query_history_is():
    with open(os.path.join(HERE, "..", "labs", "09_AIBI_Dashboard_Lakehouse_RT.md"), encoding="utf-8") as fh:
        lab = fh.read()
    assert "u_zhang_san" in lab   # <你的名字> = schema 名中 u_ 后面的部分 (the part after u_ in your schema name)
    assert "source_schema" in lab and "solutions" in lab
    assert "Query History" in lab and "→ Query history" not in lab
