"""Genie serialized_space 的离线检查（规则来自 databricks-genie-agents）(offline checks for the Genie serialized_space)."""

import importlib.util
import json
import os
import re

HERE = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location("genie_spec", os.path.join(HERE, "..", "labs", "genie_spec.py"))
gs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gs)

S = gs.genie_space("hytech_ws_catalog", "u_x")
HEX32 = re.compile(r"^[0-9a-f]{32}$")


def ids(items):
    return [i["id"] for i in items]


def test_tables_are_the_participants_gold_tables_sorted_with_sorted_column_configs():
    tables = S["data_sources"]["tables"]
    names = [t["identifier"] for t in tables]
    assert names == sorted(names) and len(names) == len(gs.GENIE_TABLES) <= 30
    assert all(n.startswith("hytech_ws_catalog.u_x.") for n in names)
    for t in tables:
        cols = [c["column_name"] for c in t.get("column_configs", [])]
        assert cols == sorted(cols)
        for c in t.get("column_configs", []):
            assert isinstance(c["synonyms"], list) and c["synonyms"]


def test_every_id_is_32_hex_unique_and_every_id_list_is_sorted():
    ins = S["instructions"]
    groups = {
        "sample_questions": S["config"]["sample_questions"],
        "text_instructions": ins["text_instructions"],
        "example_question_sqls": ins["example_question_sqls"],
        "join_specs": ins["join_specs"],
        "measures": ins["sql_snippets"]["measures"],
        "filters": ins["sql_snippets"]["filters"],
        "benchmarks": S["benchmarks"]["questions"],
    }
    every = []
    for name, items in groups.items():
        assert ids(items) == sorted(ids(items)), name
        assert all(HEX32.match(i) for i in ids(items)), name
        every += ids(items)
    assert len(every) == len(set(every))


def test_text_fields_are_arrays_and_there_is_one_text_instruction_block():
    ins = S["instructions"]
    assert len(ins["text_instructions"]) == 1 and isinstance(ins["text_instructions"][0]["content"], list)
    for q in S["config"]["sample_questions"] + S["benchmarks"]["questions"]:
        assert isinstance(q["question"], list)
    for e in ins["example_question_sqls"]:
        assert isinstance(e["question"], list) and isinstance(e["sql"], list)
    for b in S["benchmarks"]["questions"]:
        assert b["answer"] == [{"format": "SQL", "content": b["answer"][0]["content"]}] and b["answer"][0]["content"]


SQL_WORDS = {"sum", "count", "avg", "min", "max", "and", "or", "not", "case", "when", "then", "else", "end", "is",
             "null", "in", "like", "distinct", "as", "coalesce", "round", "date_sub", "current_date"}


def unqualified_columns(sql):
    """去掉字符串和「表.列」之后剩下的小写标识符 (lowercase identifiers left after removing strings and table.column)."""
    text = re.sub(r"'[^']*'", " ", sql)
    text = re.sub(r"`?\b\w+`?\.`?\w+`?", " ", text)
    return [w for w in re.findall(r"\b[a-z_][a-z0-9_]*\b", text) if w not in SQL_WORDS]


def test_the_column_checker_catches_an_unqualified_column():
    assert unqualified_columns("SUM(gold_x.a) / SUM(b)") == ["b"]
    assert unqualified_columns("gold_x.asset_class = 'CRYPTO' AND region = 'SG'") == ["region"]
    assert unqualified_columns("SUM(gold_x.a) / SUM(`gold_x`.`b`)") == []


def test_joins_and_snippets_qualify_every_column():
    for j in S["instructions"]["join_specs"]:
        assert len(j["sql"]) == 2 and j["sql"][1].startswith("--rt=FROM_RELATIONSHIP_TYPE_")
        assert j["left"]["alias"] in j["sql"][0] and j["right"]["alias"] in j["sql"][0]
        assert not unqualified_columns(j["sql"][0]), j["sql"][0]
    for kind in ("measures", "filters"):
        for s in S["instructions"]["sql_snippets"][kind]:
            assert not unqualified_columns(s["sql"][0]), s


def test_the_instructions_ask_for_simplified_chinese_and_full_numbers():
    text = " ".join(S["instructions"]["text_instructions"][0]["content"])
    assert "简体中文" in text and "万" in text


def test_ids_are_stable_for_the_same_schema_and_differ_between_schemas():
    assert gs.genie_space("hytech_ws_catalog", "u_x") == S
    other = gs.genie_space("hytech_ws_catalog", "u_y")
    assert ids(other["config"]["sample_questions"]) != ids(S["config"]["sample_questions"])
    json.dumps(S, ensure_ascii=False)


def test_the_catch_up_never_overwrites_the_agent_configured_by_hand():
    # 实验 10 让学员把自己的 Agent 命名为 "MT5 交易分析助手 · <你的名字>"；10b 按标题更新，所以必须用另一个标题
    # Lab 10 has participants title their own agent "MT5 交易分析助手 · <your_name>"; 10b updates by title, so it needs another title
    assert gs.GENIE_TITLE.format(name="zhang_wei") != "MT5 交易分析助手 · zhang_wei"
    assert "参考" in gs.GENIE_TITLE and "reference" in gs.GENIE_TITLE


def test_lab_10_configures_the_same_agent_as_the_catch_up():
    # 手动配置（实验 10）和补课（10b）必须一致 (the hand-configured agent, lab 10, and the catch-up, 10b, must match)
    with open(os.path.join(HERE, "..", "labs", "10_Genie_Agent.md"), encoding="utf-8") as fh:
        lab = fh.read()
    for line in gs.INSTRUCTIONS:
        assert line.split("。(")[0] + "。" in lab, line   # 实验里只粘贴中文部分 (the lab pastes the Chinese part only)
    for q in gs.SAMPLE_QUESTIONS:
        assert q in lab
    for table in gs.GENIE_TABLES:
        assert f"`{table}`" in lab
    assert "ib_login" in lab and "参考 (reference)" in lab


def test_lab_10_says_where_each_setting_lives_and_uses_the_right_words():
    with open(os.path.join(HERE, "..", "labs", "10_Genie_Agent.md"), encoding="utf-8") as fh:
        lab = fh.read()
    for place in ("Configure → Instructions", "Configure → Data", "Configure → Examples", "Settings → Common questions"):
        assert place in lab, place
    # 只有带参数的示例 SQL 和 UC 函数才算 trusted (only parameterized example SQL and UC functions count as trusted)
    assert "trusted" not in lab.lower() and "可信" not in lab
    assert "u_zhang_san" in lab   # <你的名字> 的提示 (the hint for <你的名字>)
