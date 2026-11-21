"""End-to-end behaviour on hand-written Australian edge cases (DuckDB engine)."""

import pytest

from addrlink import Matcher, MatchConfig


def ref(r_id, address, prem, opt=(), pc=None, state=None, street=None, entity=None):
    return dict(r_id=r_id, entity_id=entity or r_id, address=address, premise_numbers=list(prem),
                premise_numbers_optional=list(opt), postcode=pc, state=state, street_name=street)


REF = [
    ref("U2_51", "UNIT 2 51 BENOR DRIVE BELMNOT VIC 3216", ["2", "51"], pc="3216", state="VIC", street="BENOR"),
    ref("B51", "51 BENOR DRIVE BELMNOT VIC 3216", ["51"], pc="3216", state="VIC", street="BENOR"),
    ref("U731", "UNIT 731 311 GUN PLACE HILNU ACT 2605", ["731", "311"], pc="2605", state="ACT", street="GUN"),
    ref("U713", "UNIT 713 311 GUN PLACE HILNU ACT 2605", ["713", "311"], pc="2605", state="ACT", street="GUN"),
    ref("R34", "34-38 ELIZABETH STREET SYDNEY NSW 2000", ["34"], ["38"], "2000", "NSW", "ELIZABETH"),
    ref("R34s", "34 ELIZABETH STREET SYDNEY NSW 2000", ["34"], ["38"], "2000", "NSW", "ELIZABETH", entity="R34"),
    ref("U68", "UNIT 68 68 MULINA COURT LANFIELD VIC 3679", ["68", "68"], pc="3679", state="VIC", street="MULINA"),
    ref("B68", "68 MULINA COURT LANFIELD VIC 3679", ["68"], pc="3679", state="VIC", street="MULINA"),
    ref("N370", "370 NORONG CRESCENT YARBANK QLD 4489", ["370"], pc="4489", state="QLD", street="NORONG"),
    ref("L2U201", "LEVEL 2 UNIT 201 345 HARBURN STREET BALLANDS VIC 3289", ["201", "345"], ["2"], "3289", "VIC",
        "HARBURN"),
    ref("L2U202", "LEVEL 2 UNIT 202 345 HARBURN STREET BALLANDS VIC 3289", ["202", "345"], ["2"], "3289", "VIC",
        "HARBURN"),
]

CASES = [
    # id, address, expected decision, expected entity
    ("house", "51 Benor Dr, Belmnot VIC 3216", "AUTO_MATCH", "B51"),
    ("unit_slash", "2/51 Benor Drive Belmnot 3216", "AUTO_MATCH", "U2_51"),
    ("unit_anagram", "U713/311 Gun Pl Hilnu ACT 2605", "AUTO_MATCH", "U713"),
    ("state_long", "34 Elizabeth St, Sydney, New South Wales 2000, Australia", "AUTO_MATCH", "R34"),
    ("dup_numbers", "68/68 Mulina Ct, Lanfield VIC 3679", "AUTO_MATCH", "U68"),
    ("level_omitted", "Unit 201, 345 Harburn St, Ballands VIC 3289", "AUTO_MATCH", "L2U201"),
    ("level_included", "L2 201/345 Harburn St Ballands 3289", "AUTO_MATCH", "L2U201"),
    ("wrong_street", "370 George Pl Yarbank QLD 4489", None, None),
    ("unit_missing", "345 Harburn Street Ballands VIC 3289", None, None),
    ("po_box", "PO Box 12, Sydney NSW 2000", "NON_PHYSICAL", None),
    ("empty", "", "EMPTY", None),
    ("nothing", "99 Nowhere Rd Zzz QLD 4000", "NO_MATCH", None),
]


@pytest.fixture(scope="module")
def enhanced_rows():
    m = Matcher("duckdb", MatchConfig.from_preset("enhanced"))
    m.prepare_reference(REF)
    return {r["id"]: r for r in m.link([dict(id=c[0], address=c[1]) for c in CASES]).rows()}


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_enhanced_edge_cases(enhanced_rows, case):
    cid, _, decision, entity = case
    row = enhanced_rows[cid]
    if decision is None:  # must not be auto-matched
        assert row["decision"] != "AUTO_MATCH", row
    else:
        assert row["decision"] == decision, row
        assert row["entity_id"] == entity, row


def test_wrong_street_is_flagged(enhanced_rows):
    assert "STREET_MISMATCH" in enhanced_rows["wrong_street"]["reason_codes"]


def test_unit_missing_is_partial_or_review(enhanced_rows):
    assert enhanced_rows["unit_missing"]["decision"] in ("PARTIAL_MATCH", "REVIEW")


def test_paper_preset_reproduces_wrong_unit_tie():
    tie_ref = [ref("A_731", "UNIT 731 311 GUN PLACE HILNU ACT 2605", ["731", "311"]),
               ref("B_713", "UNIT 713 311 GUN PLACE HILNU ACT 2605", ["713", "311"])]
    q = [dict(id="a", address="U713/311 Gun Pl Hilnu ACT 2605")]
    m = Matcher("duckdb", MatchConfig.from_preset("paper"))
    m.prepare_reference(tie_ref)
    res = m.link(q)
    c = res.candidates()
    assert c[0]["score"] == c[1]["score"]  # Dice is order-blind: 713 and 731 tie
    assert res.rows()[0]["entity_id"] == "A_731"  # paper: id order decides -> wrong unit
    e = Matcher("duckdb", MatchConfig.from_preset("enhanced"))
    e.prepare_reference(tie_ref)
    assert e.link(q).rows()[0]["entity_id"] == "B_713"


def test_datasets_mode_paper_numeric_subset_rule():
    a = [dict(id="a1", address="12 Smith St Kenmore QLD 4069"), dict(id="a2", address="14 Smith St Kenmore QLD 4069")]
    b = [dict(id="b1", address="12 SMITH STREET KENMORE QLD 4069"),
         dict(id="b2", address="UNIT 3 12 SMITH STREET KENMORE QLD 4069")]
    m = Matcher("duckdb", MatchConfig.from_preset("paper"))
    rows = {r["id"]: r for r in m.link_datasets(a, b).rows()}
    assert rows["a1"]["entity_id"] == "b1" and rows["a1"]["decision"] == "AUTO_MATCH"
    assert rows["a2"]["decision"] == "NO_MATCH"  # 14 is inconsistent with every candidate


def test_run_log_and_sql(tmp_path):
    m = Matcher("duckdb", MatchConfig.from_preset("enhanced"))
    m.prepare_reference(REF)
    res = m.link([dict(id="x", address="51 Benor Dr Belmnot")])
    log = res.run_log.as_dict()
    assert log["config_hash"] and log["counts"]["n_ref"] == len(REF)
    assert "match_result" in m.explain_sql()
    out = tmp_path / "r.parquet"
    res.to_parquet(str(out))
    assert out.exists()
