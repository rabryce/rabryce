import pytest

from addrlink import Matcher, MatchConfig, rules
from addrlink.engines import DuckDBEngine, SparkEngine
from addrlink.gnaf import authority_synonyms, find_files, load_gnaf, load_reference
from gnaf_fixture import write_fixture


@pytest.fixture(scope="module")
def gnaf_dir(tmp_path_factory):
    return write_fixture(str(tmp_path_factory.mktemp("gnaf")))


def _rows(engine, name):
    rows = engine.fetch(f"SELECT * FROM {name} ORDER BY r_id")
    for r in rows:  # normalise array types across engines
        r["premise_numbers"] = list(r["premise_numbers"] or [])
        r["premise_numbers_optional"] = list(r["premise_numbers_optional"] or [])
    return rows


def test_find_files(gnaf_dir):
    f = find_files(gnaf_dir)
    assert len(f["ADDRESS_DETAIL"]) == 2 and len(f["STREET_TYPE_AUT"]) == 1


def test_load_gnaf_duckdb(gnaf_dir):
    e = DuckDBEngine()
    rows = {r["r_id"]: r for r in _rows(e, load_gnaf(e, gnaf_dir))}
    unit = rows["GAACT1:P:GAACT1"]
    assert unit["address"] == "LEVEL 2 UNIT 201 345 HARBURN STREET BALLANDS ACT 2600"
    assert unit["premise_numbers"] == ["201", "345"] and unit["premise_numbers_optional"] == ["2"]
    assert unit["street_name"] == "HARBURN" and unit["latitude"] == -35.3
    rng = rows["GAACT3:P:GAACT3"]
    assert rng["address"] == "34-38 ELIZABETH ALLEY NORTH BALLANDS ACT 2600"
    assert rng["premise_numbers_optional"] == ["38"]
    assert rows["GAACT3:R:GAACT3"]["address"] == "34 ELIZABETH ALLEY NORTH BALLANDS ACT 2600"
    assert rows["GAACT4:P:GAACT4"]["premise_numbers"] == ["12A"]
    lot = rows["GANSW1:P:GANSW1"]
    assert lot["address"] == "LOT 7 OLD MILL ROAD KENTHURST NSW 2795" and lot["premise_numbers"] == ["7"]
    assert rows["GANSW1:SA:NSW_SLA1"]["address"] == "LOT 7 MILL ROAD KENTHURST NSW 2795"
    assert rows["GANSW2:P:GANSW2"]["address"] == "APARTMENT 5 10 GEORGE STREET SYDNEY NSW 2000"
    assert rows["GANSW2:LA:NSW_LA1"]["address"] == "APARTMENT 5 10 GEORGE STREET HAYMARKET NSW 2000"
    alias = rows["GANSW3:P:GANSW3"]
    assert alias["entity_id"] == "GANSW4" and alias["is_principal"] is False
    assert not any(k.startswith("GAACT5") for k in rows)  # retired address excluded


def test_authority_synonyms_respect_curated(gnaf_dir):
    e = DuckDBEngine()
    load_gnaf(e, gnaf_dir)
    extra = authority_synonyms(e)
    assert extra["ALLEY"] == "ALLY" and extra["STRAIGHT"] == "STRT"
    merged = rules.merged_synonyms(extra)
    assert merged["ALLEY"] == "ALLY" and merged["ALLY"] == "ALLY"
    assert merged["STRT"] == "ST"          # curated (STREET) wins
    assert "STRAIGHT" not in merged        # pair skipped because STRT is curated


def test_match_against_gnaf_fixture(gnaf_dir):
    e = DuckDBEngine()
    ref, override = load_reference(e, gnaf_dir)
    m = Matcher(e, MatchConfig.from_preset("enhanced").override(**override))
    m.prepare_reference(ref)
    q = [
        dict(id="1", address="201/345 Harburn St, Ballands ACT 2600"),
        dict(id="2", address="34 Elizabeth Ally Nth Ballands 2600"),
        dict(id="3", address="Lot 7 Mill Rd Kenthurst NSW"),
        dict(id="4", address="Apt 5, 10 George St Haymarket NSW 2000"),
        dict(id="5", address="1 Market Lane Sydney NSW 2000"),
    ]
    rows = {r["id"]: r for r in m.link(q).rows()}
    assert rows["1"]["entity_id"] == "GAACT1" and rows["1"]["decision"] == "AUTO_MATCH"
    assert rows["2"]["entity_id"] == "GAACT3"
    assert rows["3"]["entity_id"] == "GANSW1"
    assert rows["4"]["entity_id"] == "GANSW2" and rows["4"]["decision"] == "AUTO_MATCH"
    assert rows["5"]["entity_id"] == "GANSW4"  # alias address resolves to its principal


@pytest.mark.spark
def test_load_gnaf_spark_matches_duckdb(gnaf_dir, spark):
    d, s = DuckDBEngine(), SparkEngine(spark)
    a, b = _rows(d, load_gnaf(d, gnaf_dir)), _rows(s, load_gnaf(s, gnaf_dir, prefix="sg_"))
    cols = ["r_id", "entity_id", "address", "premise_numbers", "premise_numbers_optional", "postcode", "state",
            "street_name", "variant", "is_principal"]
    assert [{c: r[c] for c in cols} for r in a] == [{c: r[c] for c in cols} for r in b]
