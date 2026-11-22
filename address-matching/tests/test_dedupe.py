import pytest

from addrlink import Matcher, MatchConfig
from addrlink.evaluate import dedupe_pairwise
from addrlink.synth import SynthConfig, generate


@pytest.fixture(scope="module")
def dup_data():
    _, qs = generate(SynthConfig(target_addresses=6000, n_localities=80, n_queries=500, max_copies=3,
                                 negative_rate=0, po_box_rate=0, seed=21))
    truth = {q["id"]: q["true_entity_id"] for q in qs}
    return [dict(id=q["id"], address=q["address"]) for q in qs], truth


def test_dedupe_pairwise_quality(dup_data):
    rows, truth = dup_data
    res = Matcher("duckdb", MatchConfig.from_preset("enhanced")).dedupe(rows)
    m = dedupe_pairwise(res.clusters(), truth)
    assert m["true_pairs"] > 100
    assert m["pairwise_precision"] >= 0.97, m
    assert m["pairwise_recall"] >= 0.75, m
    s = res.summary()
    assert s["n_records"] == len(rows) and s["n_clusters"] < len(rows)


def test_exact_duplicates_and_singletons():
    rows = [dict(id="a", address="12 Smith St Kenmore QLD 4069"), dict(id="b", address="12 SMITH STREET, Kenmore QLD 4069"),
            dict(id="c", address="12 Smith Street Kenmore Queensland 4069"), dict(id="d", address="14 Smith St Kenmore QLD 4069"),
            dict(id="e", address="PO Box 5 Kenmore QLD 4069")]
    cl = {r["id"]: r for r in Matcher("duckdb", MatchConfig.from_preset("enhanced")).dedupe(rows).clusters()}
    assert cl["a"]["cluster_id"] == cl["b"]["cluster_id"] == cl["c"]["cluster_id"]  # same normalised string
    assert cl["d"]["cluster_id"] != cl["a"]["cluster_id"]                          # different number
    assert cl["e"]["cluster_id"] == "X:e" and cl["e"]["cluster_size"] == 1


@pytest.mark.spark
def test_dedupe_spark_equals_duckdb(dup_data, spark):
    rows, _ = dup_data
    rows = rows[:300]
    a = Matcher("duckdb", MatchConfig.from_preset("enhanced")).dedupe(rows).clusters()
    b = Matcher("spark", MatchConfig.from_preset("enhanced"), spark=spark, prefix="dd_").dedupe(rows).clusters()
    assert [(r["id"], r["cluster_id"]) for r in a] == [(r["id"], r["cluster_id"]) for r in b]
