"""Oracle vs DuckDB vs Spark: identical candidates, scores, ranks and decisions (SPEC.md §13, FR10)."""

import math

import pytest

from addrlink import Matcher, MatchConfig
from addrlink.oracle import Oracle

PRESETS = ["paper", "enhanced"]


def _same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return math.isclose(a, b, rel_tol=0, abs_tol=1e-9)
    return a == b


def _diff(rows_a, rows_b, label):
    assert len(rows_a) == len(rows_b), label
    diffs = []
    for a, b in zip(rows_a, rows_b):
        for k in a:
            bv = b[k]
            if isinstance(bv, tuple):
                bv = list(bv)
            if not _same(a[k], bv):
                diffs.append((label, a.get("id", a.get("norm_key")), k, a[k], b[k]))
    assert not diffs, diffs[:10]


def _oracle(cfg, ref_in, q_in):
    o = Oracle(cfg)
    o.prepare_reference(ref_in)
    return o.link(q_in)


@pytest.mark.parametrize("preset", PRESETS)
def test_oracle_vs_duckdb(small_synth, preset):
    _, _, ref_in, q_in = small_synth
    cfg = MatchConfig.from_preset(preset)
    orows, ocand = _oracle(cfg, ref_in, q_in)
    m = Matcher("duckdb", cfg)
    m.prepare_reference(ref_in)
    res = m.link(q_in)
    _diff(orows, res.rows(), f"{preset}/rows")
    _diff(ocand, res.candidates(), f"{preset}/candidates")


@pytest.mark.parametrize("preset", PRESETS)
def test_oracle_vs_duckdb_datasets_mode(small_synth, preset):
    ref, _, _, q_in = small_synth
    cfg = MatchConfig.from_preset(preset).override(mode="datasets")
    b = [dict(id=r["r_id"], address=r["address"]) for r in ref[:1500]]
    orows, _ = _oracle(cfg, [dict(r_id=x["id"], address=x["address"]) for x in b], q_in[:200])
    res = Matcher("duckdb", cfg).link_datasets(q_in[:200], b)
    _diff(orows, res.rows(), f"{preset}/datasets")


@pytest.mark.spark
@pytest.mark.parametrize("preset", PRESETS)
def test_oracle_vs_spark(small_synth, spark, preset):
    _, _, ref_in, q_in = small_synth
    cfg = MatchConfig.from_preset(preset)
    orows, ocand = _oracle(cfg, ref_in, q_in)
    m = Matcher("spark", cfg, spark=spark, prefix=f"t_{preset}_")
    m.prepare_reference(ref_in)
    res = m.link(q_in)
    _diff(orows, res.rows(), f"spark/{preset}/rows")
    _diff(ocand, res.candidates(), f"spark/{preset}/candidates")
