"""Synthetic-benchmark quality gate (SPEC.md §11.5, §13). Fails CI on regressions."""

import pytest

from addrlink import Matcher, MatchConfig
from addrlink.evaluate import evaluate_run, wilson
from addrlink.synth import SynthConfig, generate


@pytest.fixture(scope="module")
def bench():
    ref, qs = generate(SynthConfig(target_addresses=20_000, n_localities=200, n_queries=2_000, seed=5))
    out = {}
    for preset in ("paper", "enhanced"):
        m = Matcher("duckdb", MatchConfig.from_preset(preset), prefix=f"{preset}_")
        m.prepare_reference(ref)
        out[preset] = evaluate_run(m, m.link(qs), qs, ref)["total"]
    return out


def test_enhanced_meets_gate(bench):
    t = bench["enhanced"]
    assert t["precision_auto"] >= 0.99
    assert t["recall_auto"] >= 0.88
    assert t["blocking_recall_pre_cap"] >= 0.98
    assert t["wrong_unit_rate_auto"] <= 0.01
    assert t["false_match_rate_negatives"] <= 0.05
    assert t["non_physical_routed"] == 1.0


def test_enhanced_beats_paper(bench):
    assert bench["enhanced"]["precision_auto"] > bench["paper"]["precision_auto"]
    assert bench["enhanced"]["recall_auto"] > bench["paper"]["recall_auto"]


def test_wilson_interval():
    lo, hi = wilson(59, 65)
    assert round(lo, 3) == 0.813 and round(hi, 3) == 0.957
