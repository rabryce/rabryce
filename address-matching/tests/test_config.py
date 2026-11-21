import pytest

from addrlink import MatchConfig


def test_presets_differ_and_hash_is_stable():
    p, e = MatchConfig.from_preset("paper"), MatchConfig.from_preset("enhanced")
    assert p.blocking.rule == "both_sides" and not p.blocking.fallback.enabled
    assert e.blocking.rule == "product" and e.blocking.fallback.enabled
    assert p.config_hash() != e.config_hash()
    assert e.config_hash() == MatchConfig.from_preset("enhanced").config_hash()


def test_overrides():
    c = MatchConfig.from_preset("enhanced").override(decision={"tau_auto": 0.85})
    assert c.decision.tau_auto == 0.85 and c.decision.tau_review == 0.65
    c2 = c.with_dotted(["blocking.k_ref=200", "blocking.fallback.enabled=false"])
    assert c2.blocking.k_ref == 200 and c2.blocking.fallback.enabled is False


def test_validation():
    with pytest.raises(ValueError):
        MatchConfig.from_preset("enhanced", decision={"tau_review": 0.9, "tau_auto": 0.8})
    with pytest.raises(ValueError):
        MatchConfig.from_preset("nope")
    with pytest.raises(ValueError):
        MatchConfig.from_preset("enhanced", blocking={"unknown": 1})
