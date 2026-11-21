"""Configuration models and presets (SPEC.md §8)."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NormaliseConfig(_Model):
    classify: bool = True
    ascii_fold: bool = True
    drop_noise_tokens: bool = True
    split_glued_prefixes: bool = True
    synonyms: bool = True
    collapse_duplicate_tokens: bool = True
    # Extra single-token synonym pairs, e.g. from G-NAF authority tables (see gnaf.authority_synonyms).
    extra_synonyms: dict[str, str] = Field(default_factory=dict)


class FallbackConfig(_Model):
    enabled: bool = True
    m: int = Field(3, ge=1)
    k_max: int = Field(2000, ge=1)


class BlockingConfig(_Model):
    bigram_mode: Literal["ordered", "unordered"] = "ordered"
    rule: Literal["both_sides", "product"] = "product"
    k: int = Field(100, ge=1)
    k_ref: int = Field(100, ge=1)
    p_max: int = Field(10_000, ge=1)
    fallback: FallbackConfig = FallbackConfig()
    max_candidates: int | None = Field(200, ge=1)


class ScoringConfig(_Model):
    # Thresholds in DecisionConfig apply to this score as-is (Dice by default, SPEC §2.1).
    primary: Literal["char_dice", "char_jaccard"] = "char_dice"
    # structured: use reference premise numbers when supplied (UC1, SPEC §7.6); subset: paper rule.
    numeric_rule: Literal["structured", "subset"] = "structured"
    numeric_exclude_postcodes: bool = True


class DecisionConfig(_Model):
    method: Literal["threshold", "bands"] = "bands"
    tau: float = Field(0.70, ge=0, le=1)
    tau_auto: float = Field(0.80, ge=0, le=1)
    tau_review: float = Field(0.65, ge=0, le=1)
    min_margin: float = Field(0.02, ge=0, le=1)
    top_n_numeric: int = Field(3, ge=1)


class OutputConfig(_Model):
    keep_top_k: int = Field(3, ge=1)


class MatchConfig(_Model):
    preset: str = "enhanced"
    mode: Literal["reference", "datasets", "dedupe"] = "reference"
    normalise: NormaliseConfig = NormaliseConfig()
    blocking: BlockingConfig = BlockingConfig()
    scoring: ScoringConfig = ScoringConfig()
    decision: DecisionConfig = DecisionConfig()
    output: OutputConfig = OutputConfig()

    @model_validator(mode="after")
    def _check(self) -> "MatchConfig":
        if self.blocking.p_max < self.blocking.k:
            raise ValueError("blocking.p_max must be >= blocking.k")
        if self.decision.tau_review > self.decision.tau_auto:
            raise ValueError("decision.tau_review must be <= decision.tau_auto")
        return self

    # ------------------------------------------------------------------ presets
    @classmethod
    def from_preset(cls, name: str, **overrides: Any) -> "MatchConfig":
        if name not in PRESETS:
            raise ValueError(f"unknown preset {name!r}; choose from {sorted(PRESETS)}")
        base = json.loads(json.dumps(PRESETS[name]))
        return cls.model_validate(_deep_merge(base, overrides))

    def override(self, **overrides: Any) -> "MatchConfig":
        return MatchConfig.model_validate(_deep_merge(self.model_dump(), overrides))

    def with_dotted(self, assignments: list[str]) -> "MatchConfig":
        """Apply CLI-style overrides such as ``decision.tau_auto=0.82``."""
        data = self.model_dump()
        for a in assignments:
            key, _, raw = a.partition("=")
            node = data
            parts = key.strip().split(".")
            for p in parts[:-1]:
                node = node[p]
            node[parts[-1]] = json.loads(raw) if raw not in ("", None) and _is_json(raw) else raw
        return MatchConfig.model_validate(data)

    def config_hash(self) -> str:
        blob = json.dumps(self.model_dump(), sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    @property
    def keep_ranked(self) -> int:
        return max(self.output.keep_top_k, self.decision.top_n_numeric, 2)


def _is_json(raw: str) -> bool:
    try:
        json.loads(raw)
        return True
    except ValueError:
        return False


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


PRESETS: dict[str, dict] = {
    # Faithful reproduction of arXiv:1708.01402 (with its "Jaccard" read as Dice, SPEC §2.1).
    "paper": {
        "preset": "paper",
        "normalise": {
            "classify": False,
            "ascii_fold": False,
            "drop_noise_tokens": False,
            "split_glued_prefixes": False,
            "synonyms": False,
            "collapse_duplicate_tokens": False,
        },
        "blocking": {
            "bigram_mode": "ordered",
            "rule": "both_sides",
            "k": 100,
            "fallback": {"enabled": False},
            "max_candidates": None,
        },
        "scoring": {"primary": "char_dice", "numeric_rule": "subset", "numeric_exclude_postcodes": False},
        "decision": {"method": "threshold", "tau": 0.70, "top_n_numeric": 3},
        "output": {"keep_top_k": 3},
    },
    "enhanced": {"preset": "enhanced"},
}
