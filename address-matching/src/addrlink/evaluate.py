"""Metrics, confidence intervals and comparison reports (SPEC.md §11.3)."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Iterable

from .api import Matcher, MatchResult


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return (max(0.0, centre - half), min(1.0, centre + half))


def truth_in_candidates(m: Matcher, queries: list[dict]) -> tuple[set[str], set[str]]:
    """Query ids whose true entity reached (a) blocking before the cap, (b) scoring after the cap."""
    t = m.sql.t
    rows = [dict(id=q["id"], true_entity_id=q["true_entity_id"]) for q in queries if q.get("true_entity_id")]
    rel = m.engine.source_relation(rows, t("eval_truth"), [("id", "STRING"), ("true_entity_id", "STRING")])
    union = f"SELECT q_id, r_id FROM {t('cand1')}"
    if m.config.blocking.fallback.enabled:
        union += f" UNION ALL SELECT q_id, r_id FROM {t('cand2')}"
    pre = m.engine.fetch(f"""
SELECT DISTINCT n.id FROM {t('q_norm')} n JOIN {rel} e ON e.id = n.id
JOIN ({union}) c ON c.q_id = n.norm_key
JOIN {t('r_tok')} r ON r.id = c.r_id AND r.entity_id = e.true_entity_id""")
    post = m.engine.fetch(f"""
SELECT DISTINCT n.id FROM {t('q_norm')} n JOIN {rel} e ON e.id = n.id
JOIN {t('scored')} s ON s.q_id = n.norm_key AND s.entity_id = e.true_entity_id""")
    return {r["id"] for r in pre}, {r["id"] for r in post}


def evaluate(
    results: Iterable[dict[str, Any]],
    queries: list[dict],
    building_by_entity: dict[str, str],
    blocked_pre: set[str] | None = None,
    blocked_post: set[str] | None = None,
) -> dict[str, Any]:
    res = {r["id"]: r for r in results}
    tot: Counter = Counter()
    by_seg: dict[str, Counter] = defaultdict(Counter)
    by_corr: dict[str, Counter] = defaultdict(Counter)
    for q in queries:
        r = res[q["id"]]
        dec, ent = r["decision"], r.get("entity_id")
        kind = q["kind"]
        auto = dec == "AUTO_MATCH"
        keys = [tot, by_seg[q["segment"]]] + [by_corr[c] for c in (q.get("corruptions") or [])]
        if kind == "positive" and not q.get("corruptions"):
            keys.append(by_corr["(none)"])
        for c in keys:
            c[f"n_{kind}"] += 1
            c[f"dec_{dec}"] += 1
            if auto:
                c["auto"] += 1
            if kind == "positive":
                correct = ent == q["true_entity_id"]
                c["top1_correct"] += correct
                if auto and correct:
                    c["auto_correct"] += 1
                elif auto:
                    c["auto_wrong"] += 1
                    if ent and building_by_entity.get(ent) == q["true_building_key"]:
                        c["auto_wrong_unit"] += 1
                if dec in ("REVIEW", "PARTIAL_MATCH"):
                    c["review"] += 1
                    c["review_top1_correct"] += correct
                if blocked_pre is not None:
                    c["blocked_pre"] += q["id"] in blocked_pre
                if blocked_post is not None:
                    c["blocked_post"] += q["id"] in blocked_post
            elif kind == "negative" and auto:
                c["auto_on_negative"] += 1
                if ent and building_by_entity.get(ent) == q["true_building_key"]:
                    c["auto_wrong_unit"] += 1
            elif kind == "non_physical":
                c["non_physical_ok"] += dec == "NON_PHYSICAL"
    return dict(total=_summ(tot), by_segment={k: _summ(v) for k, v in sorted(by_seg.items())},
                by_corruption={k: _summ(v) for k, v in sorted(by_corr.items())})


def _summ(c: Counter) -> dict[str, Any]:
    pos, neg, auto = c["n_positive"], c["n_negative"], c["auto"]
    phys = pos + neg
    out: dict[str, Any] = dict(counts=dict(c))
    out["n_positive"], out["n_negative"], out["n_non_physical"] = pos, neg, c["n_non_physical"]
    out["precision_auto"] = c["auto_correct"] / auto if auto else float("nan")
    out["precision_auto_ci"] = wilson(c["auto_correct"], auto)
    out["recall_auto"] = c["auto_correct"] / pos if pos else float("nan")
    out["recall_auto_ci"] = wilson(c["auto_correct"], pos)
    out["top1_accuracy"] = c["top1_correct"] / pos if pos else float("nan")
    out["review_rate"] = c["review"] / phys if phys else float("nan")
    out["wrong_unit_rate_auto"] = c["auto_wrong_unit"] / auto if auto else float("nan")
    out["false_match_rate_negatives"] = c["auto_on_negative"] / neg if neg else float("nan")
    out["blocking_recall_pre_cap"] = c["blocked_pre"] / pos if pos else float("nan")
    out["blocking_recall_post_cap"] = c["blocked_post"] / pos if pos else float("nan")
    out["non_physical_routed"] = c["non_physical_ok"] / c["n_non_physical"] if c["n_non_physical"] else float("nan")
    return out


def evaluate_run(m: Matcher, res: MatchResult, queries: list[dict], ref_rows: list[dict]) -> dict[str, Any]:
    building = {r["entity_id"]: r["building_key"] for r in ref_rows}
    pre, post = truth_in_candidates(m, queries)
    out = evaluate(res.rows(), queries, building, pre, post)
    out["run_log"] = res.run_log.as_dict()
    out["decisions"] = res.decision_counts()
    return out


def dedupe_pairwise(clusters: Iterable[dict[str, Any]], truth: dict[str, str]) -> dict[str, float]:
    """Pairwise precision/recall of predicted clusters against true entity ids (records in ``truth`` only)."""
    pred: Counter = Counter()
    true: Counter = Counter()
    both: Counter = Counter()
    for r in clusters:
        if r["id"] not in truth:
            continue
        pred[r["cluster_id"]] += 1
        true[truth[r["id"]]] += 1
        both[(r["cluster_id"], truth[r["id"]])] += 1
    pairs = lambda c: sum(n * (n - 1) // 2 for n in c.values())  # noqa: E731
    p, t, tp = pairs(pred), pairs(true), pairs(both)
    return dict(pairwise_precision=tp / p if p else float("nan"), pairwise_recall=tp / t if t else float("nan"),
                predicted_pairs=p, true_pairs=t, true_positive_pairs=tp)


# ----------------------------------------------------------------------------- report
def _pct(x: float) -> str:
    return "–" if x != x else f"{100 * x:.1f}%"


def _ci(ci: tuple[float, float]) -> str:
    return "" if ci[0] != ci[0] else f" [{100 * ci[0]:.1f}, {100 * ci[1]:.1f}]"


HEADLINE = [
    ("precision_auto", "AUTO precision (95% CI)"),
    ("recall_auto", "AUTO recall (95% CI)"),
    ("top1_accuracy", "Top-1 accuracy (any decision)"),
    ("review_rate", "REVIEW + PARTIAL rate"),
    ("wrong_unit_rate_auto", "Wrong-unit share of AUTO"),
    ("false_match_rate_negatives", "AUTO on negatives (no true match)"),
    ("blocking_recall_pre_cap", "Blocking recall (pre-cap)"),
    ("blocking_recall_post_cap", "Blocking recall (post-cap)"),
    ("non_physical_routed", "PO boxes routed NON_PHYSICAL"),
]


def report(runs: dict[str, dict[str, Any]], title: str = "addrlink benchmark") -> str:
    names = list(runs)
    lines = [f"# {title}", "", "| Metric | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    for key, label in HEADLINE:
        cells = []
        for n in names:
            t = runs[n]["total"]
            cell = _pct(t[key])
            if key + "_ci" in t:
                cell += _ci(t[key + "_ci"])
            cells.append(cell)
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines.append("| Runtime (s, match only) | " + " | ".join(
        f"{sum(v for k, v in runs[n]['run_log']['timings'].items() if not k.startswith('ref:')):.1f}" for n in names) + " |")
    lines.append("| Candidate pairs scored | " + " | ".join(
        str(runs[n]["run_log"]["counts"].get("scored", "–")) for n in names) + " |")
    t0 = runs[names[0]]["total"]
    lines += ["", f"Queries: {t0['n_positive']} positive, {t0['n_negative']} negative (true address withheld from "
              f"reference), {t0['n_non_physical']} PO boxes.", ""]
    for section, label in (("by_segment", "segment"), ("by_corruption", "corruption")):
        lines += [f"## Recall / precision by {label}", "",
                  f"| {label} | n | " + " | ".join(f"{n} recall | {n} precision | {n} top-1" for n in names) + " |",
                  "|---|---|" + "---|---|---|" * len(names)]
        keys = sorted(set().union(*(runs[n][section].keys() for n in names)))
        for k in keys:
            row = [k, str(runs[names[0]][section].get(k, {}).get("n_positive", 0))]
            for n in names:
                s = runs[n][section].get(k)
                row += [_pct(s["recall_auto"]), _pct(s["precision_auto"]), _pct(s["top1_accuracy"])] if s else ["–"] * 3
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    for n in names:
        lines.append(f"**{n}** decisions: " + ", ".join(f"{k}={v}" for k, v in runs[n]["decisions"].items()))
        lines.append("")
    return "\n".join(lines)
