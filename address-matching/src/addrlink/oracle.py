"""Pure-Python reference implementation of the full pipeline (SPEC.md §9.3).

Mirrors ``sql.py`` step for step so tests can assert that DuckDB and Spark produce
the same candidates, scores, ranks and decisions. It is also handy for explaining
a single decision without an engine. It is not built for speed.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from . import rules
from .config import MatchConfig

_FOLD = str.maketrans(rules.ACCENTS_FROM, rules.ACCENTS_TO)
_NONALNUM = re.compile("[^A-Z0-9]+")
_SPACES = re.compile(" +")
_GLUED = re.compile(rules.GLUED_PREFIX_RE)
_GLUED_SPLIT = re.compile(rules.GLUED_PREFIX_SPLIT)
_GLUED_NUM = re.compile(rules.GLUED_NUMBER_RE)
_GLUED_NUM_SPLIT = re.compile(rules.GLUED_NUMBER_SPLIT)
_NONPHYS = re.compile(rules.NON_PHYSICAL_RE)
_FOREIGN = re.compile(rules.FOREIGN_RE)
_PC = re.compile(rules.POSTCODE_TOKEN_RE)
_DIGIT = re.compile(rules.HAS_DIGIT_RE)


def numcore(x: str | None) -> str | None:
    if x is None:
        return None
    d = re.sub("^0+", "", re.sub("[^0-9]", "", x))
    return "0" if d == "" else d


def occ(xs: list[str]) -> list[str]:
    """Occurrence-index (E, E, L -> E#1, E#2, L#1) so set ops act as multiset ops."""
    seen: dict[str, int] = defaultdict(int)
    out = []
    for x in xs:
        seen[x] += 1
        out.append(f"{x}#{seen[x]}")
    return out


def light_words(x: str | None, cfg: MatchConfig) -> list[str]:
    a = x or ""
    if cfg.normalise.ascii_fold:
        a = a.translate(_FOLD)
    w = [t for t in _NONALNUM.sub(" ", a.upper()).strip(" ").split(" ") if t != ""]
    if cfg.normalise.synonyms:
        w = [rules.SYNONYMS.get(t, t) for t in w]
    return w


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


# ----------------------------------------------------------------- normalisation
def normalise(address: str | None, cfg: MatchConfig, classify: bool = True) -> tuple[str, list[str], str]:
    """Return (input_class, words, norm)."""
    n = cfg.normalise
    a = address or ""
    if n.ascii_fold:
        a = a.translate(_FOLD)
    basic = _NONALNUM.sub(" ", a.upper()).strip(" ")
    if basic == "":
        cls = "EMPTY"
    elif classify and n.classify and _NONPHYS.search(basic):
        cls = "NON_PHYSICAL"
    elif classify and n.classify and _FOREIGN.search(basic):
        cls = "OUT_OF_SCOPE"
    else:
        cls = "PHYSICAL"
    s = f" {basic} "
    phrases: list[tuple[str, str]] = []
    if n.drop_noise_tokens:
        phrases += [(p, "") for p in rules.NOISE_PHRASES]
    if n.synonyms:
        phrases += list(rules.STATE_PHRASES.items())
    for src, dst in phrases:
        rep = f" {dst} " if dst else " "
        for _ in range(2):
            s = s.replace(f" {src} ", rep)
    s = _SPACES.sub(" ", s).strip(" ")
    w = s.split(" ")
    if n.split_glued_prefixes:
        w = " ".join(
            _GLUED_SPLIT.sub(r"\1 \2", x) if _GLUED.search(x)
            else _GLUED_NUM_SPLIT.sub(r"\1 \2", x) if _GLUED_NUM.search(x) else x
            for x in w
        ).split(" ")
    if n.synonyms:
        w = [rules.SYNONYMS.get(x, x) for x in w]
    if n.drop_noise_tokens:
        w = [x for i, x in enumerate(w, 1) if not (i == len(w) and x in rules.TRAILING_NOISE)]
    if n.collapse_duplicate_tokens:
        w = [x for i, x in enumerate(w) if i == 0 or x != w[i - 1] or _DIGIT.search(x)]
    norm = " ".join(w)
    if norm == "":
        cls = "EMPTY"
    return cls, w, norm


@dataclass
class Tok:
    id: str
    norm: str
    words: list[str]
    blk: list[str]
    blk2: list[str]
    chars: list[str]
    nums: list[str]
    pc: str | None
    state: str | None
    wset: list[str]
    entity_id: str | None = None
    address: str | None = None
    prem_req: list[str] | None = None
    prem_opt: list[str] = field(default_factory=list)
    prem_req_idx: list[str] | None = None
    prem_all_idx: list[str] | None = None
    sname: list[str] = field(default_factory=list)
    widf: list[float] = field(default_factory=list)
    wnorm: float = 0.0


def tokenise(id_: str, words: list[str], norm: str, cfg: MatchConfig) -> Tok:
    b = cfg.blocking

    def pair(a: str, c: str) -> str:
        if b.bigram_mode == "unordered" and not a <= c:
            return f"{c} {a}"
        return f"{a} {c}"

    n = len(words)
    bigrams = [pair(words[i], words[i + 1]) for i in range(n - 1)]
    trigrams = [f"{words[i]} {words[i + 1]} {words[i + 2]}" for i in range(n - 2)]
    blk2 = bigrams + trigrams if b.fallback.enabled else []
    cs = list(norm.replace(" ", ""))
    chars = occ(cs)
    nums = occ([numcore(x) for x in words if _DIGIT.search(x)])  # type: ignore[misc]
    pcs = [
        words[i - 1]
        for i in range(1, n + 1)
        if _PC.search(words[i - 1]) and (i == n or (i > 1 and words[i - 2] in rules.STATES))
    ]
    states = [x for x in words if x in rules.STATES]
    return Tok(
        id=id_, norm=norm, words=words, blk=bigrams, blk2=blk2, chars=chars, nums=nums,  # type: ignore[arg-type]
        pc=numcore(pcs[-1]) if pcs else None, state=states[-1] if states else None,
        wset=sorted(set(words)),
    )


# ----------------------------------------------------------------- pipeline
class Oracle:
    def __init__(self, cfg: MatchConfig):
        self.cfg = cfg
        self.ref: dict[str, Tok] = {}

    def prepare_reference(self, rows: Iterable[dict[str, Any]]) -> int:
        cfg = self.cfg
        self.ref = {}
        for r in rows:
            _, words, norm = normalise(r.get("address"), cfg, classify=False)
            if norm == "":
                continue
            t = tokenise(str(r["r_id"]), words, norm, cfg)
            t.entity_id = str(r.get("entity_id") if r.get("entity_id") is not None else r["r_id"])
            t.address = r.get("address")
            pn, po = r.get("premise_numbers"), r.get("premise_numbers_optional")
            clean = lambda xs: [numcore(x.upper()) for x in xs if x is not None and x.strip(" ") != ""]  # noqa: E731
            t.prem_req = None if pn is None else clean(pn)
            t.prem_opt = clean(po or [])
            if t.prem_req is not None:
                t.prem_req_idx = occ(t.prem_req)
                t.prem_all_idx = occ(t.prem_req + t.prem_opt)
            t.sname = light_words(r.get("street_name"), cfg)
            if r.get("postcode") is not None:
                t.pc = numcore(r["postcode"])
            if r.get("state") is not None:
                t.state = r["state"].strip(" ").upper()
            self.ref[t.id] = t
        self.n_ref = len(self.ref)
        N = float(self.n_ref)
        self.post: dict[str, set[str]] = defaultdict(set)
        self.post2: dict[str, set[str]] = defaultdict(set)
        wdf: dict[str, int] = defaultdict(int)
        for t in self.ref.values():
            for tok in set(t.blk):
                self.post[tok].add(t.id)
            for tok in set(t.blk2):
                self.post2[tok].add(t.id)
            for w in t.wset:
                wdf[w] += 1
        self.r_df = {k: len(v) for k, v in self.post.items()}
        self.r_idf = {k: round(math.log(N / v), 6) for k, v in self.r_df.items()}
        self.r_df2 = {k: len(v) for k, v in self.post2.items()}
        self.w_idf = {w: round(math.log(N / v), 6) for w, v in wdf.items()}
        self.unseen = round(math.log(N), 6)
        for t in self.ref.values():
            self._wvec(t)
        return self.n_ref

    def _wvec(self, t: Tok) -> None:
        t.widf = [self.w_idf.get(w, self.unseen) for w in t.wset]
        acc = 0.0
        for x in t.widf:
            acc += x * x
        t.wnorm = math.sqrt(acc)

    # ------------------------------------------------------------------ link
    def link(self, rows: Iterable[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
        cfg, b = self.cfg, self.cfg.blocking
        qrows = []
        for r in rows:
            cls, words, norm = normalise(r.get("address"), cfg, classify=True)
            qrows.append(dict(id=str(r["id"]), address=r.get("address"), norm=norm, input_class=cls, norm_key=sha256(norm)))
        q: dict[str, Tok] = {}
        for norm in sorted({r["norm"] for r in qrows if r["input_class"] == "PHYSICAL"}):
            t = tokenise(sha256(norm), norm.split(" "), norm, cfg)
            self._wvec(t)
            q[t.id] = t
        q_df: dict[str, int] = defaultdict(int)
        for t in q.values():
            for tok in set(t.blk):
                q_df[tok] += 1

        cand: dict[str, dict[str, dict]] = defaultdict(dict)
        for qid, t in q.items():
            for tok in set(t.blk):
                if tok not in self.r_df:
                    continue
                qd, rd = q_df[tok], self.r_df[tok]
                ok = (qd <= b.k and rd <= b.k) if b.rule == "both_sides" else (rd <= b.k_ref and qd * rd <= b.p_max)
                if not ok:
                    continue
                for rid in self.post[tok]:
                    c = cand[qid].setdefault(rid, dict(shared_blocks=0, idf=[], blocking_pass=1))
                    c["shared_blocks"] += 1
                    c["idf"].append(self.r_idf[tok])
        if b.fallback.enabled:
            N = float(self.n_ref)
            for qid, t in q.items():
                if qid in cand:
                    continue
                toks = sorted(
                    ((self.r_df2[x], x) for x in set(t.blk2) if x in self.r_df2 and self.r_df2[x] <= b.fallback.k_max)
                )[: b.fallback.m]
                for df2, tok in toks:
                    for rid in self.post2[tok]:
                        c = cand[qid].setdefault(rid, dict(shared_blocks=0, idf=[], blocking_pass=2))
                        c["shared_blocks"] += 1
                        c["idf"].append(round(math.log(N / df2), 6))

        scored: dict[str, list[dict]] = {}
        for qid, cs in cand.items():
            items = []
            for rid, c in cs.items():
                c["idf_overlap"] = round(sum(c["idf"]), 9)
                items.append((rid, c))
            n_cand = len(items)
            items.sort(key=lambda it: (-it[1]["idf_overlap"], -it[1]["shared_blocks"], it[0]))
            if b.max_candidates:
                items = items[: b.max_candidates]
            scored[qid] = [self._score(q[qid], self.ref[rid], c, n_cand) for rid, c in items]

        decided: dict[str, dict] = {}
        cand_out: list[dict] = []
        for qid, rows_ in scored.items():
            ranked = self._rank(rows_)
            for r in ranked:
                if r["rnk"] <= cfg.output.keep_top_k:
                    cand_out.append({"norm_key": qid, **{k: r[k] for k in _CAND_COLS}})
            d = self._decide(ranked)
            if d is not None:
                decided[qid] = d

        out = []
        for r in qrows:
            row = dict(id=r["id"], input_address=r["address"], norm=r["norm"], norm_key=r["norm_key"],
                       input_class=r["input_class"])
            phys = r["input_class"] == "PHYSICAL"
            d = decided.get(r["norm_key"]) if phys else None
            has_cand = phys and r["norm_key"] in scored
            row["decision"] = r["input_class"] if not phys else (d["decision"] if d else "NO_MATCH")
            for k in _RESULT_COLS:
                row[k] = d[k] if d else None
            row["n_candidates"] = scored[r["norm_key"]][0]["n_candidates"] if has_cand else 0
            if not phys:
                row["reason_codes"] = []
            elif not has_cand:
                row["reason_codes"] = ["NO_CANDIDATES"]
            elif d is None:
                row["reason_codes"] = ["NO_CONSISTENT_CANDIDATE"]
            else:
                row["reason_codes"] = d["reason_codes"]
            out.append(row)
        out.sort(key=lambda r: r["id"])
        cand_out.sort(key=lambda r: (r["norm_key"], r["rnk"]))
        return out, cand_out

    # ------------------------------------------------------------------ scoring
    def _score(self, qt: Tok, rt: Tok, c: dict, n_cand: int) -> dict:
        cfg = self.cfg
        inter = len(set(qt.chars) & set(rt.chars))
        qa, rb = len(qt.chars), len(rt.chars)
        dice = 2.0 * inter / (qa + rb)
        score = dice if cfg.scoring.primary == "char_dice" else float(inter) / (qa + rb - inter)
        lev = levenshtein(qt.norm, rt.norm)
        lev_ratio = 1 - float(lev) / max(len(qt.norm), len(rt.norm))
        rset = set(rt.wset)
        num = 0.0
        for w, x in zip(qt.wset, qt.widf):
            num += x * x if w in rset else 0.0
        den = qt.wnorm * rt.wnorm
        cosine = round(num / den, 9) if den > 0 else 0.0

        qpc, rpc = qt.pc or "#", rt.pc or "#"
        keep = lambda x: x.split("#")[0] not in (qpc, rpc)  # noqa: E731
        if cfg.scoring.numeric_exclude_postcodes:
            na = [x for x in qt.nums if keep(x)]
            nb = [x for x in rt.nums if keep(x)]
        else:
            na, nb = qt.nums, rt.nums
        subset = "CONSISTENT" if (not set(na) - set(nb)) or (not set(nb) - set(na)) else "CONFLICT"
        if cfg.scoring.numeric_rule == "structured" and cfg.mode == "reference":
            if rt.prem_req_idx is None or rt.prem_all_idx is None:
                status = subset
            else:
                nq = [x for x in qt.nums if keep(x)]
                missing = bool(set(rt.prem_req_idx) - set(nq))
                extra = bool(set(nq) - set(rt.prem_all_idx))
                status = ("EXACT" if not extra else "QUERY_MORE_SPECIFIC") if not missing else (
                    "QUERY_LESS_SPECIFIC" if not extra else "CONFLICT")
        else:
            status = subset

        if not rt.sname:
            street = "ABSENT"
        elif all(sw in qt.words for sw in rt.sname):
            street = "MATCH"
        elif all(any(levenshtein(qw, sw) <= (0 if len(sw) <= 3 else 1 if len(sw) <= 5 else 2) for qw in qt.words)
                 for sw in rt.sname):
            street = "FUZZY"
        else:
            street = "MISMATCH"

        def st(a: str | None, b_: str | None) -> str:
            if a is None or b_ is None:
                return "ABSENT"
            return "MATCH" if a == b_ else "MISMATCH"

        return dict(
            q_id=qt.id, r_id=rt.id, entity_id=rt.entity_id, ref_address=rt.address,
            shared_blocks=c["shared_blocks"], idf_overlap=c["idf_overlap"], blocking_pass=c["blocking_pass"],
            n_candidates=n_cand, score=score, dice=dice, lev_ratio=lev_ratio, word_idf_cosine=cosine,
            numeric_status=status, postcode_status=st(qt.pc, rt.pc), state_status=st(qt.state, rt.state),
            street_status=street,
            numeric_rank={"EXACT": 0, "CONSISTENT": 0, "CONFLICT": 2}.get(status, 1),
        )

    def _key(self, r: dict) -> tuple:
        if self.cfg.preset == "paper" or self.cfg.decision.method == "threshold":
            return (-r["score"], r["entity_id"], r["r_id"])
        return (r["numeric_rank"], -r["score"], -r["shared_blocks"], -r["lev_ratio"],
                -r["word_idf_cosine"], r["entity_id"], r["r_id"])

    def _rank(self, rows: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for r in sorted(rows, key=self._key):
            best.setdefault(r["entity_id"], r)
        ranked = sorted(best.values(), key=self._key)[: self.cfg.keep_ranked]
        crk: dict[int, int] = defaultdict(int)
        for i, r in enumerate(ranked, 1):
            r["rnk"] = i
            crk[r["numeric_rank"]] += 1
            r["crk"] = crk[r["numeric_rank"]]
        return ranked

    def _decide(self, ranked: list[dict]) -> dict | None:
        cfg, dc = self.cfg, self.cfg.decision
        thr = dc.method == "threshold"
        if thr and cfg.mode == "datasets":
            sel = [r for r in ranked if r["numeric_status"] == "CONSISTENT" and r["rnk"] <= dc.top_n_numeric]
        else:
            sel = [r for r in ranked if r["rnk"] == 1]
        if not sel:
            return None
        s = dict(sel[0])
        comp = next((r for r in ranked if r["numeric_rank"] == s["numeric_rank"] and r["crk"] == s["crk"] + 1), None)
        margin = s["score"] - (comp["score"] if comp else 0.0)
        has_comp = comp is not None
        ordered = has_comp and (s["shared_blocks"] > comp["shared_blocks"] or s["lev_ratio"] > comp["lev_ratio"])
        if thr:
            decision = "AUTO_MATCH" if cfg.mode == "datasets" or s["score"] > dc.tau else "NO_MATCH"
        elif (s["score"] >= dc.tau_auto and s["numeric_rank"] == 0 and s["postcode_status"] != "MISMATCH"
              and s["state_status"] != "MISMATCH" and s["street_status"] != "MISMATCH" and (margin >= dc.min_margin or ordered)):
            decision = "AUTO_MATCH"
        elif s["numeric_status"] == "QUERY_LESS_SPECIFIC" and s["score"] >= dc.tau_review:
            decision = "PARTIAL_MATCH"
        elif s["score"] >= dc.tau_review and s["numeric_status"] != "CONFLICT":
            decision = "REVIEW"
        else:
            decision = "NO_MATCH"
        low = margin < dc.min_margin
        reasons = [
            "NUM_" + s["numeric_status"],
            "POSTCODE_MISMATCH" if s["postcode_status"] == "MISMATCH" else None,
            "STATE_MISMATCH" if s["state_status"] == "MISMATCH" else None,
            "STREET_MISMATCH" if s["street_status"] == "MISMATCH" else None,
            "STREET_FUZZY" if s["street_status"] == "FUZZY" else None,
            "LOW_MARGIN" if low else None,
            "TIE_BROKEN_BY_BLOCKS" if low and has_comp and s["shared_blocks"] > comp["shared_blocks"] else None,
            "TIE_BROKEN_BY_LEV" if low and has_comp and s["shared_blocks"] <= comp["shared_blocks"]
            and s["lev_ratio"] > comp["lev_ratio"] else None,
            "FALLBACK_BLOCKING" if s["blocking_pass"] == 2 else None,
            "CAPPED" if s["n_candidates"] > (cfg.blocking.max_candidates or 2**31 - 1) else None,
        ]
        s.update(margin=margin, decision=decision, reason_codes=[x for x in reasons if x])
        return s


_CAND_COLS = ("rnk", "r_id", "entity_id", "ref_address", "score", "dice", "shared_blocks", "idf_overlap",
              "lev_ratio", "word_idf_cosine", "numeric_status", "postcode_status", "state_status", "street_status",
              "blocking_pass")
_RESULT_COLS = ("r_id", "entity_id", "ref_address", "score", "dice", "shared_blocks", "idf_overlap", "lev_ratio",
                "word_idf_cosine", "numeric_status", "postcode_status", "state_status", "street_status", "margin",
                "blocking_pass")


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]
