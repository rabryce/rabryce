"""SQL generation for every pipeline stage, in two dialects (SPEC.md §5.3, §7, §9).

Each stage is a ``(table_name, select_sql)`` pair; an Engine materialises it.
Expressions are written once against the small ``Dialect`` interface so Spark
SQL and DuckDB SQL stay structurally identical. Index arithmetic is always
1-based in this module (``seq1`` / ``at``), which hides Spark's 0-based lambda
indices. All literals come from ``rules.py`` (validated [A-Z0-9 ] tokens).
"""

from __future__ import annotations

from dataclasses import dataclass

from . import rules
from .config import MatchConfig


# --------------------------------------------------------------------------- dialects
class Dialect:
    name = "base"
    str_type = "STRING"

    def lam(self, params: str, body: str) -> str: ...
    def transform(self, arr: str, params: str, body: str) -> str: ...
    def filter(self, arr: str, params: str, body: str) -> str: ...
    def size(self, arr: str) -> str: ...
    def at(self, arr: str, i1: str) -> str: ...
    def last(self, arr: str) -> str: ...
    def seq1(self, n: str) -> str: ...
    def slice1(self, arr: str, start: str, length: str) -> str: ...
    def contains(self, arr: str, x: str) -> str: ...
    def intersect(self, a: str, b: str) -> str: ...
    def except_size(self, a: str, b: str) -> str: ...
    def concat_arrays(self, a: str, b: str) -> str: ...
    def join(self, arr: str, sep: str) -> str: ...
    def split_space(self, s: str) -> str: ...
    def split_chars(self, s: str) -> str: ...
    def rlike(self, s: str, pat: str) -> str: ...
    def re_replace(self, s: str, pat: str, rep: str) -> str: ...
    def sort_distinct(self, arr: str) -> str: ...
    def array_of(self, items: list[str]) -> str: ...
    def empty_str_array(self) -> str: ...
    def null_str_array(self) -> str: ...
    def sum_doubles(self, arr: str) -> str: ...
    def ordered_collect(self, value: str, order: str) -> str: ...
    def sha256(self, s: str) -> str: ...
    def explode(self, arr: str) -> str: ...
    def posexplode(self, arr: str, pos: str, val: str) -> str: ...

    # shared helpers ------------------------------------------------------
    @staticmethod
    def lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    def in_list(self, x: str, items: tuple[str, ...] | list[str]) -> str:
        return f"{x} IN ({', '.join(self.lit(i) for i in items)})"

    def dbl(self, x: str) -> str:
        return f"CAST({x} AS DOUBLE)"


class SparkDialect(Dialect):
    name = "spark"

    def lam(self, params, body):
        return f"({params}) -> {body}" if "," in params else f"{params} -> {body}"

    def transform(self, arr, params, body):
        return f"transform({arr}, {self.lam(params, body)})"

    def filter(self, arr, params, body):
        return f"filter({arr}, {self.lam(params, body)})"

    def size(self, arr):
        return f"size({arr})"

    def at(self, arr, i1):
        return f"try_element_at({arr}, {i1})"

    def last(self, arr):
        return f"try_element_at({arr}, -1)"

    def seq1(self, n):
        return f"(CASE WHEN ({n}) >= 1 THEN sequence(1, {n}) ELSE CAST(array() AS ARRAY<INT>) END)"

    def slice1(self, arr, start, length):
        return f"slice({arr}, {start}, {length})"

    def contains(self, arr, x):
        return f"array_contains({arr}, {x})"

    def intersect(self, a, b):
        return f"array_intersect({a}, {b})"

    def except_size(self, a, b):
        return f"size(array_except({a}, {b}))"

    def concat_arrays(self, a, b):
        return f"concat({a}, {b})"

    def join(self, arr, sep):
        return f"array_join({arr}, {self.lit(sep)})"

    def split_space(self, s):
        return f"split({s}, ' ')"

    def split_chars(self, s):
        return f"split({s}, '')"

    def rlike(self, s, pat):
        return f"({s} RLIKE {self.lit(pat)})"

    def re_replace(self, s, pat, rep):
        return f"regexp_replace({s}, {self.lit(pat)}, {self.lit(rep.replace(chr(92), '$'))})"

    def sort_distinct(self, arr):
        return f"array_sort(array_distinct({arr}))"

    def array_of(self, items):
        return f"array({', '.join(items)})"

    def empty_str_array(self):
        return "CAST(array() AS ARRAY<STRING>)"

    def null_str_array(self):
        return "CAST(NULL AS ARRAY<STRING>)"

    def sum_doubles(self, arr):
        return f"aggregate({arr}, CAST(0 AS DOUBLE), (acc_, x_) -> acc_ + x_)"

    def ordered_collect(self, value, order):
        return f"transform(array_sort(collect_list(struct({order} AS p_, {value} AS v_))), e_ -> e_.v_)"

    def sha256(self, s):
        return f"sha2({s}, 256)"

    def explode(self, arr):
        return f"explode({arr})"

    def posexplode(self, arr, pos, val):
        return f"posexplode({arr}) AS ({pos}, {val})"


class DuckDBDialect(Dialect):
    name = "duckdb"

    def lam(self, params, body):
        return f"lambda {params}: {body}"

    def transform(self, arr, params, body):
        return f"list_transform({arr}, {self.lam(params, body)})"

    def filter(self, arr, params, body):
        return f"list_filter({arr}, {self.lam(params, body)})"

    def size(self, arr):
        return f"len({arr})"

    def at(self, arr, i1):
        return f"({arr})[{i1}]"

    def last(self, arr):
        return f"({arr})[-1]"

    def seq1(self, n):
        return f"range(1, ({n}) + 1)"

    def slice1(self, arr, start, length):
        return f"list_slice({arr}, {start}, ({start}) + ({length}) - 1)"

    def contains(self, arr, x):
        return f"list_contains({arr}, {x})"

    def intersect(self, a, b):
        return f"list_intersect({a}, {b})"

    def except_size(self, a, b):
        return f"len(list_filter({a}, lambda x_: NOT list_contains({b}, x_)))"

    def concat_arrays(self, a, b):
        return f"list_concat({a}, {b})"

    def join(self, arr, sep):
        return f"array_to_string({arr}, {self.lit(sep)})"

    def split_space(self, s):
        return f"string_split({s}, ' ')"

    def split_chars(self, s):
        return f"string_split({s}, '')"

    def rlike(self, s, pat):
        return f"regexp_matches({s}, {self.lit(pat)})"

    def re_replace(self, s, pat, rep):
        return f"regexp_replace({s}, {self.lit(pat)}, {self.lit(rep)}, 'g')"

    def sort_distinct(self, arr):
        return f"list_sort(list_distinct({arr}))"

    def array_of(self, items):
        return f"[{', '.join(items)}]"

    def empty_str_array(self):
        return "CAST([] AS VARCHAR[])"

    def null_str_array(self):
        return "CAST(NULL AS VARCHAR[])"

    def sum_doubles(self, arr):
        return f"coalesce(list_sum({arr}), CAST(0 AS DOUBLE))"

    def ordered_collect(self, value, order):
        return f"list({value} ORDER BY {order})"

    def sha256(self, s):
        return f"sha256({s})"

    def explode(self, arr):
        return f"unnest({arr})"

    def posexplode(self, arr, pos, val):
        return f"unnest({arr}) AS {val}, generate_subscripts({arr}, 1) AS {pos}"


DIALECTS = {"spark": SparkDialect, "duckdb": DuckDBDialect}


# --------------------------------------------------------------------------- builder
@dataclass
class Stage:
    name: str
    sql: str


class SqlBuilder:
    """Renders all stages for one config + dialect. ``p`` prefixes every table name."""

    def __init__(self, cfg: MatchConfig, dialect: Dialect, prefix: str = "al_"):
        self.cfg = cfg
        self.d = dialect
        self.p = prefix

    def t(self, name: str) -> str:
        return f"{self.p}{name}"

    # ------------------------------------------------------------ expressions
    def numcore(self, x: str) -> str:
        d = self.d
        digits = d.re_replace(d.re_replace(x, "[^0-9]", ""), "^0+", "")
        return f"(CASE WHEN {digits} = '' THEN '0' ELSE {digits} END)"

    def occ(self, arr: str) -> str:
        """Occurrence-index an array (E, E, L -> E#1, E#2, L#1) so set ops behave as multiset ops."""
        d = self.d
        cnt = d.size(d.filter(d.slice1(arr, "1", "i"), "y", "y = " + d.at(arr, "i")))
        return d.transform(d.seq1(d.size(arr)), "i", f"concat({d.at(arr, 'i')}, '#', CAST({cnt} AS {d.str_type}))")

    def _syn_case(self, x: str) -> str:
        whens = " ".join(
            f"WHEN {self.d.lit(k)} THEN {self.d.lit(v)}" for k, v in sorted(rules.SYNONYMS.items()) if k != v
        )
        return f"CASE {x} {whens} ELSE {x} END"

    def light_words(self, expr: str) -> str:
        """Fold/upper/strip/split (+ synonyms) for short structured fields such as street names."""
        d, n = self.d, self.cfg.normalise
        x = expr
        if n.ascii_fold:
            x = f"translate({x}, {d.lit(rules.ACCENTS_FROM)}, {d.lit(rules.ACCENTS_TO)})"
        basic = "trim(" + d.re_replace("upper(coalesce(" + x + ", ''))", "[^A-Z0-9]+", " ") + ")"
        w = d.filter(d.split_space(basic), "lw_", "lw_ <> ''")
        if n.synonyms:
            w = d.transform(w, "lw_", self._syn_case("lw_"))
        return w

    def _normalise_ctes(self, src: str, keys: str, classify: bool) -> str:
        """CTE chain producing keys, address, basic, words, norm, input_class."""
        d, n = self.d, self.cfg.normalise
        addr = "address"
        if n.ascii_fold:
            addr = f"translate(address, {d.lit(rules.ACCENTS_FROM)}, {d.lit(rules.ACCENTS_TO)})"
        upper_addr = "upper(coalesce(" + addr + ", " + d.lit("") + "))"
        basic = "trim(" + d.re_replace(upper_addr, "[^A-Z0-9]+", " ") + ")"

        cls = "CASE WHEN basic = '' THEN 'EMPTY'"
        if classify and n.classify:
            cls += f" WHEN {d.rlike('basic', rules.NON_PHYSICAL_RE)} THEN 'NON_PHYSICAL'"
            cls += f" WHEN {d.rlike('basic', rules.FOREIGN_RE)} THEN 'OUT_OF_SCOPE'"
        cls += " ELSE 'PHYSICAL' END"

        s1 = "concat(' ', basic, ' ')"
        phrases: list[tuple[str, str]] = []
        if n.drop_noise_tokens:
            phrases += [(p, "") for p in rules.NOISE_PHRASES]
        if n.synonyms:
            phrases += list(rules.STATE_PHRASES.items())
        for src_p, dst_p in phrases:
            rep = f" {dst_p} " if dst_p else " "
            for _ in range(2):  # twice: global replace consumes the shared boundary space
                s1 = f"replace({s1}, {d.lit(' ' + src_p + ' ')}, {d.lit(rep)})"
        s1 = f"trim({d.re_replace(s1, ' +', ' ')})"

        w1 = "w0"
        if n.split_glued_prefixes:
            glued = d.transform(
                "w0", "x",
                f"CASE WHEN {d.rlike('x', rules.GLUED_PREFIX_RE)} "
                f"THEN {d.re_replace('x', rules.GLUED_PREFIX_SPLIT, chr(92) + '1 ' + chr(92) + '2')} "
                f"WHEN {d.rlike('x', rules.GLUED_NUMBER_RE)} "
                f"THEN {d.re_replace('x', rules.GLUED_NUMBER_SPLIT, chr(92) + '1 ' + chr(92) + '2')} ELSE x END",
            )
            w1 = d.split_space(d.join(glued, " "))
        w2 = "w1"
        if n.synonyms:
            w2 = d.transform("w1", "x", self._syn_case("x"))
        w3 = "w2"
        if n.drop_noise_tokens:
            keep = d.seq1(d.size("w2"))
            cond = f"NOT (i = {d.size('w2')} AND {d.in_list(d.at('w2', 'i'), rules.TRAILING_NOISE)})"
            w3 = d.transform(d.filter(keep, "i", cond), "i", d.at("w2", "i"))
        w4 = "w3"
        if n.collapse_duplicate_tokens:
            keep = d.seq1(d.size("w3"))
            # only alphabetic repeats (QLD QLD); repeated numbers are meaningful (68/68 = unit 68 at no. 68)
            cond = (f"i = 1 OR {d.at('w3', 'i')} <> {d.at('w3', 'greatest(i - 1, 1)')} "
                    f"OR {d.rlike(d.at('w3', 'i'), rules.HAS_DIGIT_RE)}")
            w4 = d.transform(d.filter(keep, "i", cond), "i", d.at("w3", "i"))

        return f"""
WITH n0 AS (SELECT {keys}, address, {basic} AS basic FROM {src}),
n1 AS (SELECT n0.*, {cls} AS cls0, {s1} AS s1 FROM n0),
n2 AS (SELECT n1.*, {d.split_space('s1')} AS w0 FROM n1),
n3 AS (SELECT n2.*, {w1} AS w1 FROM n2),
n4 AS (SELECT n3.*, {w2} AS w2 FROM n3),
n5 AS (SELECT n4.*, {w3} AS w3 FROM n4),
n6 AS (SELECT n5.*, {w4} AS words FROM n5)
"""

    def _tok_select(self, src: str, id_col: str, extra: str = "") -> str:
        """Token arrays from a relation exposing id, norm, words."""
        d, b = self.d, self.cfg.blocking
        w = "words"

        def pair(a: str, c: str) -> str:
            if b.bigram_mode == "unordered":
                return f"CASE WHEN {a} <= {c} THEN concat({a}, ' ', {c}) ELSE concat({c}, ' ', {a}) END"
            return f"concat({a}, ' ', {c})"

        bigrams = d.transform(d.seq1(f"{d.size(w)} - 1"), "i", pair(d.at(w, "i"), d.at(w, "i + 1")))
        trigrams = d.transform(
            d.seq1(f"{d.size(w)} - 2"), "i",
            f"concat({d.at(w, 'i')}, ' ', {d.at(w, 'i + 1')}, ' ', {d.at(w, 'i + 2')})",
        )
        blk2 = d.concat_arrays(bigrams, trigrams) if b.fallback.enabled else d.empty_str_array()
        chars = self.occ("cs")
        nums0 = d.transform(d.filter(w, "x", d.rlike("x", rules.HAS_DIGIT_RE)), "x", self.numcore("x"))
        pc_cond = (
            f"{d.rlike(d.at(w, 'i'), rules.POSTCODE_TOKEN_RE)} AND (i = {d.size(w)} OR "
            f"(i > 1 AND {d.in_list(d.at(w, 'greatest(i - 1, 1)'), rules.STATES)}))"
        )
        pc_tok = d.last(d.transform(d.filter(d.seq1(d.size(w)), "i", pc_cond), "i", d.at(w, "i")))
        state = d.last(d.filter(w, "x", d.in_list("x", rules.STATES)))
        return f"""
SELECT {id_col} AS id, norm, words{extra},
  {bigrams} AS blk,
  {blk2} AS blk2,
  {chars} AS chars,
  {self.occ('nums0')} AS nums,
  {self.numcore(pc_tok)} AS pc,
  {state} AS state,
  {d.sort_distinct(w)} AS wset
FROM (SELECT s.*, {d.split_chars("replace(norm, ' ', '')")} AS cs, {nums0} AS nums0 FROM {src} s) z
"""

    # ------------------------------------------------------------ reference
    def reference_stages(self) -> list[Stage]:
        d, t = self.d, self.t
        ctes = self._normalise_ctes(
            t("r_src"),
            "r_id, entity_id, premise_numbers, premise_numbers_optional, postcode, state AS state_in, street_name",
            classify=False,
        )
        norm_sql = ctes + f"""
SELECT r_id, entity_id, address, premise_numbers, premise_numbers_optional, postcode, state_in, street_name,
       {d.join('words', ' ')} AS norm, words
FROM n6"""
        clean = lambda col: d.transform(  # noqa: E731
            d.filter(col, "x", f"x IS NOT NULL AND trim(x) <> ''"), "x", self.numcore("upper(x)")
        )
        extra = f""", entity_id, address,
  CASE WHEN premise_numbers IS NULL THEN {d.null_str_array()} ELSE {clean('premise_numbers')} END AS prem_req,
  {clean(f'coalesce(premise_numbers_optional, {d.empty_str_array()})')} AS prem_opt,
  {self.numcore("postcode")} AS pc_in,
  upper(trim(state_in)) AS state_in,
  {self.light_words('street_name')} AS sname"""
        tok_sql = self._tok_select(f"(SELECT * FROM {t('r_norm')} WHERE norm <> '')", "r_id", extra)
        tok_final = f"""
SELECT id, norm, words, entity_id, address, blk, blk2, chars, nums, wset, prem_req, prem_opt, sname,
       CASE WHEN prem_req IS NULL THEN {d.null_str_array()} ELSE {self.occ('prem_req')} END AS prem_req_idx,
       CASE WHEN prem_req IS NULL THEN {d.null_str_array()}
            ELSE {self.occ(d.concat_arrays('prem_req', 'prem_opt'))} END AS prem_all_idx,
       coalesce(pc_in, pc) AS pc, coalesce(state_in, state) AS state
FROM {t('r_tok0')}"""
        return [
            Stage(t("r_norm"), norm_sql),
            Stage(t("r_tok0"), tok_sql),
            Stage(t("r_tok"), tok_final),
            Stage(t("r_post"), f"SELECT DISTINCT id, token FROM (SELECT id, {d.explode('blk')} AS token FROM {t('r_tok')}) z"),
            Stage(t("r_post2"), f"SELECT DISTINCT id, token FROM (SELECT id, {d.explode('blk2')} AS token FROM {t('r_tok')}) z"),
        ]

    def reference_stats_stages(self, n_ref: int) -> list[Stage]:
        d, t = self.d, self.t
        n = d.dbl(str(n_ref))
        unseen = f"round(ln({n}), 6)"
        return [
            Stage(t("r_df"), f"SELECT token, count(*) AS df, round(ln({n} / count(*)), 6) AS idf FROM {t('r_post')} GROUP BY token"),
            Stage(t("r_df2"), f"SELECT token, count(*) AS df FROM {t('r_post2')} GROUP BY token"),
            Stage(
                t("r_wdf"),
                f"SELECT word, count(*) AS df, round(ln({n} / count(*)), 6) AS idf "
                f"FROM (SELECT id, {d.explode('wset')} AS word FROM {t('r_tok')}) z GROUP BY word",
            ),
            Stage(t("r_wv0"), self._wvec_sql(t("r_tok"), unseen)),
            Stage(t("r_wv"), f"SELECT id, widf, sqrt({d.sum_doubles(d.transform('widf', 'x', 'x * x'))}) AS wnorm FROM {t('r_wv0')}"),
        ]

    def _wvec_sql(self, tok: str, unseen: str) -> str:
        d, t = self.d, self.t
        return f"""
SELECT e.id, {d.ordered_collect(f'coalesce(f.idf, {unseen})', 'e.pos')} AS widf
FROM (SELECT id, {d.posexplode('wset', 'pos', 'word')} FROM {tok}) e
LEFT JOIN {t('r_wdf')} f ON f.word = e.word
GROUP BY e.id"""

    # ------------------------------------------------------------ queries
    def query_stages(self, n_ref: int) -> list[Stage]:
        d, t, c = self.d, self.t, self.cfg
        b = c.blocking
        n = d.dbl(str(n_ref))
        unseen = f"round(ln({n}), 6)"
        ctes = self._normalise_ctes(t("q_src"), "id", classify=True)
        norm_expr = d.join("words", " ")
        q_norm = ctes + f"""
SELECT id, address, basic, {norm_expr} AS norm,
       CASE WHEN {norm_expr} = '' THEN 'EMPTY' ELSE cls0 END AS input_class,
       {d.sha256(norm_expr)} AS norm_key
FROM n6"""
        q_base = f"""
SELECT {d.sha256('norm')} AS qid, norm, {d.split_space('norm')} AS words
FROM (SELECT DISTINCT norm FROM {t('q_norm')} WHERE input_class = 'PHYSICAL') z"""
        stages = [
            Stage(t("q_norm"), q_norm),
            Stage(t("q_base"), q_base),
            Stage(t("q_tok"), self._tok_select(t("q_base"), "qid")),
            Stage(t("q_post"), f"SELECT DISTINCT id, token FROM (SELECT id, {d.explode('blk')} AS token FROM {t('q_tok')}) z"),
            Stage(t("q_df"), f"SELECT token, count(*) AS df FROM {t('q_post')} GROUP BY token"),
        ]
        if b.rule == "both_sides":
            keep = f"qd.df <= {b.k} AND rd.df <= {b.k}"
        else:
            keep = f"rd.df <= {b.k_ref} AND CAST(qd.df AS BIGINT) * rd.df <= {b.p_max}"
        cand1 = f"""
SELECT qp.id AS q_id, rp.id AS r_id, count(*) AS shared_blocks, round(sum(rd.idf), 9) AS idf_overlap, 1 AS blocking_pass
FROM {t('q_post')} qp
JOIN {t('q_df')} qd ON qd.token = qp.token
JOIN {t('r_df')} rd ON rd.token = qp.token
JOIN {t('r_post')} rp ON rp.token = qp.token
WHERE {keep}
GROUP BY qp.id, rp.id"""
        stages.append(Stage(t("cand1"), cand1))
        cand_union = f"SELECT * FROM {t('cand1')}"
        if b.fallback.enabled:
            fb_tok = f"""
SELECT q_id, token, idf FROM (
  SELECT p.q_id, p.token, round(ln({n} / d2.df), 6) AS idf,
         row_number() OVER (PARTITION BY p.q_id ORDER BY d2.df ASC, p.token ASC) AS rn
  FROM (SELECT DISTINCT q_id, token FROM (
          SELECT q.id AS q_id, {d.explode('q.blk2')} AS token FROM {t('q_tok')} q
          WHERE NOT EXISTS (SELECT 1 FROM {t('cand1')} c WHERE c.q_id = q.id)) z) p
  JOIN {t('r_df2')} d2 ON d2.token = p.token
  WHERE d2.df <= {b.fallback.k_max}
) z WHERE rn <= {b.fallback.m}"""
            cand2 = f"""
SELECT f.q_id, rp.id AS r_id, count(*) AS shared_blocks, round(sum(f.idf), 9) AS idf_overlap, 2 AS blocking_pass
FROM {t('q_fb_tok')} f JOIN {t('r_post2')} rp ON rp.token = f.token
GROUP BY f.q_id, rp.id"""
            stages += [Stage(t("q_fb_tok"), fb_tok), Stage(t("cand2"), cand2)]
            cand_union += f" UNION ALL SELECT * FROM {t('cand2')}"
        cap = b.max_candidates
        cap_filter = f"WHERE crn <= {cap}" if cap else ""
        stages.append(Stage(t("cand"), f"""
SELECT * FROM (
  SELECT u.*, row_number() OVER (PARTITION BY q_id ORDER BY idf_overlap DESC, shared_blocks DESC, r_id ASC) AS crn,
         count(*) OVER (PARTITION BY q_id) AS n_candidates
  FROM ({cand_union}) u
) z {cap_filter}"""))
        stages.append(Stage(t("q_wv0"), self._wvec_sql(t("q_tok"), unseen)))
        stages.append(Stage(t("q_wv"), f"SELECT id, widf, sqrt({d.sum_doubles(d.transform('widf', 'x', 'x * x'))}) AS wnorm FROM {t('q_wv0')}"))
        stages += self._score_stages()
        stages += self._decision_stages()
        return stages

    # ------------------------------------------------------------ scoring
    def _score_stages(self) -> list[Stage]:
        d, t, c = self.d, self.t, self.cfg
        flat = f"""
SELECT c.q_id, c.r_id, c.shared_blocks, c.idf_overlap, c.blocking_pass, c.n_candidates,
       r.entity_id, r.address AS ref_address,
       q.norm AS q_norm, r.norm AS r_norm,
       {d.size(d.intersect('q.chars', 'r.chars'))} AS inter, {d.size('q.chars')} AS qa, {d.size('r.chars')} AS rb,
       q.nums AS q_nums, r.nums AS r_nums, q.pc AS q_pc, r.pc AS r_pc, q.state AS q_state, r.state AS r_state,
       r.prem_req_idx, r.prem_all_idx, r.sname AS r_sname, q.words AS q_words,
       q.wset AS q_wset, r.wset AS r_wset, qv.widf AS q_widf, qv.wnorm AS q_wnorm, rv.wnorm AS r_wnorm
FROM {t('cand')} c
JOIN {t('q_tok')} q ON q.id = c.q_id
JOIN {t('r_tok')} r ON r.id = c.r_id
JOIN {t('q_wv')} qv ON qv.id = c.q_id
JOIN {t('r_wv')} rv ON rv.id = c.r_id"""

        dice = f"{d.dbl('2')} * inter / (qa + rb)"
        jacc = f"{d.dbl('inter')} / (qa + rb - inter)"
        score = dice if c.scoring.primary == "char_dice" else jacc

        num = d.sum_doubles(d.transform(
            d.seq1(d.size("q_wset")), "i",
            f"CASE WHEN {d.contains('r_wset', d.at('q_wset', 'i'))} "
            f"THEN {d.at('q_widf', 'i')} * {d.at('q_widf', 'i')} ELSE {d.dbl('0')} END",
        ))
        cosine = f"CASE WHEN q_wnorm * r_wnorm > 0 THEN round(({num}) / (q_wnorm * r_wnorm), 9) ELSE {d.dbl('0')} END"

        core = "split_part(x, '#', 1)"
        excl = f"{core} <> coalesce(q_pc, '#') AND {core} <> coalesce(r_pc, '#')"
        if c.scoring.numeric_exclude_postcodes:
            na = d.filter("q_nums", "x", excl)
            nb = d.filter("r_nums", "x", excl)
        else:
            na, nb = "q_nums", "r_nums"
        subset = (
            f"CASE WHEN {d.except_size(na, nb)} = 0 OR {d.except_size(nb, na)} = 0 "
            f"THEN 'CONSISTENT' ELSE 'CONFLICT' END"
        )
        if c.scoring.numeric_rule == "structured" and c.mode == "reference":
            nq = d.filter("q_nums", "x", excl)
            missing = d.except_size("prem_req_idx", nq)
            extra = d.except_size(nq, "prem_all_idx")
            numeric = f"""CASE WHEN prem_req_idx IS NULL THEN {subset}
  WHEN {missing} = 0 AND {extra} = 0 THEN 'EXACT'
  WHEN {extra} = 0 THEN 'QUERY_LESS_SPECIFIC'
  WHEN {missing} = 0 THEN 'QUERY_MORE_SPECIFIC'
  ELSE 'CONFLICT' END"""
        else:
            numeric = subset

        tol = "CASE WHEN length(sw_) <= 3 THEN 0 WHEN length(sw_) <= 5 THEN 1 ELSE 2 END"
        fuzzy_missing = d.size(d.filter(
            "r_sname", "sw_", d.size(d.filter("q_words", "qw_", f"levenshtein(qw_, sw_) <= {tol}")) + " = 0"))
        exact_missing = d.size(d.filter("r_sname", "sw_", "NOT " + d.contains("q_words", "sw_")))
        street = (
            f"CASE WHEN r_sname IS NULL OR {d.size('r_sname')} = 0 THEN 'ABSENT' "
            f"WHEN {exact_missing} = 0 THEN 'MATCH' WHEN {fuzzy_missing} = 0 THEN 'FUZZY' ELSE 'MISMATCH' END"
        )

        def status(a: str, b_: str) -> str:
            return f"CASE WHEN {a} IS NULL OR {b_} IS NULL THEN 'ABSENT' WHEN {a} = {b_} THEN 'MATCH' ELSE 'MISMATCH' END"

        scored = f"""
SELECT q_id, r_id, entity_id, ref_address, shared_blocks, idf_overlap, blocking_pass, n_candidates,
       {score} AS score,
       {dice} AS dice,
       1 - {d.dbl(f'levenshtein(q_norm, r_norm)')} / greatest(length(q_norm), length(r_norm)) AS lev_ratio,
       {cosine} AS word_idf_cosine,
       {numeric} AS numeric_status,
       {status('q_pc', 'r_pc')} AS postcode_status,
       {status('q_state', 'r_state')} AS state_status,
       {street} AS street_status
FROM {t('scored0')}"""
        ranked_num = (
            "CASE numeric_status WHEN 'EXACT' THEN 0 WHEN 'CONSISTENT' THEN 0 "
            "WHEN 'CONFLICT' THEN 2 ELSE 1 END"
        )
        return [
            Stage(t("scored0"), flat),
            Stage(t("scored1"), scored),
            Stage(t("scored"), f"SELECT s.*, {ranked_num} AS numeric_rank FROM {t('scored1')} s"),
        ]

    # ------------------------------------------------------------ ranking + decision
    def rank_keys(self) -> str:
        if self.cfg.preset == "paper" or self.cfg.decision.method == "threshold":
            return "score DESC, entity_id ASC, r_id ASC"
        return (
            "numeric_rank ASC, score DESC, shared_blocks DESC, lev_ratio DESC, "
            "word_idf_cosine DESC, entity_id ASC, r_id ASC"
        )

    def _decision_stages(self) -> list[Stage]:
        d, t, c = self.d, self.t, self.cfg
        keys = self.rank_keys()
        ranked = f"""
SELECT * FROM (
  SELECT v.*, row_number() OVER (PARTITION BY q_id ORDER BY {keys}) AS rnk
  FROM (SELECT s.*, row_number() OVER (PARTITION BY q_id, entity_id ORDER BY {keys}) AS vrn FROM {t('scored')} s) v
  WHERE vrn = 1
) z WHERE rnk <= {c.keep_ranked}"""
        ranked2 = f"SELECT r.*, row_number() OVER (PARTITION BY q_id, numeric_rank ORDER BY rnk) AS crk FROM {t('ranked')} r"
        thr = c.decision.method == "threshold"
        if thr and c.mode == "datasets":
            sel_cond = f"numeric_status = 'CONSISTENT' AND rnk <= {c.decision.top_n_numeric}"
        else:
            sel_cond = "rnk = 1"
        sel = f"SELECT q_id, min(rnk) AS sel_rnk FROM {t('ranked2')} WHERE {sel_cond} GROUP BY q_id"

        dc = c.decision
        if thr:
            if c.mode == "datasets":
                decision = "'AUTO_MATCH'"
            else:
                decision = f"CASE WHEN s.score > {d.dbl(repr(dc.tau))} THEN 'AUTO_MATCH' ELSE 'NO_MATCH' END"
        else:
            decision = f"""CASE
  WHEN s.score >= {d.dbl(repr(dc.tau_auto))} AND s.numeric_rank = 0
       AND s.postcode_status <> 'MISMATCH' AND s.state_status <> 'MISMATCH' AND s.street_status <> 'MISMATCH'
       AND (margin >= {d.dbl(repr(dc.min_margin))} OR ordered_support) THEN 'AUTO_MATCH'
  WHEN s.numeric_status = 'QUERY_LESS_SPECIFIC' AND s.score >= {d.dbl(repr(dc.tau_review))} THEN 'PARTIAL_MATCH'
  WHEN s.score >= {d.dbl(repr(dc.tau_review))} AND s.numeric_status <> 'CONFLICT' THEN 'REVIEW'
  ELSE 'NO_MATCH' END"""
        reasons = d.filter(d.array_of([
            "concat('NUM_', s.numeric_status)",
            "CASE WHEN s.postcode_status = 'MISMATCH' THEN 'POSTCODE_MISMATCH' END",
            "CASE WHEN s.state_status = 'MISMATCH' THEN 'STATE_MISMATCH' END",
            "CASE WHEN s.street_status = 'MISMATCH' THEN 'STREET_MISMATCH' END",
            "CASE WHEN s.street_status = 'FUZZY' THEN 'STREET_FUZZY' END",
            f"CASE WHEN margin < {d.dbl(repr(dc.min_margin))} THEN 'LOW_MARGIN' END",
            f"CASE WHEN margin < {d.dbl(repr(dc.min_margin))} AND has_comp AND s.shared_blocks > comp_blocks THEN 'TIE_BROKEN_BY_BLOCKS' END",
            f"CASE WHEN margin < {d.dbl(repr(dc.min_margin))} AND has_comp AND s.shared_blocks <= comp_blocks AND s.lev_ratio > comp_lev THEN 'TIE_BROKEN_BY_LEV' END",
            "CASE WHEN s.blocking_pass = 2 THEN 'FALLBACK_BLOCKING' END",
            f"CASE WHEN s.n_candidates > {c.blocking.max_candidates or 2**31 - 1} THEN 'CAPPED' END",
        ]), "x", "x IS NOT NULL")
        decided0 = f"""
SELECT s.*, x.sel_rnk,
       s.score - coalesce(cp.score, {d.dbl('0')}) AS margin,
       cp.q_id IS NOT NULL AS has_comp,
       cp.shared_blocks AS comp_blocks, cp.lev_ratio AS comp_lev,
       coalesce(cp.q_id IS NOT NULL AND (s.shared_blocks > cp.shared_blocks OR s.lev_ratio > cp.lev_ratio), false) AS ordered_support
FROM {t('ranked2')} s
JOIN {t('sel')} x ON x.q_id = s.q_id AND x.sel_rnk = s.rnk
LEFT JOIN {t('ranked2')} cp ON cp.q_id = s.q_id AND cp.numeric_rank = s.numeric_rank AND cp.crk = s.crk + 1"""
        decided = f"SELECT s.*, {decision} AS decision, {reasons} AS reason_codes FROM {t('decided0')} s"

        result = f"""
SELECT n.id, n.address AS input_address, n.norm, n.norm_key, n.input_class,
  CASE WHEN n.input_class <> 'PHYSICAL' THEN n.input_class
       WHEN dd.decision IS NULL THEN 'NO_MATCH' ELSE dd.decision END AS decision,
  dd.r_id, dd.entity_id, dd.ref_address, dd.score, dd.dice, dd.shared_blocks, dd.idf_overlap,
  dd.lev_ratio, dd.word_idf_cosine, dd.numeric_status, dd.postcode_status, dd.state_status, dd.street_status,
  dd.margin, dd.blocking_pass, coalesce(nc.n_candidates, 0) AS n_candidates,
  CASE WHEN n.input_class <> 'PHYSICAL' THEN {d.empty_str_array()}
       WHEN nc.q_id IS NULL THEN {d.array_of(["'NO_CANDIDATES'"])}
       WHEN dd.q_id IS NULL THEN {d.array_of(["'NO_CONSISTENT_CANDIDATE'"])}
       ELSE dd.reason_codes END AS reason_codes
FROM {t('q_norm')} n
LEFT JOIN {t('decided')} dd ON dd.q_id = n.norm_key AND n.input_class = 'PHYSICAL'
LEFT JOIN (SELECT q_id, max(n_candidates) AS n_candidates FROM {t('cand')} GROUP BY q_id) nc
  ON nc.q_id = n.norm_key AND n.input_class = 'PHYSICAL'"""
        candidates = f"""
SELECT q_id AS norm_key, rnk, r_id, entity_id, ref_address, score, dice, shared_blocks, idf_overlap,
       lev_ratio, word_idf_cosine, numeric_status, postcode_status, state_status, street_status, blocking_pass
FROM {t('ranked2')} WHERE rnk <= {c.output.keep_top_k}"""
        return [
            Stage(t("ranked"), ranked),
            Stage(t("ranked2"), ranked2),
            Stage(t("sel"), sel),
            Stage(t("decided0"), decided0),
            Stage(t("decided"), decided),
            Stage(t("match_result"), result),
            Stage(t("match_candidates"), candidates),
        ]
