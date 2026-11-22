"""Public API: ``Matcher`` (SPEC.md §5.4)."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Sequence

from . import __version__
from .config import MatchConfig
from .engines import QUERY_SCHEMA, REF_SCHEMA, Engine, address_expr, make_engine
from .sql import SqlBuilder


@dataclass
class RunLog:
    run_id: str
    engine: str
    preset: str
    config_hash: str
    code_version: str
    mode: str
    timings: dict[str, float] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class MatchResult:
    engine: Engine
    result_table: str
    candidates_table: str
    run_log: RunLog
    prefix: str

    def rows(self, order_by: str = "id") -> list[dict]:
        return self.engine.fetch(f"SELECT * FROM {self.result_table} ORDER BY {order_by}")

    def candidates(self) -> list[dict]:
        return self.engine.fetch(f"SELECT * FROM {self.candidates_table} ORDER BY norm_key, rnk")

    def decision_counts(self) -> dict[str, int]:
        rows = self.engine.fetch(
            f"SELECT decision, count(*) AS n FROM {self.result_table} GROUP BY decision ORDER BY decision"
        )
        return {r["decision"]: int(r["n"]) for r in rows}

    def to_parquet(self, path: str) -> None:
        self.engine.to_parquet(self.result_table, path)  # type: ignore[attr-defined]


@dataclass
class DedupeResult:
    engine: Engine
    clusters_table: str
    edges_table: str
    match: MatchResult
    iterations: int

    def clusters(self) -> list[dict]:
        return self.engine.fetch(f"SELECT * FROM {self.clusters_table} ORDER BY id")

    def summary(self) -> dict[str, int]:
        r = self.engine.fetch(
            f"SELECT count(*) AS n_records, count(DISTINCT cluster_id) AS n_clusters, "
            f"sum(CASE WHEN cluster_size > 1 THEN 1 ELSE 0 END) AS n_in_multi FROM {self.clusters_table}"
        )[0]
        return {k: int(v or 0) for k, v in r.items()} | {"iterations": self.iterations}


class Matcher:
    """Two-round, parse-free address matcher.

    >>> m = Matcher("duckdb", MatchConfig.from_preset("enhanced"))
    >>> m.prepare_reference(reference_rows)
    >>> res = m.link(query_rows)
    """

    def __init__(
        self,
        engine: str | Engine = "duckdb",
        config: MatchConfig | None = None,
        *,
        prefix: str = "al_",
        spark: Any = None,
        con: Any = None,
        collect_counts: bool | None = None,
        **engine_kw: Any,
    ):
        self.engine = make_engine(engine, spark=spark, con=con, **engine_kw)
        self.config = config or MatchConfig.from_preset("enhanced")
        self.prefix = prefix
        self.sql = SqlBuilder(self.config, self.engine.dialect, prefix)
        self.collect_counts = self.engine.name == "duckdb" if collect_counts is None else collect_counts
        self.n_ref: int | None = None
        self._ref_timings: dict[str, float] = {}

    # ----------------------------------------------------------- reference
    def prepare_reference(
        self,
        data: Any,
        *,
        id_col: str = "r_id",
        entity_col: str | None = "entity_id",
        address_cols: Sequence[str] = ("address",),
        premise_col: str | None = "premise_numbers",
        premise_opt_col: str | None = "premise_numbers_optional",
        postcode_col: str | None = "postcode",
        state_col: str | None = "state",
        street_col: str | None = "street_name",
    ) -> int:
        d, t = self.engine.dialect, self.sql.t
        raw = self.engine.source_relation(data, t("r_raw"), REF_SCHEMA)
        have = set(self.engine.columns(raw))

        def col(name: str | None, typ: str) -> str:
            if name and name in have:
                return name
            return f"CAST(NULL AS {typ})"

        arr = "ARRAY<STRING>" if d.name == "spark" else "VARCHAR[]"
        entity = col(entity_col, d.str_type) if entity_col and entity_col in have else id_col
        src_sql = f"""
SELECT CAST({id_col} AS {d.str_type}) AS r_id, CAST({entity} AS {d.str_type}) AS entity_id,
       {address_expr(d, address_cols)} AS address,
       {col(premise_col, arr)} AS premise_numbers, {col(premise_opt_col, arr)} AS premise_numbers_optional,
       CAST({col(postcode_col, d.str_type)} AS {d.str_type}) AS postcode,
       CAST({col(state_col, d.str_type)} AS {d.str_type}) AS state,
       CAST({col(street_col, d.str_type)} AS {d.str_type}) AS street_name
FROM {raw}"""
        timings: dict[str, float] = {}
        self._run(t("r_src"), src_sql, timings)
        for st in self.sql.reference_stages():
            self._run(st.name, st.sql, timings)
        self.n_ref = self.engine.count(t("r_tok"))
        if self.n_ref == 0:
            raise ValueError("reference is empty after normalisation")
        for st in self.sql.reference_stats_stages(self.n_ref):
            self._run(st.name, st.sql, timings)
        self._ref_timings = timings
        return self.n_ref

    # ----------------------------------------------------------- matching
    def link(self, data: Any, *, id_col: str = "id", address_cols: Sequence[str] = ("address",)) -> MatchResult:
        if self.n_ref is None:
            raise RuntimeError("call prepare_reference() first")
        d, t = self.engine.dialect, self.sql.t
        log = RunLog(
            run_id=str(uuid.uuid4()),
            engine=self.engine.name,
            preset=self.config.preset,
            config_hash=self.config.config_hash(),
            code_version=__version__,
            mode=self.config.mode,
        )
        log.timings.update({f"ref:{k}": v for k, v in self._ref_timings.items()})
        raw = self.engine.source_relation(data, t("q_raw"), QUERY_SCHEMA)
        self._run(
            t("q_src"),
            f"SELECT CAST({id_col} AS {d.str_type}) AS id, {address_expr(d, address_cols)} AS address FROM {raw}",
            log.timings,
        )
        for st in self.sql.query_stages(self.n_ref):
            self._run(st.name, st.sql, log.timings)
        if self.collect_counts:
            for name in ("q_norm", "q_base", "cand1", "cand", "scored", "match_result"):
                log.counts[name] = self.engine.count(t(name))
            if self.config.blocking.fallback.enabled:
                log.counts["cand2"] = self.engine.count(t("cand2"))
        log.counts["n_ref"] = self.n_ref
        return MatchResult(self.engine, t("match_result"), t("match_candidates"), log, self.prefix)

    def link_datasets(
        self, a: Any, b: Any, *, id_col: str = "id", address_cols: Sequence[str] = ("address",)
    ) -> MatchResult:
        """UC2: link raw dataset ``a`` to raw dataset ``b`` (paper §7.2 numeric-subset rule)."""
        if self.config.mode != "datasets":
            self.config = self.config.override(mode="datasets")
            self.sql = SqlBuilder(self.config, self.engine.dialect, self.prefix)
        self.prepare_reference(b, id_col=id_col, entity_col=None, address_cols=address_cols,
                               premise_col=None, premise_opt_col=None, postcode_col=None, state_col=None,
                               street_col=None)
        return self.link(a, id_col=id_col, address_cols=address_cols)

    def dedupe(
        self, data: Any, *, id_col: str = "id", address_cols: Sequence[str] = ("address",), max_iter: int = 50
    ) -> DedupeResult:
        """UC3 (SPEC §7.10): link a dataset to itself and cluster AUTO_MATCH edges.

        Matching runs on distinct normalised strings (identical strings share a cluster
        trivially), never pairing a string with itself. Each string contributes at most one
        edge (to its selected match); clusters are connected components found by iterative
        min-label propagation in SQL, so the same code runs on Spark.
        """
        if self.config.mode != "dedupe":
            self.config = self.config.override(mode="dedupe")
            self.sql = SqlBuilder(self.config, self.engine.dialect, self.prefix)
        d, t = self.engine.dialect, self.sql.t
        timings: dict[str, float] = {}
        raw = self.engine.source_relation(data, t("q_raw"), QUERY_SCHEMA)
        self._run(t("q_src"), f"SELECT CAST({id_col} AS {d.str_type}) AS id, "
                              f"{address_expr(d, address_cols)} AS address FROM {raw}", timings)
        q_norm = self.sql.query_stages(1)[0]
        self._run(q_norm.name, q_norm.sql, timings)
        self._run(t("dd_ref"), f"SELECT norm_key AS r_id, norm_key AS entity_id, min(norm) AS address "
                               f"FROM {t('q_norm')} WHERE input_class = 'PHYSICAL' GROUP BY norm_key", timings)
        self.prepare_reference(t("dd_ref"), premise_col=None, premise_opt_col=None, postcode_col=None,
                               state_col=None, street_col=None)
        res = self.link(t("q_src"), id_col="id", address_cols=["address"])
        self._run(t("dd_edges"), f"SELECT DISTINCT q_id AS a, entity_id AS b, score, decision "
                                 f"FROM {t('decided')} WHERE decision = 'AUTO_MATCH'", timings)
        self._run(t("dd_adj"), f"SELECT a AS n, b AS m FROM {t('dd_edges')} UNION SELECT b, a FROM {t('dd_edges')}",
                  timings)
        cur = t("dd_lab0")
        self._run(cur, f"SELECT DISTINCT norm_key AS n, norm_key AS label FROM {t('q_norm')} "
                       f"WHERE input_class = 'PHYSICAL'", timings)
        it = 0
        for it in range(1, max_iter + 1):
            nxt = t(f"dd_lab{it % 2 + 1}")
            self._run(nxt, f"""
SELECT l.n, least(l.label, coalesce(min(nb.label), l.label)) AS label
FROM {cur} l LEFT JOIN {t('dd_adj')} e ON e.n = l.n LEFT JOIN {cur} nb ON nb.n = e.m
GROUP BY l.n, l.label""", timings)
            changed = self.engine.scalar(f"SELECT count(*) FROM {nxt} x JOIN {cur} y ON x.n = y.n "
                                         f"WHERE x.label <> y.label")
            cur = nxt
            if int(changed) == 0:
                break
        self._run(t("dd_clusters"), f"""
SELECT id, norm_key, input_class, cluster_id, count(*) OVER (PARTITION BY cluster_id) AS cluster_size
FROM (SELECT n.id, n.norm_key, n.input_class,
             CASE WHEN n.input_class = 'PHYSICAL' THEN l.label ELSE concat('X:', n.id) END AS cluster_id
      FROM {t('q_norm')} n LEFT JOIN {cur} l ON l.n = n.norm_key) z""", timings)
        res.run_log.timings.update({f"dedupe:{k}": v for k, v in timings.items()})
        return DedupeResult(self.engine, t("dd_clusters"), t("dd_edges"), res, it)

    def explain_sql(self) -> str:
        n = self.n_ref or 1000
        parts = [*self.sql.reference_stages(), *self.sql.reference_stats_stages(n), *self.sql.query_stages(n)]
        return "\n\n".join(f"-- {s.name}\n{s.sql.strip()};" for s in parts)

    def _run(self, name: str, sql: str, timings: dict[str, float]) -> None:
        t0 = time.perf_counter()
        try:
            self.engine.materialise(name, sql)
        except Exception as e:  # surface the failing stage with its SQL
            raise RuntimeError(f"stage {name} failed on {self.engine.name}: {e}\n--- SQL ---\n{sql}") from e
        timings[name.removeprefix(self.prefix)] = round(time.perf_counter() - t0, 4)
