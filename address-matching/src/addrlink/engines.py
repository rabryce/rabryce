"""Execution engines: DuckDB (local/CI) and Spark (Databricks). SPEC.md §5.3, §9."""

from __future__ import annotations

import os
from typing import Any, Iterable

from .sql import Dialect, DuckDBDialect, SparkDialect

QUERY_SCHEMA = [("id", "STRING"), ("address", "STRING")]
REF_SCHEMA = [
    ("r_id", "STRING"),
    ("entity_id", "STRING"),
    ("address", "STRING"),
    ("premise_numbers", "ARRAY<STRING>"),
    ("premise_numbers_optional", "ARRAY<STRING>"),
    ("postcode", "STRING"),
    ("state", "STRING"),
    ("street_name", "STRING"),
]


class Engine:
    dialect: Dialect
    name = "base"

    def materialise(self, name: str, sql: str) -> None: ...
    def scalar(self, sql: str) -> Any: ...
    def fetch(self, sql: str) -> list[dict]: ...
    def columns(self, relation: str) -> list[str]: ...
    def source_relation(self, data: Any, name: str, schema: list[tuple[str, str]]) -> str: ...
    def count(self, name: str) -> int:
        return int(self.scalar(f"SELECT count(*) FROM {name}"))


# ------------------------------------------------------------------------- DuckDB
class DuckDBEngine(Engine):
    name = "duckdb"

    def __init__(self, con: Any = None, threads: int | None = None):
        import duckdb

        self.con = con if con is not None else duckdb.connect()
        if threads:
            self.con.execute(f"SET threads = {int(threads)}")
        self.dialect = DuckDBDialect()

    def materialise(self, name: str, sql: str) -> None:
        self.con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")

    def scalar(self, sql: str) -> Any:
        return self.con.execute(sql).fetchone()[0]

    def fetch(self, sql: str) -> list[dict]:
        cur = self.con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def columns(self, relation: str) -> list[str]:
        return [r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()]

    def source_relation(self, data: Any, name: str, schema: list[tuple[str, str]]) -> str:
        if isinstance(data, str):
            if data.endswith(".parquet") or os.path.isdir(data):
                path = data if data.endswith(".parquet") else os.path.join(data, "*.parquet")
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
                return name
            return data  # an existing table / view name
        if hasattr(data, "create_view"):  # DuckDB relation
            data.create_view(name, replace=True)
            return name
        rows = list(data)
        cols = list(rows[0].keys()) if rows else [c for c, _ in schema]
        types = dict(schema)
        ddl = ", ".join(f'"{c}" {_duck_type(types.get(c, "STRING"))}' for c in cols)
        if not rows:
            self.con.execute(f"CREATE OR REPLACE TABLE {name} ({ddl})")
            return name
        # Bulk-load via NDJSON: orders of magnitude faster than executemany for list columns.
        import json
        import tempfile

        spec = ", ".join(f"'{c}': '{_duck_type(types.get(c, 'STRING'))}'" for c in cols)
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps({c: r.get(c) for c in cols}, default=str) + "\n")
            path = f.name
        try:
            self.con.execute(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM "
                f"read_json('{path}', format='newline_delimited', columns={{{spec}}})"
            )
        finally:
            os.remove(path)
        return name

    def to_parquet(self, name: str, path: str) -> None:
        self.con.execute(f"COPY (SELECT * FROM {name}) TO '{path}' (FORMAT PARQUET)")


def _duck_type(t: str) -> str:
    return {"STRING": "VARCHAR", "ARRAY<STRING>": "VARCHAR[]"}.get(t, t)


# ------------------------------------------------------------------------- Spark
class SparkEngine(Engine):
    """How each stage is materialised. Every option except ``view`` cuts the logical-plan
    lineage between stages; without that, Spark re-analyses the whole normalisation
    plan inside every later stage and the analyser runs out of memory.

    * ``checkpoint`` (default): ``DataFrame.localCheckpoint()`` + temp view. Fast and local.
    * ``table:<catalog.schema>``: ``CREATE OR REPLACE TABLE`` (Delta on Databricks). Use on
      shared / serverless compute and for anything large or audited.
    * ``view``: lazy temp views, only for tiny debugging runs.
    """

    name = "spark"

    def __init__(self, spark: Any, storage: str = "checkpoint"):
        if not (storage in ("checkpoint", "view") or storage.startswith("table:")):
            raise ValueError("storage must be 'checkpoint', 'view' or 'table:<catalog.schema>'")
        self.spark = spark
        self.storage = storage
        self.dialect = SparkDialect()

    def _qualified(self, name: str) -> str:
        if self.storage.startswith("table:"):
            return f"{self.storage.split(':', 1)[1]}.{name}"
        return name

    def materialise(self, name: str, sql: str) -> None:
        if self.storage.startswith("table:"):
            # DROP + CREATE (not CREATE OR REPLACE) so it also works for non-Delta v1 tables.
            self.spark.sql(f"DROP TABLE IF EXISTS {self._qualified(name)}")
            self.spark.sql(f"CREATE TABLE {self._qualified(name)} AS {sql}")
            self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM {self._qualified(name)}")
            return
        if self.storage == "view":
            self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {sql}")
            return
        self.spark.sql(sql).localCheckpoint(eager=True).createOrReplaceTempView(name)

    def scalar(self, sql: str) -> Any:
        return self.spark.sql(sql).collect()[0][0]

    def fetch(self, sql: str) -> list[dict]:
        return [r.asDict(recursive=True) for r in self.spark.sql(sql).collect()]

    def columns(self, relation: str) -> list[str]:
        return self.spark.table(relation).columns

    def source_relation(self, data: Any, name: str, schema: list[tuple[str, str]]) -> str:
        if isinstance(data, str):
            if data.endswith(".parquet") or data.startswith(("/", "dbfs:", "s3:", "abfss:")):
                self.spark.read.parquet(data).createOrReplaceTempView(name)
                return name
            return data
        if hasattr(data, "createOrReplaceTempView"):
            data.createOrReplaceTempView(name)
            return name
        rows = list(data)
        cols = list(rows[0].keys()) if rows else [c for c, _ in schema]
        types = dict(schema)
        ddl = ", ".join(f"{c} {types.get(c, 'STRING')}" for c in cols)
        self.spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], ddl).createOrReplaceTempView(name)
        return name

    def to_parquet(self, name: str, path: str) -> None:
        self.spark.table(name).write.mode("overwrite").parquet(path)


def make_engine(engine: str | Engine, spark: Any = None, con: Any = None, **kw: Any) -> Engine:
    if isinstance(engine, Engine):
        return engine
    if engine == "duckdb":
        return DuckDBEngine(con=con, **kw)
    if engine == "spark":
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        return SparkEngine(spark, **kw)
    raise ValueError(f"unknown engine {engine!r}")


def address_expr(d: Dialect, cols: Iterable[str]) -> str:
    cols = list(cols)
    if len(cols) == 1:
        return f"CAST({cols[0]} AS {d.str_type})"
    return "concat_ws(' ', " + ", ".join(f"CAST({c} AS {d.str_type})" for c in cols) + ")"
