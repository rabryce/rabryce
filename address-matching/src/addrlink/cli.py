"""Command-line interface: ``addrlink synth | gnaf | match | dedupe | benchmark | explain | sql``."""

from __future__ import annotations

import argparse
import json
import sys
import time

from .config import MatchConfig


def _config(args: argparse.Namespace) -> MatchConfig:
    cfg = MatchConfig.from_preset(args.preset)
    if getattr(args, "config", None):
        import json as _json

        with open(args.config) as f:
            cfg = cfg.override(**_json.load(f))
    if getattr(args, "set", None):
        cfg = cfg.with_dotted(args.set)
    return cfg


def _spark_kw(args: argparse.Namespace) -> dict:
    if args.engine != "spark":
        return {}
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return {"spark": spark}


def cmd_synth(args: argparse.Namespace) -> None:
    from .synth import SynthConfig, generate, write

    ref, qs = generate(SynthConfig(target_addresses=args.n_ref, n_queries=args.n_queries,
                                   n_localities=max(20, args.n_ref // 100), seed=args.seed))
    r, q = write(ref, qs, args.out)
    print(f"wrote {len(ref)} reference rows -> {r}\nwrote {len(qs)} queries -> {q}")


def cmd_match(args: argparse.Namespace) -> None:
    from .api import Matcher

    m = Matcher(args.engine, _config(args), **_spark_kw(args))
    n = m.prepare_reference(args.reference)
    res = m.link(args.queries, id_col=args.id_col, address_cols=args.address_cols)
    res.to_parquet(args.out)
    print(json.dumps({"n_ref": n, "decisions": res.decision_counts(), "run_log": res.run_log.as_dict()}, indent=2))


def cmd_benchmark(args: argparse.Namespace) -> None:
    from .api import Matcher
    from .evaluate import evaluate_run, report
    from .synth import SynthConfig, generate

    ref, qs = generate(SynthConfig(target_addresses=args.n_ref, n_queries=args.n_queries,
                                   n_localities=max(20, args.n_ref // 100), seed=args.seed))
    if args.sweep:
        return _sweep(args, ref, qs)
    runs = {}
    for preset in args.presets.split(","):
        cfg = MatchConfig.from_preset(preset)
        if args.set:
            cfg = cfg.with_dotted(args.set)
        m = Matcher(args.engine, cfg, prefix=f"{preset}_", collect_counts=True, **_spark_kw(args))
        t0 = time.perf_counter()
        m.prepare_reference(ref)
        res = m.link(qs)
        runs[preset] = evaluate_run(m, res, qs, ref)
        print(f"{preset}: {time.perf_counter() - t0:.1f}s {res.decision_counts()}", file=sys.stderr)
    md = report(runs, title=f"addrlink benchmark ({len(ref)} reference rows, {len(qs)} queries, "
                            f"engine={args.engine}, seed={args.seed})")
    if args.report:
        with open(args.report, "w") as f:
            f.write(md + "\n")
    print(md)


def _sweep(args: argparse.Namespace, ref: list[dict], qs: list[dict]) -> None:
    """One parameter, several values, first preset: the precision/recall/review trade-off."""
    from .api import Matcher
    from .evaluate import _ci, _pct, evaluate_run

    key, _, vals = args.sweep.partition("=")
    preset = args.presets.split(",")[0]
    lines = [f"# Sweep of `{key}` ({preset}; {len(ref)} reference rows, {len(qs)} queries, seed={args.seed})", "",
             f"| {key} | AUTO precision (95% CI) | AUTO recall | REVIEW+PARTIAL | wrong-unit/AUTO | AUTO on negatives |",
             "|---|---|---|---|---|---|"]
    for v in vals.split(","):
        cfg = MatchConfig.from_preset(preset).with_dotted([*(args.set or []), f"{key}={v}"])
        m = Matcher(args.engine, cfg, prefix=f"sw_", collect_counts=True, **_spark_kw(args))
        m.prepare_reference(ref)
        t = evaluate_run(m, m.link(qs), qs, ref)["total"]
        lines.append(f"| {v} | {_pct(t['precision_auto'])}{_ci(t['precision_auto_ci'])} | {_pct(t['recall_auto'])} | "
                     f"{_pct(t['review_rate'])} | {_pct(t['wrong_unit_rate_auto'])} | "
                     f"{_pct(t['false_match_rate_negatives'])} |")
        print(lines[-1], file=sys.stderr)
    md = "\n".join(lines)
    if args.report:
        with open(args.report, "w") as f:
            f.write(md + "\n")
    print(md)


def cmd_gnaf(args: argparse.Namespace) -> None:
    from .engines import DuckDBEngine
    from .gnaf import authority_synonyms, load_gnaf

    e = DuckDBEngine()
    name = load_gnaf(e, args.root)
    e.to_parquet(name, args.out)
    syn = authority_synonyms(e)
    if args.synonyms_out:
        with open(args.synonyms_out, "w") as f:
            json.dump({"normalise": {"extra_synonyms": syn}}, f, indent=2, sort_keys=True)
    print(json.dumps({"reference_rows": e.count(name), "out": args.out, "extra_synonyms": len(syn)}))


def cmd_dedupe(args: argparse.Namespace) -> None:
    from .api import Matcher

    m = Matcher(args.engine, _config(args), **_spark_kw(args))
    res = m.dedupe(args.input, id_col=args.id_col, address_cols=args.address_cols)
    m.engine.to_parquet(res.clusters_table, args.out)  # type: ignore[attr-defined]
    print(json.dumps(res.summary()))


def cmd_explain(args: argparse.Namespace) -> None:
    from .api import Matcher

    m = Matcher("duckdb", _config(args))
    m.prepare_reference(args.reference)
    res = m.link([{"id": "q", "address": args.address}])
    row = res.rows()[0]
    print(json.dumps({k: row[k] for k in row if k != "input_address"}, indent=2, default=str))
    for c in res.candidates():
        print(f"  #{c['rnk']} {c['score']:.3f} {c['numeric_status']:<20} {c['street_status']:<8} {c['ref_address']}")


def cmd_sql(args: argparse.Namespace) -> None:
    from .sql import DIALECTS, SqlBuilder

    b = SqlBuilder(_config(args), DIALECTS[args.engine]())
    stages = [*b.reference_stages(), *b.reference_stats_stages(1_000_000), *b.query_stages(1_000_000)]
    for s in stages:
        print(f"-- {s.name}\n{s.sql.strip()};\n")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="addrlink", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp: argparse.ArgumentParser, engine: bool = True) -> None:
        sp.add_argument("--preset", default="enhanced", choices=["paper", "enhanced"])
        sp.add_argument("--config", help="JSON file of config overrides")
        sp.add_argument("--set", action="append", help="dotted override, e.g. decision.tau_auto=0.82")
        if engine:
            sp.add_argument("--engine", default="duckdb", choices=["duckdb", "spark"])

    sp = sub.add_parser("synth", help="generate a synthetic G-NAF-like reference and corrupted queries")
    sp.add_argument("--out", required=True)
    sp.add_argument("--n-ref", type=int, default=60_000)
    sp.add_argument("--n-queries", type=int, default=6_000)
    sp.add_argument("--seed", type=int, default=7)
    sp.set_defaults(fn=cmd_synth)

    sp = sub.add_parser("match", help="match a query file against a reference file (Parquet)")
    common(sp)
    sp.add_argument("--reference", required=True)
    sp.add_argument("--queries", required=True)
    sp.add_argument("--out", required=True)
    sp.add_argument("--id-col", default="id")
    sp.add_argument("--address-cols", nargs="+", default=["address"])
    sp.set_defaults(fn=cmd_match)

    sp = sub.add_parser("benchmark", help="synthetic benchmark comparing presets")
    sp.add_argument("--engine", default="duckdb", choices=["duckdb", "spark"])
    sp.add_argument("--presets", default="paper,enhanced")
    sp.add_argument("--set", action="append")
    sp.add_argument("--n-ref", type=int, default=60_000)
    sp.add_argument("--n-queries", type=int, default=6_000)
    sp.add_argument("--seed", type=int, default=7)
    sp.add_argument("--report", help="write the markdown report here")
    sp.add_argument("--sweep", help="KEY=v1,v2,... e.g. decision.tau_auto=0.75,0.8,0.85 (uses first preset)")
    sp.set_defaults(fn=cmd_benchmark)

    sp = sub.add_parser("gnaf", help="flatten a G-NAF PSV release into a reference Parquet file")
    sp.add_argument("--root", required=True, help="directory containing the extracted G-NAF release")
    sp.add_argument("--out", required=True)
    sp.add_argument("--synonyms-out", help="write authority-table synonyms as a --config JSON file")
    sp.set_defaults(fn=cmd_gnaf)

    sp = sub.add_parser("dedupe", help="cluster duplicate addresses within one file (UC3)")
    common(sp)
    sp.add_argument("--input", required=True)
    sp.add_argument("--out", required=True)
    sp.add_argument("--id-col", default="id")
    sp.add_argument("--address-cols", nargs="+", default=["address"])
    sp.set_defaults(fn=cmd_dedupe)

    sp = sub.add_parser("explain", help="match one address and show the candidates")
    common(sp, engine=False)
    sp.add_argument("--reference", required=True)
    sp.add_argument("address")
    sp.set_defaults(fn=cmd_explain)

    sp = sub.add_parser("sql", help="print the generated SQL for a dialect")
    common(sp)
    sp.set_defaults(fn=cmd_sql)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
