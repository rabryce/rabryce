# addrlink — parse-free address matching (prototype)

`addrlink` is a Python package for matching messy addresses to a reference address file such as G-NAF,
or to each other, at Spark scale. It implements AUSTRAC's two-round, inverted-index algorithm
([Zhang, Churchill & Ng, arXiv:1708.01402](https://arxiv.org/abs/1708.01402)) and the hardening
described in [`docs/SPEC.md`](docs/SPEC.md).

- **Two engines, one algorithm:** Spark SQL (Databricks) and DuckDB (laptop/CI) run the same ~25
  generated SQL stages. A pure-Python oracle re-implements everything, and the tests assert that all
  three return identical candidates, scores, ranks and decisions.
- **Two presets:** `paper` reproduces the paper faithfully; `enhanced` adds the SPEC §7 improvements.
- **Explainable decisions:** `AUTO_MATCH` / `REVIEW` / `PARTIAL_MATCH` / `NO_MATCH` / `NON_PHYSICAL` /
  `EMPTY` / `OUT_OF_SCOPE`, each with features and reason codes.
- **Three use cases:**
  - reference linking to G-NAF (UC1);
  - linking two raw datasets (UC2);
  - de-duplication with clustering inside one dataset (UC3).
- **G-NAF loader:** flattens the official PSV release into the reference format, including aliases,
  range variants and authority-table synonyms.

## Results on the synthetic benchmark

These numbers come from 201,701 G-NAF-like reference rows and 6,000 corrupted queries, on DuckDB in a single cloud container.
The full breakdown by segment and corruption type is in [`docs/benchmark-synthetic.md`](docs/benchmark-synthetic.md).

| Metric | `paper` | `enhanced` |
|---|---|---|
| AUTO precision (95% CI) | 87.0% [86.1, 87.9] | **99.7%** [99.5, 99.8] |
| AUTO recall (95% CI) | 80.0% [78.9, 81.0] | **93.4%** [92.7, 94.0] |
| Top-1 accuracy (any decision) | 80.6% | 98.2% |
| Sent to REVIEW / PARTIAL | 0% | 4.6% |
| Wrong-unit share of AUTO | 9.4% | 0.2% |
| AUTO on queries with no true match | 37.7% | 1.0% |
| Blocking recall (pre-cap) | 90.1% | 99.5% |

**At scale:** 1,977,652 reference rows (about an eighth of G-NAF) and 60,000 queries ran on DuckDB with 4 cores.
The reference build took 36 s and matching took 19 s, about 3,100 queries per second. Quality held:
99.8% precision, 93.5% recall, and 0.2% auto-match on queries with no true match.
**De-duplication:** 12,081 records were made from 6,000 addresses typed 1–3 times each.
`enhanced` reached 99.8% pairwise precision and 86.8% pairwise recall; `paper` reached 96.7% and 79.2%.
**Threshold sweep** ([`docs/sweep-tau-auto.md`](docs/sweep-tau-auto.md)): precision stays at 99.7% for
every `tau_auto` from 0.70 to 0.90. The structural checks carry precision, and `tau_auto` only
trades recall against review volume.

The `paper` preset lands close to the paper's own figure: ~12% false positives at τ = 0.7, against our 13%.
Most remaining `enhanced` errors are transposed unit or street numbers (UNIT 26 typed for UNIT 62).
Those inputs describe a different address that really exists, so no matcher can recover them.
**These are synthetic results.** The next step is to validate on real data with a clerically
reviewed gold set (SPEC §11.2).

## Quickstart

```bash
pip install -e ".[dev]"            # add ".[spark]" for local Spark
addrlink benchmark                 # synthetic paper-vs-enhanced comparison
addrlink synth --out data/         # write reference.parquet + queries.parquet
addrlink gnaf --root ~/G-NAF --out gnaf.parquet --synonyms-out gnaf_syn.json   # real G-NAF release
addrlink match --reference data/reference.parquet --queries data/queries.parquet --out results.parquet
addrlink dedupe --input data/queries.parquet --out clusters.parquet
addrlink benchmark --presets enhanced --sweep decision.tau_auto=0.75,0.8,0.85
addrlink explain --reference data/reference.parquet "U713/311 Gun Pl Hilnu ACT 2605"
addrlink sql --engine spark        # print the generated Spark SQL
pytest                             # 68 tests incl. oracle/DuckDB/Spark parity (-m "not spark" to skip Spark)
```

```python
from addrlink import Matcher, MatchConfig

m = Matcher("duckdb", MatchConfig.from_preset("enhanced"))
m.prepare_reference("reference.parquet")          # or a list of dicts / table name / Spark DataFrame
res = m.link("queries.parquet", id_col="id", address_cols=["address"])
res.decision_counts(); res.rows(); res.candidates(); res.run_log.as_dict()
```

### On Databricks

See [`examples/databricks_match.py`](examples/databricks_match.py). The short version:

```python
m = Matcher("spark", MatchConfig.from_preset("enhanced"), spark=spark,
            storage="table:main.addrlink_work")   # each stage becomes a Delta table
m.prepare_reference(spark.table("main.ref.gnaf_flat"))
res = m.link(spark.table("main.cust.addresses"), id_col="address_id",
             address_cols=["line1", "line2", "suburb", "state", "postcode"])
spark.table(res.result_table).write.saveAsTable("main.addrlink.match_result")
```

Spark stages must cut the logical-plan lineage between them. Without that, the analyser re-expands
the whole normalisation plan inside every later stage and runs out of memory. Two storage modes do this:

- `storage="checkpoint"` (default): uses `localCheckpoint()`.
- `storage="table:<catalog.schema>"`: writes Delta tables. Use it on shared or serverless compute,
  and for audited runs.

## Inputs

| Reference column | Required | Purpose |
|---|---|---|
| `r_id` | yes | Unique row id (one per variant) |
| `entity_id` | – | Address id, e.g. G-NAF `address_detail_pid`. Alias/variant rows share it. Defaults to `r_id`. |
| `address` (or `address_cols`) | yes | Canonical address text |
| `premise_numbers` | – | Required numbers: flat, street number or lot. Enables structure-aware numeric checks. |
| `premise_numbers_optional` | – | Numbers a customer may omit: level, range end |
| `postcode`, `state`, `street_name` | – | Enable `postcode_status`, `state_status` and `street_status` |

Queries need an `id` and one or more address columns. Multiple columns are concatenated with spaces.

## Outputs

`match_result` has one row per input row: `decision`, `entity_id`, `ref_address`, `score` (character
multiset Dice), `shared_blocks`, `idf_overlap`, `lev_ratio`, `word_idf_cosine`, `numeric_status`,
`postcode_status`, `state_status`, `street_status`, `margin`, `blocking_pass`, `n_candidates`,
`reason_codes` and `norm_key`.

`match_candidates` holds the top-K candidates per distinct normalised input, for review.

## Layout

```
src/addrlink/
  config.py     presets (paper / enhanced) + overrides + config hash
  rules.py      synonyms, phrases, classification patterns (shared by SQL and oracle)
  sql.py        Spark/DuckDB dialects + every pipeline stage as SQL
  engines.py    DuckDB and Spark execution (checkpoint / Delta table materialisation)
  api.py        Matcher, MatchResult, RunLog
  oracle.py     pure-Python reference implementation (parity tests, debugging)
  gnaf.py       G-NAF PSV release -> reference table (+ authority-table synonyms)
  synth.py      synthetic G-NAF-like reference + corrupted queries with known truth
  evaluate.py   precision/recall/blocking recall with Wilson CIs, markdown report
  cli.py        addrlink synth | match | benchmark | explain | sql
research/       paper-metric verification + minimal Spark prototype
docs/SPEC.md    review, research, specification (see §16 for prototype status)
```

## Status

This is a working prototype; the status table is SPEC §16.

**Not yet verified:**
- The G-NAF loader is tested only on a fixture in the published PSV layout. Run it on a real
  release before relying on it.
- Nothing has run on a Databricks cluster. Spark is tested locally, in both storage modes.

**Next:**
- run on a real G-NAF release and real bank data;
- build a clerically reviewed gold set and calibrate thresholds on it (SPEC §11.2);
- the optional learned scorer (SPEC §7.9).
