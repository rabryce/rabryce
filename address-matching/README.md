# addrlink — address matching (design stage)

A planned Python package for large-scale address matching. It is based on AUSTRAC's
parse-free, inverted-index approach ([arXiv:1708.01402](https://arxiv.org/abs/1708.01402)),
hardened for linking Australian addresses to G-NAF on Databricks (Spark) and DuckDB.

| Path | Contents |
|---|---|
| [`docs/SPEC.md`](docs/SPEC.md) | Paper review, wider research, solution specification and delivery plan |
| [`research/verify_similarity_metric.py`](research/verify_similarity_metric.py) | Shows that the paper's "Jaccard" is multiset Sørensen–Dice, by recomputing its printed scores |
| [`research/spark_prototype.py`](research/spark_prototype.py) | The paper's core algorithm in ~6 Spark SQL statements (requires `pyspark` + Java) |

```bash
python research/verify_similarity_metric.py     # stdlib only
pip install pyspark && python research/spark_prototype.py
```
