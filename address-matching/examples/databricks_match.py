# Databricks notebook source
# MAGIC %md
# MAGIC # addrlink on Databricks
# MAGIC
# MAGIC Match customer addresses to a flattened reference address table (e.g. G-NAF).
# MAGIC Install the wheel on the cluster first (`pip install addrlink-0.1.0-py3-none-any.whl`),
# MAGIC or `%pip install /Workspace/.../address-matching`.

# COMMAND ----------

from pyspark.sql import functions as F

from addrlink import Matcher, MatchConfig

WORK_SCHEMA = "main.addrlink_work"    # stage tables (Delta); needs CREATE TABLE
REFERENCE = "main.ref.gnaf_flat"      # r_id, entity_id, address, premise_numbers, ... (see README)
QUERIES = "main.cust.addresses"
OUTPUT = "main.addrlink.match_result"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WORK_SCHEMA}")

# COMMAND ----------

cfg = MatchConfig.from_preset("enhanced")      # or .with_dotted(["decision.tau_auto=0.82"])
m = Matcher("spark", cfg, spark=spark, storage=f"table:{WORK_SCHEMA}", collect_counts=True)

n_ref = m.prepare_reference(spark.table(REFERENCE))
res = m.link(
    spark.table(QUERIES),
    id_col="address_id",
    address_cols=["line1", "line2", "suburb", "state", "postcode"],
)
print(n_ref, res.decision_counts())
print(res.run_log.as_dict())

# COMMAND ----------

(spark.table(res.result_table)
      .withColumn("run_id", F.lit(res.run_log.run_id))
      .write.mode("append").saveAsTable(OUTPUT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review queue
# MAGIC Rows with `decision IN ('REVIEW', 'PARTIAL_MATCH')`; join `match_candidates` on `norm_key`
# MAGIC to show the top-3 alternatives to a reviewer.

# COMMAND ----------

display(spark.sql(f"""
  SELECT r.id, r.input_address, r.decision, r.reason_codes, c.rnk, c.ref_address, c.score,
         c.numeric_status, c.street_status
  FROM {res.result_table} r JOIN {res.candidates_table} c ON c.norm_key = r.norm_key
  WHERE r.decision IN ('REVIEW', 'PARTIAL_MATCH')
  ORDER BY r.id, c.rnk
  LIMIT 200"""))
