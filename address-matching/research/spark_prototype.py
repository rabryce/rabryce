"""Minimal Spark SQL prototype of the paper's batch linkage (arXiv:1708.01402).

Runs the same statements as SPEC.md section 9.4 on a handful of toy rows to prove
the SQL is valid and to show the order-blindness of character similarity
(UNIT 713 vs UNIT 731 tie on Dice, broken by shared bigrams / Levenshtein).

Run (needs Java + pyspark):  python spark_prototype.py
"""

from __future__ import annotations

from pyspark.sql import SparkSession

K = 100  # paper's token-recurrence cap

QUERY = [
    (1, "51 Benor Dr, Belmnot VIC 3216"),
    (2, "U 713/311 Gun Pl Hilnu ACT 5035"),
]
REFERENCE = [
    (10, "UNIT 2 51 BENOR DRIVE BELMNOT VIC 3216"),
    (11, "51 BENOR DRIVE BELMNOT VIC 3216"),
    (20, "UNIT 731 311 GUN PLACE HILNU ACT 5035"),
    (21, "UNIT 713 311 GUN PLACE HILNU ACT 5035"),
]

# S1 normalise + word tokens + order-preserving adjacent word bigrams
S1_TOKENS = """
CREATE OR REPLACE TEMP VIEW {side}_tok AS
SELECT addr_id, norm, words,
       CASE WHEN size(words) > 1
            THEN transform(sequence(0, size(words) - 2), i -> concat(words[i], ' ', words[i + 1]))
            ELSE array() END AS bigrams
FROM (SELECT addr_id, norm, split(norm, ' ') AS words
      FROM (SELECT addr_id, trim(regexp_replace(upper(address), '[^A-Z0-9]+', ' ')) AS norm
            FROM {side}_input)
      WHERE norm <> '')
"""
# S2 postings (inverted index in long form)
S2_POSTINGS = """
CREATE OR REPLACE TEMP VIEW {side}_post AS
SELECT DISTINCT addr_id, token FROM {side}_tok LATERAL VIEW explode(bigrams) t AS token
"""
# S3 document frequency per token
S3_DF = """
CREATE OR REPLACE TEMP VIEW {side}_df AS
SELECT token, count(*) AS df FROM {side}_post GROUP BY token
"""
# S5 occurrence-indexed character multiset: E#1, E#2, ... so set ops == multiset ops
S5_CHARS = """
CREATE OR REPLACE TEMP VIEW {side}_chr AS
SELECT addr_id, collect_list(concat(ch, '#', k)) AS chars
FROM (SELECT addr_id, ch, row_number() OVER (PARTITION BY addr_id, ch ORDER BY pos) AS k
      FROM (SELECT addr_id, posexplode(split(replace(norm, ' ', ''), '')) AS (pos, ch) FROM {side}_tok)
      WHERE ch <> '')
GROUP BY addr_id
"""
# S4 candidates: join the two inverted indexes on recurrence-capped tokens
S4_CANDIDATES = f"""
CREATE OR REPLACE TEMP VIEW cand AS
SELECT qp.addr_id AS q_id, rp.addr_id AS r_id, count(*) AS shared_blocks
FROM q_post qp
JOIN q_df qd  ON qd.token = qp.token AND qd.df <= {K}
JOIN r_df rd  ON rd.token = qp.token AND rd.df <= {K}
JOIN r_post rp ON rp.token = qp.token
GROUP BY qp.addr_id, rp.addr_id
"""
# S6 + S7 score (multiset Dice) and rank with deterministic tie-breaks
S6_S7_SCORE_RANK = """
SELECT q_id, r_id, shared_blocks, round(dice, 3) AS dice, lev,
       row_number() OVER (PARTITION BY q_id
                          ORDER BY dice DESC, shared_blocks DESC, lev ASC, r_id ASC) AS rnk
FROM (SELECT c.q_id, c.r_id, c.shared_blocks,
             2.0 * size(array_intersect(a.chars, b.chars)) / (size(a.chars) + size(b.chars)) AS dice,
             levenshtein(qt.norm, rt.norm) AS lev
      FROM cand c
      JOIN q_chr a  ON a.addr_id  = c.q_id
      JOIN r_chr b  ON b.addr_id  = c.r_id
      JOIN q_tok qt ON qt.addr_id = c.q_id
      JOIN r_tok rt ON rt.addr_id = c.r_id)
ORDER BY q_id, rnk
"""


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.createDataFrame(QUERY, "addr_id long, address string").createOrReplaceTempView("q_input")
    spark.createDataFrame(REFERENCE, "addr_id long, address string").createOrReplaceTempView("r_input")
    for side in ("q", "r"):
        for stmt in (S1_TOKENS, S2_POSTINGS, S3_DF, S5_CHARS):
            spark.sql(stmt.format(side=side))
    spark.sql(S4_CANDIDATES)
    spark.sql(S6_S7_SCORE_RANK).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
