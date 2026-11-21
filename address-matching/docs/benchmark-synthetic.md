# addrlink benchmark (201701 reference rows, 6000 queries, engine=duckdb, seed=7)

| Metric | paper | enhanced |
|---|---|---|
| AUTO precision (95% CI) | 87.0% [86.1, 87.9] | 99.7% [99.5, 99.8] |
| AUTO recall (95% CI) | 80.0% [78.9, 81.0] | 93.4% [92.7, 94.0] |
| Top-1 accuracy (any decision) | 80.6% | 98.2% |
| REVIEW + PARTIAL rate | 0.0% | 4.6% |
| Wrong-unit share of AUTO | 9.4% | 0.2% |
| AUTO on negatives (no true match) | 37.7% | 1.0% |
| Blocking recall (pre-cap) | 90.1% | 99.5% |
| Blocking recall (post-cap) | 90.1% | 98.5% |
| PO boxes routed NON_PHYSICAL | 0.0% | 100.0% |
| Runtime (s, match only) | 2.3 | 3.3 |
| Candidate pairs scored | 103236 | 193809 |

Queries: 5640 positive, 300 negative (true address withheld from reference), 60 PO boxes.

## Recall / precision by segment

| segment | n | paper recall | paper precision | paper top-1 | enhanced recall | enhanced precision | enhanced top-1 |
|---|---|---|---|---|---|---|---|
| DENSE_UNIT | 2600 | 84.7% | 95.3% | 85.5% | 93.8% | 99.5% | 99.0% |
| HOUSE | 1794 | 85.0% | 93.8% | 85.7% | 91.8% | 99.8% | 96.3% |
| LOT | 18 | 100.0% | 100.0% | 100.0% | 94.4% | 100.0% | 100.0% |
| PO_BOX | 0 | – | 0.0% | – | – | – | – |
| RANGE | 58 | 93.1% | 98.2% | 94.8% | 93.1% | 100.0% | 96.6% |
| UNIT | 1170 | 60.8% | 61.1% | 61.1% | 94.8% | 100.0% | 99.4% |

## Recall / precision by corruption

| corruption | n | paper recall | paper precision | paper top-1 | enhanced recall | enhanced precision | enhanced top-1 |
|---|---|---|---|---|---|---|---|
| (none) | 205 | 98.5% | 98.5% | 98.5% | 100.0% | 100.0% | 100.0% |
| C1_LEVEL_INCLUDED | 768 | 87.1% | 97.1% | 87.2% | 94.4% | 100.0% | 99.3% |
| C1_STATE_FULL | 459 | 83.4% | 91.4% | 83.7% | 93.7% | 99.8% | 97.2% |
| C1_STREET_TYPE_ABBREV | 3038 | 80.9% | 89.1% | 81.7% | 93.5% | 99.8% | 98.4% |
| C2_LOCALITY_MISSING | 229 | 67.7% | 85.6% | 80.8% | 64.6% | 100.0% | 97.4% |
| C2_POSTCODE_MISSING | 442 | 67.9% | 78.1% | 70.1% | 90.3% | 99.3% | 96.6% |
| C2_RANGE_END_DROPPED | 82 | 90.2% | 93.7% | 90.2% | 92.7% | 100.0% | 97.6% |
| C2_STATE_MISSING | 539 | 79.0% | 86.8% | 80.3% | 90.5% | 99.6% | 98.5% |
| C2_STREET_TYPE_MISSING | 429 | 77.2% | 87.6% | 79.5% | 88.3% | 99.5% | 96.0% |
| C3_UNIT_APT | 391 | 81.1% | 83.0% | 81.1% | 95.4% | 99.5% | 99.2% |
| C3_UNIT_COMMA | 369 | 96.5% | 96.0% | 96.5% | 96.5% | 99.7% | 99.7% |
| C3_UNIT_FORMAL | 1176 | 94.7% | 94.7% | 94.7% | 94.2% | 99.6% | 99.0% |
| C3_UNIT_GLUED | 382 | 14.7% | 43.4% | 14.9% | 94.5% | 99.4% | 99.2% |
| C3_UNIT_SLASH | 1452 | 73.7% | 75.6% | 75.3% | 93.0% | 99.9% | 99.1% |
| C4_TYPO_LOCALITY | 311 | 83.0% | 89.6% | 83.0% | 92.9% | 99.7% | 96.5% |
| C4_TYPO_STREET | 671 | 68.3% | 95.0% | 68.6% | 83.8% | 100.0% | 91.5% |
| C5_NUMBER_TRANSPOSED | 45 | 20.0% | 64.3% | 20.0% | 0.0% | 0.0% | 51.1% |
| C5_POSTCODE_WRONG | 131 | 70.2% | 76.7% | 71.8% | 0.0% | – | 99.2% |
| C5_UNIT_TRANSPOSED | 27 | 3.7% | 7.1% | 3.7% | 0.0% | 0.0% | 7.4% |
| C6_LOCALITY_ALIAS | 49 | 75.5% | 86.0% | 75.5% | 95.9% | 100.0% | 95.9% |
| C6_LOCALITY_NEIGHBOUR | 56 | 62.5% | 74.5% | 67.9% | 51.8% | 100.0% | 91.1% |
| C7_COUNTRY_SUFFIX | 185 | 84.3% | 93.4% | 84.3% | 93.5% | 99.4% | 98.4% |
| C7_MIXED_CASE | 2289 | 80.0% | 87.3% | 80.6% | 93.7% | 99.7% | 98.5% |
| C7_NOISE_PREFIX | 147 | 84.4% | 93.2% | 84.4% | 89.8% | 100.0% | 98.0% |
| C7_STATE_DOUBLED | 155 | 87.1% | 90.6% | 87.1% | 96.1% | 100.0% | 100.0% |
| C8_GLUED_NUMBER | 110 | 17.3% | 82.6% | 17.3% | 96.4% | 100.0% | 99.1% |
| C8_POSTCODE_FIRST | 219 | 82.2% | 88.7% | 83.1% | 94.5% | 100.0% | 98.6% |

**paper** decisions: AUTO_MATCH=5184, NO_MATCH=816

**enhanced** decisions: AUTO_MATCH=5281, NON_PHYSICAL=60, NO_MATCH=265, PARTIAL_MATCH=18, REVIEW=376


## Scale point (enhanced, DuckDB, 4 cores, seed=9)

1,977,652 reference rows (about an eighth of G-NAF) and 60,000 queries:

| | |
|---|---|
| Reference build (incl. bulk load from Python) | 36 s |
| Link (all query stages) | 19 s (≈3,100 queries/s) |
| Candidate pairs scored | 2,356,093 |
| AUTO precision / recall | 99.76% / 93.47% |
| AUTO on negatives | 0.23% |
| Blocking recall pre / post cap | 99.39% / 98.50% |

## De-duplication (UC3, seed=21)

12,081 records from 6,000 addresses, each typed 1–3 times with corruptions:

| | paper | enhanced |
|---|---|---|
| Pairwise precision | 96.7% | 99.8% |
| Pairwise recall | 79.2% | 86.8% |

## Threshold sweep

See [`sweep-tau-auto.md`](sweep-tau-auto.md).
