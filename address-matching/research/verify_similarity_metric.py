"""Which similarity did Zhang, Churchill & Ng (arXiv:1708.01402) actually compute?

The paper calls its round-2 measure the "Jaccard index", but its worked example
(6/8 = 0.75) and its linear-time algorithm (common += 2, total += 1 per token)
describe the Sorensen-Dice coefficient on multisets: 2|A n B| / (|A| + |B|).
Equation 2, however, describes true Jaccard: |A n B| / |A u B|.

This script recomputes the scores printed in Tables 1 and 2 of the paper with
three candidate metrics over alphanumeric characters and reports the error.
Note: the paper altered the strings for privacy (street/suburb names replaced
by sorted letter sets, some words truncated), so an exact match is impossible;
Table 2 is the less-altered of the two.

Run:  python verify_similarity_metric.py
"""

from __future__ import annotations

import re
from collections import Counter

# (query address, linked address, score printed in the paper)
TABLE_1 = [
    ("33 34-38 EHMNTV DIU NSW 6561", "UNIT 33 34-38 EHMNTV STRET OUT DIR NSW 6561", 0.88),
    ("53 741 ADGNR EFORST AKLR QLD 9368", "UNIT 53 741 ADGNR AVENUE EFORST LAKE QLD 9168", 0.80),
    ("972 4 CEOPRW LMOW NEW WALES 5133", "UNIT 972 4 CEOPRW AFHRW ROADWAY LMOW NSW 5133", 0.90),
    ("713 311 GUN HILNU ACT 5035", "UNIT 731 311 GUN PLACE HILNU ACT 5035", 0.93),
    ("3 59 FGIS DEKNOR QLD QLD 9173", "UNIT 3 59 FGIS STRET DEKNOR QLD 9173", 0.90),
    ("9 NO 7 TO 2 CELMNT ADEGNO VIC 7362", "UNIT 9 7-2 CELMNT STRET ADEGNO VIC 7362", 0.91),
    ("313 0 EGKNORW ABEHILTZ BAY 5133", "UNIT 313 0 EGKNORW AVENUE ABEHILTZ BAY NSW 5133", 0.91),
    ("MARGETIC 6 715 ABDFORST BELMNORU VIC 7123", "FLAT 6 715 ABDFORST STRET HNORT BELMNORU VIC 7123", 0.92),
    ("43 3345 ACDHINSV AGRTV QLD 9355", "UNIT 43 3345 ACDEHINSV ROAD MOUNT AGRTV EAST QLD 9355", 0.82),
    ("78 03 ADELMNOR BELM VIC VIC 7133", "UNIT 78 03 ADELMNOR STRET ACFORSTY VIC 7133", 0.83),
]
TABLE_2 = [
    ("4 EGNORU AEHLMT VIC 3095", "4 EGNORU CRT AEHLMT NORTH VIC 3095", 0.84),
    ("528 LTUY HLU LTUY HLU QLD 4854", "528 LTUY ADEHLSU RD LTUY QLD 4854", 0.87),
    ("45 EGHIMNS ADEGNO VIC 3175", "RM 8 45 EGHIMNS ST ADEGNO VIC 3175", 0.88),
    ("6 EILS CEIMNRTY ABILRSUY DNOSW SA 5108", "6 EILS CEIMNRTY RD ABILRSUY DNOSW SA 5108", 0.96),
    ("EL EILOSU 137 AILNT EFNRY EOV QUEN SLAND 4055", "137 AILNT RD EFNRY EGORV QLD 4055", 0.74),
    ("80 ABEGLNRU DEHILS VI 3037", "80 ABEGLNRU DR DEHILS VIC 3037", 0.94),
    ("141 ACEHLRS EHPRT 6005", "141 ACEHLRS ST ESTW EHPRT WA 6005", 0.80),
    ("51 BENOR BELMNOT VIC 3216", "2/51 BENOR DR BELMNOT VIC 3216", 0.91),
    ("97 ELOXY ABDPRUY WA 6025 TRA LIA", "97 ELOXY AVE ABDPRUY WA 6025", 0.87),
    ("9 DLORS DENSY 2077", "9 DLORS AVE AHIQSTU NSW 2077", 0.68),
]


def chars(address: str) -> Counter[str]:
    """Multiset of alphanumeric characters (whitespace and punctuation dropped)."""
    return Counter(re.sub(r"[^A-Z0-9]", "", address.upper()))


def multiset_dice(a: Counter[str], b: Counter[str]) -> float:
    inter = sum((a & b).values())
    return 2 * inter / (sum(a.values()) + sum(b.values()))


def multiset_jaccard(a: Counter[str], b: Counter[str]) -> float:
    return sum((a & b).values()) / sum((a | b).values())


def set_jaccard(a: Counter[str], b: Counter[str]) -> float:
    sa, sb = set(a), set(b)
    return len(sa & sb) / len(sa | sb)


METRICS = {"multiset_dice": multiset_dice, "multiset_jaccard": multiset_jaccard, "set_jaccard": set_jaccard}


def main() -> None:
    for name, table in (("Table 1", TABLE_1), ("Table 2", TABLE_2)):
        print(f"\n{name}: paper score vs recomputed")
        print(f"{'paper':>6} " + " ".join(f"{m:>17}" for m in METRICS))
        errors = {m: [] for m in METRICS}
        for q, r, paper in table:
            a, b = chars(q), chars(r)
            row = []
            for m, fn in METRICS.items():
                v = fn(a, b)
                errors[m].append(abs(v - paper))
                row.append(f"{v:17.2f}")
            print(f"{paper:6.2f} " + " ".join(row))
        print("mean |error|: " + ", ".join(f"{m}={sum(e) / len(e):.3f}" for m, e in errors.items()))

    print("\nDice -> Jaccard equivalents (J = D / (2 - D)):")
    for d in (0.6, 0.7, 0.8):
        print(f"  Dice {d:.1f} == Jaccard {d / (2 - d):.3f}")


if __name__ == "__main__":
    main()
