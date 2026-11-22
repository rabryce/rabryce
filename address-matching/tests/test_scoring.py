"""Similarity properties and the paper's printed scores (SPEC.md §2.1, Appendix B)."""

import re
from collections import Counter

from hypothesis import given
from hypothesis import strategies as st

from addrlink.oracle import levenshtein, occ

# Paper Table 2 (the less privacy-altered table): (query, linked, printed score)
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


def chars(s):
    return occ(list(re.sub("[^A-Z0-9]", "", s.upper())))


def dice_sets(a, b):
    return 2.0 * len(set(a) & set(b)) / (len(a) + len(b))


def dice_counter(x, y):
    a, b = Counter(re.sub("[^A-Z0-9]", "", x)), Counter(re.sub("[^A-Z0-9]", "", y))
    return 2.0 * sum((a & b).values()) / (sum(a.values()) + sum(b.values()))


def test_paper_table2_reproduced_by_multiset_dice():
    errs = [abs(dice_sets(chars(q), chars(r)) - p) for q, r, p in TABLE_2]
    assert max(errs) <= 0.05
    assert sum(errs) / len(errs) < 0.02


def test_digit_anagrams_tie_on_dice_but_not_on_levenshtein():
    q = "713 311 GUN PL"
    a, b = "U 713 311 GUN PL", "U 731 311 GUN PL"
    assert dice_sets(chars(q), chars(a)) == dice_sets(chars(q), chars(b))
    assert levenshtein(q, a) < levenshtein(q, b)


alnum = st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ", min_size=1, max_size=40).filter(
    lambda s: re.sub("[^A-Z0-9]", "", s) != "")


@given(alnum, alnum)
def test_occurrence_indexed_set_dice_equals_multiset_dice(x, y):
    assert abs(dice_sets(chars(x), chars(y)) - dice_counter(x, y)) < 1e-12


@given(alnum, alnum)
def test_dice_symmetric_bounded(x, y):
    d = dice_sets(chars(x), chars(y))
    assert 0.0 <= d <= 1.0
    assert d == dice_sets(chars(y), chars(x))
    assert dice_sets(chars(x), chars(x)) == 1.0


@given(st.text(max_size=12), st.text(max_size=12))
def test_levenshtein_symmetric(a, b):
    assert levenshtein(a, b) == levenshtein(b, a)
    assert levenshtein(a, a) == 0
