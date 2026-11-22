import pytest

from addrlink import MatchConfig
from addrlink import rules
from addrlink.oracle import light_words, normalise, numcore, occ

ENH = MatchConfig.from_preset("enhanced")
PAPER = MatchConfig.from_preset("paper")


def norm(a, cfg=ENH):
    return normalise(a, cfg)[2]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("51 Benor Dr, Belmnot VIC 3216", "51 BENOR DR BELMNOT VIC 3216"),
        ("34 Elizabeth Street, Sydney, New South Wales 2000, Australia", "34 ELIZABETH ST SYDNEY NSW 2000"),
        ("U713/311 Gun Pl", "U 713 311 GUN PL"),
        ("Apartment 5, 10 Saint George Avenue", "U 5 10 ST GEORGE AVE"),
        ("Level 3, 5/10 X Rd", "L 3 5 10 X RD"),
        ("LVL3 UNIT12A 7 Y ST", "L 3 U 12A 7 Y ST"),
        ("12Smith St Townsville QLD QLD 4810", "12 SMITH ST TOWNSVILLE QLD 4810"),
        ("C/O J Smith, 2 Main Rd", "J SMITH 2 MAIN RD"),
        ("Zoë's Café, 1 Rue St", "ZOE S CAFE 1 RUE ST"),
        ("12A High Street North", "12A HIGH ST N"),
    ],
)
def test_enhanced_normalisation(raw, expected):
    assert norm(raw) == expected


def test_paper_normalisation_is_upper_and_strip_only():
    assert norm("51 Benor Dr., Belmnot VIC 3216", PAPER) == "51 BENOR DR BELMNOT VIC 3216"
    assert norm("U713/311 Gun Pl", PAPER) == "U713 311 GUN PL"
    assert norm("1 X Street New South Wales", PAPER) == "1 X STREET NEW SOUTH WALES"


@pytest.mark.parametrize(
    "raw, cls",
    [
        ("PO Box 12, Sydney NSW 2000", "NON_PHYSICAL"),
        ("P.O. Box 12", "NON_PHYSICAL"),
        ("GPO BOX 1", "NON_PHYSICAL"),
        ("Locked Bag 7 Parramatta", "NON_PHYSICAL"),
        ("POBOX 99", "NON_PHYSICAL"),
        ("", "EMPTY"),
        ("  ,,  ", "EMPTY"),
        ("ATTN", "EMPTY"),
        ("12 Queen St, Auckland, New Zealand", "OUT_OF_SCOPE"),
        ("12 Box Hill Rd", "PHYSICAL"),
        ("1 Post Office Lane", "PHYSICAL"),
    ],
)
def test_classification(raw, cls):
    assert normalise(raw, ENH)[0] == cls


def test_paper_does_not_classify():
    assert normalise("PO Box 12", PAPER)[0] == "PHYSICAL"


def test_numcore_and_occ():
    assert numcore("12A") == "12"
    assert numcore("007") == "7"
    assert numcore("0") == "0"
    assert numcore("0800") == "800"
    assert numcore(None) is None
    assert occ(["68", "68", "3"]) == ["68#1", "68#2", "3#1"]


def test_light_words():
    assert light_words("Saint George", ENH) == ["ST", "GEORGE"]
    assert light_words(None, ENH) == []


def test_vocabulary_is_idempotent():
    rules.validate_vocabulary()
    for v in set(rules.SYNONYMS.values()):
        assert rules.SYNONYMS[v] == v
