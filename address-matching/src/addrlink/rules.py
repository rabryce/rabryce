"""Normalisation vocabulary and patterns (SPEC.md §6.2, §7.1, §7.2).

Everything here is plain data so the same rules can be rendered into Spark SQL,
DuckDB SQL and the pure-Python oracle. All tokens are upper-case [A-Z0-9] only,
which keeps them safe to inline as SQL literals.

Canonical form is the SHORT form, applied identically to both sides, so
ambiguous expansions (ST = STREET or SAINT) collapse harmlessly.
"""

from __future__ import annotations

import re

STATES: tuple[str, ...] = ("NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT")

# Multi-word phrases replaced on the space-padded string (longest first).
STATE_PHRASES: dict[str, str] = {
    "AUSTRALIAN CAPITAL TERRITORY": "ACT",
    "NORTHERN TERRITORY": "NT",
    "NEW SOUTH WALES": "NSW",
    "WESTERN AUSTRALIA": "WA",
    "SOUTH AUSTRALIA": "SA",
}

# Phrases removed entirely (C/O becomes "C O" after punctuation stripping).
NOISE_PHRASES: tuple[str, ...] = ("CARE OF", "C O", "ATTENTION", "ATTN")

# Trailing tokens dropped when they are the last token.
TRAILING_NOISE: tuple[str, ...] = ("AUSTRALIA", "AUS")

_SYNONYM_GROUPS: dict[str, tuple[str, ...]] = {
    # street types
    "ST": ("STREET", "STR", "STRT", "SAINT"),
    "RD": ("ROAD",),
    "AVE": ("AVENUE", "AV", "AVNU"),
    "DR": ("DRIVE", "DRV"),
    "CT": ("COURT", "CRT"),
    "PL": ("PLACE",),
    "CR": ("CRESCENT", "CRES", "CRS"),
    "CL": ("CLOSE",),
    "PDE": ("PARADE",),
    "LN": ("LANE",),
    "WAY": ("WY",),
    "HWY": ("HIGHWAY",),
    "TCE": ("TERRACE", "TER"),
    "BVD": ("BOULEVARD", "BLVD"),
    "CCT": ("CIRCUIT",),
    "GR": ("GROVE",),
    "SQ": ("SQUARE",),
    "ESP": ("ESPLANADE",),
    "PWY": ("PARKWAY",),
    "CIR": ("CIRCLE",),
    "TRK": ("TRACK",),
    "WLK": ("WALK",),
    "VW": ("VIEW",),
    "GDNS": ("GARDENS",),
    "HTS": ("HEIGHTS",),
    # suffixes / directions
    "N": ("NORTH", "NTH"),
    "S": ("SOUTH", "STH"),
    "E": ("EAST",),
    "W": ("WEST", "WST"),
    "MT": ("MOUNT",),
    # flat / level types (interchangeable in practice)
    "U": ("UNIT", "APARTMENT", "APT", "FLAT", "UNT"),
    "SHOP": ("SHP",),
    "SE": ("SUITE", "STE"),
    "L": ("LEVEL", "LVL", "FLOOR", "FLR"),
    "G": ("GROUND",),
    # single-word states
    "VIC": ("VICTORIA",),
    "QLD": ("QUEENSLAND",),
    "TAS": ("TASMANIA",),
}

SYNONYMS: dict[str, str] = {}
for _canon, _variants in _SYNONYM_GROUPS.items():
    SYNONYMS[_canon] = _canon
    for _v in _variants:
        SYNONYMS[_v] = _canon

# Glued unit/level prefixes: U12 -> U 12, LEVEL3 -> LEVEL 3 (longest alternatives first).
GLUED_PREFIX_RE = "^(APARTMENT|LEVEL|SUITE|UNIT|SHOP|FLAT|APT|LVL|LOT|U|L)([0-9]+[A-Z]?)$"
GLUED_PREFIX_SPLIT = "^(APARTMENT|LEVEL|SUITE|UNIT|SHOP|FLAT|APT|LVL|LOT|U|L)([0-9])"
# Glued number + street name: 12SMITH -> 12 SMITH (3+ letters, so suffixes like 12A survive).
GLUED_NUMBER_RE = "^[0-9]+[A-Z]{3,}$"
GLUED_NUMBER_SPLIT = "^([0-9]+)([A-Z])"

# Input classification patterns, applied to the "basic" string
# (upper-case, non-alphanumerics -> single spaces, trimmed).
NON_PHYSICAL_RE = (
    "(^| )(P ?O|G ?P ?O|POST OFFICE|LOCKED|PRIVATE|PARCEL) ?(BOX|BAG|LOCKER)( |$)"
    "|(^| )(POBOX|GPOBOX|PMB|RMB|RSD|CMB|CARE PO)( |$)"
)
FOREIGN_RE = (
    "(^| )(NEW ZEALAND|NZ|UNITED KINGDOM|UK|ENGLAND|USA|UNITED STATES|CANADA|"
    "SINGAPORE|HONG KONG|INDIA|CHINA|PHILIPPINES|MALAYSIA|FIJI)$"
)

POSTCODE_TOKEN_RE = "^[0-9]{4}$"
HAS_DIGIT_RE = "[0-9]"

# Accent folding via translate() so every engine folds identically.
ACCENTS_FROM = "ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿÑñÇç"
ACCENTS_TO = "AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuYyyNnCc"

_TOKEN_OK = re.compile(r"^[A-Z0-9]+( [A-Z0-9]+)*$")


def validate_vocabulary() -> None:
    """Fail fast if the vocabulary would break SQL inlining or idempotency."""
    for k, v in SYNONYMS.items():
        assert _TOKEN_OK.match(k) and _TOKEN_OK.match(v), (k, v)
        assert SYNONYMS.get(v) == v, f"canonical form {v!r} must map to itself"
    for k, v in STATE_PHRASES.items():
        assert _TOKEN_OK.match(k) and v in STATES, (k, v)
    for p in NOISE_PHRASES + TRAILING_NOISE:
        assert _TOKEN_OK.match(p), p
    assert len(ACCENTS_FROM) == len(ACCENTS_TO)


validate_vocabulary()
