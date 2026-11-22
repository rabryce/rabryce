"""Writes a tiny, fictional G-NAF release in the official PSV layout (for loader tests)."""

from __future__ import annotations

import os

AD_COLS = [
    "ADDRESS_DETAIL_PID", "DATE_CREATED", "DATE_LAST_MODIFIED", "DATE_RETIRED", "BUILDING_NAME",
    "LOT_NUMBER_PREFIX", "LOT_NUMBER", "LOT_NUMBER_SUFFIX", "FLAT_TYPE_CODE", "FLAT_NUMBER_PREFIX", "FLAT_NUMBER",
    "FLAT_NUMBER_SUFFIX", "LEVEL_TYPE_CODE", "LEVEL_NUMBER_PREFIX", "LEVEL_NUMBER", "LEVEL_NUMBER_SUFFIX",
    "NUMBER_FIRST_PREFIX", "NUMBER_FIRST", "NUMBER_FIRST_SUFFIX", "NUMBER_LAST_PREFIX", "NUMBER_LAST",
    "NUMBER_LAST_SUFFIX", "STREET_LOCALITY_PID", "LOCATION_DESCRIPTION", "LOCALITY_PID", "ALIAS_PRINCIPAL",
    "POSTCODE", "PRIVATE_STREET", "LEGAL_PARCEL_ID", "CONFIDENCE", "ADDRESS_SITE_PID", "LEVEL_GEOCODED_CODE",
    "PROPERTY_PID", "GNAF_PROPERTY_PID", "PRIMARY_SECONDARY",
]


def _ad(pid, sl, loc, pc, number_first=None, number_last=None, flat_type=None, flat=None, level_type=None,
        level=None, lot=None, suffix=None, alias="P", retired=None):
    r = dict.fromkeys(AD_COLS, "")
    r.update(ADDRESS_DETAIL_PID=pid, DATE_CREATED="2020-01-01", STREET_LOCALITY_PID=sl, LOCALITY_PID=loc,
             POSTCODE=pc, ALIAS_PRINCIPAL=alias, CONFIDENCE="2", DATE_RETIRED=retired or "")
    r.update(NUMBER_FIRST=number_first or "", NUMBER_LAST=number_last or "", FLAT_TYPE_CODE=flat_type or "",
             FLAT_NUMBER=flat or "", LEVEL_TYPE_CODE=level_type or "", LEVEL_NUMBER=level or "",
             LOT_NUMBER=lot or "", NUMBER_FIRST_SUFFIX=suffix or "")
    return r


ADDRESS_DETAIL = {
    "ACT": [
        _ad("GAACT1", "ACT_SL1", "ACT_L1", "2600", number_first="345", flat_type="UNIT", flat="201",
            level_type="L", level="2"),
        _ad("GAACT2", "ACT_SL1", "ACT_L1", "2600", number_first="345"),
        _ad("GAACT3", "ACT_SL2", "ACT_L1", "2600", number_first="34", number_last="38"),
        _ad("GAACT4", "ACT_SL2", "ACT_L1", "2600", number_first="12", suffix="A"),
        _ad("GAACT5", "ACT_SL2", "ACT_L1", "2600", number_first="40", retired="2024-05-01"),
    ],
    "NSW": [
        _ad("GANSW1", "NSW_SL1", "NSW_L1", "2795", lot="7"),
        _ad("GANSW2", "NSW_SL2", "NSW_L2", "2000", number_first="10", flat_type="APT", flat="5"),
        _ad("GANSW3", "NSW_SL3", "NSW_L2", "2000", number_first="1", alias="A"),  # alias of GANSW4
        _ad("GANSW4", "NSW_SL2", "NSW_L2", "2000", number_first="12"),
    ],
}
STREET_LOCALITY = {
    "ACT": [dict(STREET_LOCALITY_PID="ACT_SL1", STREET_NAME="HARBURN", STREET_TYPE_CODE="STREET",
                 STREET_SUFFIX_CODE="", LOCALITY_PID="ACT_L1"),
            dict(STREET_LOCALITY_PID="ACT_SL2", STREET_NAME="ELIZABETH", STREET_TYPE_CODE="ALLEY",
                 STREET_SUFFIX_CODE="N", LOCALITY_PID="ACT_L1")],
    "NSW": [dict(STREET_LOCALITY_PID="NSW_SL1", STREET_NAME="OLD MILL", STREET_TYPE_CODE="ROAD",
                 STREET_SUFFIX_CODE="", LOCALITY_PID="NSW_L1"),
            dict(STREET_LOCALITY_PID="NSW_SL2", STREET_NAME="GEORGE", STREET_TYPE_CODE="STREET",
                 STREET_SUFFIX_CODE="", LOCALITY_PID="NSW_L2"),
            dict(STREET_LOCALITY_PID="NSW_SL3", STREET_NAME="MARKET", STREET_TYPE_CODE="LANE",
                 STREET_SUFFIX_CODE="", LOCALITY_PID="NSW_L2")],
}
STREET_LOCALITY_ALIAS = {
    "NSW": [dict(STREET_LOCALITY_ALIAS_PID="NSW_SLA1", STREET_LOCALITY_PID="NSW_SL1", STREET_NAME="MILL",
                 STREET_TYPE_CODE="ROAD", STREET_SUFFIX_CODE="", ALIAS_TYPE_CODE="SYN")],
}
LOCALITY = {
    "ACT": [dict(LOCALITY_PID="ACT_L1", LOCALITY_NAME="BALLANDS", STATE_PID="8", PRIMARY_POSTCODE="2600")],
    "NSW": [dict(LOCALITY_PID="NSW_L1", LOCALITY_NAME="KENTHURST", STATE_PID="1", PRIMARY_POSTCODE="2795"),
            dict(LOCALITY_PID="NSW_L2", LOCALITY_NAME="SYDNEY", STATE_PID="1", PRIMARY_POSTCODE="2000")],
}
LOCALITY_ALIAS = {
    "NSW": [dict(LOCALITY_ALIAS_PID="NSW_LA1", LOCALITY_PID="NSW_L2", NAME="HAYMARKET", POSTCODE="2000",
                 ALIAS_TYPE_CODE="SYN", STATE_PID="1")],
}
STATE = {
    "ACT": [dict(STATE_PID="8", STATE_NAME="AUSTRALIAN CAPITAL TERRITORY", STATE_ABBREVIATION="ACT")],
    "NSW": [dict(STATE_PID="1", STATE_NAME="NEW SOUTH WALES", STATE_ABBREVIATION="NSW")],
}
ADDRESS_ALIAS = {"NSW": [dict(ADDRESS_ALIAS_PID="AA1", PRINCIPAL_PID="GANSW4", ALIAS_PID="GANSW3",
                              ALIAS_TYPE_CODE="SYN")]}
GEOCODE = {
    "ACT": [dict(ADDRESS_DEFAULT_GEOCODE_PID="G1", ADDRESS_DETAIL_PID="GAACT1", GEOCODE_TYPE_CODE="PC",
                 LONGITUDE="149.1", LATITUDE="-35.3")],
}
AUTHORITY = {
    "STREET_TYPE_AUT": [("STREET", "ST", "STREET"), ("ROAD", "RD", "ROAD"), ("LANE", "LANE", "LANE"),
                        ("ALLEY", "ALLY", "ALLEY"), ("STRAIGHT", "STRT", "STRAIGHT")],
    "STREET_SUFFIX_AUT": [("N", "NORTH", "NORTH"), ("S", "SOUTH", "SOUTH")],
    "FLAT_TYPE_AUT": [("UNIT", "UNIT", "UNIT"), ("APT", "APARTMENT", "APARTMENT")],
    "LEVEL_TYPE_AUT": [("L", "LEVEL", "LEVEL"), ("G", "GROUND", "GROUND FLOOR")],
}


def _write(path: str, cols: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("|".join(cols) + "\n")
        for r in rows:
            f.write("|".join(str(r.get(c, "")) for c in cols) + "\n")


def write_fixture(root: str) -> str:
    rel = os.path.join(root, "G-NAF", "G-NAF NOVEMBER 2025")
    std, aut = os.path.join(rel, "Standard"), os.path.join(rel, "Authority Code")
    tables = {
        "ADDRESS_DETAIL": (AD_COLS, ADDRESS_DETAIL),
        "STREET_LOCALITY": (["STREET_LOCALITY_PID", "DATE_CREATED", "DATE_RETIRED", "STREET_CLASS_CODE",
                             "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE", "LOCALITY_PID"],
                            STREET_LOCALITY),
        "STREET_LOCALITY_ALIAS": (["STREET_LOCALITY_ALIAS_PID", "DATE_CREATED", "DATE_RETIRED",
                                   "STREET_LOCALITY_PID", "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE",
                                   "ALIAS_TYPE_CODE"], STREET_LOCALITY_ALIAS),
        "LOCALITY": (["LOCALITY_PID", "DATE_CREATED", "DATE_RETIRED", "LOCALITY_NAME", "PRIMARY_POSTCODE",
                      "LOCALITY_CLASS_CODE", "STATE_PID", "GNAF_LOCALITY_PID", "GNAF_RELIABILITY_CODE"], LOCALITY),
        "LOCALITY_ALIAS": (["LOCALITY_ALIAS_PID", "DATE_CREATED", "DATE_RETIRED", "LOCALITY_PID", "NAME",
                            "POSTCODE", "ALIAS_TYPE_CODE", "STATE_PID"], LOCALITY_ALIAS),
        "STATE": (["STATE_PID", "DATE_CREATED", "DATE_RETIRED", "STATE_NAME", "STATE_ABBREVIATION"], STATE),
        "ADDRESS_ALIAS": (["ADDRESS_ALIAS_PID", "DATE_CREATED", "DATE_RETIRED", "PRINCIPAL_PID", "ALIAS_PID",
                           "ALIAS_TYPE_CODE", "ALIAS_COMMENT"], ADDRESS_ALIAS),
        "ADDRESS_DEFAULT_GEOCODE": (["ADDRESS_DEFAULT_GEOCODE_PID", "DATE_CREATED", "DATE_RETIRED",
                                     "ADDRESS_DETAIL_PID", "GEOCODE_TYPE_CODE", "LONGITUDE", "LATITUDE"], GEOCODE),
    }
    for table, (cols, by_state) in tables.items():
        for st in ("ACT", "NSW"):
            _write(os.path.join(std, f"{st}_{table}_psv.psv"), cols, by_state.get(st, []))
    for table, rows in AUTHORITY.items():
        _write(os.path.join(aut, f"Authority_Code_{table}_psv.psv"), ["CODE", "NAME", "DESCRIPTION"],
               [dict(CODE=a, NAME=b, DESCRIPTION=c) for a, b, c in rows])
    return root
