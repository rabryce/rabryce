"""G-NAF loader: PSV release -> addrlink reference table (SPEC.md §6).

Reads a Geoscape G-NAF release (``Standard/<STATE>_<TABLE>_psv.psv`` and
``Authority Code/Authority_Code_<TABLE>_psv.psv``), flattens it, and renders one
row per matchable variant in the reference contract used by
``Matcher.prepare_reference``:

    r_id, entity_id, address, premise_numbers, premise_numbers_optional,
    postcode, state, street_name, + address_detail_pid, variant, is_principal,
    latitude, longitude, confidence, street_locality_pid, locality_pid

Variants (all sharing ``entity_id`` = the principal ADDRESS_DETAIL_PID):
  P  the address as recorded (alias addresses map to their principal via ADDRESS_ALIAS)
  LA each locality alias name (LOCALITY_ALIAS)
  SA each street-locality alias (STREET_LOCALITY_ALIAS)
  R  range start only, for number ranges ("34" for "34-38")

Rendering puts LEVEL before the flat so flat and street numbers stay adjacent (SPEC §16.2).
Required premise numbers are the flat, the street number, or the lot when there is no
street number. Optional premise numbers are the level, the range end, and the lot when a
street number exists.

NOTE: column names follow the published G-NAF data model. The loader is tested against a
fixture written in that format (tests/gnaf_fixture.py); validate it against a real release
before production use.
"""

from __future__ import annotations

import os
import re
from typing import Any

from .engines import DuckDBEngine, Engine, SparkEngine

STATE_PREFIXES = ("ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA")
STANDARD_TABLES = (
    "ADDRESS_DETAIL", "ADDRESS_ALIAS", "ADDRESS_DEFAULT_GEOCODE", "STREET_LOCALITY",
    "STREET_LOCALITY_ALIAS", "LOCALITY", "LOCALITY_ALIAS", "STATE",
)
AUTHORITY_TABLES = ("FLAT_TYPE_AUT", "LEVEL_TYPE_AUT", "STREET_TYPE_AUT", "STREET_SUFFIX_AUT")
OPTIONAL_TABLES = {"ADDRESS_ALIAS", "ADDRESS_DEFAULT_GEOCODE", "STREET_LOCALITY_ALIAS", "LOCALITY_ALIAS"}


def find_files(root: str) -> dict[str, list[str]]:
    """Map table name -> list of PSV paths found anywhere under ``root``."""
    std = re.compile(rf"^({'|'.join(STATE_PREFIXES)})_({'|'.join(STANDARD_TABLES)})_psv\.psv$", re.I)
    aut = re.compile(rf"^Authority_Code_({'|'.join(AUTHORITY_TABLES)})_psv\.psv$", re.I)
    found: dict[str, list[str]] = {}
    for dirpath, _, files in os.walk(root):
        for f in sorted(files):
            m = std.match(f)
            if m:
                found.setdefault(m.group(2).upper(), []).append(os.path.join(dirpath, f))
                continue
            m = aut.match(f)
            if m:
                found.setdefault(m.group(1).upper(), []).append(os.path.join(dirpath, f))
    missing = [t for t in STANDARD_TABLES + AUTHORITY_TABLES if t not in found and t not in OPTIONAL_TABLES]
    if missing:
        raise FileNotFoundError(f"G-NAF tables not found under {root}: {missing}")
    return found


def _register(engine: Engine, name: str, paths: list[str], columns: list[str]) -> None:
    """Register PSV files as a relation with upper-case string columns (missing columns -> NULL)."""
    if isinstance(engine, DuckDBEngine):
        files = "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
        engine.con.execute(
            f"CREATE OR REPLACE VIEW {name}_raw AS SELECT * FROM read_csv({files}, delim='|', header=true, "
            f"all_varchar=true, union_by_name=true, quote='')"
        )
        have = {c.upper(): c for c in engine.columns(f"{name}_raw")}
    elif isinstance(engine, SparkEngine):
        df = engine.spark.read.csv(paths, sep="|", header=True, quote="")
        df.createOrReplaceTempView(f"{name}_raw")
        have = {c.upper(): c for c in df.columns}
    else:  # pragma: no cover
        raise TypeError(engine)
    sel = ", ".join(
        f"NULLIF(trim({have[c]}), '') AS {c}" if c in have else f"CAST(NULL AS STRING) AS {c}" for c in columns
    )
    engine.materialise(name, f"SELECT {sel} FROM {name}_raw")


_COLUMNS: dict[str, list[str]] = {
    "ADDRESS_DETAIL": [
        "ADDRESS_DETAIL_PID", "DATE_RETIRED", "BUILDING_NAME", "LOT_NUMBER_PREFIX", "LOT_NUMBER",
        "LOT_NUMBER_SUFFIX", "FLAT_TYPE_CODE", "FLAT_NUMBER_PREFIX", "FLAT_NUMBER", "FLAT_NUMBER_SUFFIX",
        "LEVEL_TYPE_CODE", "LEVEL_NUMBER_PREFIX", "LEVEL_NUMBER", "LEVEL_NUMBER_SUFFIX", "NUMBER_FIRST_PREFIX",
        "NUMBER_FIRST", "NUMBER_FIRST_SUFFIX", "NUMBER_LAST_PREFIX", "NUMBER_LAST", "NUMBER_LAST_SUFFIX",
        "STREET_LOCALITY_PID", "LOCALITY_PID", "ALIAS_PRINCIPAL", "POSTCODE", "CONFIDENCE",
    ],
    "ADDRESS_ALIAS": ["PRINCIPAL_PID", "ALIAS_PID", "DATE_RETIRED"],
    "ADDRESS_DEFAULT_GEOCODE": ["ADDRESS_DETAIL_PID", "LONGITUDE", "LATITUDE", "DATE_RETIRED"],
    "STREET_LOCALITY": ["STREET_LOCALITY_PID", "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE",
                        "LOCALITY_PID", "DATE_RETIRED"],
    "STREET_LOCALITY_ALIAS": ["STREET_LOCALITY_ALIAS_PID", "STREET_LOCALITY_PID", "STREET_NAME",
                              "STREET_TYPE_CODE", "STREET_SUFFIX_CODE", "DATE_RETIRED"],
    "LOCALITY": ["LOCALITY_PID", "LOCALITY_NAME", "STATE_PID", "PRIMARY_POSTCODE", "DATE_RETIRED"],
    "LOCALITY_ALIAS": ["LOCALITY_ALIAS_PID", "LOCALITY_PID", "NAME", "POSTCODE", "DATE_RETIRED"],
    "STATE": ["STATE_PID", "STATE_NAME", "STATE_ABBREVIATION", "DATE_RETIRED"],
}
for _t in AUTHORITY_TABLES:
    _COLUMNS[_t] = ["CODE", "NAME", "DESCRIPTION"]


def load_gnaf(engine: Engine, root: str, prefix: str = "gnaf_") -> str:
    """Load a G-NAF release from ``root`` and return the name of the flattened reference table."""
    d = engine.dialect
    files = find_files(root)
    t = lambda n: f"{prefix}{n.lower()}"  # noqa: E731
    for table, cols in _COLUMNS.items():
        if table in files:
            _register(engine, t(table), files[table], cols)
        else:  # optional table absent: empty relation with the right columns
            empty = ", ".join(f"CAST(NULL AS STRING) AS {c}" for c in cols)
            engine.materialise(t(table), f"SELECT {empty} WHERE 1 = 0")

    def full(alias: str) -> str:  # the longer of CODE / NAME is the full word
        return f"CASE WHEN length({alias}.NAME) >= length({alias}.CODE) THEN {alias}.NAME ELSE {alias}.CODE END"

    def piece(*cols: str) -> str:
        return "NULLIF(concat(" + ", ".join(f"coalesce({c}, '')" for c in cols) + "), '')"

    live = "DATE_RETIRED IS NULL"
    base = f"""
SELECT ad.ADDRESS_DETAIL_PID AS pid,
       coalesce(aa.PRINCIPAL_PID, ad.ADDRESS_DETAIL_PID) AS entity_id,
       CASE WHEN aa.PRINCIPAL_PID IS NULL THEN true ELSE false END AS is_principal,
       coalesce({full('ft')}, ad.FLAT_TYPE_CODE) AS flat_type,
       {piece('ad.FLAT_NUMBER_PREFIX', 'ad.FLAT_NUMBER', 'ad.FLAT_NUMBER_SUFFIX')} AS flat,
       coalesce({full('lt')}, ad.LEVEL_TYPE_CODE) AS level_type,
       {piece('ad.LEVEL_NUMBER_PREFIX', 'ad.LEVEL_NUMBER', 'ad.LEVEL_NUMBER_SUFFIX')} AS level,
       {piece('ad.NUMBER_FIRST_PREFIX', 'ad.NUMBER_FIRST', 'ad.NUMBER_FIRST_SUFFIX')} AS number_first,
       {piece('ad.NUMBER_LAST_PREFIX', 'ad.NUMBER_LAST', 'ad.NUMBER_LAST_SUFFIX')} AS number_last,
       {piece('ad.LOT_NUMBER_PREFIX', 'ad.LOT_NUMBER', 'ad.LOT_NUMBER_SUFFIX')} AS lot,
       sl.STREET_NAME AS street_name, coalesce({full('st')}, sl.STREET_TYPE_CODE) AS street_type,
       coalesce({full('ss')}, sl.STREET_SUFFIX_CODE) AS street_suffix,
       l.LOCALITY_NAME AS locality, s.STATE_ABBREVIATION AS state, ad.POSTCODE AS postcode,
       CAST(g.LATITUDE AS DOUBLE) AS latitude, CAST(g.LONGITUDE AS DOUBLE) AS longitude,
       CAST(ad.CONFIDENCE AS INT) AS confidence, ad.STREET_LOCALITY_PID AS street_locality_pid,
       ad.LOCALITY_PID AS locality_pid
FROM (SELECT * FROM {t('ADDRESS_DETAIL')} WHERE {live}) ad
LEFT JOIN (SELECT * FROM {t('ADDRESS_ALIAS')} WHERE {live}) aa ON aa.ALIAS_PID = ad.ADDRESS_DETAIL_PID
LEFT JOIN {t('STREET_LOCALITY')} sl ON sl.STREET_LOCALITY_PID = ad.STREET_LOCALITY_PID
LEFT JOIN {t('LOCALITY')} l ON l.LOCALITY_PID = ad.LOCALITY_PID
LEFT JOIN {t('STATE')} s ON s.STATE_PID = l.STATE_PID
LEFT JOIN (SELECT * FROM {t('ADDRESS_DEFAULT_GEOCODE')} WHERE {live}) g ON g.ADDRESS_DETAIL_PID = ad.ADDRESS_DETAIL_PID
LEFT JOIN {t('FLAT_TYPE_AUT')} ft ON ft.CODE = ad.FLAT_TYPE_CODE
LEFT JOIN {t('LEVEL_TYPE_AUT')} lt ON lt.CODE = ad.LEVEL_TYPE_CODE
LEFT JOIN {t('STREET_TYPE_AUT')} st ON st.CODE = sl.STREET_TYPE_CODE
LEFT JOIN {t('STREET_SUFFIX_AUT')} ss ON ss.CODE = sl.STREET_SUFFIX_CODE"""
    engine.materialise(t("base"), base)

    variants = f"""
SELECT b.*, 'P' AS variant, b.pid AS vkey, street_name AS v_street_name, street_type AS v_street_type,
       street_suffix AS v_street_suffix, locality AS v_locality, false AS range_start FROM {t('base')} b
UNION ALL
SELECT b.*, 'LA', la.LOCALITY_ALIAS_PID, street_name, street_type, street_suffix, la.NAME, false
FROM {t('base')} b JOIN (SELECT * FROM {t('LOCALITY_ALIAS')} WHERE {live}) la ON la.LOCALITY_PID = b.locality_pid
UNION ALL
SELECT b.*, 'SA', sa.STREET_LOCALITY_ALIAS_PID, sa.STREET_NAME, coalesce({full('st')}, sa.STREET_TYPE_CODE),
       coalesce({full('ss')}, sa.STREET_SUFFIX_CODE), locality, false
FROM {t('base')} b JOIN (SELECT * FROM {t('STREET_LOCALITY_ALIAS')} WHERE {live}) sa
  ON sa.STREET_LOCALITY_PID = b.street_locality_pid
LEFT JOIN {t('STREET_TYPE_AUT')} st ON st.CODE = sa.STREET_TYPE_CODE
LEFT JOIN {t('STREET_SUFFIX_AUT')} ss ON ss.CODE = sa.STREET_SUFFIX_CODE
UNION ALL
SELECT b.*, 'R', b.pid, street_name, street_type, street_suffix, locality, true
FROM {t('base')} b WHERE b.number_last IS NOT NULL AND b.number_first IS NOT NULL"""
    engine.materialise(t("variants"), variants)

    number = """CASE WHEN number_first IS NOT NULL THEN
                  concat(number_first, CASE WHEN number_last IS NOT NULL AND NOT range_start
                                            THEN concat('-', number_last) ELSE '' END)
                WHEN lot IS NOT NULL THEN concat('LOT ', lot) END"""
    level_part = "CASE WHEN level IS NOT NULL OR level_type IS NOT NULL THEN concat_ws(' ', level_type, level) END"
    flat_part = "CASE WHEN flat IS NOT NULL OR flat_type IS NOT NULL THEN concat_ws(' ', flat_type, flat) END"
    req = d.filter(d.array_of(["flat", "number_first", "CASE WHEN number_first IS NULL THEN lot END"]),
                   "x", "x IS NOT NULL")
    opt = d.filter(d.array_of(["level", "number_last", "CASE WHEN number_first IS NOT NULL THEN lot END"]),
                   "x", "x IS NOT NULL")
    ref = f"""
SELECT concat(pid, ':', variant, ':', vkey) AS r_id, entity_id,
       concat_ws(' ', {level_part}, {flat_part}, {number}, v_street_name, v_street_type, v_street_suffix,
                 v_locality, state, postcode) AS address,
       {req} AS premise_numbers, {opt} AS premise_numbers_optional,
       postcode, state, v_street_name AS street_name,
       pid AS address_detail_pid, variant, is_principal, latitude, longitude, confidence,
       street_locality_pid, locality_pid
FROM {t('variants')}"""
    engine.materialise(t("reference"), ref)
    return t("reference")


def authority_synonyms(engine: Engine, prefix: str = "gnaf_") -> dict[str, str]:
    """CODE/NAME pairs from the loaded authority tables, for ``normalise.extra_synonyms``."""
    out: dict[str, str] = {}
    for table in AUTHORITY_TABLES:
        for r in engine.fetch(f"SELECT CODE, NAME FROM {prefix}{table.lower()} WHERE CODE IS NOT NULL AND NAME IS NOT NULL"):
            a, b = str(r["CODE"]).upper().strip(), str(r["NAME"]).upper().strip()
            if a != b and re.fullmatch("[A-Z0-9]+", a) and re.fullmatch("[A-Z0-9]+", b):
                out[a] = b
    return out


def load_reference(engine: Engine, root: str, prefix: str = "gnaf_") -> tuple[str, dict[str, Any]]:
    """Convenience: load and return (reference table name, extra_synonyms override dict)."""
    name = load_gnaf(engine, root, prefix)
    return name, {"normalise": {"extra_synonyms": authority_synonyms(engine, prefix)}}
