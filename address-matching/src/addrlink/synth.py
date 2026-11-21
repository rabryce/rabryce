"""Synthetic G-NAF-like reference + corrupted queries with known truth (SPEC.md §11.1).

The reference imitates G-NAF's structure: localities with shared postcodes,
popular street names repeated across localities, ranges, suffixes, lots, unit
buildings (including dense towers that make bigram blocking hard), levels and
locality aliases. Queries are "customer-entered" renderings of reference
addresses with logged corruptions (C1-C8), plus negatives whose true address is
withheld from the reference, and PO boxes. Fully deterministic for a seed.

No real addresses are used; names are generated from syllables plus a pool of
common street names.
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass, field
from typing import Any

STATES = {
    "NSW": ("NEW SOUTH WALES", 2000, 2999, 0.32),
    "VIC": ("VICTORIA", 3000, 3999, 0.26),
    "QLD": ("QUEENSLAND", 4000, 4999, 0.20),
    "WA": ("WESTERN AUSTRALIA", 6000, 6999, 0.10),
    "SA": ("SOUTH AUSTRALIA", 5000, 5999, 0.07),
    "TAS": ("TASMANIA", 7000, 7999, 0.02),
    "ACT": ("AUSTRALIAN CAPITAL TERRITORY", 2600, 2620, 0.02),
    "NT": ("NORTHERN TERRITORY", 800, 899, 0.01),
}
STREET_TYPES = [  # (full, abbreviation, weight)
    ("STREET", "ST", 30), ("ROAD", "RD", 18), ("AVENUE", "AVE", 10), ("DRIVE", "DR", 9), ("COURT", "CT", 8),
    ("PLACE", "PL", 7), ("CRESCENT", "CRES", 5), ("CLOSE", "CL", 4), ("PARADE", "PDE", 2), ("LANE", "LN", 3),
    ("WAY", "WAY", 3), ("HIGHWAY", "HWY", 1), ("TERRACE", "TCE", 2), ("CIRCUIT", "CCT", 2), ("GROVE", "GR", 2),
    ("BOULEVARD", "BVD", 1),
]
COMMON_STREETS = [
    "GEORGE", "KING", "QUEEN", "VICTORIA", "CHURCH", "HIGH", "MAIN", "PARK", "STATION", "RAILWAY", "WILLIAM",
    "ELIZABETH", "JOHN", "PRINCES", "MARKET", "BRIDGE", "MILL", "SCHOOL", "HILL", "LAKE", "OCEAN", "BEACH", "RIVER",
    "FOREST", "GARDEN", "CHARLES", "EDWARD", "ALBERT", "MARY", "ANN",
]
SYL_A = ["BAL", "KEN", "WAR", "MOR", "GLEN", "ASH", "BRI", "CAR", "DAR", "ELL", "FERN", "HAR", "KIL", "LAN", "MAR",
         "NOR", "PEN", "ROS", "TAR", "WIL", "YAR", "BEL", "COL", "DUN", "HAW", "KOO", "MUL", "TOO", "WOL", "BUR"]
SYL_B = ["TON", "WOOD", "VALE", "FIELD", "DALE", "BURN", "LEA", "MONT", "BROOK", "HILL", "GROVE", "LANDS", "RIDGE",
         "BANK", "WORTH", "FORD", "STON", "ARA", "OOKA", "INDI", "ONG", "ERRA", "ULLA", "AWA", "INA"]
LOC_PREFIX = ["NORTH ", "SOUTH ", "EAST ", "WEST ", "MOUNT ", "ST ", "PORT ", "UPPER ", "GLEN ", "NEW "]
LOC_SUFFIX = [" NORTH", " SOUTH", " EAST", " WEST", " HEIGHTS", " BEACH", " PARK", " VALE"]


@dataclass
class SynthConfig:
    n_localities: int = 600
    target_addresses: int = 60_000
    n_queries: int = 6_000
    negative_rate: float = 0.05
    po_box_rate: float = 0.01
    seed: int = 7
    # corruption probabilities (per query)
    p_street_type_abbrev: float = 0.55
    p_street_type_missing: float = 0.08
    p_locality_missing: float = 0.04
    p_locality_alias: float = 0.30
    p_locality_neighbour: float = 0.02
    p_state_full: float = 0.08
    p_state_missing: float = 0.10
    p_state_doubled: float = 0.03
    p_postcode_missing: float = 0.08
    p_postcode_wrong: float = 0.02
    p_postcode_before_state: float = 0.05
    p_typo: float = 0.12
    p_digit_transpose: float = 0.01
    p_lowercase: float = 0.40
    p_noise_prefix: float = 0.02
    p_country_suffix: float = 0.03
    p_glue_number: float = 0.02
    p_level_omitted: float = 0.50
    extra: dict[str, Any] = field(default_factory=dict)


def _name(rng: random.Random) -> str:
    return rng.choice(SYL_A) + rng.choice(SYL_B)


def generate(cfg: SynthConfig | None = None) -> tuple[list[dict], list[dict]]:
    """Return (reference_rows, query_rows)."""
    cfg = cfg or SynthConfig()
    rng = random.Random(cfg.seed)

    # ------------------------------------------------------------ localities
    localities: list[dict] = []
    used: set[str] = set()
    states = list(STATES)
    weights = [STATES[s][3] for s in states]
    for i in range(cfg.n_localities):
        st = rng.choices(states, weights)[0]
        while True:
            nm = _name(rng)
            r = rng.random()
            if r < 0.12:
                nm = rng.choice(LOC_PREFIX) + nm
            elif r < 0.20:
                nm = nm + rng.choice(LOC_SUFFIX)
            if (nm, st) not in used:
                used.add((nm, st))  # type: ignore[arg-type]
                break
        lo, hi = STATES[st][1], STATES[st][2]
        pc = rng.randint(lo, hi)
        localities.append(dict(id=i, name=nm, state=st, postcode=f"{pc:04d}",
                               alias=_name(rng) if rng.random() < 0.03 else None))
    by_pc: dict[str, list[int]] = {}
    for loc in localities:  # make postcodes shared by nearby localities
        by_pc.setdefault(loc["postcode"], []).append(loc["id"])
    for loc in localities:
        if rng.random() < 0.35:
            same_state = [x for x in localities if x["state"] == loc["state"] and x["id"] != loc["id"]]
            if same_state:
                loc["postcode"] = rng.choice(same_state)["postcode"]
    by_pc = {}
    for loc in localities:
        by_pc.setdefault(loc["postcode"], []).append(loc["id"])

    # ------------------------------------------------------------ streets + addresses
    st_types, st_w = [t[:2] for t in STREET_TYPES], [t[2] for t in STREET_TYPES]
    street_pool = COMMON_STREETS + [_name(rng) for _ in range(800)]
    pool_w = [40.0 / (i + 1) ** 0.6 if i < len(COMMON_STREETS) else 1.0 for i in range(len(street_pool))]
    entities: list[dict] = []
    streets: list[dict] = []
    per_loc = max(1, cfg.target_addresses // cfg.n_localities)
    eid = 0
    for loc in localities:
        n_left = int(rng.expovariate(1 / per_loc)) + 5
        names_here: set[tuple[str, str]] = set()
        while n_left > 0:
            sname = rng.choices(street_pool, pool_w)[0]
            stype = rng.choices(st_types, st_w)[0]
            if (sname, stype[0]) in names_here:
                continue
            names_here.add((sname, stype[0]))
            suffix = rng.choice(["NORTH", "SOUTH", "EAST", "WEST"]) if rng.random() < 0.02 else None
            street = dict(id=len(streets), name=sname, type=stype, suffix=suffix, loc=loc)
            streets.append(street)
            n_numbers = min(n_left, int(rng.expovariate(1 / 25)) + 2)
            rural = stype[0] in ("ROAD", "HIGHWAY") and rng.random() < 0.3
            numbers = sorted(rng.sample(range(1, 400), min(n_numbers, 399)))
            for num in numbers:
                base = dict(street=street, number_first=str(num), number_last=None, lot=None,
                            flat_type=None, flat=None, level=None, building=f"{street['id']}|{num}")
                r = rng.random()
                if rural and r < 0.25:
                    base.update(number_first=None, lot=str(num), building=f"{street['id']}|L{num}")
                elif r < 0.03:
                    base["number_last"] = str(num + 2 * rng.randint(1, 3))
                elif r < 0.06:
                    base["number_first"] = f"{num}{rng.choice('ABC')}"
                    base["building"] = f"{street['id']}|{base['number_first']}"
                if not rural and rng.random() < 0.08:  # unit building
                    dense = rng.random() < 0.10
                    k = rng.randint(60, 300) if dense else rng.randint(2, 16)
                    ftype = rng.choices(["UNIT", "APARTMENT", "FLAT", "SHOP"], [80, 10, 7, 3])[0]
                    levels = dense and rng.random() < 0.6
                    if rng.random() < 0.7:
                        entities.append(dict(base, dense=dense))
                    for u in range(1, k + 1):
                        unit = dict(base, flat_type=ftype, dense=dense)
                        if levels:
                            lvl = (u - 1) // 10 + 1
                            unit.update(level=str(lvl), flat=str(lvl * 100 + (u - 1) % 10 + 1))
                        else:
                            unit["flat"] = str(u)
                        entities.append(unit)
                else:
                    entities.append(dict(base, dense=False))
            n_left -= n_numbers
    for e in entities:
        e["entity_id"] = f"GA{eid:09d}"
        eid += 1

    # ------------------------------------------------------------ negatives (withheld)
    n_neg = int(cfg.n_queries * cfg.negative_rate)
    unit_ents = [e for e in entities if e["flat"]]
    other_ents = [e for e in entities if not e["flat"]]
    withheld: list[dict] = rng.sample(unit_ents, min(len(unit_ents), n_neg // 2))
    withheld += rng.sample(other_ents, min(len(other_ents), n_neg - len(withheld)))
    withheld_ids = {e["entity_id"] for e in withheld}

    # ------------------------------------------------------------ reference rows
    ref_rows: list[dict] = []
    for e in entities:
        if e["entity_id"] in withheld_ids:
            continue
        loc = e["street"]["loc"]
        variants = [("P", loc["name"], False)] + ([("A", loc["alias"], False)] if loc["alias"] else [])
        if e["number_last"]:  # G-NAF-style range: also index the range start alone
            variants.append(("R", loc["name"], True))
        # Required premise numbers: flat, street number / lot. Level and range end are optional:
        # customers routinely omit them, and the flat number already identifies the unit.
        req = [x for x in (e["flat"], e["number_first"], e["lot"]) if x]
        opt = [x for x in (e["level"], e["number_last"]) if x]
        for vtag, locname, range_start in variants:
            ref_rows.append(dict(
                r_id=f"{e['entity_id']}{vtag}", entity_id=e["entity_id"],
                address=_render_ref(e, locname, range_start),
                premise_numbers=req, premise_numbers_optional=opt,
                postcode=loc["postcode"], state=loc["state"], street_name=e["street"]["name"],
                building_key=e["building"],
                segment=_segment(e),
            ))

    # ------------------------------------------------------------ queries
    queries: list[dict] = []
    n_pos = cfg.n_queries - len(withheld) - int(cfg.n_queries * cfg.po_box_rate)
    pos_ents = rng.sample([e for e in entities if e["entity_id"] not in withheld_ids], n_pos)
    qid = 0
    for e, kind in [(e, "positive") for e in pos_ents] + [(e, "negative") for e in withheld]:
        addr, corr = _render_query(e, rng, cfg, localities, by_pc)
        queries.append(dict(
            id=f"Q{qid:07d}", address=addr, kind=kind,
            true_entity_id=e["entity_id"] if kind == "positive" else None,
            true_building_key=e["building"], segment=_segment(e), corruptions=corr,
        ))
        qid += 1
    for _ in range(int(cfg.n_queries * cfg.po_box_rate)):
        loc = rng.choice(localities)
        box = rng.choice(["PO BOX", "P.O. Box", "GPO Box", "Locked Bag", "PO Box"])
        queries.append(dict(
            id=f"Q{qid:07d}", address=f"{box} {rng.randint(1, 9999)}, {loc['name'].title()} {loc['state']} {loc['postcode']}",
            kind="non_physical", true_entity_id=None, true_building_key=None, segment="PO_BOX", corruptions=[],
        ))
        qid += 1
    rng.shuffle(queries)
    return ref_rows, queries


def _segment(e: dict) -> str:
    if e["flat"]:
        return "DENSE_UNIT" if e.get("dense") else "UNIT"
    if e["lot"]:
        return "LOT"
    if e["number_last"]:
        return "RANGE"
    return "HOUSE"


def _number(e: dict, range_start: bool = False) -> str:
    if e["lot"]:
        return f"LOT {e['lot']}"
    if e["number_last"] and not range_start:
        return f"{e['number_first']}-{e['number_last']}"
    return e["number_first"]


def _render_ref(e: dict, locname: str, range_start: bool = False) -> str:
    """Canonical rendering. LEVEL goes before the flat so that the flat number and the
    street number stay adjacent ("UNIT 201 345 ..."), which is how customers write them
    and what the bigram blocking keys rely on."""
    s, loc = e["street"], e["street"]["loc"]
    parts = []
    if e["level"]:
        parts.append(f"LEVEL {e['level']}")
    if e["flat"]:
        parts.append(f"{e['flat_type']} {e['flat']}")
    parts += [_number(e, range_start), s["name"], s["type"][0]]
    if s["suffix"]:
        parts.append(s["suffix"])
    parts += [locname, loc["state"], loc["postcode"]]
    return " ".join(parts)


def _typo(word: str, rng: random.Random) -> str:
    if len(word) < 4:
        return word
    i = rng.randrange(1, len(word) - 1)
    op = rng.choice(["del", "ins", "sub", "swap"])
    letters = "ABCDEFGHIJKLMNOPRSTUVW"
    if op == "del":
        return word[:i] + word[i + 1:]
    if op == "ins":
        return word[:i] + rng.choice(letters) + word[i:]
    if op == "sub":
        return word[:i] + rng.choice(letters) + word[i + 1:]
    return word[:i] + word[i + 1] + word[i] + word[i + 2:]


def _transpose_digits(n: str, rng: random.Random) -> str:
    digits = [c for c in n if c.isdigit()]
    if len(digits) < 2 or len(set(digits)) < 2:
        return n
    for _ in range(5):
        i = rng.randrange(len(n) - 1)
        if n[i].isdigit() and n[i + 1].isdigit() and n[i] != n[i + 1]:
            return n[:i] + n[i + 1] + n[i] + n[i + 2:]
    return n


def _render_query(e: dict, rng: random.Random, cfg: SynthConfig, localities: list[dict],
                  by_pc: dict[str, list[int]]) -> tuple[str, list[str]]:
    corr: list[str] = []
    s, loc = e["street"], e["street"]["loc"]
    flat, number_first = e["flat"], e["number_first"]
    if rng.random() < cfg.p_digit_transpose:
        if flat and len(flat) > 1:
            flat2 = _transpose_digits(flat, rng)
            if flat2 != flat:
                flat, _ = flat2, corr.append("C5_UNIT_TRANSPOSED")
        elif number_first and len(number_first) > 1:
            n2 = _transpose_digits(number_first, rng)
            if n2 != number_first:
                number_first, _ = n2, corr.append("C5_NUMBER_TRANSPOSED")
    num = f"LOT {e['lot']}" if e["lot"] else (
        f"{number_first}-{e['number_last']}" if e["number_last"] and rng.random() < 0.6 else number_first)
    if e["number_last"] and "-" not in (num or ""):
        corr.append("C2_RANGE_END_DROPPED")

    # street name / type
    sname = s["name"]
    if rng.random() < cfg.p_typo:
        sname2 = _typo(sname, rng)
        if sname2 != sname:
            sname, _ = sname2, corr.append("C4_TYPO_STREET")
    r = rng.random()
    if r < cfg.p_street_type_missing:
        stype, _ = "", corr.append("C2_STREET_TYPE_MISSING")
    elif r < cfg.p_street_type_missing + cfg.p_street_type_abbrev:
        stype = s["type"][1]
        if stype != s["type"][0]:
            corr.append("C1_STREET_TYPE_ABBREV")
    else:
        stype = s["type"][0]
    street = " ".join(x for x in (sname, stype, s["suffix"] or "") if x)

    # unit / level notation
    premise = num or ""
    if flat:
        style = rng.choices(["formal", "slash", "glued", "apt", "comma"], [30, 40, 10, 10, 10])[0]
        ftype = e["flat_type"]
        if style == "slash":
            premise = f"{flat}/{num}"
        elif style == "glued":
            premise = f"U{flat} {num}"
        elif style == "apt":
            premise = f"APT {flat} {num}"
        elif style == "comma":
            premise = f"{ftype.title()} {flat}, {num}"
        else:
            premise = f"{ftype} {flat} {num}"
        corr.append(f"C3_UNIT_{style.upper()}")
        if e["level"] and rng.random() >= cfg.p_level_omitted:
            lv = rng.choice(["LEVEL", "L", "LVL"])
            premise = f"{lv} {e['level']}, {premise}"
            corr.append("C1_LEVEL_INCLUDED")
    if rng.random() < cfg.p_glue_number and premise and premise[-1].isdigit():
        premise_street = f"{premise}{street}"
        corr.append("C8_GLUED_NUMBER")
    else:
        premise_street = f"{premise} {street}".strip()

    # locality
    locname = loc["name"]
    r = rng.random()
    if loc["alias"] and rng.random() < cfg.p_locality_alias:
        locname, _ = loc["alias"], corr.append("C6_LOCALITY_ALIAS")
    elif r < cfg.p_locality_missing:
        locname, _ = "", corr.append("C2_LOCALITY_MISSING")
    elif r < cfg.p_locality_missing + cfg.p_locality_neighbour:
        nbrs = [localities[i] for i in by_pc.get(loc["postcode"], []) if i != loc["id"]]
        if nbrs:
            locname, _ = rng.choice(nbrs)["name"], corr.append("C6_LOCALITY_NEIGHBOUR")
    if locname and rng.random() < cfg.p_typo / 2:
        l2 = _typo(locname, rng)
        if l2 != locname:
            locname, _ = l2, corr.append("C4_TYPO_LOCALITY")

    # state / postcode
    state = loc["state"]
    r = rng.random()
    if r < cfg.p_state_missing:
        state, _ = "", corr.append("C2_STATE_MISSING")
    elif r < cfg.p_state_missing + cfg.p_state_full:
        state, _ = STATES[loc["state"]][0], corr.append("C1_STATE_FULL")
    elif r < cfg.p_state_missing + cfg.p_state_full + cfg.p_state_doubled:
        state, _ = f"{state} {state}", corr.append("C7_STATE_DOUBLED")
    pc = loc["postcode"]
    r = rng.random()
    if r < cfg.p_postcode_missing:
        pc, _ = "", corr.append("C2_POSTCODE_MISSING")
    elif r < cfg.p_postcode_missing + cfg.p_postcode_wrong:
        pc, _ = f"{(int(pc) + rng.choice([-1, 1])) % 10000:04d}", corr.append("C5_POSTCODE_WRONG")
    if pc and state and rng.random() < cfg.p_postcode_before_state:
        tail, _ = f"{pc} {state}", corr.append("C8_POSTCODE_FIRST")
    else:
        tail = " ".join(x for x in (state, pc) if x)

    sep = rng.choice([", ", " ", ", "])
    addr = sep.join(x for x in (premise_street, locname, tail) if x)
    if rng.random() < cfg.p_noise_prefix:
        addr, _ = "C/O J SMITH, " + addr, corr.append("C7_NOISE_PREFIX")
    if rng.random() < cfg.p_country_suffix:
        addr, _ = addr + ", Australia", corr.append("C7_COUNTRY_SUFFIX")
    if rng.random() < cfg.p_lowercase:
        addr, _ = addr.title(), corr.append("C7_MIXED_CASE")
    return addr, corr


def write(ref_rows: list[dict], queries: list[dict], out_dir: str) -> tuple[str, str]:
    """Write reference.parquet / queries.parquet via DuckDB (no pandas/pyarrow needed)."""
    import duckdb

    os.makedirs(out_dir, exist_ok=True)
    con = duckdb.connect()
    paths = []
    for name, rows in (("reference", ref_rows), ("queries", queries)):
        jl = os.path.join(out_dir, f"{name}.jsonl")
        with open(jl, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        pq = os.path.join(out_dir, f"{name}.parquet")
        con.execute(f"COPY (SELECT * FROM read_json_auto('{jl}', format='newline_delimited')) TO '{pq}' (FORMAT PARQUET)")
        os.remove(jl)
        paths.append(pq)
    return paths[0], paths[1]
