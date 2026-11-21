import shutil

import pytest

from addrlink.synth import SynthConfig, generate

REF_COLS = ["r_id", "entity_id", "address", "premise_numbers", "premise_numbers_optional", "postcode", "state",
            "street_name"]


@pytest.fixture(scope="session")
def small_synth():
    ref, qs = generate(SynthConfig(target_addresses=3000, n_localities=50, n_queries=400, seed=11))
    ref_in = [{k: r[k] for k in REF_COLS} for r in ref]
    q_in = [{"id": q["id"], "address": q["address"]} for q in qs]
    return ref, qs, ref_in, q_in


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java not available")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
    del pyspark
