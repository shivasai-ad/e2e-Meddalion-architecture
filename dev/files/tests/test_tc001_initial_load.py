"""TC-001 - Verify initial full load from source to Bronze.

Architecture rule checked: even with no watermark yet, the very first Bronze
run must load every seed row as a new current version, with full history
columns populated correctly. See common/pipeline.py::run_bronze, the
`from_version is None` branch.
"""

from common.pipeline import run_bronze


def test_first_run_loads_all_seed_rows_as_current(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment

    result = run_bronze(spark, catalog, **pipeline_kwargs)

    assert result["inserted"] == 5, f"expected 5 new rows on first load, got {result}"
    assert result["closed"] == 0, "first load should never close anything"

    bronze = spark.table(names["bronze"])
    assert bronze.count() == 5
    assert bronze.filter("_bronze_is_current = TRUE").count() == 5
    assert bronze.filter("_bronze_valid_to IS NOT NULL").count() == 0

    row = bronze.filter("order_id = 'ORD-1001'").collect()[0]
    assert row["customer_name"] == "Alice Smith"
    assert row["status"] == "NEW"
    assert row["_bronze_hash"] is not None
