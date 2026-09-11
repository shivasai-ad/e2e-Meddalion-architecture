"""TC-104 - Verify deduplication logic.

Architecture rule checked: several changes to the same key within one
Bronze run collapse to exactly one new version (the latest), never one
version per change. See common/cdf.py::latest_per_key.
"""

from common.pipeline import run_bronze


def test_two_rapid_updates_collapse_to_one_new_version(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)

    # Two updates to the SAME key before Bronze ever runs again.
    spark.sql(
        f"""UPDATE {names['stg']} SET status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP()
            WHERE order_id = 'ORD-1004'"""
    )
    spark.sql(
        f"""UPDATE {names['stg']} SET status = 'DELIVERED', updated_at = CURRENT_TIMESTAMP()
            WHERE order_id = 'ORD-1004'"""
    )

    result = run_bronze(spark, catalog, **pipeline_kwargs)

    assert result["closed"] == 1, "one old version closed, not two"
    assert result["inserted"] == 1, "one new version inserted, not two"

    versions = spark.table(names["bronze"]).filter("order_id = 'ORD-1004'").collect()
    assert len(versions) == 2, (
        "original version + exactly one new version -- the intermediate "
        "update must not survive as its own row"
    )

    current = [v for v in versions if v["_bronze_is_current"]][0]
    assert current["status"] == "DELIVERED", "the LATEST update wins, not the intermediate one"
