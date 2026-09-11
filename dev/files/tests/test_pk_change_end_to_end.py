"""PK change, end to end -- not just the readiness check (test_pk_change_readiness.py),
but an actual run under a widened composite key, proving Bronze's MERGE `ON`
clause and Silver's MERGE `ON` clause both pick up `ENTITY_CONFIG["keys"]"]`
fresh on every call, with zero code change beyond the config value itself.
"""

from common import config
from common.pipeline import run_bronze, run_silver


def test_widened_composite_key_flows_through_bronze_and_silver(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)

    try:
        # Backfill BEFORE widening the key -- exactly what
        # notebooks/92_pk_change_readiness_check.py tells you to do, and what
        # test_pk_change_readiness.py already proves is required for safety.
        spark.sql(f"ALTER TABLE {names['stg']} ADD COLUMN region_id STRING")
        spark.sql(f"UPDATE {names['stg']} SET region_id = 'EU', updated_at = CURRENT_TIMESTAMP()")
        run_bronze(spark, catalog, **pipeline_kwargs)

        original_keys = list(config.ENTITY_CONFIG["keys"])
        config.ENTITY_CONFIG["keys"] = ["order_id", "region_id"]
        try:
            # A genuine change under the new composite key -- if the MERGE
            # `ON` clauses didn't pick up the new keys, this would either
            # fail to match (creating a bogus duplicate "NEW" row instead of
            # a CHANGED one) or crash outright.
            spark.sql(
                f"""UPDATE {names['stg']} SET status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP()
                    WHERE order_id = 'ORD-1001'"""
            )
            result = run_bronze(spark, catalog, **pipeline_kwargs)
            assert result["closed"] == 1, "the update must close the old version, not create a stray duplicate"
            assert result["inserted"] == 1

            versions = (
                spark.table(names["bronze"]).filter("order_id = 'ORD-1001'")
                .orderBy("_bronze_valid_from").collect()
            )
            # 3, not 2: (1) initial ingest, (2) closed when the region_id
            # backfill above changed its hash (every order's hash changes
            # then, not just ORD-1001's), (3) the new current row from this
            # composite-key status change.
            assert len(versions) == 3, "one from initial load, one from the region_id backfill, one from the composite-key status change"

            silver_result = run_silver(spark, catalog, **pipeline_kwargs)
            assert silver_result["silver_rows"] == silver_result["bronze_current"]

            silver_row = spark.table(names["silver"]).filter("order_id = 'ORD-1001'").collect()[0]
            assert silver_row["status"] == "SHIPPED"
            assert silver_row["region_id"] == "EU"
        finally:
            config.ENTITY_CONFIG["keys"] = original_keys
    finally:
        spark.sql(f"ALTER TABLE {names['stg']} DROP COLUMN IF EXISTS region_id")
        # Bronze is Liquid Clustered on ENTITY_CONFIG["keys"] (see
        # ensure_environment) -- while the key was widened above, region_id
        # became a clustering column, and Delta refuses to DROP COLUMN a
        # column that's still a clustering key. Reset clustering back to the
        # original key(s) first so the drop below actually succeeds, instead
        # of raising mid-cleanup and leaving region_id stranded on Bronze for
        # the next test to trip over.
        spark.sql(f"ALTER TABLE {names['bronze']} CLUSTER BY ({', '.join(original_keys)})")
        spark.sql(f"ALTER TABLE {names['bronze']} DROP COLUMN IF EXISTS region_id")
        spark.sql(f"ALTER TABLE {names['silver']} DROP COLUMN IF EXISTS region_id")
