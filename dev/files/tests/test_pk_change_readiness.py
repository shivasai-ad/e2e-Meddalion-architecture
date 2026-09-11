"""PK-change readiness check -- verifies common/pk_migration.py catches the
real risk of widening/changing a primary key: a new key column that exists
on Bronze but has NULL on historical current rows, which would silently stop
matching future changes if the config were flipped without a backfill.

Walks the exact workflow notebooks/92_pk_change_readiness_check.py documents:
missing column -> present but NULL on current rows -> backfilled -> safe.
"""

from common.pipeline import run_bronze
from common.pk_migration import check_key_change_readiness


def test_readiness_report_through_a_full_pk_widening_cycle(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)  # initial load, 5 seed rows

    # Unchanged key is always safe -- order_id is NOT NULL and already the key.
    report = check_key_change_readiness(spark, names["bronze"], ["order_id"])
    assert report["safe"] is True
    assert report["missing_columns"] == []
    assert report["null_key_rows"] == {}

    # 1. Candidate key column doesn't exist anywhere yet.
    report = check_key_change_readiness(spark, names["bronze"], ["order_id", "region_id"])
    assert report["safe"] is False
    assert report["missing_columns"] == ["region_id"]

    try:
        # Add it to staging and let one new row carry it through -- Bronze's
        # mergeSchema (common/pipeline.py::run_bronze) widens Bronze automatically.
        spark.sql(f"ALTER TABLE {names['stg']} ADD COLUMN region_id STRING")
        spark.sql(
            f"""INSERT INTO {names['stg']} (order_id, customer_name, status, amount, updated_at, region_id)
                VALUES ('ORD-3001', 'New Region Row', 'NEW', 10.00, CURRENT_TIMESTAMP(), 'EU')"""
        )
        run_bronze(spark, catalog, **pipeline_kwargs)

        # 2. Column exists now, but the 5 original seed rows never got a value.
        report = check_key_change_readiness(spark, names["bronze"], ["order_id", "region_id"])
        assert report["safe"] is False
        assert report["missing_columns"] == []
        assert report["null_key_rows"] == {"region_id": 5}

        # Backfill in staging (never Bronze directly), then let it flow through
        # the normal SCD2 path as an ordinary UPDATE.
        spark.sql(
            f"""UPDATE {names['stg']} SET region_id = 'UNKNOWN', updated_at = CURRENT_TIMESTAMP()
                WHERE region_id IS NULL"""
        )
        run_bronze(spark, catalog, **pipeline_kwargs)

        # 3. Every current row now has a non-NULL region_id -- safe to flip the config.
        report = check_key_change_readiness(spark, names["bronze"], ["order_id", "region_id"])
        assert report["safe"] is True
    finally:
        # reset_environment (fresh_environment) TRUNCATEs data, never
        # schema -- a test-added column would otherwise persist forever on
        # this shared catalog and leak into every later test/manual run.
        spark.sql(f"ALTER TABLE {names['stg']} DROP COLUMN IF EXISTS region_id")
        spark.sql(f"ALTER TABLE {names['bronze']} DROP COLUMN IF EXISTS region_id")
