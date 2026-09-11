"""Schema-change audit log -- verifies common/schema_audit.py detects and
records column-level drift (added / dropped / type-changed) on staging,
Bronze, AND Silver, wired into common/pipeline.py's run_bronze/run_silver so
no separate job is needed to catch it.

Also the first test to actually exercise Silver's schema-evolution path
(the ALTER-TABLE-widen-before-MERGE step added when run_silver switched from
CREATE OR REPLACE to an incremental MERGE) -- confirms a new column both
gets added to Silver's schema AND carries real data through, not just a
schema-only no-op.
"""

from common.config import schema_names
from common.pipeline import run_bronze, run_silver


def test_new_column_propagates_through_bronze_silver_and_is_logged(spark, catalog, fresh_environment, pipeline_kwargs):
    audit_schema = schema_names()["audit"]
    audit_log_fqn = f"{catalog}.{audit_schema}.schema_audit_log"
    names = fresh_environment

    run_bronze(spark, catalog, **pipeline_kwargs)
    run_silver(spark, catalog, **pipeline_kwargs)
    assert spark.table(audit_log_fqn).count() == 0, "first-ever run has nothing to diff against"

    try:
        spark.sql(f"ALTER TABLE {names['stg']} ADD COLUMN priority STRING")
        spark.sql(
            f"""INSERT INTO {names['stg']} (order_id, customer_name, status, amount, updated_at, priority)
                VALUES ('ORD-4001', 'Priority Row', 'NEW', 5.00, CURRENT_TIMESTAMP(), 'HIGH')"""
        )
        run_bronze(spark, catalog, **pipeline_kwargs)  # staging drift seen at the start, bronze drift at the end
        run_silver(spark, catalog, **pipeline_kwargs)  # silver widened via ALTER TABLE, then MERGEd

        rows = spark.table(audit_log_fqn).collect()
        assert len(rows) == 3, f"expected one logged change each for staging, bronze, and silver, got {rows}"

        staging_change = next(r for r in rows if r["schema"].endswith("_stg"))
        assert staging_change["change_type"] == "COLUMN_ADDED"
        assert staging_change["column_name"] == "priority"
        assert staging_change["old_type"] is None
        assert staging_change["new_type"] == "string"

        bronze_change = next(r for r in rows if r["schema"].endswith("_bronze"))
        assert bronze_change["change_type"] == "COLUMN_ADDED"
        assert bronze_change["column_name"] == "priority"

        silver_change = next(r for r in rows if r["schema"].endswith("_silver"))
        assert silver_change["change_type"] == "COLUMN_ADDED"
        assert silver_change["column_name"] == "priority"

        # Confirm it landed with real data in Silver, not just a schema-only change.
        new_row = spark.table(names["silver"]).filter("order_id = 'ORD-4001'").collect()[0]
        assert new_row["priority"] == "HIGH"
        # And a pre-existing row correctly got NULL, not left stale/missing.
        old_row = spark.table(names["silver"]).filter("order_id = 'ORD-1001'").collect()[0]
        assert old_row["priority"] is None

        # Running again with no further schema change must add nothing new.
        run_bronze(spark, catalog, **pipeline_kwargs)
        run_silver(spark, catalog, **pipeline_kwargs)
        assert spark.table(audit_log_fqn).count() == len(rows), "no new drift -- no new rows"
    finally:
        # reset_environment (fresh_environment) TRUNCATEs data, never
        # schema -- a test-added column would otherwise persist forever on
        # this shared catalog and leak into every later test/manual run.
        for fqn in (names["stg"], names["bronze"], names["silver"]):
            spark.sql(f"ALTER TABLE {fqn} DROP COLUMN IF EXISTS priority")
