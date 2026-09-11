"""Column removal -- the scenario test_schema_audit_log.py's ADD case
doesn't cover. Tested behavior: when a source column is dropped, the
pipeline now raises RuntimeError with halt_on_drop=True (after logging the
drop to schema_audit_log for diagnostics). The raise happens early, before
the DELTA_MERGE_UNRESOLVED_EXPRESSION crash that would otherwise occur when
MERGE tries to UPDATE SET * against a source that's missing columns the
target still has.
"""

from common.config import schema_names
from common.pipeline import run_bronze, run_silver


def test_column_dropped_from_bronze_is_dropped_from_silver_not_a_crash(spark, catalog, fresh_environment, pipeline_kwargs):
    audit_schema = schema_names()["audit"]
    audit_log_fqn = f"{catalog}.{audit_schema}.schema_audit_log"
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)
    run_silver(spark, catalog, **pipeline_kwargs)

    try:
        # Get a real column onto both Bronze and Silver first, so there's
        # something genuine to drop afterward.
        spark.sql(f"ALTER TABLE {names['stg']} ADD COLUMN priority STRING")
        spark.sql(f"UPDATE {names['stg']} SET priority = 'HIGH', updated_at = CURRENT_TIMESTAMP()")
        run_bronze(spark, catalog, **pipeline_kwargs)
        run_silver(spark, catalog, **pipeline_kwargs)
        assert "priority" in [f.name for f in spark.table(names["silver"]).schema.fields]

        # The column disappears from Bronze -- now halt_on_drop=True raises
        # RuntimeError after logging the drop to schema_audit_log.
        spark.sql(f"ALTER TABLE {names['bronze']} DROP COLUMN priority")
        try:
            run_silver(spark, catalog, **pipeline_kwargs)
            assert False, "run_silver should have raised RuntimeError for dropped column"
        except RuntimeError as e:
            assert "dropped" in str(e).lower(), f"Expected drop-related error, got: {e}"
            assert "priority" in str(e), f"Expected column name in error, got: {e}"

        # Column drop must be logged to schema_audit_log for diagnostics
        # (even though the run halted).
        drop_events = [
            r for r in spark.table(audit_log_fqn).collect()
            if r["change_type"] == "COLUMN_DROPPED" and r["column_name"] == "priority"
        ]
        assert len(drop_events) == 1, "the drop itself must be logged by schema_audit, on the silver_orders watch"
    finally:
        spark.sql(f"ALTER TABLE {names['stg']} DROP COLUMN IF EXISTS priority")
        spark.sql(f"ALTER TABLE {names['bronze']} DROP COLUMN IF EXISTS priority")
        spark.sql(f"ALTER TABLE {names['silver']} DROP COLUMN IF EXISTS priority")
