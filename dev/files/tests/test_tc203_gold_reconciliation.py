"""TC-203 - Verify row counts match expected after aggregation.

Architecture rule checked: Gold's business aggregation must account for
every current Silver row exactly once -- no rows dropped, none double
counted.
"""

from common.pipeline import run_bronze, run_gold, run_silver


def test_status_summary_counts_reconcile_with_silver(spark, catalog, fresh_environment, pipeline_kwargs):
    run_bronze(spark, catalog, **pipeline_kwargs)
    run_silver(spark, catalog, **pipeline_kwargs)
    result = run_gold(spark, catalog, **pipeline_kwargs)

    assert result["summary_total"] == result["silver_rows"], (
        f"orders_status_summary counts ({result['summary_total']}) must add up "
        f"to Silver's current row count ({result['silver_rows']})"
    )
