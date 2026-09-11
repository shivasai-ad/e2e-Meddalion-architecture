"""TC-005 + TC-403 - Verify watermarking tracks processed records correctly,
and that re-running produces the same result (idempotency).

These two ticket categories collapse to the same underlying architecture
rule in this pattern: the watermark only advances after a Bronze write
succeeds, and unchanged rows are dropped before either write happens -- so
replaying the exact same CDF range twice is a no-op, never a duplicate.
"""

from common.pipeline import run_bronze


def test_replay_with_no_new_changes_is_a_no_op(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    first = run_bronze(spark, catalog, **pipeline_kwargs)
    assert first["inserted"] == 5

    count_after_first = spark.table(names["bronze"]).count()

    second = run_bronze(spark, catalog, **pipeline_kwargs)

    assert second["ran"] is False, "watermark already caught up -- should skip, not rescan"
    assert spark.table(names["bronze"]).count() == count_after_first


def test_replay_after_a_real_change_does_not_duplicate(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)

    spark.sql(
        f"""UPDATE {names['stg']} SET status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP()
            WHERE order_id = 'ORD-1002'"""
    )
    run_bronze(spark, catalog, **pipeline_kwargs)
    count_after_update = spark.table(names["bronze"]).count()

    # Running again with no further source changes must add nothing new.
    replay = run_bronze(spark, catalog, **pipeline_kwargs)
    assert replay["ran"] is False
    assert spark.table(names["bronze"]).count() == count_after_update
