"""TC-301 - Full E2E: insert a record at source -> verify it reaches Gold.

Architecture rule checked: a single new row inserted at the "source" reaches
Gold correctly, through every layer, in one pass -- proving the whole chain
holds together end-to-end, not just each layer in isolation.
"""

from common.config import schema_names
from common.pipeline import run_bronze, run_gold, run_silver


def test_new_order_reaches_gold_end_to_end(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)
    run_silver(spark, catalog, **pipeline_kwargs)
    run_gold(spark, catalog, **pipeline_kwargs)

    # 3 orders already have status=NEW in the seed data before this insert
    # (ORD-1001, ORD-1004).
    spark.sql(
        f"""INSERT INTO {names['stg']} (order_id, customer_name, status, amount, updated_at)
            VALUES ('ORD-9001', 'Grace Kim', 'NEW', 250.00, CURRENT_TIMESTAMP())"""
    )

    run_bronze(spark, catalog, **pipeline_kwargs)
    run_silver(spark, catalog, **pipeline_kwargs)
    result = run_gold(spark, catalog, **pipeline_kwargs)

    gold_schema = schema_names()["gold"]

    bronze_row = spark.table(names["bronze"]).filter("order_id = 'ORD-9001'").collect()[0]
    assert bronze_row["_bronze_is_current"] is True

    silver_row = spark.table(names["silver"]).filter("order_id = 'ORD-9001'").collect()[0]
    assert silver_row["status"] == "NEW"

    assert result["summary_total"] == result["silver_rows"]

    summary_new = (
        spark.table(f"{catalog}.{gold_schema}.orders_status_summary")
        .filter("status = 'NEW'")
        .collect()[0]
    )
    assert summary_new["order_count"] == 3, "ORD-1001 + ORD-1004 + the new ORD-9001"
