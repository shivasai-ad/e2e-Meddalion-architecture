"""TC-002 / TC-003 / TC-004 - Verify CDC captures INSERT / UPDATE / DELETE.

Architecture rules checked:
- A new row at the source becomes a new current Bronze row (TC-002).
- An updated row closes its old Bronze version (`_bronze_is_current=FALSE`,
  `_bronze_valid_to` set) and opens a new current one with the new value
  (TC-003) -- history is preserved, never overwritten in place.
- A deleted row closes its Bronze version with NO replacement current row --
  a soft delete, never erased from history (TC-004).
"""

from common.pipeline import run_bronze


def test_insert_creates_new_current_row(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)  # initial load

    spark.sql(
        f"""INSERT INTO {names['stg']} (order_id, customer_name, status, amount, updated_at)
            VALUES ('ORD-2001', 'Fatima Noor', 'NEW', 42.00, CURRENT_TIMESTAMP())"""
    )
    result = run_bronze(spark, catalog, **pipeline_kwargs)

    assert result["inserted"] == 1
    assert result["closed"] == 0
    row = spark.table(names["bronze"]).filter("order_id = 'ORD-2001'").collect()[0]
    assert row["_bronze_is_current"] is True
    assert row["status"] == "NEW"


def test_update_closes_old_opens_new_current_version(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)  # initial load

    spark.sql(
        f"""UPDATE {names['stg']} SET status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP()
            WHERE order_id = 'ORD-1001'"""
    )
    result = run_bronze(spark, catalog, **pipeline_kwargs)

    assert result["closed"] == 1
    assert result["inserted"] == 1

    versions = (
        spark.table(names["bronze"])
        .filter("order_id = 'ORD-1001'")
        .orderBy("_bronze_valid_from")
        .collect()
    )
    assert len(versions) == 2, "an update must add a new version, never overwrite the old one"
    assert versions[0]["status"] == "NEW"
    assert versions[0]["_bronze_is_current"] is False
    assert versions[0]["_bronze_valid_to"] is not None
    assert versions[1]["status"] == "SHIPPED"
    assert versions[1]["_bronze_is_current"] is True
    assert versions[1]["_bronze_valid_to"] is None


def test_delete_closes_with_no_replacement_row(spark, catalog, fresh_environment, pipeline_kwargs):
    names = fresh_environment
    run_bronze(spark, catalog, **pipeline_kwargs)  # initial load

    spark.sql(f"DELETE FROM {names['stg']} WHERE order_id = 'ORD-1005'")
    result = run_bronze(spark, catalog, **pipeline_kwargs)

    assert result["closed"] == 1
    assert result["inserted"] == 0, "a delete must never produce a new current row"

    versions = spark.table(names["bronze"]).filter("order_id = 'ORD-1005'").collect()
    assert len(versions) == 1, "the historical row must still exist -- soft delete, not erased"
    assert versions[0]["_bronze_is_current"] is False
    assert versions[0]["_bronze_valid_to"] is not None

    current = spark.table(names["bronze"]).filter(
        "order_id = 'ORD-1005' AND _bronze_is_current = TRUE"
    )
    assert current.count() == 0
