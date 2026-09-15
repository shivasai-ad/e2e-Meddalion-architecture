# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Test: Schema drift detection (`common/schema_audit.py`)
# MAGIC
# MAGIC Exercises `schema_audit.detect_drift_against_config` directly -- the
# MAGIC config-vs-live-schema comparator Gold uses (see `notebooks/published/gold.py`,
# MAGIC `process_gold_entity`) -- against a throwaway table, so this test never
# MAGIC touches the real `orders` pipeline tables and can be re-run freely.
# MAGIC
# MAGIC Two scenarios:
# MAGIC 1. **Regression check**: a genuinely dropped column (one the config still
# MAGIC    expects) is reported as `COLUMN_DROPPED` and halts with `RuntimeError`
# MAGIC    when `halt_on_drop=True`, and gets logged to `schema_audit_log`.
# MAGIC 2. **The new fix**: `total_tax_amount` is declared in
# MAGIC    `gold_compliance_metadata.yml` for `orders` but is a `CALCULATED_COLUMNS`
# MAGIC    entry in `gold.py` -- it is never expected to exist in Silver. Confirms
# MAGIC    that excluding it from the expected-columns list (`gold_source_columns`,
# MAGIC    exactly as `process_gold_entity` builds it) does NOT raise a false
# MAGIC    `COLUMN_DROPPED`, while still checking every other Gold column.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import get_catalog, get_schemas, get_use_external_tables, get_s3_bucket
from common.gold_compliance_validator import load_compliance_config
from common import schema_audit

CATALOG = get_catalog()
SCHEMAS = get_schemas()
USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None

COMPLIANCE_CONFIG = load_compliance_config(os.path.join(PROJECT_ROOT, "config/gold_compliance_metadata.yml"))
ORDERS_ENTRY = COMPLIANCE_CONFIG["orders"]

# Same CALCULATED_COLUMNS gold.py defines -- kept in sync manually since this
# notebook doesn't import gold.py itself (importing it would trigger its
# dbutils widget-driven ALL-entity run as a side effect). If gold.py's
# CALCULATED_COLUMNS ever changes, update this line to match.
CALCULATED_COLUMNS = {"total_tax_amount"}

GOLD_COLUMNS = [
    (name, meta["type"]) for name, meta in ORDERS_ENTRY["columns"].items()
    if name != "_gold_refreshed_at"  # framework audit column, not in this list either in gold.py
]
GOLD_SOURCE_COLUMNS = [(name, t) for name, t in GOLD_COLUMNS if name not in CALCULATED_COLUMNS]

print(f"Gold columns (from config):        {[c[0] for c in GOLD_COLUMNS]}")
print(f"Expected in Silver (calc excluded): {[c[0] for c in GOLD_SOURCE_COLUMNS]}")

assert "total_tax_amount" in dict(GOLD_COLUMNS), "test config assumption broken: total_tax_amount missing from orders columns"
assert "total_tax_amount" not in dict(GOLD_SOURCE_COLUMNS), "test config assumption broken: exclusion did not filter it out"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test table setup
# MAGIC
# MAGIC A throwaway table under the audit schema, shaped like Silver `orders`
# MAGIC WITHOUT `total_tax_amount` -- exactly what real Silver looks like, since
# MAGIC Silver never writes that column (it's computed by Gold, not stored
# MAGIC upstream).

# COMMAND ----------

TEST_SCHEMA = SCHEMAS["audit"]
TEST_TABLE = f"{CATALOG}.{TEST_SCHEMA}.schema_drift_test_silver_orders"

spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
spark.sql(f"""
    CREATE TABLE {TEST_TABLE} (
        order_id      STRING,
        customer_name STRING,
        status        STRING,
        amount        DOUBLE
    ) USING DELTA
""")
spark.sql(f"INSERT INTO {TEST_TABLE} VALUES ('ORD-9001', 'Test Customer', 'pending', 100.0)")

print(f"✓ Created throwaway test table: {TEST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1 -- calculated column must NOT trigger a false drop
# MAGIC
# MAGIC This is the exact call `process_gold_entity` makes for `orders`, just
# MAGIC pointed at the throwaway table instead of the real Silver table.

# COMMAND ----------

changes = schema_audit.detect_drift_against_config(
    spark, CATALOG, TEST_SCHEMA, TEST_TABLE, "test_silver_orders",
    GOLD_SOURCE_COLUMNS,
    ignored_columns=[],
    use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True,
)

dropped = [c for c in changes if c["change_type"] == "COLUMN_DROPPED"]
assert not dropped, f"FAILED: total_tax_amount exclusion did not work, false COLUMN_DROPPED raised: {dropped}"
print(f"✓ PASSED: no RuntimeError, no false COLUMN_DROPPED for total_tax_amount. Changes seen: {changes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2 -- a genuinely dropped column must still halt
# MAGIC
# MAGIC Drop a real business column (`status`) from the test table, then run
# MAGIC the identical config-comparison. This must raise.

# COMMAND ----------

spark.sql(f"ALTER TABLE {TEST_TABLE} DROP COLUMN status")

raised = False
try:
    schema_audit.detect_drift_against_config(
        spark, CATALOG, TEST_SCHEMA, TEST_TABLE, "test_silver_orders",
        GOLD_SOURCE_COLUMNS,
        ignored_columns=[],
        use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True,
    )
except RuntimeError as e:
    raised = True
    assert "status" in str(e), f"expected 'status' named in the error, got: {e}"
    print(f"✓ PASSED: RuntimeError raised as expected: {e}")

assert raised, "FAILED: dropping a real column (status) did not raise RuntimeError"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the drop was logged to `schema_audit_log`

# COMMAND ----------

audit_log_fqn = f"{CATALOG}.{TEST_SCHEMA}.schema_audit_log"
drop_events = spark.sql(f"""
    SELECT * FROM {audit_log_fqn}
    WHERE `table` = 'schema_drift_test_silver_orders'
      AND change_type = 'COLUMN_DROPPED'
      AND column_name = 'status'
""").collect()

assert len(drop_events) >= 1, "FAILED: the status column drop was not logged to schema_audit_log"
print(f"✓ PASSED: {len(drop_events)} COLUMN_DROPPED row(s) logged for 'status' in schema_audit_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
print(f"✓ Dropped throwaway test table: {TEST_TABLE}")
print("\nALL SCHEMA DRIFT TESTS PASSED")
