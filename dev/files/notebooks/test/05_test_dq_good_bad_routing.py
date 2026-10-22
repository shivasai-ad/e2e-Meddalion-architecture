# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Test: DQ good/bad routing (`common/dq.py`)
# MAGIC
# MAGIC Checks that the DQ gate does what we agreed:
# MAGIC - a row that fails ANY check goes to the `formatted_exception` schema
# MAGIC   (quarantine), and does NOT continue
# MAGIC - a row that passes every check continues
# MAGIC - the run does not raise
# MAGIC
# MAGIC It uses a synthetic `orders`-shaped DataFrame and a throwaway quarantine
# MAGIC table, so it never touches the real pipeline tables and can be re-run.
# MAGIC
# MAGIC Expected result for the 5 rows below:
# MAGIC
# MAGIC | id | case | outcome |
# MAGIC |---|---|---|
# MAGIC | T-1 | valid | continues |
# MAGIC | T-2 | customer_name empty | quarantined |
# MAGIC | T-3 | status `shipped` (not allowed) | quarantined |
# MAGIC | T-4 | amount = -5 | quarantined |
# MAGIC | NULL | order_id is null | quarantined |

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import get_catalog, get_schemas
from common import dq

CATALOG = get_catalog()
SCHEMAS = get_schemas()
GOLD_DQ_RULES = os.path.join(PROJECT_ROOT, "config/dq_rules/silver_to_gold.yml")

QUARANTINE_FQN = f"{CATALOG}.{SCHEMAS['formatted_exception']}.dq_routing_test_quarantine"

# COMMAND ----------

# DBTITLE 1,Build the test batch (5 rows, mixed outcomes)
from pyspark.sql import Row

test_rows = [
    Row(order_id="T-1", customer_name="Alice", status="completed", amount=100.0),
    Row(order_id="T-2", customer_name="", status="pending", amount=50.0),
    Row(order_id="T-3", customer_name="Carol", status="shipped", amount=75.0),
    Row(order_id="T-4", customer_name="Dan", status="refunded", amount=-5.0),
    Row(order_id=None, customer_name="Eve", status="completed", amount=20.0),
]
test_df = spark.createDataFrame(test_rows)

# Start from a clean quarantine so the counts below are exact
spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_FQN}")

# COMMAND ----------

# DBTITLE 1,Run the gate with the real orders rules
orders_rules = dq.load_table_rules(GOLD_DQ_RULES, "orders")
assert orders_rules, "orders DQ rules not found"

# Same rules the Gold job uses for orders (order_id not-null is one of them)
checks = orders_rules

good_df = dq.quality_gate(
    spark,
    entity="orders",
    df=test_df,
    checks=checks,
    quarantine_fqn=QUARANTINE_FQN,
    label="routing test",
)

# COMMAND ----------

# DBTITLE 1,Assertions
good_ids = sorted([r.order_id for r in good_df.collect() if r.order_id is not None])
quarantine_ids = sorted(
    [r.order_id for r in spark.table(QUARANTINE_FQN).collect() if r.order_id is not None]
)
quarantine_null_pk = spark.table(QUARANTINE_FQN).where("order_id IS NULL").count()

print(f"good (continue):      {good_ids}")
print(f"quarantined (error):  {quarantine_ids} + {quarantine_null_pk} row(s) with NULL order_id")

failures = []
if good_ids != ["T-1"]:
    failures.append(f"expected good ['T-1'], got {good_ids}")
if quarantine_ids != ["T-2", "T-3", "T-4"]:
    failures.append(f"expected quarantined ['T-2','T-3','T-4'], got {quarantine_ids}")
if quarantine_null_pk != 1:
    failures.append(f"expected 1 NULL-key row quarantined, got {quarantine_null_pk}")
if good_df.count() != 1:
    failures.append(f"good_df should have 1 row, has {good_df.count()}")
if "_errors" in good_df.columns or "_warnings" in good_df.columns:
    failures.append("result columns should be dropped from good_df")

if failures:
    for f in failures:
        print(f"FAIL: {f}")
    raise AssertionError("DQ routing test FAILED: " + "; ".join(failures))

print("PASS: passing rows continue, failing rows are quarantined, the run did not raise")
