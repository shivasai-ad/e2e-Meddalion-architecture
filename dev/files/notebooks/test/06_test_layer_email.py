# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Test: ONE DQ email per layer (`common/dq.py`, `common/dq_email.py`)
# MAGIC
# MAGIC Checks that a layer sends a single email for all its tables, not one per table.
# MAGIC
# MAGIC It runs the real gate on three synthetic, `orders`-shaped tables with the real orders rules, the way
# MAGIC Silver and Gold do (a loop over tables, then one `send_layer_email` call):
# MAGIC
# MAGIC | table | rows | outcome |
# MAGIC |---|---|---|
# MAGIC | `orders` | 5 | 4 rejected (empty name, status `shipped`, amount -5, null key) |
# MAGIC | `customers_sample` | 2 | all pass |
# MAGIC | `payments_sample` | 2 | all pass |
# MAGIC
# MAGIC Expected: **one** email, with a row per table and a quarantine link only for `orders`.
# MAGIC The quarantine tables are throwaway (`*_layer_email_test_quarantine`) and dropped at the start and the end,
# MAGIC so the real pipeline tables and the real `dq_gate_log` are not touched.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from pyspark.sql import Row
from common.config_loader import get_catalog, get_schemas
from common import dq, dq_email

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("team_name", "rls_testing", "Squad key (email)")
dq_email.configure(dbutils.widgets.get("environment").strip(), dbutils.widgets.get("team_name").strip())

CATALOG = get_catalog()
SCHEMAS = get_schemas()
RULES = dq.load_table_rules(os.path.join(PROJECT_ROOT, "config/dq_rules/silver_to_gold.yml"), "orders")
assert RULES, "orders DQ rules not found"

TABLES = {
    "orders": [
        Row(order_id="T-1", customer_name="Alice", status="completed", amount=100.0),
        Row(order_id="T-2", customer_name="", status="pending", amount=50.0),
        Row(order_id="T-3", customer_name="Carol", status="shipped", amount=75.0),
        Row(order_id="T-4", customer_name="Dan", status="refunded", amount=-5.0),
        Row(order_id=None, customer_name="Eve", status="completed", amount=20.0),
    ],
    "customers_sample": [
        Row(order_id="C-1", customer_name="Fay", status="pending", amount=10.0),
        Row(order_id="C-2", customer_name="Gus", status="completed", amount=12.0),
    ],
    "payments_sample": [
        Row(order_id="P-1", customer_name="Hal", status="completed", amount=30.0),
        Row(order_id="P-2", customer_name="Ivy", status="refunded", amount=8.0),
    ],
}


def quarantine_fqn(entity):
    return f"{CATALOG}.{SCHEMAS['formatted_exception']}.{entity}_layer_email_test_quarantine"


def drop_quarantine():
    for entity in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {quarantine_fqn(entity)}")


drop_quarantine()
dq_email.RESULTS.clear()

# COMMAND ----------

# DBTITLE 1,Run the gate for each table, then send the layer email once
try:
    for entity, rows in TABLES.items():
        dq.quality_gate(
            spark, entity, spark.createDataFrame(rows), RULES,
            quarantine_fqn=quarantine_fqn(entity), label="layer email test",
            notify={"layer": "Silver", "dbutils": dbutils},
        )
    kept = len(dq_email.RESULTS)  # results kept for the email, nothing sent yet
finally:
    sent = dq.send_layer_email(spark, dbutils, "Silver")

# COMMAND ----------

# DBTITLE 1,Assertions
failures = []
if kept != 3:
    failures.append(f"expected 3 gate results kept for one email, got {kept}")
if dq_email.RESULTS:
    failures.append(f"the layer email should use up the results, {len(dq_email.RESULTS)} left")
if not spark.catalog.tableExists(quarantine_fqn("orders")):
    failures.append("orders should have a quarantine table")
for clean in ("customers_sample", "payments_sample"):
    if spark.catalog.tableExists(quarantine_fqn(clean)):
        failures.append(f"{clean} passed every row, it should have no quarantine table")
print(f"results kept: {kept}; email sent: {sent}")
drop_quarantine()

if failures:
    for f in failures:
        print(f"FAIL: {f}")
    raise AssertionError("Layer email test FAILED: " + "; ".join(failures))
print("PASS: three tables gave three kept results and one email (check the inbox: one row per table)")
