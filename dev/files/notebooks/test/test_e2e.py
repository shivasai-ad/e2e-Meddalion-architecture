# Databricks notebook source
# MAGIC %md
# MAGIC # E2E Testing: Staging → Bronze SCD2 Flow
# MAGIC
# MAGIC Step-by-step manual test of the medallion pipeline:
# MAGIC 1. Insert test data into staging
# MAGIC 2. Run Bronze (initial load)
# MAGIC 3. Update a row in staging
# MAGIC 4. Run Bronze (SCD2: close old, insert new)
# MAGIC 5. Delete a row in staging
# MAGIC 6. Run Bronze (mark as deleted)
# MAGIC
# MAGIC Run this cell by cell, reading output at each step to understand the flow.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import get_catalog, get_fqn
from pyspark.sql import functions as F

CATALOG = get_catalog()
ENTITY = "orders"
STG_FQN = get_fqn(ENTITY, "stg")
BRONZE_FQN = get_fqn(ENTITY, "bronze")

print(f"Testing entity: {ENTITY}")
print(f"Staging: {STG_FQN}")
print(f"Bronze:  {BRONZE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: INSERT test data into staging

# COMMAND ----------

# Insert 3 initial rows
spark.sql(f"""
INSERT INTO {STG_FQN} (order_id, customer_name, status, amount)
VALUES
  ('ORD-001', 'Alice Johnson', 'pending', 150.00),
  ('ORD-002', 'Bob Smith', 'completed', 75.50),
  ('ORD-003', 'Carol White', 'pending', 200.00)
""")

print(f"\nInserted 3 rows into {STG_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check staging data before Bronze run

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {STG_FQN} ORDER BY order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Bronze (initial load — all rows inserted as current)

# COMMAND ----------

# MAGIC %run ../formatted_stg/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Bronze output

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id, customer_name, status, amount,
  _bronze_is_current, _bronze_valid_from, _bronze_valid_to, _bronze_hash
FROM {BRONZE_FQN}
ORDER BY order_id, _bronze_valid_from DESC
"""))

# COMMAND ----------

print(f"""
Expected after Step 2 (INSERT):
- 3 rows in Bronze
- All have _bronze_is_current = TRUE
- All have _bronze_valid_to = NULL (current)
- _bronze_valid_from = when Bronze ran
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: UPDATE a row in staging (ORD-002: status from 'completed' → 'refunded')

# COMMAND ----------

spark.sql(f"""
UPDATE {STG_FQN}
SET status = 'refunded'
WHERE order_id = 'ORD-002'
""")

print(f"Updated ORD-002 status to 'refunded' in staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check staging after update

# COMMAND ----------

display(spark.sql(f"SELECT order_id, status FROM {STG_FQN} ORDER BY order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run Bronze again (SCD2: close old ORD-002, insert updated version)

# COMMAND ----------

# MAGIC %run ../formatted_stg/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Bronze after update — SCD2 behavior

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id, customer_name, status, amount,
  _bronze_is_current, _bronze_valid_from, _bronze_valid_to
FROM {BRONZE_FQN}
WHERE order_id = 'ORD-002'
ORDER BY _bronze_valid_from
"""))

# COMMAND ----------

print(f"""
Expected after Step 4 (UPDATE):
- ORD-002 should have 2 rows:
  1. OLD row: status='completed', _bronze_is_current=FALSE, _bronze_valid_to set
  2. NEW row: status='refunded', _bronze_is_current=TRUE, _bronze_valid_to=NULL
- Hash changed → new row inserted, old closed
- ORD-001 and ORD-003 unchanged
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: DELETE a row in staging (ORD-003 removed)

# COMMAND ----------

spark.sql(f"""
DELETE FROM {STG_FQN}
WHERE order_id = 'ORD-003'
""")

print(f"Deleted ORD-003 from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check staging after delete

# COMMAND ----------

display(spark.sql(f"SELECT order_id, status FROM {STG_FQN} ORDER BY order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run Bronze again (SCD2: close ORD-003, mark as deleted)

# COMMAND ----------

# MAGIC %run ../formatted_stg/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Bronze after delete — SCD2 behavior

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id, customer_name, status, amount,
  _bronze_is_current, _bronze_valid_from, _bronze_valid_to
FROM {BRONZE_FQN}
WHERE order_id = 'ORD-003'
ORDER BY _bronze_valid_from
"""))

# COMMAND ----------

print(f"""
Expected after Step 6 (DELETE):
- ORD-003 should have 2 rows:
  1. OLD row: _bronze_is_current=TRUE, _bronze_valid_to=NULL
  2. After delete: _bronze_is_current=FALSE, _bronze_valid_to set (row closed)
- Deletion in staging = hash disappears = MERGED as DELETED
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Bronze Summary (all entities, all versions)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id, status, amount,
  _bronze_is_current,
  _bronze_valid_from,
  _bronze_valid_to,
  DATEDIFF(second, _bronze_valid_from, COALESCE(_bronze_valid_to, CURRENT_TIMESTAMP())) AS lifetime_seconds
FROM {BRONZE_FQN}
ORDER BY order_id, _bronze_valid_from DESC
"""))

# COMMAND ----------

print(f"""
=== TEST SUMMARY ===
✓ INSERT: All rows inserted as current
✓ UPDATE: Old version closed, new version inserted (SCD2)
✓ DELETE: Deletion marked by closing the row

Next steps:
1. Check that liquid clustering is applied (DESCRIBE TABLE {BRONZE_FQN})
2. Run Silver to see cleansing/DQ rules
3. Run Gold to see final aggregation
""")
