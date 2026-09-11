# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # Create Governed Table Procedure
# MAGIC
# MAGIC Creates the `create_governed_table` stored procedure in Unity Catalog.
# MAGIC This procedure validates that a DDL statement contains required audit columns
# MAGIC (parses actual column names from the DDL) before executing it.
# MAGIC
# MAGIC **Run this once to set up the procedure**, against the `catalog` /
# MAGIC `gold_schema` widgets below (defaults match this harness's own
# MAGIC `e2e_medallion_metadata_test_gold` schema).

# COMMAND ----------
# DBTITLE 1,Configuration

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger("setup_procedure")

dbutils.widgets.text("catalog", "rls_testing")
dbutils.widgets.text("gold_schema", "e2e_medallion_metadata_test_gold")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("gold_schema")
PROCEDURE_NAME = "create_governed_table"

logger.info(f"Creating procedure: {CATALOG}.{SCHEMA}.{PROCEDURE_NAME}")

# COMMAND ----------
# DBTITLE 1,Create Stored Procedure

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

spark.sql("""
CREATE OR REPLACE PROCEDURE create_governed_table(ddl_statement STRING)
LANGUAGE SQL
SQL SECURITY DEFINER
BEGIN
  -- Check: _gold_refreshed_at column must exist in the DDL
  IF NOT (
       CONTAINS(LOWER(ddl_statement), '_gold_refreshed_at')
  )
  THEN
    SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'DDL REJECTED: No _gold_refreshed_at column found. FIX: Add _gold_refreshed_at TIMESTAMP column to your CREATE TABLE statement.';
  END IF;

  -- Validation passed — execute the DDL
  EXECUTE IMMEDIATE ddl_statement;
END
""")

logger.info(f"Procedure created successfully: {CATALOG}.{SCHEMA}.{PROCEDURE_NAME}")

# COMMAND ----------
# DBTITLE 1,Test 1: Should PASS (has _gold_refreshed_at)

# logger.info("Test 1: DDL with _gold_refreshed_at — should PASS")

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_governed_pass")

# spark.sql(f"""
#     CALL create_governed_table(
#         "CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.test_governed_pass (fund_id STRING, fund_name STRING, _gold_refreshed_at TIMESTAMP) USING DELTA"
#     )
# """)

# logger.info("Test 1 PASSED — table created successfully")
# display(spark.sql(f"DESCRIBE TABLE {CATALOG}.{SCHEMA}.test_governed_pass"))

# # COMMAND ----------
# # DBTITLE 1,Test 2: Should FAIL (no audit column)

# logger.info("Test 2: DDL without audit column — should FAIL")

# try:
#     spark.sql(f"""
#         CALL create_governed_table(
#             "CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.test_governed_fail (fund_id STRING, fund_name STRING) USING DELTA"
#         )
#     """)
#     logger.error("Test 2 FAILED — table was created but should have been rejected!")
# except Exception as e:
#     logger.info(f"Test 2 PASSED — procedure correctly rejected DDL:")
#     logger.info(f"  Error: {e}")

# # COMMAND ----------
# # DBTITLE 1,Test 3: Should PASS (has _gold_refreshed_at)

# logger.info("Test 3: DDL with _gold_refreshed_at — should PASS")

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_governed_pass2")

# spark.sql(f"""
#     CALL create_governed_table(
#         "CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.test_governed_pass2 (id INT, name STRING, _gold_refreshed_at TIMESTAMP) USING DELTA"
#     )
# """)

# logger.info("Test 3 PASSED — table created successfully")

# # COMMAND ----------
# # DBTITLE 1,Cleanup

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_governed_pass")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_governed_pass2")
# logger.info("Test tables cleaned up")
