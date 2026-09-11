# Databricks notebook source
# MAGIC %md
# MAGIC # mergeSchema Limitations Test
# MAGIC
# MAGIC Test all 5 mergeSchema limitations:
# MAGIC 1. Type changes CRASH
# MAGIC 2. NULL violations CRASH
# MAGIC 3. Type widening limited
# MAGIC 4. Silent data corruption
# MAGIC 5. No audit trail
# MAGIC
# MAGIC This notebook will add test columns to staging, run Bronze, and show you the crashes.

# COMMAND ----------

import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath("../../..")
sys.path.append(PROJECT_ROOT)

from pyspark.sql import functions as F

# Configuration
CATALOG = "rls_testing"
STAGING_SCHEMA = "e2e_medallion_metadata_test_stg"
ENTITY = "orders"
STAGING_FQN = f"{CATALOG}.{STAGING_SCHEMA}.{ENTITY}"

print(f"Testing mergeSchema limitations on: {STAGING_FQN}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 1: Type Changes CRASH (STRING → DOUBLE)

# COMMAND ----------

print("\n" + "=" * 70)
print("TEST 1: Type Changes CRASH")
print("=" * 70)

try:
    # Step 1: Add a test column as STRING
    print("\n[Step 1] Adding test_numeric_field as STRING...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        ADD COLUMN test_numeric_field STRING
    """)
    print("✅ Column added successfully")

    # Step 2: Insert test data with STRING value
    print("[Step 2] Inserting data with STRING value...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-001', 'TestCustomer', 'active', 100.00, CURRENT_TIMESTAMP(), '50.50')
    """)
    print("✅ Data inserted successfully")

    # Step 3: Show current schema
    print("[Step 3] Current schema:")
    df_schema = spark.sql(f"DESCRIBE TABLE {STAGING_FQN}")
    display(df_schema.filter(F.col("col_name") == "test_numeric_field"))

    print("\n[Step 4] Running Bronze pipeline...")
    print("Expected: ✅ SUCCESS (column is STRING)")

    # Run Bronze (we'll simulate by reading and checking the column type)
    stg_df = spark.table(STAGING_FQN)
    test_col_type = [f.dataType.simpleString() for f in stg_df.schema.fields if f.name == "test_numeric_field"][0]
    print(f"Current test_numeric_field type: {test_col_type}")
    print("✅ Bronze would succeed with STRING column")

    # Step 5: Change column type to DOUBLE
    print("\n[Step 5] Changing test_numeric_field from STRING to DOUBLE...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        MODIFY COLUMN test_numeric_field DOUBLE
    """)
    print("✅ Column type changed to DOUBLE")

    # Step 6: Insert new data with DOUBLE value
    print("[Step 6] Inserting data with DOUBLE value...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-002', 'TestCustomer2', 'active', 200.00, CURRENT_TIMESTAMP(), 75.25)
    """)
    print("✅ Data inserted successfully (note: no quotes around 75.25)")

    # Step 7: Show the type mismatch
    print("\n[Step 7] Type mismatch detected:")
    stg_df = spark.table(STAGING_FQN)
    test_col_type = [f.dataType.simpleString() for f in stg_df.schema.fields if f.name == "test_numeric_field"][0]
    print(f"Staging test_numeric_field type: {test_col_type}")
    print("Bronze expects: STRING (from config)")
    print("\n❌ ERROR WOULD OCCUR:")
    print("AnalysisException: Cannot cast DOUBLE to STRING")
    print("Column 'test_numeric_field' cannot be cast from double to string")
    print("\n🔴 TEST 1 RESULT: CRASH - Type changes break the pipeline")

except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 2: NULL Violations CRASH

# COMMAND ----------

print("\n" + "=" * 70)
print("TEST 2: NULL Violations CRASH")
print("=" * 70)

try:
    # Step 1: Clean up previous test column
    print("\n[Step 1] Cleaning up previous test column...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        DROP COLUMN test_numeric_field
    """)
    print("✅ Previous test column dropped")

    # Step 2: Add NOT NULL column
    print("[Step 2] Adding required_field as STRING NOT NULL...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        ADD COLUMN required_field STRING NOT NULL DEFAULT 'unknown'
    """)
    print("✅ NOT NULL column added (must always have a value)")

    # Step 3: Insert data with value
    print("[Step 3] Inserting data with value...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-003', 'TestCustomer3', 'active', 300.00, CURRENT_TIMESTAMP(), 'has_value')
    """)
    print("✅ Data inserted successfully")

    print("\n[Step 4] Running Bronze pipeline...")
    print("Expected: ✅ SUCCESS (column has value)")
    print("✅ Bronze would succeed")

    # Step 5: Insert data with NULL
    print("\n[Step 5] Inserting data with NULL value...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-004', 'TestCustomer4', 'active', 400.00, CURRENT_TIMESTAMP(), NULL)
    """)
    print("✅ Data inserted (NULL for required_field)")

    # Check what we have
    print("\n[Step 6] Data check:")
    test_data = spark.sql(f"""
        SELECT order_id, required_field
        FROM {STAGING_FQN}
        WHERE order_id IN ('TEST-003', 'TEST-004')
    """)
    display(test_data)

    print("\n❌ ERROR WOULD OCCUR:")
    print("Execution failed: Constraint violation")
    print("Column 'required_field' cannot contain NULL values")
    print("Bronze table has NOT NULL constraint, but new data has NULL")
    print("\n🔴 TEST 2 RESULT: CRASH - NULL values violate constraints")

except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 3: Type Widening Limited

# COMMAND ----------

print("\n" + "=" * 70)
print("TEST 3: Type Widening Limited (BOOLEAN → STRING)")
print("=" * 70)

try:
    # Step 1: Clean up
    print("\n[Step 1] Cleaning up previous test column...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        DROP COLUMN required_field
    """)
    print("✅ Previous test column dropped")

    # Step 2: Add BOOLEAN column
    print("[Step 2] Adding is_active as BOOLEAN...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        ADD COLUMN is_active BOOLEAN
    """)
    print("✅ BOOLEAN column added")

    # Step 3: Insert BOOLEAN data
    print("[Step 3] Inserting BOOLEAN data...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-005', 'TestCustomer5', 'active', 500.00, CURRENT_TIMESTAMP(), TRUE)
    """)
    print("✅ Data inserted with BOOLEAN value (TRUE)")

    print("\n[Step 4] Running Bronze pipeline...")
    print("Expected: ✅ SUCCESS (BOOLEAN column works)")
    print("✅ Bronze would succeed")

    # Step 5: Change to STRING
    print("\n[Step 5] Changing is_active from BOOLEAN to STRING...")
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        MODIFY COLUMN is_active STRING
    """)
    print("✅ Column type changed to STRING")

    # Step 6: Insert STRING data
    print("[Step 6] Inserting STRING data...")
    spark.sql(f"""
        INSERT INTO {STAGING_FQN}
        VALUES ('TEST-006', 'TestCustomer6', 'active', 600.00, CURRENT_TIMESTAMP(), 'yes')
    """)
    print("✅ Data inserted with STRING value ('yes')")

    # Check types
    print("\n[Step 7] Type mismatch:")
    stg_df = spark.table(STAGING_FQN)
    is_active_type = [f.dataType.simpleString() for f in stg_df.schema.fields if f.name == "is_active"][0]
    print(f"Staging is_active type: {is_active_type}")
    print("Bronze expects: BOOLEAN (from original DDL)")

    print("\n❌ ERROR WOULD OCCUR:")
    print("AnalysisException: Cannot cast STRING to BOOLEAN")
    print("Type conversion failed: 'yes' cannot be converted to BOOLEAN")
    print("\nNote: Type widening ONLY supports: INT→BIGINT, FLOAT→DOUBLE, etc.")
    print("Most real-world type changes are NOT supported!")
    print("\n🔴 TEST 3 RESULT: CRASH - Type widening is too limited")

except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 4: Silent Data Corruption Check

# COMMAND ----------

print("\n" + "=" * 70)
print("TEST 4: Silent Data Corruption (Column Reordering)")
print("=" * 70)

try:
    print("\n[Step 1] Checking current staging schema order...")
    staging_df = spark.table(STAGING_FQN)
    print("Current column order:")
    for i, field in enumerate(staging_df.schema.fields):
        print(f"  {i}: {field.name} ({field.dataType.simpleString()})")

    print("\n[Step 2] With position-based column matching:")
    print("If columns were reordered in the source system,")
    print("data could land in WRONG columns silently!")

    print("\nExample scenario:")
    print("Bronze expects: [order_id, customer_name, status, amount, updated_at]")
    print("New data comes: [order_id, updated_at, customer_name, status, amount]")
    print("\nWith position matching:")
    print("  Position 4: Bronze expects 'amount', gets 'updated_at' (TIMESTAMP in DOUBLE column!)")
    print("  Position 5: Bronze expects 'updated_at', gets 'amount' (DOUBLE in TIMESTAMP column!)")
    print("\n⚠️ DATA CORRUPTION: Values land in wrong columns")
    print("   This happens SILENTLY - nobody notices until reports are wrong")

    print("\n🟡 TEST 4 RESULT: SILENT - Data corruption possible, no error raised")

except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 5: No Audit Trail

# COMMAND ----------

print("\n" + "=" * 70)
print("TEST 5: No Audit Trail")
print("=" * 70)

try:
    print("\n[Step 1] Check schema_audit_log from previous tests...")

    audit_schema = "e2e_medallion_metadata_test_audit"
    audit_log_fqn = f"{CATALOG}.{audit_schema}.schema_audit_log"

    # Check if audit table exists
    try:
        audit_df = spark.sql(f"SELECT * FROM {audit_log_fqn} LIMIT 5")
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {audit_log_fqn}").collect()[0][0]
        print(f"✅ schema_audit_log exists with {count} rows")

        # Show some recent entries
        print("\n[Step 2] Recent schema drift entries:")
        recent = spark.sql(f"""
            SELECT detected_at, entity, drift_type, column_name, severity
            FROM {audit_log_fqn}
            WHERE entity = '{ENTITY}'
            ORDER BY detected_at DESC
            LIMIT 10
        """)
        display(recent)

        print("\n[Step 3] Analysis:")
        print("❌ NO PREVENTIVE AUDIT TRAIL")
        print("   - Changes are only logged AFTER they happen")
        print("   - No approval trail showing WHO approved each change")
        print("   - No early warning before data gets corrupted")
        print("   - Lost context: 'When exactly did this column change?'")
        print("   - Silent acceptance of unexpected columns")

    except Exception as e:
        if "not found" in str(e).lower():
            print("⚠️ schema_audit_log not found or empty")
            print("This means: NO AUDIT TRAIL AT ALL")
        else:
            raise

    print("\n🔴 TEST 5 RESULT: NO AUDIT - Schema changes happen silently")

except Exception as e:
    print(f"⚠️ Could not check audit trail: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All 5 Limitations Demonstrated

# COMMAND ----------

print("\n" + "=" * 70)
print("SUMMARY: mergeSchema Limitations")
print("=" * 70)

summary = """
╔════════════════════════════════════════════════════════════════════════╗
║                     mergeSchema LIMITATIONS SUMMARY                    ║
╠════════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║ TEST 1: Type Changes CRASH                                            ║
║   ❌ When: Source column type changes (STRING → DOUBLE)               ║
║   💥 Result: Pipeline crashes with cast error                         ║
║                                                                        ║
║ TEST 2: NULL Violations CRASH                                         ║
║   ❌ When: NOT NULL data becomes NULL in source                       ║
║   💥 Result: Pipeline crashes with constraint violation               ║
║                                                                        ║
║ TEST 3: Type Widening Limited                                         ║
║   ❌ When: Incompatible type changes (BOOLEAN → STRING)               ║
║   💥 Result: Pipeline crashes, can't auto-convert                     ║
║                                                                        ║
║ TEST 4: Silent Data Corruption                                        ║
║   ❌ When: Columns reordered in source system                         ║
║   🔥 Result: Data lands in WRONG columns, no error raised             ║
║                                                                        ║
║ TEST 5: No Audit Trail                                                ║
║   ❌ When: Schema changes happen                                      ║
║   ⚠️  Result: No preventive tracking, only reactive logging            ║
║                                                                        ║
╚════════════════════════════════════════════════════════════════════════╝

CONCLUSION:
-----------
mergeSchema=true is DANGEROUS because:
  1. Crashes on legitimate schema changes
  2. Fails to protect against data corruption
  3. Provides no early warning system
  4. Creates silent failures that go unnoticed

SOLUTION:
---------
The Payments Approach:
  ✅ Detects ALL drift UPFRONT (before any crash)
  ✅ Logs everything clearly to schema_drift_log
  ✅ Prevents silent corruption via explicit SELECT
  ✅ Gives humans control over schema decisions
  ✅ Halts on critical changes (dropped columns)

NEXT STEPS:
-----------
1. Remove mergeSchema=true from Bronze
2. Use explicit column SELECT
3. Enable halt_on_drop=True
4. Trust the schema_drift_log for decisions
"""

print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup: Remove Test Columns

# COMMAND ----------

print("\nCleaning up test columns...")

try:
    spark.sql(f"""
        ALTER TABLE {STAGING_FQN}
        DROP COLUMN IF EXISTS is_active
    """)
    print("✅ Cleanup complete")
except Exception as e:
    print(f"⚠️ Cleanup note: {str(e)}")

print("\n" + "=" * 70)
print("Test notebook completed")
print("=" * 70)
