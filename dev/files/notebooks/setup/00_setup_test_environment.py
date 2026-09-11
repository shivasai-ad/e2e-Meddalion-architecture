# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: isolated test environment
# MAGIC
# MAGIC Creates the 5 schemas this harness owns (`_stg`, `_bronze`, `_silver`, `_gold`,
# MAGIC `_audit`) inside the target catalog. Schema names come from
# MAGIC `common/config_loader.py::get_schemas()`.
# MAGIC
# MAGIC Also creates the 3 **framework-owned** audit tables (`audit_log`,
# MAGIC `job_flags`, `schema_audit_log`) — these are shared across every entity
# MAGIC and every layer, not owned by any one entity's pipeline, so they belong
# MAGIC here rather than being lazily created inside Bronze/Silver/Gold.
# MAGIC
# MAGIC **This notebook never creates entity tables.** Staging tables are created
# MAGIC by `stg/staging.py`; Bronze/Silver/Gold tables are created by their own
# MAGIC pipeline notebooks on first run. Those notebooks verify the schemas and
# MAGIC audit tables already exist and fail fast (pointing back here) if this
# MAGIC notebook hasn't been run yet.
# MAGIC
# MAGIC **Reset (optional):** pass `reset=true` to TRUNCATE every table that
# MAGIC already exists in these 5 schemas — not DROP SCHEMA CASCADE. For an
# MAGIC EXTERNAL table, DROP only removes the catalog metadata; the physical
# MAGIC Delta files at the fixed S3 LOCATION survive, so a later
# MAGIC `CREATE TABLE IF NOT EXISTS` at that path would silently resurrect old
# MAGIC data instead of starting fresh. TRUNCATE actually empties the table. If
# MAGIC a schema has no tables yet, reset does nothing for that schema.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import (
    get_catalog,
    get_schemas,
    get_use_external_tables,
    get_s3_bucket,
    get_s3_location,
    get_fqn,
    load_bronze_config,
)
from common import control, audit_log, schema_audit, gold_freshness

# COMMAND ----------

dbutils.widgets.text("reset", "false")

RESET = dbutils.widgets.get("reset").strip().lower() == "true"
CATALOG = get_catalog()
SCHEMAS = get_schemas()
USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None

print(f"catalog={CATALOG} reset={RESET} use_external_tables={USE_EXTERNAL}")
print(f"Schemas: {list(SCHEMAS.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note: Schemas are now created per-layer
# MAGIC
# MAGIC Each layer notebook (staging, bronze, silver, gold) creates its own schema
# MAGIC when it first runs, rather than pre-creating all schemas here.
# MAGIC This keeps schema ownership with the layer that owns it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (optional): DROP framework audit tables + delete their physical
# MAGIC ## files, TRUNCATE entity tables
# MAGIC
# MAGIC Runs **before** audit-table creation below. Framework audit tables
# MAGIC (job_flags, audit_log, schema_audit_log) are EXTERNAL tables at a fixed
# MAGIC S3 LOCATION: DROP TABLE only removes the Unity Catalog metadata pointer,
# MAGIC it does NOT delete the underlying Delta transaction log/files at that
# MAGIC path. If reset ran only *after* trying to (re)create these tables, the
# MAGIC create call below would already have failed against the stale Delta log
# MAGIC (DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY) before this cell ever ran --
# MAGIC so cleanup must happen first. Entity tables (staging, bronze, silver,
# MAGIC gold per entity) are only truncated (emptied but schema preserved).

# COMMAND ----------

if RESET:
    AUDIT_SCHEMA = SCHEMAS["audit"]
    audit_tables = ["job_flags", "audit_log", "schema_audit_log", "gold_refresh_log"]
    for table_name in audit_tables:
        fqn = f"{CATALOG}.{AUDIT_SCHEMA}.{table_name}"
        if spark.catalog.tableExists(fqn):
            if USE_EXTERNAL:
                location = spark.sql(f"DESCRIBE DETAIL {fqn}").select("location").first()[0]
            else:
                location = None
            spark.sql(f"DROP TABLE {fqn}")
            print(f"Dropped {fqn}")
            if location:
                dbutils.fs.rm(location, recurse=True)
                print(f"  Deleted physical files at {location}")
        elif USE_EXTERNAL:
            # Table isn't registered in the catalog (e.g. a prior run failed
            # mid-CREATE), but stale files may still sit at the fixed
            # LOCATION -- clear it unconditionally so creation below starts
            # from a clean path.
            location = f"s3://{AUDIT_BUCKET}/audit/client_e2e_medallion_metadata_test/{table_name}/"
            dbutils.fs.rm(location, recurse=True)
            print(f"  Cleared (possibly stale) files at {location}")

    # Drop entity tables (staging, bronze, silver, gold) and, for external
    # tables, delete their physical files too -- same reasoning as the audit
    # tables above. TRUNCATE alone is not enough here: it doesn't fix a
    # schema/property mismatch (e.g. after editing bronze_config.yml /
    # staging_config.yml), so a config change would leave TRUNCATE unable to
    # get the table back in sync. DROP + recreate (by stg/staging.py and
    # bronze.py on their next run) always picks up the current config.
    #
    # Entity names come from bronze_config.yml -- the same set of entities
    # exists (by name) in every layer. We also proactively clear each
    # layer's physical location even when the table isn't registered in the
    # catalog, since an orphaned Delta log from an earlier run (e.g. before
    # a config change) would otherwise make the next CREATE TABLE fail with
    # DELTA_CREATE_TABLE_SCHEME_MISMATCH / DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY.
    entity_layers = ["stg", "bronze", "silver", "gold"]
    entities = list(load_bronze_config(os.path.join(PROJECT_ROOT, "config/bronze_config.yml")).get("tables", {}).keys())

    dropped = []
    for layer in entity_layers:
        for entity in entities:
            fqn = get_fqn(entity, layer)
            if spark.catalog.tableExists(fqn):
                if USE_EXTERNAL:
                    location = spark.sql(f"DESCRIBE DETAIL {fqn}").select("location").first()[0]
                else:
                    location = None
                spark.sql(f"DROP TABLE {fqn}")
                dropped.append(fqn)
                if location:
                    dbutils.fs.rm(location, recurse=True)
            elif USE_EXTERNAL:
                location = get_s3_location(entity, layer)
                try:
                    dbutils.fs.rm(location, recurse=True)
                except Exception:
                    pass  # nothing there yet -- fine

    if dropped:
        print(f"\nDropped {len(dropped)} entity table(s):")
        for fqn in dropped:
            print(f"  {fqn}")
    else:
        print("\nNo entity tables found to drop.")
else:
    print("reset=false — skipping reset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create framework-owned audit tables (idempotent)
# MAGIC
# MAGIC `audit_log` (per-write audit trail), `job_flags` (generic control table --
# MAGIC watermarks live here), and `schema_audit_log` (schema drift log). One of
# MAGIC each, shared by every entity and every layer.

# COMMAND ----------

AUDIT_SCHEMA = SCHEMAS["audit"]

# ═══════════════════════════════════════════════════════════════════════════
# CREATE SCHEMAS (once, idempotent)
# ═══════════════════════════════════════════════════════════════════════════

for layer_name, schema_name in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
    print(f"✓ Schema ready: {CATALOG}.{schema_name}")

# ═══════════════════════════════════════════════════════════════════════════
# CREATE FRAMEWORK-OWNED AUDIT TABLES (idempotent)
# ═══════════════════════════════════════════════════════════════════════════

control.ensure_control_table(spark, CATALOG, AUDIT_SCHEMA, USE_EXTERNAL, AUDIT_BUCKET)
audit_log.AuditLogger(spark, CATALOG, AUDIT_SCHEMA, USE_EXTERNAL, AUDIT_BUCKET).ensure_table()
schema_audit.ensure_table(spark, CATALOG, AUDIT_SCHEMA, USE_EXTERNAL, AUDIT_BUCKET)
gold_freshness.ensure_table(spark, CATALOG, AUDIT_SCHEMA, USE_EXTERNAL, AUDIT_BUCKET)

print(f"✓ Audit tables ready in {CATALOG}.{AUDIT_SCHEMA}:")
print(f"  - job_flags")
print(f"  - audit_log")
print(f"  - schema_audit_log")
print(f"  - gold_refresh_log")

# ═══════════════════════════════════════════════════════════════════════════
# CREATE GOLD SCHEMA STORED PROCEDURES (idempotent)
# ═══════════════════════════════════════════════════════════════════════════

GOLD_SCHEMA = SCHEMAS["gold"]

# Create stored procedure: create_governed_table
# This procedure validates that Gold table DDL includes _gold_refreshed_at audit column
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

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

print(f"✓ Stored procedure ready: {CATALOG}.{GOLD_SCHEMA}.create_governed_table")
