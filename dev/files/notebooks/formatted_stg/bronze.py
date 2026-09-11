# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - BRONZE (v2): SCD Type 2 History
# MAGIC
# MAGIC Staging → Bronze using Change Data Feed (CDF) with SCD2 tracking.
# MAGIC
# MAGIC All configuration (columns, primary keys, table properties, audit columns)
# MAGIC comes from `config/bronze_config.yml`. Runs for all entities or a single
# MAGIC entity specified via widget.
# MAGIC
# MAGIC **Depends on `stg/staging.py` having already been run** — this notebook
# MAGIC only reads from staging, it never creates the staging table.
# MAGIC
# MAGIC **Difference from `bronze.py`:** this version splits the Bronze table's
# MAGIC *first-time load* out into its own explicit branch (create table, one
# MAGIC plain INSERT of everything currently in Staging, no CDF/classify at all)
# MAGIC instead of always going through `cdf.read_changes(..., from_version=None,
# MAGIC ...)`. Every run after that goes through the normal incremental
# MAGIC CDF+hash+MERGE path — mirrors the split already used in
# MAGIC `notebooks/published/gold_v2.py` (`_bootstrap_gold_table` /
# MAGIC `_incremental_gold_merge`), and the first-load/incremental split in the
# MAGIC reference Payments `bronze_ingest.py` notebook.
# MAGIC
# MAGIC **SCD2 Mechanics (incremental path only):**
# MAGIC - Reads CDF since last watermark, hashes business columns for change detection
# MAGIC - Closes superseded rows (sets _bronze_valid_to and _bronze_is_current=False)
# MAGIC - Inserts new current rows with SCD2 metadata (_bronze_valid_from, _bronze_is_current=True)
# MAGIC
# MAGIC See `docs/databricks_objects/15_medallion_pattern_rds_to_databricks/medallion-cdc-fed-pattern.md`
# MAGIC for the pattern documentation.

# COMMAND ----------

import os
import sys
from typing import Dict, List, Tuple

# This notebook lives at notebooks/formatted_stg/bronze_v2.py -- two levels
# below the project root (dev/files) where common/ and config/ live.
# Databricks sets a notebook's CWD to its own containing folder, so ".."
# would only reach notebooks/, not dev/files.
PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from pyspark.sql import functions as F
from common.config_loader import (
    load_bronze_config,
    get_catalog,
    get_use_external_tables,
    get_schemas,
    get_fqn,
    get_s3_bucket,
)
from common import ddl, scd2, cdf, audit_log, control, schema_audit

# COMMAND ----------

# Widget for entity selection
dbutils.widgets.text("entity", "ALL", label="Entity (or ALL)")
ENTITY = dbutils.widgets.get("entity").strip()

# Constants
CATALOG = get_catalog()
USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None

# Load configurations
BRONZE_CONFIG = load_bronze_config(os.path.join(PROJECT_ROOT, "config/bronze_config.yml"))
CONFIG_ENTITIES = list(BRONZE_CONFIG.get("tables", {}).keys())

# Case-insensitive entity lookup: map lowercase keys to actual config keys
entity_map = {key.lower(): key for key in CONFIG_ENTITIES}
entity_normalized = ENTITY.lower()

# Filter to specified entity if not ALL
if entity_normalized != "all":
    if entity_normalized not in entity_map:
        raise ValueError(f"Entity '{ENTITY}' not found in bronze_config. Available: {CONFIG_ENTITIES}")
    ENTITIES = [entity_map[entity_normalized]]  # Use actual config key
else:
    ENTITIES = CONFIG_ENTITIES  # Use all entities as-is

print(f"Processing Bronze for entities: {ENTITIES}")

# ═══════════════════════════════════════════════════════════════════════════
# CASE-INSENSITIVE CONFIG LOOKUP HELPER
# ═══════════════════════════════════════════════════════════════════════════

def get_config_entity(entity_name: str, config_dict: dict) -> tuple:
    """Normalize entity name and return (actual_key, config).
    Ensures case-insensitive lookup while preserving config's own key casing."""
    normalized = entity_name.lower()
    for key in config_dict.keys():
        if key.lower() == normalized:
            return key, config_dict[key]
    return None, None

# COMMAND ----------


def process_bronze_entity(spark, catalog: str, entity: str, config: Dict, schemas: Dict) -> Dict:
    """
    Run Bronze for a single entity: first-time load if the Bronze table
    doesn't exist yet, incremental CDF+hash MERGE otherwise.

    Args:
        spark: Spark session
        catalog: Catalog name
        entity: Entity name
        config: Entity config from bronze_config.yml["tables"][entity]
        schemas: Dict of schema names

    Returns:
        Dict with metrics: {closed: int, inserted: int, version: int, ran: bool}
    """
    # Get table names
    stg_fqn = get_fqn(entity, "stg")
    bronze_fqn = get_fqn(entity, "bronze")

    # Build column list from config
    columns = [(col["name"], col["type"]) for col in config.get("columns", [])]
    primary_keys = config.get("primary_keys", [])
    business_cols = [name for name, _ in columns]
    # Liquid clustering on primary keys for MERGE performance in Bronze.
    # (Note: _bronze_is_current is BOOLEAN, which Delta does not support in CLUSTER BY)
    clustering_keys = primary_keys

    # Get table properties and audit columns from config defaults
    defaults = BRONZE_CONFIG.get("defaults", {})
    table_properties = defaults.get("table_properties", {})

    # Build audit columns from config: (name, type + nullable)
    audit_columns = []
    # Plain name -> type map (no "NOT NULL" suffix) so the SCD2 write paths
    # below can .cast(...) each audit column to its config-declared type
    # instead of hardcoding "timestamp"/"boolean" string literals.
    audit_col_types = {}
    for audit_col in defaults.get("audit_columns", []):
        col_name = audit_col["name"]
        col_type = audit_col["type"]
        audit_col_types[col_name] = col_type.lower()
        nullable = audit_col.get("nullable", True)
        # Append NOT NULL only if not nullable
        if not nullable:
            col_type = f"{col_type} NOT NULL"
        audit_columns.append((col_name, col_type))

    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP: Verify staging table exists, detect schema drift
    # ═══════════════════════════════════════════════════════════════════════════

    # Bronze schema is created by 00_setup_test_environment.py, not here.
    # Staging is owned and created by stg/staging.py, not here — Bronze only
    # ever reads from it.
    if not spark.catalog.tableExists(stg_fqn):
        raise RuntimeError(
            f"Staging table {stg_fqn} does not exist. Run stg/staging.py for "
            f"entity '{entity}' first."
        )

    # Schema validation: compare staging's LIVE schema against bronze_config.yml (config is source of truth)
    # This is the Payments approach: fail early if config expects columns that are missing from staging.
    # Must run BEFORE any Bronze logic, so we catch mismatches early instead of inside a cryptic MERGE error.
    staging_ignored = defaults.get("ignored_columns", [])
    schema_audit.detect_drift_against_config(
        spark, catalog, schemas["audit"], stg_fqn, f"staging_{entity}",
        columns,  # Expected columns from config
        ignored_columns=staging_ignored,  # Per-entity config + global _ALWAYS_IGNORED_COLUMNS
        use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True
    )

    # Schema drift snapshot: track changes from previous run (for historical audit trail)
    schema_audit.detect_and_log(
        spark, catalog, schemas["audit"], stg_fqn, f"staging_{entity}",
        USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=False  # Already halted above if critical
    )

    audit_logger = audit_log.AuditLogger(spark, catalog, schemas["audit"], USE_EXTERNAL, AUDIT_BUCKET)

    # ═══════════════════════════════════════════════════════════════════════════
    # DETERMINE PATH: table exists or not
    # ═══════════════════════════════════════════════════════════════════════════

    bronze_table_exists = spark.catalog.tableExists(bronze_fqn)

    if not bronze_table_exists:
        return _bootstrap_bronze_table(
            spark, catalog, entity, bronze_fqn, stg_fqn, columns, primary_keys,
            business_cols, clustering_keys, audit_columns, audit_col_types, table_properties,
            audit_logger, schemas
        )
    else:
        return _incremental_bronze_merge(
            spark, catalog, entity, bronze_fqn, stg_fqn, primary_keys,
            business_cols, audit_col_types, audit_logger, schemas
        )


def _bootstrap_bronze_table(
    spark, catalog: str, entity: str, bronze_fqn: str, stg_fqn: str, columns: List[Tuple[str, str]],
    primary_keys: List[str], business_cols: List[str], clustering_keys: List[str],
    audit_columns: List[Tuple[str, str]], audit_col_types: Dict[str, str], table_properties: Dict,
    audit_logger, schemas: Dict
) -> Dict:
    """First-time load: create Bronze table, then a single plain INSERT of
    everything currently in Staging (no CDF, no classify -- there is no
    prior Bronze state to diff against yet)."""

    # Bronze not existing is only a valid bootstrap if there's also no prior
    # watermark for it -- table existence and watermark presence should
    # always move together. If a watermark IS found here, Bronze was dropped
    # (or never fully created) without its job_flags row being cleared, and
    # silently bootstrapping would hide that instead of surfacing it: it
    # would re-derive Bronze from a full Staging snapshot under a *new*
    # watermark, discarding whatever history/version range the stale
    # watermark implied had already been processed.
    existing_watermark = control.get_watermark(spark, catalog, schemas["audit"], entity)
    if existing_watermark is not None:
        raise RuntimeError(
            f"Inconsistent state for '{entity}': Bronze table {bronze_fqn} does not exist, "
            f"but job_flags already has a watermark (last_processed_version={existing_watermark}) "
            f"for it. Bronze was likely dropped/recreated without clearing its job_flags row. "
            f"Resolve manually (clear the job_flags row for this entity if the table's history "
            f"is truly gone, or restore the table) before rerunning -- refusing to silently "
            f"re-bootstrap over a non-None watermark."
        )

    print(f"\n{'='*70}")
    print(f"BOOTSTRAP: Creating Bronze table '{bronze_fqn}'")
    print(f"   ✓ Confirmed no prior watermark for '{entity}' — safe to bootstrap")
    print(f"{'='*70}")

    bronze_ddl = ddl.generate_bronze_ddl(
        bronze_fqn, entity, columns, audit_columns, table_properties, USE_EXTERNAL, clustering_keys, primary_keys
    )
    print("DDL:")
    print(bronze_ddl)
    print(f"{'='*70}\n")
    spark.sql(bronze_ddl)
    if clustering_keys:
        print(f"✓ Applied liquid clustering on columns: {clustering_keys}")

    # Snapshot Staging's version BEFORE reading it (not after), and read this
    # exact version -- so the watermark we store afterwards, and the rows we
    # actually inserted, refer to the same point in time. A commit landing on
    # Staging while this bootstrap is running is simply picked up by the next
    # (incremental) run instead of silently getting lost past the watermark.
    stg_bootstrap_version = spark.sql(f"DESCRIBE HISTORY {stg_fqn}").agg(F.max("version")).collect()[0][0]
    staging_df = spark.read.format("delta").option("versionAsOf", stg_bootstrap_version).table(stg_fqn)

    atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

    new_rows = (
        staging_df.select(*business_cols)
        .withColumn("_bronze_hash", scd2.row_hash(business_cols, set()))
        .withColumn("_bronze_valid_from", F.lit(atomic_ts).cast(audit_col_types["_bronze_valid_from"]))
        .withColumn("_bronze_valid_to", F.lit(None).cast(audit_col_types["_bronze_valid_to"]))
        .withColumn("_bronze_is_current", F.lit(True).cast(audit_col_types["_bronze_is_current"]))
        .withColumn("_bronze_ingested_at", F.lit(atomic_ts).cast(audit_col_types["_bronze_ingested_at"]))
    )

    row_count = new_rows.count()
    if row_count > 0:
        with audit_logger.audit(bronze_fqn, audit_log.Action.INSERT) as log_ctx:
            new_rows.write.format("delta").mode("append").saveAsTable(bronze_fqn)
            log_ctx.set_count(row_count)
        print(f"✓ First-time load: inserted {row_count} rows from Staging")
    else:
        print(f"⚠ Staging table is empty — Bronze table created but no data inserted")

    # Seed Bronze's watermark at the bootstrap version so the first
    # incremental run's CDF read starts strictly after this snapshot.
    control.set_watermark(spark, catalog, schemas["audit"], entity, stg_bootstrap_version, 0, row_count)

    # Log Bronze's own schema (checked after the write, same as the incremental path).
    schema_audit.detect_and_log(spark, catalog, schemas["audit"], bronze_fqn, f"bronze_{entity}", USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=True)

    return {"closed": 0, "inserted": row_count, "version": stg_bootstrap_version, "ran": True}


def _incremental_bronze_merge(
    spark, catalog: str, entity: str, bronze_fqn: str, stg_fqn: str, primary_keys: List[str],
    business_cols: List[str], audit_col_types: Dict[str, str], audit_logger, schemas: Dict
) -> Dict:
    """Incremental: read Staging's CDF since Bronze's own watermark, hash +
    classify each change against current Bronze, close superseded rows and
    insert new current rows."""

    print(f"\n{'='*70}")
    print(f"INCREMENTAL: Bronze table '{bronze_fqn}' already exists")
    print(f"{'='*70}")

    # ═══════════════════════════════════════════════════════════════════════════
    # READ CHANGES VIA CDF
    # ═══════════════════════════════════════════════════════════════════════════

    # Get staging history version range
    to_version = spark.sql(f"DESCRIBE HISTORY {stg_fqn}").agg(F.max("version")).collect()[0][0]
    from_version = control.get_watermark(spark, catalog, schemas["audit"], entity)

    # If already at or past this version, nothing to do
    if from_version is not None and from_version >= to_version:
        print(f"   ℹ Already at target version — skipping processing\n")
        return {"closed": 0, "inserted": 0, "version": to_version, "ran": False}

    # ═══════════════════════════════════════════════════════════════════════════
    # LOG INCREMENTAL DETAILS
    # ═══════════════════════════════════════════════════════════════════════════

    print(f"\n📊 CDF (Change Data Feed) DETAILS:")
    print(f"   Bronze watermark: from_version={from_version} → to_version={to_version}")
    print(f"   Processing versions: {from_version + 1} to {to_version}")

    # Read CDF changes
    changes = cdf.read_changes(spark, stg_fqn, from_version, to_version)

    changes_count = changes.count()
    print(f"   ✓ Total CDF changes read: {changes_count}")

    if changes.isEmpty():
        # No changes to process — advance watermark and return
        control.set_watermark(spark, catalog, schemas["audit"], entity, to_version, 0, 0)
        print(f"   ℹ No new changes — skipping SCD2 processing\n")
        return {"closed": 0, "inserted": 0, "version": to_version, "ran": True}

    # ═══════════════════════════════════════════════════════════════════════════
    # HASH AND CLASSIFY CHANGES
    # ═══════════════════════════════════════════════════════════════════════════

    # Get latest state per key, ordered by Delta's own commit version
    changes = cdf.latest_per_key(changes, primary_keys)

    # Add hash for change detection (hash all business columns)
    changes = changes.withColumn("_bronze_hash", scd2.row_hash(business_cols, set()))

    # Classify as NEW / CHANGED / DELETED / UNCHANGED
    classified = scd2.classify(spark, changes, bronze_fqn, primary_keys)

    # Split by action
    to_close = classified.filter(F.col("_action").isin("CHANGED", "DELETED"))
    to_insert = classified.filter(F.col("_action").isin("CHANGED", "NEW"))
    n_close, n_insert = to_close.count(), to_insert.count()

    # Capture atomic timestamp ONCE — used by both CLOSE and INSERT to keep them synchronized
    atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

    # ═══════════════════════════════════════════════════════════════════════════
    # EXECUTE MERGE: CLOSE SUPERSEDED ROWS
    # ═══════════════════════════════════════════════════════════════════════════

    if n_close:
        to_close.select(*primary_keys).distinct().createOrReplaceTempView("_e2e_close_keys")
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in primary_keys)
        valid_to_type = audit_col_types["_bronze_valid_to"].upper()
        try:
            spark.sql(
                f"""
                MERGE INTO {bronze_fqn} AS t
                USING _e2e_close_keys AS s
                ON {on} AND t._bronze_is_current = TRUE
                WHEN MATCHED THEN UPDATE SET
                    t._bronze_valid_to   = CAST('{atomic_ts}' AS {valid_to_type}),
                    t._bronze_is_current = FALSE
                """
            )
        except Exception as e:
            audit_logger.log(bronze_fqn, audit_log.Action.MERGE_UPDATE, status="failure",
                           error_type=type(e).__name__, error_message=str(e))
            raise

        metrics = spark.sql(f"DESCRIBE HISTORY {bronze_fqn} LIMIT 1").select("operationMetrics").first()[0]
        audit_logger.log_merge(bronze_fqn, metrics)

    # ═══════════════════════════════════════════════════════════════════════════
    # EXECUTE INSERT: ADD NEW CURRENT ROWS
    # ═══════════════════════════════════════════════════════════════════════════

    if n_insert:
        # Use atomic timestamp captured earlier — ensures close and insert share the same instant
        new_rows = (
            to_insert.withColumn("_bronze_valid_from", F.lit(atomic_ts).cast(audit_col_types["_bronze_valid_from"]))
            .withColumn("_bronze_valid_to", F.lit(None).cast(audit_col_types["_bronze_valid_to"]))
            .withColumn("_bronze_is_current", F.lit(True).cast(audit_col_types["_bronze_is_current"]))
            .withColumn("_bronze_ingested_at", F.lit(atomic_ts).cast(audit_col_types["_bronze_ingested_at"]))
            .drop(*scd2.STAGING_META, "_action")
        )
        with audit_logger.audit(bronze_fqn, audit_log.Action.INSERT) as log_ctx:
            (
                new_rows.write.format("delta")
                .mode("append")
                .saveAsTable(bronze_fqn)
            )
            log_ctx.set_count(n_insert)

    # ═══════════════════════════════════════════════════════════════════════════
    # FINALIZE
    # ═══════════════════════════════════════════════════════════════════════════

    # Advance watermark after both writes succeed
    control.set_watermark(spark, catalog, schemas["audit"], entity, to_version, n_close, n_insert)

    # Log schema changes (checked after writes, so mergeSchema changes are captured)
    schema_audit.detect_and_log(spark, catalog, schemas["audit"], bronze_fqn, f"bronze_{entity}", USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=True)

    return {"closed": n_close, "inserted": n_insert, "version": to_version, "ran": True}


# COMMAND ----------

# Schemas and framework-owned audit tables are created by
# 00_setup_test_environment.py, not here. Fail fast with a clear message if
# that hasn't been run yet, rather than a confusing error deep inside a
# write.
schemas = get_schemas()
for _audit_table in ("audit_log", "job_flags", "schema_audit_log"):
    _fqn = get_fqn(_audit_table, "audit")
    if not spark.catalog.tableExists(_fqn):
        raise RuntimeError(f"Audit table {_fqn} does not exist. Run 00_setup_test_environment.py first.")

# Process each entity
results = {}
for entity in ENTITIES:
    print(f"\n{'='*70}")
    print(f"Processing Bronze for entity: {entity}")
    print(f"{'='*70}")

    actual_key, entity_config = get_config_entity(entity, BRONZE_CONFIG["tables"])
    if not entity_config:
        print(f"WARNING: Entity '{entity}' has no config in bronze_config.yml")
        continue

    result = process_bronze_entity(spark, CATALOG, entity, entity_config, schemas)
    results[entity] = result
    print(f"Result: {result}")

print(f"\n{'='*70}")
print(f"Bronze Processing Complete")
print(f"{'='*70}")
for entity, result in results.items():
    print(f"{entity}: {result}")

# COMMAND ----------

# Display sample data from first entity's Bronze table
if ENTITIES:
    first_entity = ENTITIES[0]
    bronze_fqn = get_fqn(first_entity, "bronze")
    print(f"\nSample data from {bronze_fqn}:")
    display(spark.table(bronze_fqn).orderBy("_bronze_valid_from").limit(10))
