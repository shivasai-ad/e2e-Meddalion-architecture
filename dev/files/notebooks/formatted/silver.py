# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - SILVER (v2): Current-State Conform
# MAGIC
# MAGIC Bronze → Silver using Bronze's own Change Data Feed (CDF) -- the exact
# MAGIC same mechanic Bronze uses to read Staging, just one layer up. Silver has
# MAGIC no SCD2 history of its own: it holds only the current state per key, kept
# MAGIC in sync with Bronze's `_bronze_is_current` flag via MERGE.
# MAGIC
# MAGIC All configuration (columns, primary keys, audit column mappings) comes
# MAGIC from `config/silver_config.yml`. Runs for all entities or a single entity
# MAGIC specified via widget.
# MAGIC
# MAGIC **Depends on `formatted_stg/bronze.py` (or `bronze_v2.py`) having already
# MAGIC been run** -- this notebook only reads from Bronze, it never creates the
# MAGIC Bronze table.
# MAGIC
# MAGIC **Difference from `silver.py`:** this version splits the Silver table's
# MAGIC *first-time load* out into its own explicit branch (create table, one
# MAGIC plain INSERT of Bronze's current rows, no CDF at all) instead of always
# MAGIC routing through `cdf.read_changes(..., from_version=None, ...)`. Every
# MAGIC run after that goes through the normal incremental CDF+MERGE path --
# MAGIC mirrors the same bootstrap/incremental split used in `bronze_v2.py`
# MAGIC (`_bootstrap_bronze_table` / `_incremental_bronze_merge`) and
# MAGIC `gold_v2.py` (`_bootstrap_gold_table` / `_incremental_gold_merge`).
# MAGIC
# MAGIC **Silver mechanics (incremental path only):**
# MAGIC - Reads Bronze's CDF since Silver's own last watermark (a *different*
# MAGIC   watermark than Bronze's -- see `common/control.py`'s `layer` param),
# MAGIC   never a full scan of Bronze.
# MAGIC - Collapses multiple CDF rows for the same key in one run down to the
# MAGIC   latest (by Bronze's own commit version) -- this is what makes an
# MAGIC   ordinary update (Bronze: close old + insert new, two CDF rows) resolve
# MAGIC   to a single Silver UPDATE instead of a spurious DELETE+INSERT.
# MAGIC - `_bronze_is_current = TRUE` on the latest row for a key -> upsert.
# MAGIC   `_bronze_is_current = FALSE` with no newer row for that key -> delete.

# COMMAND ----------

import os
import sys
from typing import Dict, List, Tuple

# This notebook lives at notebooks/formatted/silver_v2.py -- two levels below
# the project root (dev/files) where common/ and config/ live. Databricks
# sets a notebook's CWD to its own containing folder, so ".." would only
# reach notebooks/, not dev/files.
PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from pyspark.sql import functions as F
from common.config_loader import (
    load_silver_config,
    get_catalog,
    get_use_external_tables,
    get_schemas,
    get_fqn,
    get_s3_bucket,
)
from common import ddl, cdf, audit_log, control, schema_audit

# COMMAND ----------

# Widget for entity selection
dbutils.widgets.text("entity", "ALL", label="Entity (or ALL)")
ENTITY = dbutils.widgets.get("entity").strip()

# Constants
CATALOG = get_catalog()
USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None

# Load configuration
SILVER_CONFIG = load_silver_config(os.path.join(PROJECT_ROOT, "config/silver_config.yml"))
CONFIG_ENTITIES = list(SILVER_CONFIG.get("tables", {}).keys())

# Case-insensitive entity lookup: map lowercase keys to actual config keys
entity_map = {key.lower(): key for key in CONFIG_ENTITIES}
entity_normalized = ENTITY.lower()

# Filter to specified entity if not ALL
if entity_normalized != "all":
    if entity_normalized not in entity_map:
        raise ValueError(f"Entity '{ENTITY}' not found in silver_config. Available: {CONFIG_ENTITIES}")
    ENTITIES = [entity_map[entity_normalized]]  # Use actual config key
else:
    ENTITIES = CONFIG_ENTITIES  # Use all entities as-is

print(f"Processing Silver for entities: {ENTITIES}")

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


def process_silver_entity(spark, catalog: str, entity: str, config: Dict, schemas: Dict) -> Dict:
    """
    Run Silver for a single entity: first-time load if the Silver table
    doesn't exist yet, incremental CDF+MERGE from Bronze otherwise.

    Args:
        spark: Spark session
        catalog: Catalog name
        entity: Entity name
        config: Entity config from silver_config.yml["tables"][entity]
        schemas: Dict of schema names

    Returns:
        Dict with metrics: {deleted: int, upserted: int, version: int, ran: bool}
    """
    # Get table names
    bronze_fqn = get_fqn(entity, "bronze")
    silver_fqn = get_fqn(entity, "silver")

    # Build business column list from config
    business_columns = [(col["name"], col["type"]) for col in config.get("columns", [])]
    business_col_names = [name for name, _ in business_columns]
    primary_keys = config.get("primary_keys", [])

    # ═══════════════════════════════════════════════════════════════════════════
    # Build audit column plan from silver_config.yml's defaults.audit_columns.
    # Two kinds:
    #   - "maps_from": copied straight from a Bronze column at merge/insert time
    #     (e.g. _record_effective_date <- _bronze_valid_from)
    #   - "generated_by: framework": computed here in code, not read from
    #     Bronze (e.g. _silver_processed_at, stamped with this run's atomic
    #     timestamp -- same pattern as Bronze's own atomic_ts)
    # ═══════════════════════════════════════════════════════════════════════════
    defaults = SILVER_CONFIG.get("defaults", {})
    audit_columns = []       # (name, type [+ NOT NULL]) for DDL
    mapped_audit = {}        # silver_col_name -> bronze_col_name
    framework_audit = []     # [silver_col_name, ...] stamped in code
    # Plain name -> type map (no "NOT NULL" suffix) so framework-generated
    # audit columns can be .cast(...) to their config-declared type below
    # instead of hardcoding "timestamp".
    audit_col_types = {}

    for audit_col in defaults.get("audit_columns", []):
        col_name = audit_col["name"]
        col_type = audit_col["type"]
        audit_col_types[col_name] = col_type.lower()
        nullable = audit_col.get("nullable", True)
        if not nullable:
            col_type = f"{col_type} NOT NULL"
        audit_columns.append((col_name, col_type))

        if "maps_from" in audit_col:
            mapped_audit[col_name] = audit_col["maps_from"]
        elif audit_col.get("generated_by") == "framework":
            framework_audit.append(col_name)

    table_properties = defaults.get("table_properties", {})

    # All Silver columns in the order the DDL/merge will use them.
    target_columns = business_col_names + list(mapped_audit.keys()) + framework_audit

    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP: Detect Bronze schema drift, verify Bronze exists
    # ═══════════════════════════════════════════════════════════════════════════

    # Silver schema is created by 00_setup_test_environment.py, not here.
    # Bronze is owned and created by formatted_stg/bronze.py, not here --
    # Silver only ever reads from it.
    if not spark.catalog.tableExists(bronze_fqn):
        raise RuntimeError(
            f"Bronze table {bronze_fqn} does not exist. Run formatted_stg/bronze.py "
            f"for entity '{entity}' first."
        )

    # Schema validation: compare Bronze's LIVE schema against silver_config.yml's expected columns
    # (config is source of truth). Validates that Bronze has all the columns Silver expects to read.
    # Must run BEFORE any Silver logic, so we catch mismatches early instead of inside a cryptic MERGE error.
    # Include: business columns + mapped audit source columns + _bronze_is_current (used in MERGE condition)
    mapped_source_cols = [(col, "TIMESTAMP") for col in mapped_audit.values()]  # Maps to Bronze source columns
    is_current_col = [("_bronze_is_current", "BOOLEAN")]  # Used in MERGE ON condition
    expected_from_bronze = business_columns + mapped_source_cols + is_current_col

    bronze_ignored = defaults.get("ignored_columns", [])  # Only per-entity config ignored columns
    schema_audit.detect_drift_against_config(
        spark, catalog, schemas["audit"], bronze_fqn, f"bronze_{entity}",
        expected_from_bronze,
        ignored_columns=bronze_ignored,  # Per-entity config + global _ALWAYS_IGNORED_COLUMNS
        use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True, halt_on_type_change=True
    )

    # Schema drift snapshot: track changes from previous run (for historical audit trail)
    # Same snapshot key Bronze itself uses (f"bronze_{entity}") -- there's only one schema for
    # bronze_fqn to watch, so both layers share the same baseline rather than tracking two
    # independent snapshots of the same table.
    schema_audit.detect_and_log(
        spark, catalog, schemas["audit"], bronze_fqn, f"bronze_{entity}",
        USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=False  # Already halted above if critical
    )

    audit_logger = audit_log.AuditLogger(spark, catalog, schemas["audit"], USE_EXTERNAL, AUDIT_BUCKET)

    # ═══════════════════════════════════════════════════════════════════════════
    # DETERMINE PATH: table exists or not
    # ═══════════════════════════════════════════════════════════════════════════

    silver_table_exists = spark.catalog.tableExists(silver_fqn)

    if not silver_table_exists:
        return _bootstrap_silver_table(
            spark, catalog, entity, silver_fqn, bronze_fqn, business_columns, business_col_names,
            primary_keys, audit_columns, audit_col_types, mapped_audit, framework_audit,
            table_properties, audit_logger, schemas
        )
    else:
        return _incremental_silver_merge(
            spark, catalog, entity, silver_fqn, bronze_fqn, primary_keys, mapped_audit,
            framework_audit, audit_col_types, target_columns, audit_logger, schemas
        )


def _bootstrap_silver_table(
    spark, catalog: str, entity: str, silver_fqn: str, bronze_fqn: str,
    business_columns: List[Tuple[str, str]], business_col_names: List[str], primary_keys: List[str],
    audit_columns: List[Tuple[str, str]], audit_col_types: Dict[str, str], mapped_audit: Dict[str, str],
    framework_audit: List[str], table_properties: Dict, audit_logger, schemas: Dict
) -> Dict:
    """First-time load: create Silver table, then a single plain INSERT of
    Bronze's current rows (_bronze_is_current = TRUE), no CDF/MERGE -- there
    is no prior Silver state to reconcile against yet."""

    # Silver not existing is only a valid bootstrap if there's also no prior
    # watermark for it -- same consistency check bronze_v2.py's
    # _bootstrap_bronze_table does. A non-None watermark here means Silver
    # was dropped/recreated without its job_flags row being cleared;
    # silently bootstrapping over that would discard whatever version range
    # the stale watermark implied had already been processed.
    existing_watermark = control.get_watermark(spark, catalog, schemas["audit"], entity, layer="silver")
    if existing_watermark is not None:
        raise RuntimeError(
            f"Inconsistent state for '{entity}': Silver table {silver_fqn} does not exist, "
            f"but job_flags already has a watermark (last_processed_version={existing_watermark}) "
            f"for it. Silver was likely dropped/recreated without clearing its job_flags row. "
            f"Resolve manually (clear the job_flags row for this entity if the table's state "
            f"is truly gone, or restore the table) before rerunning -- refusing to silently "
            f"re-bootstrap over a non-None watermark."
        )

    print(f"\n{'='*70}")
    print(f"BOOTSTRAP: Creating Silver table '{silver_fqn}'")
    print(f"   ✓ Confirmed no prior watermark for '{entity}' — safe to bootstrap")
    print(f"{'='*70}")

    silver_ddl = ddl.generate_silver_ddl(
        silver_fqn, entity, business_columns, audit_columns, table_properties, USE_EXTERNAL, primary_keys
    )
    print("DDL:")
    print(silver_ddl)
    print(f"{'='*70}\n")
    spark.sql(silver_ddl)

    # Snapshot Bronze's version BEFORE reading it, and read this exact
    # version -- so the watermark stored afterwards, and the rows actually
    # inserted, refer to the same point in time. A commit landing on Bronze
    # while this bootstrap is running is simply picked up by the next
    # (incremental) run instead of silently getting lost past the watermark.
    bronze_bootstrap_version = spark.sql(f"DESCRIBE HISTORY {bronze_fqn}").agg(F.max("version")).collect()[0][0]
    bronze_df = (
        spark.read.format("delta").option("versionAsOf", bronze_bootstrap_version).table(bronze_fqn)
        .filter(F.col("_bronze_is_current"))
    )

    atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

    select_exprs = [F.col(c) for c in business_col_names]
    select_exprs += [F.col(bronze_col).alias(silver_col) for silver_col, bronze_col in mapped_audit.items()]

    new_rows = bronze_df.select(*select_exprs)
    for fw_col in framework_audit:
        new_rows = new_rows.withColumn(fw_col, F.lit(atomic_ts).cast(audit_col_types[fw_col]))

    row_count = new_rows.count()
    if row_count > 0:
        with audit_logger.audit(silver_fqn, audit_log.Action.INSERT) as log_ctx:
            new_rows.write.format("delta").mode("append").saveAsTable(silver_fqn)
            log_ctx.set_count(row_count)
        print(f"✓ First-time load: inserted {row_count} current rows from Bronze")
    else:
        print(f"⚠ Bronze has no current rows — Silver table created but no data inserted")

    # Seed Silver's watermark at the bootstrap version so the first
    # incremental run's CDF read starts strictly after this snapshot.
    control.set_watermark(spark, catalog, schemas["audit"], entity, bronze_bootstrap_version, 0, row_count, layer="silver")

    # Log Silver's own schema (checked after the write, same as the incremental path).
    schema_audit.detect_and_log(spark, catalog, schemas["audit"], silver_fqn, f"silver_{entity}", USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=True, halt_on_type_change=True)

    return {"deleted": 0, "upserted": row_count, "version": bronze_bootstrap_version, "ran": True}


def _incremental_silver_merge(
    spark, catalog: str, entity: str, silver_fqn: str, bronze_fqn: str, primary_keys: List[str],
    mapped_audit: Dict[str, str], framework_audit: List[str], audit_col_types: Dict[str, str],
    target_columns: List[str], audit_logger, schemas: Dict
) -> Dict:
    """Incremental: read Bronze's CDF since Silver's own watermark, collapse
    to the latest row per key, and MERGE (upsert/delete) into Silver."""

    print(f"\n{'='*70}")
    print(f"INCREMENTAL: Silver table '{silver_fqn}' already exists")
    print(f"{'='*70}")

    # ═══════════════════════════════════════════════════════════════════════════
    # READ CHANGES VIA BRONZE'S OWN CDF (incremental -- never a full scan)
    # ═══════════════════════════════════════════════════════════════════════════

    to_version = spark.sql(f"DESCRIBE HISTORY {bronze_fqn}").agg(F.max("version")).collect()[0][0]
    from_version = control.get_watermark(spark, catalog, schemas["audit"], entity, layer="silver")

    # If already at or past this version, nothing to do
    if from_version is not None and from_version >= to_version:
        print(f"   ℹ Already at target version — skipping processing\n")
        return {"deleted": 0, "upserted": 0, "version": to_version, "ran": False}

    # ═══════════════════════════════════════════════════════════════════════════
    # LOG INCREMENTAL DETAILS
    # ═══════════════════════════════════════════════════════════════════════════

    print(f"\n📊 CDF (Change Data Feed) DETAILS:")
    print(f"   Bronze watermark: from_version={from_version} → to_version={to_version}")
    print(f"   Processing versions: {from_version + 1} to {to_version}")

    changes = cdf.read_changes(spark, bronze_fqn, from_version, to_version)

    changes_count = changes.count()
    print(f"   ✓ Total CDF changes read: {changes_count}")

    if changes.isEmpty():
        control.set_watermark(spark, catalog, schemas["audit"], entity, to_version, 0, 0, layer="silver")
        print(f"   ℹ No new changes — skipping MERGE processing\n")
        return {"deleted": 0, "upserted": 0, "version": to_version, "ran": True}

    # Show sample of changes
    print(f"\n   📋 Sample CDF changes (first 5):")
    changes.select(*primary_keys, "_change_type", "_commit_timestamp").limit(5).show(truncate=False)

    # Collapse multiple CDF rows for the same key down to the latest --
    # ordered by Bronze's own commit_version, so this correctly resolves
    # "close old + insert new" (two rows, same run, but two separate
    # commits) to a single UPDATE, and a genuine delete (one row,
    # is_current=FALSE, no newer row) to a DELETE.
    changes = cdf.latest_per_key(changes, primary_keys)

    # ═══════════════════════════════════════════════════════════════════════════
    # BUILD THE MERGE SOURCE: business cols + mapped audit cols (renamed from
    # Bronze) + framework audit cols (stamped with this run's atomic timestamp)
    # ═══════════════════════════════════════════════════════════════════════════

    # Capture atomic timestamp ONCE -- every row touched by this run's merge
    # gets the same _silver_processed_at-style stamp, same reasoning as
    # Bronze's atomic_ts for close+insert.
    atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

    business_col_names = [c for c in changes.columns if c not in cdf.CDF_META and c not in (
        list(mapped_audit.values()) + ["_bronze_hash", "_bronze_valid_from", "_bronze_valid_to", "_bronze_is_current", "_bronze_ingested_at", "_rescued_data"]
    )]

    select_exprs = [F.col(c) for c in business_col_names]
    select_exprs += [F.col(bronze_col).alias(silver_col) for silver_col, bronze_col in mapped_audit.items()]
    select_exprs.append(F.col("_bronze_is_current"))

    source = changes.select(*select_exprs)
    for fw_col in framework_audit:
        source = source.withColumn(fw_col, F.lit(atomic_ts).cast(audit_col_types[fw_col]))

    source.createOrReplaceTempView("_silver_changes")

    on = " AND ".join(f"t.{k} <=> s.{k}" for k in primary_keys)
    update_set = ", ".join(f"t.{c} = s.{c}" for c in target_columns)
    insert_cols = ", ".join(target_columns)
    insert_vals = ", ".join(f"s.{c}" for c in target_columns)

    try:
        spark.sql(
            f"""
            MERGE INTO {silver_fqn} AS t
            USING _silver_changes AS s
            ON {on}
            WHEN MATCHED AND s._bronze_is_current = FALSE THEN DELETE
            WHEN MATCHED AND s._bronze_is_current = TRUE THEN UPDATE SET {update_set}
            WHEN NOT MATCHED AND s._bronze_is_current = TRUE
                THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
        )
    except Exception as e:
        audit_logger.log(silver_fqn, audit_log.Action.MERGE_UPDATE, status="failure",
                       error_type=type(e).__name__, error_message=str(e))
        raise

    metrics = spark.sql(f"DESCRIBE HISTORY {silver_fqn} LIMIT 1").select("operationMetrics").first()[0]
    audit_logger.log_merge(silver_fqn, metrics)

    n_deleted = int(metrics.get("numTargetRowsDeleted", 0))
    n_upserted = int(metrics.get("numTargetRowsUpdated", 0)) + int(metrics.get("numTargetRowsInserted", 0))

    # ═══════════════════════════════════════════════════════════════════════════
    # FINALIZE
    # ═══════════════════════════════════════════════════════════════════════════

    control.set_watermark(spark, catalog, schemas["audit"], entity, to_version, n_deleted, n_upserted, layer="silver")

    # Log Silver's own schema (checked after the write, so a widened/changed
    # schema is captured the same run it happens, same pattern as Bronze).
    # Halt on critical (dropped) column changes.
    schema_audit.detect_and_log(spark, catalog, schemas["audit"], silver_fqn, f"silver_{entity}", USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=True, halt_on_type_change=True)

    return {"deleted": n_deleted, "upserted": n_upserted, "version": to_version, "ran": True}


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
    print(f"Processing Silver for entity: {entity}")
    print(f"{'='*70}")

    actual_key, entity_config = get_config_entity(entity, SILVER_CONFIG["tables"])
    if not entity_config:
        print(f"WARNING: Entity '{entity}' has no config in silver_config.yml")
        continue

    result = process_silver_entity(spark, CATALOG, entity, entity_config, schemas)
    results[entity] = result
    print(f"Result: {result}")

print(f"\n{'='*70}")
print(f"Silver Processing Complete")
print(f"{'='*70}")
for entity, result in results.items():
    print(f"{entity}: {result}")

# COMMAND ----------

# Display sample data from first entity's Silver table
if ENTITIES:
    first_entity = ENTITIES[0]
    silver_fqn = get_fqn(first_entity, "silver")
    print(f"\nSample data from {silver_fqn}:")
    display(spark.table(silver_fqn).limit(10))
