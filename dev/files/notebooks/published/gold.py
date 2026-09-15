# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - GOLD: Published (Governed) Layer
# MAGIC
# MAGIC Silver → Gold with compliance metadata validation and incremental merge.
# MAGIC
# MAGIC Gold is the final published layer, governed by compliance metadata from
# MAGIC `config/gold_compliance_metadata.yml`. Every table must have a compliance
# MAGIC entry before creation, or be explicitly marked as exempt.
# MAGIC
# MAGIC **Depends on `formatted/silver.py` having already been run** — this
# MAGIC notebook only reads from Silver, it never creates the Silver table.
# MAGIC
# MAGIC **Gold mechanics:**
# MAGIC - Reads compliance config for each entity; errors if missing (unless exempt)
# MAGIC - First run: creates Gold table, applies metadata, inserts all Silver rows
# MAGIC - Subsequent runs: checks config hash; if changed, re-applies metadata
# MAGIC - Incremental: reads Silver's CDF from Gold's own watermark (`job_flags`,
# MAGIC   `layer="gold"`) to Silver's latest version -- same watermark mechanic
# MAGIC   Bronze/Silver already use, not a timestamp-column lookup
# MAGIC - Collapses multiple CDF rows for the same key within one incremental
# MAGIC   window down to the latest, ordered by Delta's own `_commit_version` --
# MAGIC   same collapse Silver does against Bronze's CDF, applied here against
# MAGIC   Silver's CDF. Without this, two events for the same key in one window
# MAGIC   (e.g. an update followed by a delete) would both apply, landing the
# MAGIC   key in contradictory buckets.
# MAGIC - Deletes are branched on directly in the MERGE (CDF `_change_type = 'delete'`),
# MAGIC   never via `WHEN NOT MATCHED BY SOURCE` -- the CDF source is a delta batch,
# MAGIC   not a full snapshot, so a key's absence from a batch doesn't mean it was deleted
# MAGIC - Merges with 3 branches (DELETE, UPDATE, INSERT) as one atomic Delta
# MAGIC   operation; stamps `_gold_refreshed_at` on all touched rows (one atomic timestamp)
# MAGIC - Primary keys, and DDL column names/types, both come from
# MAGIC   `gold_compliance_metadata.yml`'s own `columns`/`constraints` blocks --
# MAGIC   the single source of truth for Gold's DDL and its compliance gate.
# MAGIC   `_gold_refreshed_at` and table properties are hardcoded, global
# MAGIC   Python constants in this file (`AUDIT_COLUMNS`, `TABLE_PROPERTIES`),
# MAGIC   not config. Most Gold columns are read directly from Silver by the
# MAGIC   same name; a small hardcoded set (`CALCULATED_COLUMNS`) are computed
# MAGIC   from other Silver columns instead -- still declared in
# MAGIC   gold_compliance_metadata.yml like any other column, just with their
# MAGIC   formula in Python rather than YAML.

# COMMAND ----------

import os
import sys
from typing import Dict, List, Tuple

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from pyspark.sql import functions as F
from common.config_loader import (
    get_catalog,
    get_use_external_tables,
    get_schemas,
    get_fqn,
    get_s3_bucket,
)
from common import ddl, audit_log, control, schema_audit, cdf
from common.gold_compliance_validator import (
    load_compliance_config,
    compute_config_hash,
    get_table_tag_value,
    apply_compliance_metadata,
)

# COMMAND ----------

# Widget for entity selection
dbutils.widgets.text("entity", "ALL", label="Entity (or ALL)")
ENTITY = dbutils.widgets.get("entity").strip()

# Constants
CATALOG = get_catalog()
USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None
CONFIG_HASH_TAG = "metadata_hash"

# Framework-owned audit columns and table properties: same for every Gold
# table, so these are hardcoded here rather than duplicated per-entity in a
# YAML config. (name, type, not_nullable)
AUDIT_COLUMNS: List[Tuple[str, str, bool]] = [
    ("_gold_refreshed_at", "TIMESTAMP", True),
]
TABLE_PROPERTIES: Dict[str, str] = {
    "delta.enableChangeDataFeed": "false",
    "delta.columnMapping.mode": "name",
    "delta.enableIcebergCompatV2": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

# Load compliance config -- single source of truth for both compliance
# metadata (description/owner/tags/constraints) and Gold's DDL column list
# (name + type per column, read alongside).
COMPLIANCE_CONFIG = load_compliance_config(
    os.path.join(PROJECT_ROOT, "config/gold_compliance_metadata.yml")
)

CONFIG_ENTITIES = list(COMPLIANCE_CONFIG.keys())

# Case-insensitive entity lookup: map lowercase keys to actual config keys
entity_map = {key.lower(): key for key in CONFIG_ENTITIES}
entity_normalized = ENTITY.lower()

# Filter to specified entity if not ALL
if entity_normalized != "all":
    if entity_normalized not in entity_map:
        raise ValueError(f"Entity '{ENTITY}' not found in gold_compliance_metadata.yml. Available: {CONFIG_ENTITIES}")
    ENTITIES = [entity_map[entity_normalized]]  # Use actual config key
else:
    ENTITIES = CONFIG_ENTITIES  # Use all entities as-is

print(f"Processing Gold for entities: {ENTITIES}")

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


def get_primary_keys(compliance_entry: Dict, entity: str) -> List[str]:
    """Primary key columns come from gold_compliance_metadata.yml's own
    PRIMARY_KEY constraint -- the compliance gate that already runs before
    any Gold table is created -- instead of being assumed from the first
    entry in the `columns` dict (which has no guaranteed relationship to
    identity)."""
    for constraint in compliance_entry.get("constraints", []):
        if constraint.get("type") == "PRIMARY_KEY":
            columns = constraint.get("columns", [])
            if columns:
                return list(columns)
    raise ValueError(
        f"No PRIMARY_KEY constraint found in gold_compliance_metadata.yml for entity '{entity}'. "
        f"Add one under '{entity}' -> 'constraints':\n"
        f"  constraints:\n"
        f"    - name: \"pk_{entity}\"\n"
        f"      type: \"PRIMARY_KEY\"\n"
        f"      columns: [\"<primary_key_column>\"]"
    )


def get_gold_columns(compliance_entry: Dict) -> List[Tuple[str, str]]:
    """Data columns (name, type) for this entity, read directly from
    gold_compliance_metadata.yml's own `columns` block -- in its own
    iteration order. Excludes audit columns (those are the global
    AUDIT_COLUMNS constant, not per-entity config) since audit columns are
    generated by the framework, not read from Silver by name."""
    audit_names = {name for name, _, _ in AUDIT_COLUMNS}
    return [
        (name, meta["type"])
        for name, meta in compliance_entry.get("columns", {}).items()
        if name not in audit_names
    ]


# A small number of Gold columns are computed from Silver rather than read
# by name. This is a hardcoded Python mapping, not YAML, since the formula
# itself isn't config data -- the column's (name, type) still lives in
# gold_compliance_metadata.yml like any other column.
CALCULATED_COLUMNS: Dict[str, str] = {
    "total_tax_amount": "amount * 1.18",
}


def _gold_column_expr(name: str) -> F.Column:
    """Select expression for one Gold column: its hardcoded formula if it's
    in CALCULATED_COLUMNS, otherwise a direct pass-through read from Silver
    by the same name."""
    if name in CALCULATED_COLUMNS:
        return F.expr(CALCULATED_COLUMNS[name]).alias(name)
    return F.col(name).alias(name)


def _audit_columns_ddl() -> List[Tuple[str, str]]:
    """AUDIT_COLUMNS as (name, type) DDL fragments, e.g. TIMESTAMP NOT NULL."""
    return [
        (name, f"{col_type} NOT NULL" if not_nullable else col_type)
        for name, col_type, not_nullable in AUDIT_COLUMNS
    ]


def _audit_select_exprs(atomic_ts) -> Dict[str, F.Column]:
    """AUDIT_COLUMNS as select expressions, stamped with one atomic timestamp
    per write so every row touched in a run gets the same _gold_refreshed_at."""
    return {
        name: F.lit(atomic_ts).cast(col_type.lower())
        for name, col_type, _ in AUDIT_COLUMNS
    }

# COMMAND ----------


def process_gold_entity(spark, catalog: str, entity: str, schemas: Dict) -> Dict:
    """
    Sync or create a single entity's Gold table from Silver.

    Args:
        spark: Spark session
        catalog: Catalog name
        entity: Entity name
        schemas: Dict of schema names

    Returns:
        Dict with metrics: {deleted: int, upserted: int, version: int, ran: bool}
    """
    # Get table names
    silver_fqn = get_fqn(entity, "silver")
    gold_fqn = get_fqn(entity, "gold")

    # ═══════════════════════════════════════════════════════════════════════════
    # COMPLIANCE CHECK: Table must have an entry in gold_compliance_metadata.yml
    # ═══════════════════════════════════════════════════════════════════════════

    _, compliance_entry = get_config_entity(entity, COMPLIANCE_CONFIG)

    if not compliance_entry:
        error_msg = (
            f"{'='*70}\n"
            f"CONFIG NOT FOUND: Table '{entity}' missing from gold_compliance_metadata.yml\n"
            f"{'='*70}\n"
            f"FIX: Add this table to config/gold_compliance_metadata.yml with:\n"
            f"  {entity}:\n"
            f"    requires_compliance: true  (or false if exempt)\n"
            f"    description: \"<description>\"\n"
            f"    owner: \"<owner>\"\n"
            f"    tags: {{source_system: ..., source_squad: ..., ...}}\n"
            f"    columns: {{...}}\n"
            f"    constraints: [...]\n"
            f"See gold_compliance_metadata.yml for full template.\n"
            f"{'='*70}"
        )
        print(error_msg)
        raise ValueError(error_msg)


    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP: Verify Silver exists
    # ═══════════════════════════════════════════════════════════════════════════

    # Gold schema is created by 00_setup_test_environment.py, not here.
    if not spark.catalog.tableExists(silver_fqn):
        raise RuntimeError(
            f"Silver table {silver_fqn} does not exist. Run formatted/silver.py for "
            f"entity '{entity}' first."
        )

    # Gold's full column list: most are read directly from Silver by name;
    # a few (CALCULATED_COLUMNS) are computed from other Silver columns
    # instead -- the audit column is generated by the framework, not read
    # from Silver, so it's excluded here too.
    gold_columns = get_gold_columns(compliance_entry)

    # Schema validation: compare Silver's LIVE schema against the compliance
    # config's expected columns (config is source of truth for what Gold
    # reads from Silver). Must run BEFORE any Gold logic, so we catch
    # mismatches early instead of inside a cryptic MERGE error. Calculated
    # columns are excluded -- they're derived, not read from Silver by name,
    # so Silver was never expected to have them.
    gold_source_columns = [
        (name, col_type) for name, col_type in gold_columns
        if name not in CALCULATED_COLUMNS
    ]
    schema_audit.detect_drift_against_config(
        spark, catalog, schemas["audit"], silver_fqn, f"silver_{entity}",
        gold_source_columns,
        ignored_columns=[],
        use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True, halt_on_type_change=True
    )

    # Schema drift snapshot: track changes from previous run (for historical audit trail)
    # Same snapshot key Silver itself uses (f"silver_{entity}") -- there's only one schema for
    # silver_fqn to watch, so both layers share the same baseline rather than tracking two
    # independent snapshots of the same table.
    schema_audit.detect_and_log(
        spark, catalog, schemas["audit"], silver_fqn, f"silver_{entity}",
        USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=False  # Already halted above if critical
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # PRIMARY KEYS: sourced from compliance metadata's own PRIMARY_KEY
    # constraint. Needed by both bootstrap (sanity check) and incremental
    # (delete + merge). No cross-file consistency check needed anymore --
    # there's only one config file, so PK columns and DDL columns can never
    # be out of sync with each other.
    # ═══════════════════════════════════════════════════════════════════════════

    primary_keys = get_primary_keys(compliance_entry, entity)

    # ═══════════════════════════════════════════════════════════════════════════
    # DETERMINE PATH: table exists or not
    # ═══════════════════════════════════════════════════════════════════════════

    gold_table_exists = spark.catalog.tableExists(gold_fqn)

    if not gold_table_exists:
        return _bootstrap_gold_table(
            spark, catalog, entity, gold_fqn, silver_fqn, gold_columns, compliance_entry, schemas, primary_keys
        )
    else:
        return _incremental_gold_merge(
            spark, catalog, entity, gold_fqn, silver_fqn, gold_columns, compliance_entry, schemas, primary_keys
        )


def _bootstrap_gold_table(
    spark, catalog: str, entity: str, gold_fqn: str, silver_fqn: str, gold_columns: List[Tuple[str, str]],
    compliance_entry: Dict, schemas: Dict, primary_keys: List[str]
) -> Dict:
    """Bootstrap: create Gold table, apply metadata, insert all Silver rows."""

    print(f"\n{'='*70}")
    print(f"BOOTSTRAP: Creating Gold table '{gold_fqn}'")
    print(f"{'='*70}")

    # ═══════════════════════════════════════════════════════════════════════════
    # GENERATE DDL: entity columns from gold_compliance_metadata.yml, audit
    # columns + table properties hardcoded globally above.
    # ═══════════════════════════════════════════════════════════════════════════

    gold_ddl = ddl.generate_gold_ddl(gold_fqn, entity, gold_columns, _audit_columns_ddl(), TABLE_PROPERTIES, USE_EXTERNAL)

    print(f"\n{'='*70}")
    print(f"GENERATED DDL FOR GOLD TABLE: {gold_fqn}")
    print(f"{'='*70}")
    print(gold_ddl)
    print(f"{'='*70}\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # CREATE TABLE: use governed procedure if compliance required
    # ═══════════════════════════════════════════════════════════════════════════

    if compliance_entry.get("requires_compliance", False):
        # Validate DDL via governed procedure (checks for _gold_refreshed_at, etc.)
        try:
            escaped_ddl = gold_ddl.replace("'", "\\'")
            spark.sql(
                f"CALL {catalog}.{schemas['gold']}.create_governed_table('{escaped_ddl}')"
            )
            print(f"✓ Gold table created (validated by governed procedure): {gold_fqn}")
        except Exception as e:
            error_msg = f"Governed procedure rejected DDL: {str(e)}"
            print(f"✗ {error_msg}")
            raise ValueError(error_msg)
    else:
        # Direct DDL execution (no compliance validation)
        spark.sql(gold_ddl)
        print(f"✓ Gold table created (no compliance validation): {gold_fqn}")

    # ═══════════════════════════════════════════════════════════════════════════
    # APPLY COMPLIANCE METADATA (before any data)
    # ═══════════════════════════════════════════════════════════════════════════

    if compliance_entry.get("requires_compliance", False):
        result = apply_compliance_metadata(catalog, schemas["gold"], entity, compliance_entry, spark)
        print(f"\n{'='*60}")
        print(f"METADATA APPLIED SUCCESSFULLY to '{gold_fqn}'")
        print(f"{'='*60}")
        for item in result["applied"]:
            print(f"  ✓ {item}")
        print(f"{'='*60}\n")
    else:
        print(f"ℹ No compliance metadata to apply (table is exempt)")

    # ═══════════════════════════════════════════════════════════════════════════
    # INSERT ALL SILVER ROWS (with audit columns)
    # ═══════════════════════════════════════════════════════════════════════════

    audit_logger = audit_log.AuditLogger(spark, catalog, schemas["audit"], USE_EXTERNAL, AUDIT_BUCKET)

    # Snapshot Silver's version BEFORE reading it, so the watermark we store
    # afterwards reflects "everything up to and including this version is
    # already in Gold" -- the next incremental run's CDF read starts strictly
    # after this point instead of re-reading from version 0.
    silver_bootstrap_version = spark.sql(f"DESCRIBE HISTORY {silver_fqn}").agg(F.max("version")).collect()[0][0]

    silver_df = spark.table(silver_fqn)
    silver_count = silver_df.count()

    if silver_count > 0:
        atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

        # Every Gold column is read directly from Silver by name (or computed
        # via CALCULATED_COLUMNS), plus the framework-generated audit columns
        # stamped with one atomic timestamp.
        col_exprs = [_gold_column_expr(name) for name, _ in gold_columns]
        audit_exprs = [expr.alias(name) for name, expr in _audit_select_exprs(atomic_ts).items()]
        gold_insert = silver_df.select(*(col_exprs + audit_exprs))

        with audit_logger.audit(gold_fqn, audit_log.Action.INSERT) as log_ctx:
            gold_insert.write.mode("append").insertInto(gold_fqn)
            log_ctx.set_count(silver_count)

        print(f"✓ Inserted {silver_count} rows into Gold from Silver")
    else:
        print(f"⚠ Silver table is empty — Gold table created but no data inserted")

    # Seed Gold's watermark at bootstrap version so the first incremental run
    # reads only what changed in Silver AFTER this snapshot, not from scratch.
    control.set_watermark(spark, catalog, schemas["audit"], entity, silver_bootstrap_version, 0, silver_count, layer="gold")

    return {"deleted": 0, "upserted": silver_count, "version": silver_bootstrap_version, "ran": True}


def _incremental_gold_merge(
    spark, catalog: str, entity: str, gold_fqn: str, silver_fqn: str, gold_columns: List[Tuple[str, str]],
    compliance_entry: Dict, schemas: Dict, primary_keys: List[str]
) -> Dict:
    """Incremental: check hash, re-apply metadata if changed, merge from Silver."""

    print(f"\n{'='*70}")
    print(f"INCREMENTAL: Gold table '{gold_fqn}' already exists")
    print(f"{'='*70}")

    # ═══════════════════════════════════════════════════════════════════════════
    # HASH-BASED METADATA DRIFT DETECTION
    # ═══════════════════════════════════════════════════════════════════════════

    if compliance_entry.get("requires_compliance", False):
        current_hash = compute_config_hash(compliance_entry)
        stored_hash = get_table_tag_value(catalog, schemas["gold"], entity, CONFIG_HASH_TAG, spark)

        if current_hash == stored_hash:
            print(f"✓ Config unchanged (hash match) — skipping metadata re-application")
        else:
            print(f"⚠ Config changed (hash mismatch) — re-applying compliance metadata")
            result = apply_compliance_metadata(catalog, schemas["gold"], entity, compliance_entry, spark)
            print(f"{'='*60}")
            print(f"METADATA RE-APPLIED")
            print(f"{'='*60}")
            for item in result["applied"]:
                print(f"  ✓ {item}")
            print(f"{'='*60}\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # WATERMARK: read Gold's own last-processed Silver version (job_flags,
    # layer="gold") -- same mechanic Bronze/Silver already use, instead of
    # reverse-engineering a version number from a _gold_refreshed_at timestamp.
    # ═══════════════════════════════════════════════════════════════════════════

    audit_logger = audit_log.AuditLogger(spark, catalog, schemas["audit"], USE_EXTERNAL, AUDIT_BUCKET)

    silver_version = spark.sql(f"DESCRIBE HISTORY {silver_fqn}").agg(F.max("version")).collect()[0][0]
    silver_from_version = control.get_watermark(spark, catalog, schemas["audit"], entity, layer="gold")

    is_first_incremental_read = silver_from_version is None

    print(f"\n📊 INCREMENTAL CDF WATERMARK DETAILS:")
    print(f"   Gold watermark: from_version={silver_from_version} → to_version={silver_version}")
    if not is_first_incremental_read:
        print(f"   Processing Silver versions: {silver_from_version + 1} to {silver_version}")
    else:
        print(f"   ⚠ No prior Gold watermark found — reading ALL Silver via CDF (versions 0-{silver_version})")

    if silver_from_version is not None and silver_from_version >= silver_version:
        print(f"   ℹ Already at target version — skipping processing\n")
        return {"deleted": 0, "upserted": 0, "version": silver_version, "ran": False}

    # Use CDF to capture INSERT/UPDATE/DELETE in one pass. read_changes()
    # already tags each row's _change_type (insert / update_postimage /
    # delete -- update_preimage is dropped there), so the MERGE below can
    # branch on that column directly instead of running a separate DELETE
    # statement first.
    source = cdf.read_changes(spark, silver_fqn, silver_from_version, silver_version)
    source_count = source.count()
    print(f"   ✓ Total CDF changes (INSERT/UPDATE/DELETE): {source_count}")

    if source_count == 0:
        control.set_watermark(spark, catalog, schemas["audit"], entity, silver_version, 0, 0, layer="gold")
        print(f"ℹ No new changes in Silver — skipping merge\n")
        return {"deleted": 0, "upserted": 0, "version": silver_version, "ran": True}

    # Collapse multiple CDF rows for the same key within this incremental
    # window down to the latest one, ordered by Delta's own commit version.
    # Without this, two events for the same key in one window (e.g. an
    # update followed by a delete, or vice versa) could both survive into
    # the merge source -- and Delta's MERGE raises "multiple source rows
    # matched" when more than one source row matches the same target row.
    source = cdf.latest_per_key(source, primary_keys)

    # Show sample of what's being processed (uses this entity's actual
    # primary key columns -- a hardcoded column name here previously broke
    # every entity other than "orders").
    print(f"\n   📋 Sample CDF changes (after latest_per_key collapse):")
    source.select(*primary_keys, "_change_type", "_commit_timestamp").limit(5).show(truncate=False)

    # ═══════════════════════════════════════════════════════════════════════════
    # BUILD MERGE SOURCE: Select all columns + audit
    # ═══════════════════════════════════════════════════════════════════════════

    atomic_ts = spark.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0][0]

    audit_col_names = [name for name, _, _ in AUDIT_COLUMNS]

    # Every Gold column is read directly from Silver by name (or computed via
    # CALCULATED_COLUMNS), plus the framework-generated audit columns stamped
    # with one atomic timestamp.
    col_exprs_merge = [_gold_column_expr(name) for name, _ in gold_columns]
    audit_exprs_merge = [expr.alias(name) for name, expr in _audit_select_exprs(atomic_ts).items()]

    # _change_type carried through unaliased -- not a target column, only
    # used by the MERGE's WHEN clauses below to decide DELETE vs UPSERT.
    all_col_names = [name for name, _ in gold_columns] + audit_col_names
    merge_source = source.select(*(col_exprs_merge + audit_exprs_merge + [F.col("_change_type")]))

    merge_source.createOrReplaceTempView("_gold_merge_source")

    # ═══════════════════════════════════════════════════════════════════════════
    # EXECUTE 3-BRANCH MERGE (primary_keys passed in from process_gold_entity,
    # sourced from gold_compliance_metadata.yml's PRIMARY_KEY constraint).
    # Insert, update and delete happen as one atomic Delta operation, branching
    # on CDF's own _change_type the same way the ON clause branches on key
    # match -- there is no separate DELETE statement.
    # ═══════════════════════════════════════════════════════════════════════════

    on_clause = " AND ".join(f"t.{k} <=> s.{k}" for k in primary_keys)
    target_cols = all_col_names
    update_set = ", ".join(f"t.{c} = s.{c}" for c in target_cols)
    insert_cols = ", ".join(target_cols)
    insert_vals = ", ".join(f"s.{c}" for c in target_cols)

    merge_sql = f"""
        MERGE INTO {gold_fqn} AS t
        USING _gold_merge_source AS s
        ON {on_clause}
        WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED AND s._change_type != 'delete' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    try:
        spark.sql(merge_sql)
    except Exception as e:
        audit_logger.log(gold_fqn, audit_log.Action.MERGE_UPDATE, status="failure",
                       error_type=type(e).__name__, error_message=str(e))
        raise

    metrics = spark.sql(f"DESCRIBE HISTORY {gold_fqn} LIMIT 1").select("operationMetrics").first()[0]
    audit_logger.log_merge(gold_fqn, metrics)

    n_deleted = int(metrics.get("numTargetRowsDeleted", 0))
    n_upserted = int(metrics.get("numTargetRowsUpdated", 0)) + int(metrics.get("numTargetRowsInserted", 0))

    # ═══════════════════════════════════════════════════════════════════════════
    # FINALIZE: advance Gold's own watermark now that the merge has succeeded.
    # ═══════════════════════════════════════════════════════════════════════════

    control.set_watermark(spark, catalog, schemas["audit"], entity, silver_version, n_deleted, n_upserted, layer="gold")

    print(f"✓ Merge complete: {n_upserted} upserted, {n_deleted} deleted")

    return {"deleted": n_deleted, "upserted": n_upserted, "version": silver_version, "ran": True}


# COMMAND ----------

# Schemas and framework-owned audit tables are created by
# 00_setup_test_environment.py, not here.
schemas = get_schemas()
for _audit_table in ("audit_log", "job_flags", "schema_audit_log"):
    _fqn = get_fqn(_audit_table, "audit")
    if not spark.catalog.tableExists(_fqn):
        raise RuntimeError(f"Audit table {_fqn} does not exist. Run 00_setup_test_environment.py first.")

# Process each entity
results = {}
for entity in ENTITIES:
    print(f"\n{'='*70}")
    print(f"Processing Gold for entity: {entity}")
    print(f"{'='*70}")

    actual_key, entity_compliance_entry = get_config_entity(entity, COMPLIANCE_CONFIG)
    if not entity_compliance_entry:
        print(f"⚠ Entity '{entity}' has no config in gold_compliance_metadata.yml")
        continue

    result = process_gold_entity(spark, CATALOG, entity, schemas)
    results[entity] = result
    print(f"Result: {result}")

    # Schema validation: compare Gold's OWN schema against gold_compliance_metadata.yml's
    # expected columns (config is source of truth). Validates that Gold was
    # created/maintained correctly. Checked after the write, so schema changes
    # are captured the same run they happen.
    gold_fqn = get_fqn(entity, "gold")
    gold_expected_cols = get_gold_columns(entity_compliance_entry)
    gold_audit_names = [(name, col_type) for name, col_type, _ in AUDIT_COLUMNS]

    schema_audit.detect_drift_against_config(
        spark, CATALOG, schemas["audit"], gold_fqn, f"gold_{entity}",
        gold_expected_cols + gold_audit_names,
        ignored_columns=[],
        use_external=USE_EXTERNAL, bucket=AUDIT_BUCKET, halt_on_drop=True, halt_on_type_change=True
    )

    # Schema drift snapshot: track changes from previous run (for historical audit trail)
    # Checked after the write, so schema changes are captured the same run they happen.
    schema_audit.detect_and_log(
        spark, CATALOG, schemas["audit"], gold_fqn, f"gold_{entity}",
        USE_EXTERNAL, AUDIT_BUCKET, halt_on_drop=False  # Already halted above if critical
    )

print(f"\n{'='*70}")
print(f"Gold Processing Complete")
print(f"{'='*70}")
for entity, result in results.items():
    status = "✓ COMPLETE" if result["ran"] else "ℹ SKIPPED"
    print(f"{entity}: {status} — {result['upserted']} upserted, {result['deleted']} deleted")

# COMMAND ----------

# Display sample data from first entity's Gold table
if ENTITIES:
    first_entity = ENTITIES[0]
    gold_fqn = get_fqn(first_entity, "gold")
    print(f"\nSample data from {gold_fqn}:")
    display(spark.table(gold_fqn).limit(10))
