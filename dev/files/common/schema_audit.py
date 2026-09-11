"""Schema-change audit log.

Detects and records column-level schema drift (added / dropped / type
changed) on any table this harness manages. This is a different concern
from common/audit_log.py, which records per-write row counts/status -- that
table has no idea whether a run's schema looked different from the last
one. This module fills that gap.

The "last known schema" snapshot is stored as a JSON string in job_flags
(common/job_flags.py) under job_name="schema_audit", one key per watched
table -- reusing the doc's own generic key-value control table rather than
inventing a second snapshot-storage mechanism, same principle
common/control.py already applies to watermarks.

Call detect_and_log() once per table, right after that table's own write for
this run completes (or, for staging -- which this pipeline never writes to
itself -- right before it's read) so drift is logged the same run it's first
observed, not one run late.
"""

import json

from pyspark.sql import functions as F

from . import job_flags

SCHEMA_AUDIT_TABLE = "schema_audit_log"
SCHEMA_AUDIT_JOB_NAME = "schema_audit"

# Global columns to always ignore (framework-managed, not user data)
_ALWAYS_IGNORED_COLUMNS = {"_rescued_data"}  # Lakeflow/Databricks framework column


def _fqn(catalog: str, audit_schema: str) -> str:
    return f"{catalog}.{audit_schema}.{SCHEMA_AUDIT_TABLE}"


def ensure_table(spark, catalog: str, audit_schema: str, use_external: bool = False, bucket: str = None) -> str:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{audit_schema}")
    fqn = _fqn(catalog, audit_schema)
    if spark.catalog.tableExists(fqn):
        # Skip reissuing CREATE TABLE for an existing table -- Databricks
        # validates specified TBLPROPERTIES against the existing table's
        # properties even with IF NOT EXISTS, and Iceberg UniForm conversion
        # asynchronously adds a Databricks-managed property to the table
        # after creation that we can never specify ourselves. See
        # common/gold_freshness.py::ensure_table for the full explanation.
        return fqn
    loc = ""
    if use_external:
        if not bucket:
            raise ValueError("use_external=true but no bucket configured for schema_audit_log.")
        loc = f"LOCATION 's3://{bucket}/audit/client_e2e_medallion_metadata_test/schema_audit_log/'"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            audit_id           BIGINT    NOT NULL,   -- xxhash64 of (catalog, schema, table, column_name, change_type, event_timestamp)
            event_timestamp    TIMESTAMP NOT NULL,
            `catalog`          STRING    NOT NULL,
            `schema`           STRING    NOT NULL,
            `table`            STRING    NOT NULL,
            change_type        STRING    NOT NULL,   -- COLUMN_ADDED | COLUMN_DROPPED | TYPE_CHANGED
            column_name        STRING    NOT NULL,
            old_type           STRING,
            new_type           STRING,
            x_ad_meta_year     STRING    NOT NULL,
            x_ad_meta_month    STRING    NOT NULL,
            x_ad_meta_day      STRING    NOT NULL
        ) USING DELTA
        PARTITIONED BY (x_ad_meta_year, x_ad_meta_month, x_ad_meta_day)
        {loc}
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.deletedFileRetentionDuration' = 'interval 30 days',
            'delta.enableIcebergCompatV2' = 'true',
            'delta.universalFormat.enabledFormats' = 'iceberg'
        )
        """
    )
    return fqn


def _current_schema(spark, fqn: str) -> dict:
    return {f.name: f.dataType.simpleString() for f in spark.table(fqn).schema.fields}


def _snapshot_key(table_short_name: str) -> str:
    return f"schema_watch_{table_short_name}"


def detect_and_log(
    spark, catalog: str, audit_schema: str, table_fqn: str, table_short_name: str,
    use_external: bool = False, bucket: str = None, halt_on_drop: bool = False,
) -> list:
    """Compares table_fqn's live schema against the snapshot recorded last
    time this was called for table_short_name. Logs any drift to
    schema_audit_log, then updates the stored snapshot to the current
    schema either way. Returns the list of changes found (empty if none, or
    if this is the first time this table has ever been watched -- there is
    nothing to diff a brand-new baseline against).

    halt_on_drop=False by default -- a dropped column is logged but never
    stops the pipeline, which is what tests/test_column_drop_handling.py and
    93_schema_evolution_playbook.py Scenario B both rely on ("must NOT
    raise"). Pass halt_on_drop=True (wired through as an opt-in job
    parameter, see notebooks 01/02) to get the stricter halt-on-drop
    behavior instead, for a caller that wants a dropped business column to
    stop the run rather than be silently absorbed. The raise happens after
    _log_changes, so a halted run is still fully diagnosable from
    schema_audit_log afterward."""
    ensure_table(spark, catalog, audit_schema, use_external, bucket)
    job_flags.ensure_table(spark, catalog, audit_schema, use_external, bucket)

    current = _current_schema(spark, table_fqn)
    key = _snapshot_key(table_short_name)
    previous_json = job_flags.get(spark, catalog, audit_schema, SCHEMA_AUDIT_JOB_NAME, key, None)

    if previous_json is None:
        job_flags.set(spark, catalog, audit_schema, SCHEMA_AUDIT_JOB_NAME, key, json.dumps(current))
        return []

    previous = json.loads(previous_json)
    changes = []
    for col, dtype in current.items():
        if col not in previous:
            changes.append({"change_type": "COLUMN_ADDED", "column_name": col, "old_type": None, "new_type": dtype})
        elif previous[col] != dtype:
            changes.append({"change_type": "TYPE_CHANGED", "column_name": col, "old_type": previous[col], "new_type": dtype})
    for col, dtype in previous.items():
        if col not in current:
            changes.append({"change_type": "COLUMN_DROPPED", "column_name": col, "old_type": dtype, "new_type": None})

    if changes:
        _log_changes(spark, catalog, audit_schema, table_fqn, changes)
        job_flags.set(spark, catalog, audit_schema, SCHEMA_AUDIT_JOB_NAME, key, json.dumps(current))

        dropped = [c["column_name"] for c in changes if c["change_type"] == "COLUMN_DROPPED"]
        if halt_on_drop and dropped:
            # Raised only after the audit rows above are written, so a
            # halted run is still fully diagnosable from schema_audit_log.
            raise RuntimeError(
                f"Column(s) {dropped} dropped from {table_fqn} -- halting "
                f"because halt_on_drop=True was requested for this run."
            )
    return changes


def detect_drift_against_config(
    spark, catalog: str, audit_schema: str, table_fqn: str, table_short_name: str,
    expected_columns: list, ignored_columns: list = None,
    use_external: bool = False, bucket: str = None, halt_on_drop: bool = False,
) -> list:
    """Compares table_fqn's live schema against expected_columns (from config).

    Unlike detect_and_log() which compares against the previous snapshot,
    this compares against the SOURCE OF TRUTH (your config), catching:
    - COLUMN_ADDED: staging has columns not in config (usually noise)
    - COLUMN_DROPPED: config expects columns missing from staging (CRITICAL)
    - TYPE_CHANGED: column type doesn't match config (WARNING)

    This is the Payments approach: config is authoritative, not staging's live schema.

    Args:
        spark: Spark session
        catalog: Catalog name
        audit_schema: Audit schema name
        table_fqn: Fully qualified table name (e.g., "catalog.schema.table")
        table_short_name: Short name for logging (e.g., "staging_orders")
        expected_columns: List of (column_name, column_type) tuples from config
        ignored_columns: Optional list of column names to ignore
        use_external: Whether to use external storage
        bucket: S3 bucket if use_external=True
        halt_on_drop: If True, raise RuntimeError when columns are dropped

    Returns:
        List of changes found (empty if none)
    """
    ensure_table(spark, catalog, audit_schema, use_external, bucket)

    # Get actual schema from the table
    current = _current_schema(spark, table_fqn)

    # Convert expected columns list to dict for comparison
    expected = {name: dtype.upper() for name, dtype in expected_columns}
    current_upper = {name: dtype.upper() for name, dtype in current.items()}

    # Columns to ignore: combine global always-ignored + per-call ignored_columns
    ignored = _ALWAYS_IGNORED_COLUMNS | set(ignored_columns or [])

    # Find drift
    changes = []

    # 1. COLUMN_ADDED: in staging but not in config
    for col in current_upper:
        if col not in expected and col not in ignored:
            changes.append({
                "change_type": "COLUMN_ADDED",
                "column_name": col,
                "old_type": None,
                "new_type": current_upper[col]
            })

    # 2. COLUMN_DROPPED: in config but NOT in staging (CRITICAL!)
    for col in expected:
        if col not in current_upper:
            changes.append({
                "change_type": "COLUMN_DROPPED",
                "column_name": col,
                "old_type": expected[col],
                "new_type": None
            })

    # 3. TYPE_CHANGED: in both but type mismatch
    for col in current_upper:
        if col in expected and current_upper[col] != expected[col]:
            changes.append({
                "change_type": "TYPE_CHANGED",
                "column_name": col,
                "old_type": expected[col],
                "new_type": current_upper[col]
            })

    # Log all changes
    if changes:
        _log_changes(spark, catalog, audit_schema, table_fqn, changes)

        # Print summary to logs
        for c in changes:
            severity = "CRITICAL" if c["change_type"] == "COLUMN_DROPPED" else \
                      "WARNING" if c["change_type"] == "TYPE_CHANGED" else "INFO"
            print(f"[SCHEMA DRIFT][{severity}] {table_short_name}.{c['column_name']}: "
                  f"{c['change_type']} (expected={c['old_type']}, actual={c['new_type']})")

        # Halt if critical
        dropped = [c["column_name"] for c in changes if c["change_type"] == "COLUMN_DROPPED"]
        if halt_on_drop and dropped:
            raise RuntimeError(
                f"Schema validation failed for {table_fqn}: column(s) {dropped} are declared in config "
                f"but no longer exist in staging. Update config or restore the column(s) before re-running. "
                f"See schema_audit_log for full details."
            )

    return changes


def _log_changes(spark, catalog: str, audit_schema: str, table_fqn: str, changes: list) -> None:
    catalog_part, schema_part, table_part = table_fqn.split(".")
    rows = [
        (catalog_part, schema_part, table_part, c["change_type"], c["column_name"], c["old_type"], c["new_type"])
        for c in changes
    ]
    df = spark.createDataFrame(
        rows,
        "catalog string, schema string, table string, change_type string, column_name string, old_type string, new_type string",
    )
    df = (
        df.withColumn("event_timestamp", F.current_timestamp())
        .withColumn(
            "audit_id",
            F.xxhash64(
                F.concat_ws(
                    "|",
                    F.col("catalog"), F.col("schema"), F.col("table"),
                    F.col("column_name"), F.col("change_type"), F.col("event_timestamp").cast("string"),
                )
            ),
        )
        .withColumn("x_ad_meta_year", F.date_format(F.col("event_timestamp"), "yyyy"))
        .withColumn("x_ad_meta_month", F.date_format(F.col("event_timestamp"), "MM"))
        .withColumn("x_ad_meta_day", F.date_format(F.col("event_timestamp"), "dd"))
    )
    fqn = _fqn(catalog, audit_schema)
    target_cols = [f.name for f in spark.table(fqn).schema.fields]
    df.select(*target_cols).write.format("delta").mode("append").saveAsTable(fqn)
