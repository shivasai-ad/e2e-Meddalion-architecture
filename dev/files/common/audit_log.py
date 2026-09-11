"""AuditLogger -- records every Bronze/Silver/Gold write to a shared
audit_log table, per medallion-cdc-fed-pattern.md's "Operational monitoring"
section: "use AuditLogger... to record every Bronze/Silver/Gold write.
log_merge() decomposes a MERGE's operationMetrics into separate
insert/update/delete audit rows automatically."

Two ways to log a write, matching the doc's own two examples:

    with audit_logger.audit(full_table_name, AuditLogger.Action.INSERT) as log_ctx:
        ...write...
        log_ctx.set_count(row_count)
    # success -> one row, status='success'. exception -> one row,
    # status='failure' with error_type/error_message, then re-raises.

    metrics = <MERGE's operationMetrics dict>
    audit_logger.log_merge(full_table_name, metrics)
    # -> up to 3 rows: merge_insert / merge_update / merge_delete

job_id/run_id/task_run_id/task_key are read from spark.conf when this is
running as a job task (the only place those confs are set); a hand-run
notebook or pytest simply gets None back for each, same as before -- no
threading of job context into run_bronze/run_silver/run_gold's own
signatures needed for this.
"""

import time
from contextlib import contextmanager

from pyspark.sql import functions as F


def _safe_conf_get(spark, key):
    """spark.conf.get(key, None) on Spark Connect (serverless) raises
    AnalysisException [CONFIG_NOT_AVAILABLE...] instead of returning the
    default when the config is unset/disallowed-for-read -- unlike classic
    Spark, where the default is returned silently. Catch broadly here since
    the failure mode is only "config not available", never a real error we'd
    want surfaced from an audit-logging path."""
    try:
        return spark.conf.get(key, None)
    except Exception:
        return None

AUDIT_LOG_TABLE = "audit_log"


class Action:
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MERGE_INSERT = "merge_insert"
    MERGE_UPDATE = "merge_update"
    MERGE_DELETE = "merge_delete"
    OVERWRITE = "overwrite"
    FULL_REFRESH = "full_refresh"


class _LogContext:
    def __init__(self):
        self.row_count = 0

    def set_count(self, n: int) -> None:
        self.row_count = int(n)


class AuditLogger:
    Action = Action

    def __init__(self, spark, catalog: str, audit_schema: str, use_external: bool = False, bucket: str = None):
        self.spark = spark
        self.catalog = catalog
        self.audit_schema = audit_schema
        self.use_external = use_external
        self.bucket = bucket
        self.fqn = f"{catalog}.{audit_schema}.{AUDIT_LOG_TABLE}"

    def ensure_table(self) -> str:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.audit_schema}")
        if self.spark.catalog.tableExists(self.fqn):
            # Skip reissuing CREATE TABLE for an existing table -- Databricks
            # validates specified TBLPROPERTIES against the existing table's
            # properties even with IF NOT EXISTS, and Iceberg UniForm
            # conversion asynchronously adds a Databricks-managed property to
            # the table after creation that we can never specify ourselves.
            # See common/gold_freshness.py::ensure_table for the full
            # explanation.
            return self.fqn
        loc = ""
        if self.use_external:
            if not self.bucket:
                raise ValueError("use_external=true but no bucket configured for audit_log.")
            loc = f"LOCATION 's3://{self.bucket}/audit/client_e2e_medallion_metadata_test/audit_log/'"
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fqn} (
                audit_id           BIGINT    NOT NULL,
                event_timestamp    TIMESTAMP NOT NULL,
                `catalog`          STRING    NOT NULL,
                `schema`           STRING    NOT NULL,
                `table`            STRING    NOT NULL,
                action             STRING    NOT NULL,
                row_count          BIGINT    NOT NULL,
                status             STRING    NOT NULL,
                error_type         STRING,
                error_message      STRING,
                actor              STRING,
                job_id             STRING,
                run_id             STRING,
                task_run_id        STRING,
                task_key           STRING,
                duration_ms        BIGINT,
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
        return self.fqn

    def _write(self, full_table_name, action, row_count, status, duration_ms,
               error_type=None, error_message=None):
        catalog, schema, table = full_table_name.split(".")
        err_msg = str(error_message)[:4000] if error_message else None
        row = [(
            catalog, schema, table, action, int(row_count), status,
            error_type, err_msg, int(duration_ms),
        )]
        df = self.spark.createDataFrame(
            row,
            "catalog string, schema string, table string, action string, row_count long, "
            "status string, error_type string, error_message string, duration_ms long",
        )
        # Only set when this write is running as a job task; None otherwise
        # (interactive notebook, pytest) -- same fallback as before, now
        # actually populated when the context exists instead of always NULL.
        job_id = _safe_conf_get(self.spark, "spark.databricks.job.id")
        run_id = _safe_conf_get(self.spark, "spark.databricks.job.runId")
        task_run_id = _safe_conf_get(self.spark, "spark.databricks.job.taskRunId")
        task_key = _safe_conf_get(self.spark, "spark.databricks.job.taskKey")
        df = (
            df.withColumn("event_timestamp", F.current_timestamp())
            .withColumn("actor", F.current_user())
            .withColumn("job_id", F.lit(job_id).cast("string"))
            .withColumn("run_id", F.lit(run_id).cast("string"))
            .withColumn("task_run_id", F.lit(task_run_id).cast("string"))
            .withColumn("task_key", F.lit(task_key).cast("string"))
            .withColumn(
                "audit_id",
                F.xxhash64(
                    F.concat_ws(
                        "|",
                        F.col("catalog"), F.col("schema"), F.col("table"),
                        F.col("action"), F.col("status"), F.col("actor"),
                        F.col("event_timestamp").cast("string"),
                    )
                ),
            )
            .withColumn("x_ad_meta_year", F.date_format(F.col("event_timestamp"), "yyyy"))
            .withColumn("x_ad_meta_month", F.date_format(F.col("event_timestamp"), "MM"))
            .withColumn("x_ad_meta_day", F.date_format(F.col("event_timestamp"), "dd"))
        )
        target_cols = [f.name for f in self.spark.table(self.fqn).schema.fields]
        df.select(*target_cols).write.format("delta").mode("append").saveAsTable(self.fqn)

    @contextmanager
    def audit(self, full_table_name: str, action: str):
        ctx = _LogContext()
        start = time.time()
        try:
            yield ctx
        except Exception as e:
            self._write(
                full_table_name, action, ctx.row_count, "failure",
                int((time.time() - start) * 1000),
                error_type=type(e).__name__, error_message=str(e),
            )
            raise
        else:
            self._write(
                full_table_name, action, ctx.row_count, "success",
                int((time.time() - start) * 1000),
            )

    def log(self, full_table_name: str, action: str, row_count: int = 0,
             status: str = "failure", error_type: str = None,
             error_message: str = None, duration_ms: int = 0) -> None:
        """Explicit, direct audit write for a statement that isn't wrapped
        in .audit() -- e.g. a MERGE run outside a context manager, caught in
        a plain try/except. Call this in the except block, before re-raising,
        so the failure is on record even though nothing else here catches it."""
        self._write(full_table_name, action, row_count, status, duration_ms,
                    error_type=error_type, error_message=error_message)

    def log_merge(self, full_table_name: str, operation_metrics: dict) -> None:
        """Decompose a MERGE's operationMetrics into merge_insert/
        merge_update/merge_delete audit rows -- one per action, always,
        matching the doc's own example (even a 0-count action gets a row,
        so a quiet run is visible, not indistinguishable from "didn't run")."""
        inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
        updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
        deleted = int(operation_metrics.get("numTargetRowsDeleted", 0))
        self._write(full_table_name, Action.MERGE_INSERT, inserted, "success", 0)
        self._write(full_table_name, Action.MERGE_UPDATE, updated, "success", 0)
        self._write(full_table_name, Action.MERGE_DELETE, deleted, "success", 0)
