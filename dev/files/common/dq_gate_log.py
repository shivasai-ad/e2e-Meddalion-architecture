"""DqGateLogger -- one row per DQ gate run, in the shared audit schema.

Table: `{catalog}.{audit_schema}.dq_gate_log`. A row is written for every run of
`dq.quality_gate`, whether every row passed or some were quarantined, so a clean run
leaves a record too. One table serves every layer: filter on `layer` ('silver', 'gold').
It sits next to `audit_log` and joins to it on `target_table` and `run_id`.

    gate_log = dq_gate_log.DqGateLogger(spark, catalog, schemas["audit"], "gold", gold_fqn,
                                        USE_EXTERNAL, AUDIT_BUCKET)
    dq.quality_gate(spark, entity, df, checks, quarantine_fqn, label, gate_log=gate_log)

The table is created on the first log() if it does not exist yet.
"""

from .config_loader import HARNESS_NAMESPACE
from .job_context import job_value
from pyspark.sql import functions as F
from pyspark.sql import types as T

DQ_GATE_LOG_TABLE = "dq_gate_log"


class DqGateLogger:
    def __init__(self, spark, catalog: str, audit_schema: str, layer: str,
                 target_table: str = None, use_external: bool = False, bucket: str = None):
        self.spark = spark
        self.catalog = catalog
        self.audit_schema = audit_schema
        self.layer = layer
        self.target_table = target_table
        self.use_external = use_external
        self.bucket = bucket
        self.fqn = f"{catalog}.{audit_schema}.{DQ_GATE_LOG_TABLE}"

    def ensure_table(self) -> str:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.audit_schema}")
        if self.spark.catalog.tableExists(self.fqn):
            # Same reason as audit_log.py: do not reissue CREATE TABLE for an existing table
            return self.fqn
        loc = ""
        if self.use_external:
            if not self.bucket:
                raise ValueError("use_external=true but no bucket configured for dq_gate_log.")
            loc = f"LOCATION 's3://{self.bucket}/audit/client_{HARNESS_NAMESPACE}/{DQ_GATE_LOG_TABLE}/'"
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fqn} (
                event_timestamp    TIMESTAMP NOT NULL,
                layer              STRING    NOT NULL,
                entity             STRING    NOT NULL,
                target_table       STRING,
                label              STRING,
                status             STRING    NOT NULL,
                rows_checked       BIGINT    NOT NULL,
                rows_passed        BIGINT    NOT NULL,
                rows_quarantined   BIGINT    NOT NULL,
                failed_rules       MAP<STRING, BIGINT>,
                quarantine_table   STRING,
                actor              STRING,
                job_id             STRING,
                run_id             STRING,
                task_run_id        STRING,
                task_key           STRING,
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

    def log(self, entity: str, label: str, rows_checked: int, rows_passed: int,
            rows_quarantined: int, failed_rules: dict, quarantine_table: str = None) -> None:
        self.ensure_table()
        actor = self.spark.sql("SELECT current_user() AS u").collect()[0]["u"]
        row = [(
            self.layer, entity, self.target_table, label,
            "quarantined" if rows_quarantined else "passed",
            int(rows_checked), int(rows_passed), int(rows_quarantined),
            {k: int(v) for k, v in (failed_rules or {}).items()} or None,
            quarantine_table if rows_quarantined else None,
            actor,
            job_value(self.spark, "spark.databricks.job.id", "job_id"),
            job_value(self.spark, "spark.databricks.job.runId", "job_run_id"),
            job_value(self.spark, "spark.databricks.task.runId", "task_run_id"),
            job_value(self.spark, "spark.databricks.job.taskKey", "task_key"),
        )]
        # Explicit schema: several columns are legitimately None on a clean run.
        row_schema = T.StructType([
            T.StructField("layer", T.StringType()),
            T.StructField("entity", T.StringType()),
            T.StructField("target_table", T.StringType()),
            T.StructField("label", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("rows_checked", T.LongType()),
            T.StructField("rows_passed", T.LongType()),
            T.StructField("rows_quarantined", T.LongType()),
            T.StructField("failed_rules", T.MapType(T.StringType(), T.LongType())),
            T.StructField("quarantine_table", T.StringType()),
            T.StructField("actor", T.StringType()),
            T.StructField("job_id", T.StringType()),
            T.StructField("run_id", T.StringType()),
            T.StructField("task_run_id", T.StringType()),
            T.StructField("task_key", T.StringType()),
        ])
        df = (
            self.spark.createDataFrame(row, row_schema)
            .withColumn("event_timestamp", F.current_timestamp())
            .withColumn("x_ad_meta_year", F.date_format(F.col("event_timestamp"), "yyyy"))
            .withColumn("x_ad_meta_month", F.date_format(F.col("event_timestamp"), "MM"))
            .withColumn("x_ad_meta_day", F.date_format(F.col("event_timestamp"), "dd"))
        )
        target_cols = [f.name for f in self.spark.table(self.fqn).schema.fields]
        df.select(*target_cols).write.format("delta").mode("append").saveAsTable(self.fqn)
