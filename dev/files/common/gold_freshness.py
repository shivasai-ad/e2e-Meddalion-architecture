"""Durable Gold freshness log.

Every Gold object already carries an inline `_gold_refreshed_at` column
(see common/pipeline.py::run_gold), but that only tells you "when was this
row computed" from inside a query against the object itself -- a freshness
check that wants to alert on a *stale* or *missing* refresh needs a place to
look that doesn't depend on the object being queryable at all. This module
is that place: one durable, append-only row per Gold refresh, in its own
table, alongside audit_log/schema_audit_log in the same audit schema.

Call log_refresh() once, right after a Gold object's publish succeeds
(common/pipeline.py::run_gold does this for orders_status_summary).
notebooks/95_gold_freshness_check.py reads this table to answer "how long
ago was object X last refreshed?"
"""

from pyspark.sql import functions as F

GOLD_FRESHNESS_TABLE = "gold_refresh_log"


def _fqn(catalog: str, audit_schema: str) -> str:
    return f"{catalog}.{audit_schema}.{GOLD_FRESHNESS_TABLE}"


def ensure_table(spark, catalog: str, audit_schema: str, use_external: bool = False, bucket: str = None) -> str:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{audit_schema}")
    fqn = _fqn(catalog, audit_schema)
    if spark.catalog.tableExists(fqn):
        # Table already exists. Do NOT reissue CREATE TABLE -- Databricks
        # validates specified TBLPROPERTIES against the existing table's
        # properties even with IF NOT EXISTS, and Iceberg UniForm conversion
        # asynchronously adds delta.universalFormat.iceberg.atomicConversion.supported
        # to the table after the fact. That property is Databricks-managed
        # and cannot be set manually (DELTA_UNKNOWN_CONFIGURATION if you try),
        # so re-running CREATE TABLE here would always fail the property
        # comparison. Skipping is safe: the table's schema/properties were
        # already correct when first created.
        return fqn
    loc = ""
    if use_external:
        if not bucket:
            raise ValueError("use_external=true but no bucket configured for gold_refresh_log.")
        loc = f"LOCATION 's3://{bucket}/audit/client_e2e_medallion_metadata_test/gold_refresh_log/'"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            object_fqn         STRING    NOT NULL,
            refreshed_at        TIMESTAMP NOT NULL,
            row_count           BIGINT    NOT NULL,
            x_ad_meta_year      STRING    NOT NULL,
            x_ad_meta_month     STRING    NOT NULL,
            x_ad_meta_day       STRING    NOT NULL
        ) USING DELTA
        PARTITIONED BY (x_ad_meta_year, x_ad_meta_month, x_ad_meta_day)
        {loc}
        COMMENT 'Durable record of every Gold publish, for staleness checks that cannot depend on the Gold object itself being queryable.'
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.deletedFileRetentionDuration' = 'interval 30 days',
            'delta.enableIcebergCompatV2' = 'true',
            'delta.universalFormat.enabledFormats' = 'iceberg'
        )
        """
    )
    return fqn


def log_refresh(
    spark, catalog: str, audit_schema: str, object_fqn: str, row_count: int,
    use_external: bool = False, bucket: str = None,
) -> None:
    fqn = ensure_table(spark, catalog, audit_schema, use_external, bucket)
    df = spark.createDataFrame([(object_fqn, int(row_count))], "object_fqn string, row_count long")
    df = (
        df.withColumn("refreshed_at", F.current_timestamp())
        .withColumn("x_ad_meta_year", F.date_format(F.col("refreshed_at"), "yyyy"))
        .withColumn("x_ad_meta_month", F.date_format(F.col("refreshed_at"), "MM"))
        .withColumn("x_ad_meta_day", F.date_format(F.col("refreshed_at"), "dd"))
    )
    target_cols = [f.name for f in spark.table(fqn).schema.fields]
    df.select(*target_cols).write.format("delta").mode("append").saveAsTable(fqn)
