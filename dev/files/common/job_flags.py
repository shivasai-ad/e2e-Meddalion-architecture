"""Generic idempotent key-value store backed by a Delta table.

This is the exact utility medallion-cdc-fed-pattern.md specifies every
pipeline should reuse for control-table needs: "Don't invent a new
`_ingestion_control` table per pipeline. Reuse
`notebooks/common_utils/job_flags.py`." This harness previously did exactly
that (a bespoke `_ingestion_control` table) -- fixed here.

One shared table, `job_flags`, holds every pipeline's control values as plain
(job_name, key, value) rows. `set()` always INSERTs (append-only, never
UPDATE/DELETE); `get()` returns the value from the row with the latest
`insertion_timestamp` for that (job_name, key) pair, or `default_value` if
none exists yet -- that absence is how a first-ever run is detected.

External table at `s3://{bucket}/audit/client_e2e_medallion_metadata_test/job_flags/`
when use_external=True, matching the doc's own LOCATION for this exact
table. Kept self-contained (no import of common/ddl.py) on purpose -- the
doc frames job_flags.py as a single standalone file you `%run`, with no
other dependencies.
"""

JOB_FLAGS_TABLE = "job_flags"


def _fqn(catalog: str, audit_schema: str) -> str:
    return f"{catalog}.{audit_schema}.{JOB_FLAGS_TABLE}"


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
            raise ValueError("use_external=true but no bucket configured for job_flags.")
        loc = f"LOCATION 's3://{bucket}/audit/client_e2e_medallion_metadata_test/job_flags/'"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            job_name             STRING,
            key                  STRING,
            value                STRING,
            insertion_timestamp  TIMESTAMP,
            x_ad_meta_year       STRING,
            x_ad_meta_month      STRING,
            x_ad_meta_day        STRING,
            x_ad_meta_hour       STRING,
            x_ad_meta_minute     STRING
        ) USING DELTA
        PARTITIONED BY (x_ad_meta_year, x_ad_meta_month, x_ad_meta_day)
        {loc}
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.deletedFileRetentionDuration' = 'interval 30 days',
            'delta.enableChangeDataFeed' = 'true',
            'delta.enableIcebergCompatV2' = 'true',
            'delta.universalFormat.enabledFormats' = 'iceberg',
            'delta.parquet.compression.codec' = 'zstd'
        )
        """
    )
    return fqn


def get(spark, catalog: str, audit_schema: str, job_name: str, key: str, default_value=None):
    """Value from the row with the latest insertion_timestamp for
    (job_name, key), or default_value if no row exists yet."""
    fqn = _fqn(catalog, audit_schema)
    rows = spark.sql(
        f"""
        SELECT value
        FROM {fqn} a
        WHERE job_name = '{job_name}' AND key = '{key}'
        AND insertion_timestamp = (
            SELECT MAX(insertion_timestamp) FROM {fqn} z
            WHERE z.job_name = a.job_name AND z.key = a.key
        )
        """
    ).collect()
    if not rows:
        return default_value
    return str(rows[0][0])


def set(spark, catalog: str, audit_schema: str, job_name: str, key: str, value: str) -> None:
    """Always INSERTs a new row -- append-only, never updates or deletes."""
    fqn = _fqn(catalog, audit_schema)
    spark.sql(
        f"""
        INSERT INTO {fqn}
            (job_name, key, value, insertion_timestamp,
             x_ad_meta_year, x_ad_meta_month, x_ad_meta_day, x_ad_meta_hour, x_ad_meta_minute)
        SELECT
            '{job_name}', '{key}', '{value}', CURRENT_TIMESTAMP(),
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy'),
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'MM'),
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'dd'),
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH'),
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'mm')
        """
    )
