"""Generic Change Data Feed (CDF) read utilities.

Used by any layer that reads a Delta table's own CDF -- Bronze reading
Staging, Silver reading Bronze. No SCD2-specific logic here (no hashing, no
NEW/CHANGED/DELETED/UNCHANGED classification, no valid_from/valid_to) --
that mechanics is Bronze-only and lives in common/scd2.py. This module only
knows how to read a version range off any CDF-enabled Delta table and
collapse multiple changes to the same key down to the latest one.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

CDF_META = ["_change_type", "_commit_version", "_commit_timestamp"]


def read_changes(spark, table_fqn: str, from_version, to_version):
    """CDF slice (from_version, to_version]. from_version=None means this is
    the first-ever run against this table -- treat the whole current table as
    one big batch of 'insert' events (e.g. Bronze's initial full load from
    Staging, or Silver's initial full load from Bronze)."""
    if from_version is None:
        return (
            spark.table(table_fqn)
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(to_version).cast("long"))
            .withColumn("_commit_timestamp", F.current_timestamp())
        )
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version + 1)
        .option("endingVersion", to_version)
        .table(table_fqn)
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )


def latest_per_key(df, keys):
    """Collapse several changes for the same key within one batch down to
    the most recent, ordered by Delta's own commit version -- the source of
    truth for commit order, unlike any custom processing timestamp column."""
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col("_commit_version").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
