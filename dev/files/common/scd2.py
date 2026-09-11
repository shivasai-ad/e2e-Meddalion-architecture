"""SCD2 change-detection mechanics for Bronze.

Generic trim of Payments' notebooks/common_utils/scd2_bronze.py -- no BINARY
key handling here since this harness's synthetic `orders` entity has no
binary columns. Implements the golden path's documented Bronze MERGE mechanics
(medallion-cdc-fed-pattern.md, "Bronze mechanics" section):

1. Hash over every business column (sorted, fixed order), NULL-safe.
2. Classify each change as NEW / CHANGED / DELETED / UNCHANGED against the
   current Bronze row -- dropping UNCHANGED is what makes a replay idempotent.

CDF reading and same-key collapsing are generic (not SCD2-specific) and live
in common/cdf.py instead -- Silver reuses those against Bronze's own CDF
without pulling in any SCD2 mechanics.
"""

from pyspark.sql import functions as F

from .cdf import CDF_META

# `_rescued_data` is Lakeflow's own metadata column (or, here, its stand-in --
# see common/ddl.py), never a business column -- excluded from the hash for
# the same reason `_bronze_*` audit columns are: hashing your own metadata
# would make the hash change even when nothing business-relevant did.
STAGING_META = CDF_META + ["_rescued_data"]

# Distinguishes a NULL from an empty string in the hash input, so ("a", NULL)
# and ("a", "") don't collide. 0x1F cannot appear in normal source text.
NULL_SENTINEL = "\x1fNULL"


def row_hash(business_cols, exclude):
    """MD5 over business columns in a fixed (sorted) order, excluding
    volatile source-side audit stamps such as `updated_at`."""
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL))
        for c in sorted(business_cols)
        if c not in exclude
    ]
    if not parts:
        raise ValueError("row_hash needs at least one non-excluded column")
    return F.md5(F.concat_ws("|", *parts))


def classify(spark, changes, bronze_fqn: str, keys):
    """Tag each change NEW / CHANGED / DELETED / UNCHANGED against current
    Bronze. Dropping UNCHANGED downstream is what makes replaying the same
    CDF range a no-op (TC-005 / TC-403)."""
    current = (
        spark.table(bronze_fqn)
        .filter(F.col("_bronze_is_current"))
        .select(*keys, F.col("_bronze_hash").alias("_cur_hash"))
    )#
    for k in keys:
        current = current.withColumnRenamed(k, f"_cur_{k}")

    cond = None
    for k in keys:
        eq = F.col(k) == F.col(f"_cur_{k}")
        cond = eq if cond is None else (cond & eq)

    joined = changes.join(current, cond, "left")
    return joined.withColumn(
        "_action",
        F.when(F.col("_change_type") == "delete", F.lit("DELETED"))
        .when(F.col("_cur_hash").isNull(), F.lit("NEW"))
        .when(F.col("_cur_hash") != F.col("_bronze_hash"), F.lit("CHANGED"))
        .otherwise(F.lit("UNCHANGED")),
        # `_cur_hash` (like `_cur_{k}`) is this function's own join artifact,
        # never meant to leave it -- caught leaking all the way into Bronze's
        # real persisted schema (and from there into Silver too, via the
        # ALTER-TABLE-widen step in run_silver) once run_bronze switched to
        # a drop-known-metadata list instead of a fixed target-column
        # SELECT. The old fixed SELECT accidentally caught this; the new
        # approach must drop it explicitly instead.
    ).drop(*[f"_cur_{k}" for k in keys], "_cur_hash")
