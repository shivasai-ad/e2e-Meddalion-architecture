"""Watermark tracking for incremental Bronze loads.

Built on top of common/job_flags.py, the generic key-value store
medallion-cdc-fed-pattern.md specifies every pipeline should reuse instead of
inventing its own control table. This module previously built its own
`_ingestion_control` table -- literally the anti-pattern the doc names by
that exact table name -- fixed here: it now only stores/reads
`last_processed_version`, `rows_closed`, `rows_inserted` as job_flags keys
under job_name `bronze_{entity}`, matching the doc's own usage example.

Kept as a thin adapter (rather than having common/pipeline.py call
job_flags directly) so the watermark's call signature stays unchanged for
its one caller.
"""

from . import job_flags


def ensure_control_table(spark, catalog: str, audit_schema: str, use_external: bool = False, bucket: str = None) -> str:
    return job_flags.ensure_table(spark, catalog, audit_schema, use_external, bucket)


def get_watermark(spark, catalog: str, audit_schema: str, entity: str, layer: str = "bronze"):
    """Last processed source version for this layer, or None if this
    entity has never run at this layer. `layer` keys the watermark so
    Bronze (reading Staging's CDF) and Silver (reading Bronze's CDF) each
    track their own position, even though both call it "commit_version" --
    they're positions in two different tables' transaction logs."""
    job_name = f"{layer}_{entity}"
    value = job_flags.get(spark, catalog, audit_schema, job_name, "last_processed_version", None)
    return int(value) if value is not None else None


def set_watermark(spark, catalog, audit_schema, entity, version, closed, inserted, layer: str = "bronze"):
    """Advance the watermark for this layer. Called only after this layer's
    write(s) for the run have succeeded."""
    job_name = f"{layer}_{entity}"
    job_flags.set(spark, catalog, audit_schema, job_name, "last_processed_version", str(version))
    job_flags.set(spark, catalog, audit_schema, job_name, "rows_closed", str(closed))
    job_flags.set(spark, catalog, audit_schema, job_name, "rows_inserted", str(inserted))
