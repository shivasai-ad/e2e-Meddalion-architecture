# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - PLATINUM: Gold -> Platinum publish (ADP-1026)
# MAGIC
# MAGIC Platinum is the customer-facing layer, owned by the platform team. This notebook promotes
# MAGIC a subset of Gold into it, using only the tags already on the Gold tables:
# MAGIC
# MAGIC - **Table gate:** a Gold table is promoted only if its `table_type` tag is `platinum`.
# MAGIC - **Column rule:** each column's `sensitivity` tag decides what reaches Platinum
# MAGIC   (`public`/`confidential` copied, `internal` left out, `restricted` last 4 characters only;
# MAGIC   no tag or `pii = true` left out). See `common/platinum.py`.
# MAGIC
# MAGIC **Runs after Gold.** It only reads Gold; it never changes it.
# MAGIC
# MAGIC **Plan first, then publish:** every run prints a plan (promoted, skipped, and why). With
# MAGIC `dry_run = true` it stops after the plan and writes nothing.
# MAGIC
# MAGIC **Target is a setting.** The catalog and schema it writes to are widgets, set from
# MAGIC `databricks.yml`; moving to the dedicated Platinum catalog is a `--var` change, not a code change.
# MAGIC
# MAGIC Each table is rebuilt in full (`CREATE OR REPLACE TABLE ... AS SELECT`) and its Gold tags are
# MAGIC copied across, so tag-based masks and policies match Platinum too. A table that fails is
# MAGIC reported and skipped; the run carries on and fails at the end if any table failed.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import get_schemas, get_use_external_tables, get_s3_bucket
from common import audit_log, job_context, platinum

# COMMAND ----------

dbutils.widgets.text("source_catalog", "rls_testing", label="Gold source catalog")
dbutils.widgets.text("source_schema", "ad_published", label="Gold source schema")
dbutils.widgets.text("target_catalog", "rls_testing", label="Platinum catalog")
dbutils.widgets.text("target_schema", "ad_platinum", label="Platinum schema")
dbutils.widgets.text("entity", "ALL", label="Table (or ALL)")
dbutils.widgets.text("dry_run", "false", label="Dry run (true = plan only)")
for _name in ("job_id", "job_run_id", "task_run_id", "task_key"):
    dbutils.widgets.text(_name, "")  # set by the job (base_parameters), for the audit log
job_context.set_from_widgets(dbutils)

SOURCE_CATALOG = dbutils.widgets.get("source_catalog").strip()
SOURCE_SCHEMA = dbutils.widgets.get("source_schema").strip()
TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
ENTITY = dbutils.widgets.get("entity").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

USE_EXTERNAL = get_use_external_tables()
AUDIT_BUCKET = get_s3_bucket("audit") if USE_EXTERNAL else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan

# COMMAND ----------

only = None if ENTITY.lower() == "all" else ENTITY
plans = platinum.plan_schema(spark, SOURCE_CATALOG, SOURCE_SCHEMA, only)
if not plans:
    raise ValueError(f"No Gold table found in {SOURCE_CATALOG}.{SOURCE_SCHEMA} for entity '{ENTITY}'.")

report = []
for p in plans:
    if p.promoted:
        for c in p.columns:
            report.append((p.table, "promoted" if c.treatment != platinum.OMIT else "promoted (column left out)",
                           c.name, c.sensitivity or "", c.treatment, c.reason))
    else:
        report.append((p.table, "skipped", "", "", "", p.reason))

report_df = spark.createDataFrame(
    report, "gold_table string, table_status string, column_name string, sensitivity string, treatment string, reason string"
)
display(report_df)

promoted = [p for p in plans if p.promoted]
print(f"Plan: {len(promoted)} of {len(plans)} Gold table(s) will be promoted to {TARGET_CATALOG}.{TARGET_SCHEMA}.")
for p in plans:
    left_out = [c.name for c in p.columns if c.treatment == platinum.OMIT]
    print(f"  {p.table}: {'PROMOTE' if p.promoted else 'skip'} - {p.reason}"
          + (f" | {len(p.kept)} column(s) kept, {len(left_out)} left out" if p.promoted else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish

# COMMAND ----------

failures = []
if DRY_RUN:
    print("dry_run = true: nothing written.")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
    audit_logger = audit_log.AuditLogger(
        spark, SOURCE_CATALOG, get_schemas()["audit"], USE_EXTERNAL, AUDIT_BUCKET
    )
    audit_logger.ensure_table()

    for p in promoted:
        source_fqn = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{p.table}"
        target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{p.table}"
        try:
            with audit_logger.audit(target_fqn, audit_log.Action.OVERWRITE) as log_ctx:
                n = platinum.publish_table(
                    spark, p, source_fqn, target_fqn,
                    platinum.read_columns(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
                    platinum.read_table_tags(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
                )
                log_ctx.set_count(n)
            print(f"  published {target_fqn}: {n} row(s), {len(p.kept)} column(s)")
        except Exception as e:  # the audit context has already logged the failure
            failures.append((p.table, str(e)[:300]))
            print(f"  FAILED {target_fqn}: {e}")

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} table(s) failed to publish: {failures}")
print("Done.")
