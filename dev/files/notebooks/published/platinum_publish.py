# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - PLATINUM: Gold -> Platinum views (ADP-1026)
# MAGIC
# MAGIC Platinum is the customer-facing layer, owned by the platform team. For each Gold table it creates a
# MAGIC view, using only the tags already on the Gold tables:
# MAGIC
# MAGIC - **Table gate:** a Gold table gets a view only if its `table_type` tag is `platinum`.
# MAGIC - **Column rule:** each column's `sensitivity` tag decides whether the column is in the view
# MAGIC   (`public` and `confidential` are; `internal`, `restricted`, untagged and `pii = true` are not).
# MAGIC   See `common/platinum.py`.
# MAGIC
# MAGIC There is no data filtering and no DQ in Platinum: the view shows every row of Gold, live.
# MAGIC
# MAGIC **First run:** the view is created. **Every later run:** the `metadata_hash` tag of the Gold table is
# MAGIC compared with the one on the Platinum view. If they differ, the view is rebuilt (so its columns follow the
# MAGIC current `sensitivity` tags) and the tags are copied again. If they match, nothing is changed.
# MAGIC
# MAGIC **Plan first:** every run prints what it will do for each table (create, update, unchanged, skipped).
# MAGIC With `dry_run = true` it stops after the plan and writes nothing.
# MAGIC
# MAGIC **Target is a setting.** The catalog and schema it writes to are widgets, set from `databricks.yml`.
# MAGIC The audit log is written next to the views, in `<target_schema>_audit`.
# MAGIC A table that fails is reported and the run carries on; it fails at the end if any table failed.

# COMMAND ----------

import json
import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common import audit_log, job_context, platinum

# COMMAND ----------

dbutils.widgets.text("source_catalog", "rls_testing", label="Gold source catalog")
dbutils.widgets.text("source_schema", "ad_published", label="Gold source schema")
dbutils.widgets.text("target_catalog", "dev_platinum_catalog", label="Platinum catalog")
dbutils.widgets.text("target_schema", "ad_rls_testing_platinum", label="Platinum schema")
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

AUDIT_SCHEMA = f"{TARGET_SCHEMA}_audit"

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
            report.append((p.table, "promoted", c.name, c.sensitivity or "", c.treatment, c.reason))
    else:
        report.append((p.table, "skipped", "", "", "", p.reason))
display(spark.createDataFrame(
    report, "gold_table string, table_status string, column_name string, sensitivity string, treatment string, reason string"
))

promoted = [p for p in plans if p.promoted]
actions = {}
print(f"Plan: {len(promoted)} of {len(plans)} Gold table(s) get a view in {TARGET_CATALOG}.{TARGET_SCHEMA}.")
for p in plans:
    if not p.promoted:
        print(f"  {p.table}: skip - {p.reason}")
        continue
    target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{p.table}"
    gold_tags = platinum.read_table_tags(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table)
    actions[p.table] = platinum.view_action(spark, target_fqn, gold_tags)
    left_out = [c.name for c in p.columns if c.treatment == platinum.OMIT]
    print(f"  {p.table}: {actions[p.table].upper()} | {len(p.kept)} column(s) in the view, {len(left_out)} left out"
          f" | gold {platinum.HASH_TAG}: {(gold_tags.get(platinum.HASH_TAG) or 'none')[:12]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish

# COMMAND ----------

results = {p.table: actions[p.table] for p in promoted}  # what each table ended as
failures = []
if DRY_RUN:
    print("dry_run = true: nothing written.")
else:
    for schema in (TARGET_SCHEMA, AUDIT_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema}")
    audit_logger = audit_log.AuditLogger(spark, TARGET_CATALOG, AUDIT_SCHEMA, False, None)
    audit_logger.ensure_table()

    for p in promoted:
        source_fqn = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{p.table}"
        target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{p.table}"
        try:
            status = platinum.apply_view(
                spark, p, source_fqn, target_fqn,
                platinum.read_columns(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
                platinum.read_table_tags(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
            )
            results[p.table] = status
            if status != platinum.UNCHANGED:  # an unchanged view writes nothing, so nothing is logged
                audit_logger.log(target_fqn, audit_log.Action.OVERWRITE, row_count=0, status="success")
            print(f"  {p.table}: {status}")
        except Exception as e:
            results[p.table] = "failed"
            failures.append((p.table, str(e)[:300]))
            audit_logger.log(target_fqn, audit_log.Action.OVERWRITE, status="failure",
                             error_type=type(e).__name__, error_message=str(e))
            print(f"  FAILED {target_fqn}: {e}")

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} view(s) failed: {failures}")
print("Done.")
dbutils.notebook.exit(json.dumps({"dry_run": DRY_RUN, "views": results}))
