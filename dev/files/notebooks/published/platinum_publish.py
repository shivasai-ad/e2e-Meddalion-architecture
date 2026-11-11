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
# MAGIC **DQ gate:** after the column handling, the rules in `config/dq_rules/gold_to_platinum.yml` run on the
# MAGIC rows that will be published. A row that fails any check goes to `<table>_quarantine` in the Platinum
# MAGIC exception schema; passing rows are published. A rule may only use a column Platinum keeps. A promoted table with no rules is not published.
# MAGIC The audit log, the DQ gate log and the quarantine tables sit in the Platinum catalog
# MAGIC (`<target_schema>_audit`, `<target_schema>_exception`), owned by the platform team.
# MAGIC
# MAGIC **Publish as a table or a view** (`publish_as`): a table is a rebuilt copy; a view reads Gold live, its
# MAGIC `WHERE` clause is the DQ rules written as SQL, and `<table>_quarantine` becomes a view of the failing rows.
# MAGIC
# MAGIC Each table is rebuilt in full and its Gold tags are copied across, so tag-based masks and
# MAGIC policies match Platinum too. If no row passes, the existing Platinum table is left unchanged.
# MAGIC A table that fails is reported and skipped; the run carries on and fails at the end if any table failed.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common import audit_log, dq, dq_gate_log, job_context, platinum

# COMMAND ----------

dbutils.widgets.text("source_catalog", "rls_testing", label="Gold source catalog")
dbutils.widgets.text("source_schema", "ad_published", label="Gold source schema")
dbutils.widgets.text("target_catalog", "dev_platinum_catalog", label="Platinum catalog")
dbutils.widgets.text("target_schema", "ad_rls_testing_platinum", label="Platinum schema")
dbutils.widgets.text("entity", "ALL", label="Table (or ALL)")
dbutils.widgets.text("dry_run", "false", label="Dry run (true = plan only)")
dbutils.widgets.text("publish_as", "table", label="Publish as (table or view)")
for _name in ("job_id", "job_run_id", "task_run_id", "task_key"):
    dbutils.widgets.text(_name, "")  # set by the job (base_parameters), for the audit log
job_context.set_from_widgets(dbutils)

SOURCE_CATALOG = dbutils.widgets.get("source_catalog").strip()
SOURCE_SCHEMA = dbutils.widgets.get("source_schema").strip()
TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
ENTITY = dbutils.widgets.get("entity").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
PUBLISH_AS = dbutils.widgets.get("publish_as").strip().lower()
if PUBLISH_AS not in ("table", "view"):
    raise ValueError(f"publish_as must be 'table' or 'view', got '{PUBLISH_AS}'.")

# The audit log, the DQ gate log and the quarantine tables live in the Platinum catalog, next to the
# published schema, so the platform team owns them. The Datahub principal gets no access to them.
AUDIT_SCHEMA = f"{TARGET_SCHEMA}_audit"
EXCEPTION_SCHEMA = f"{TARGET_SCHEMA}_exception"
RULES_FILE = os.path.join(PROJECT_ROOT, "config/dq_rules/gold_to_platinum.yml")

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
print(f"Plan: {len(promoted)} of {len(plans)} Gold table(s) will be promoted to {TARGET_CATALOG}.{TARGET_SCHEMA} as a {PUBLISH_AS}.")
rules = {p.table: dq.load_table_rules(RULES_FILE, p.table) for p in promoted}
treats = {p.table: platinum.load_treat(RULES_FILE, p.table) for p in promoted}
for p in plans:
    left_out = [c.name for c in p.columns if c.treatment == platinum.OMIT]
    dq_note = ""
    if p.promoted:
        n_rules = len(rules[p.table] or [])
        dq_note = (f" | DQ: {n_rules} check(s), "
                   f"{len(treats[p.table])} column(s) forced to NULL" if n_rules
                   else " | DQ: NO RULES in gold_to_platinum.yml, will not be published")
    print(f"  {p.table}: {'PROMOTE' if p.promoted else 'skip'} - {p.reason}"
          + (f" | {len(p.kept)} column(s) kept, {len(left_out)} left out" if p.promoted else "") + dq_note)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish

# COMMAND ----------

failures = []
if DRY_RUN:
    print("dry_run = true: nothing written.")
else:
    for schema in (TARGET_SCHEMA, AUDIT_SCHEMA, EXCEPTION_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema}")
    # Platinum is created as managed tables, so the loggers never need an S3 location.
    audit_logger = audit_log.AuditLogger(spark, TARGET_CATALOG, AUDIT_SCHEMA, False, None)
    audit_logger.ensure_table()

    for p in promoted:
        source_fqn = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{p.table}"
        target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{p.table}"
        if not rules[p.table]:
            failures.append((p.table, "no DQ rules in gold_to_platinum.yml; not published"))
            print(f"  FAILED {target_fqn}: no DQ rules, not published")
            continue
        try:
            with audit_logger.audit(target_fqn, audit_log.Action.OVERWRITE) as log_ctx:
                publish = platinum.publish_view if PUBLISH_AS == "view" else platinum.publish_table
                n = publish(
                    spark, p, source_fqn, target_fqn,
                    platinum.read_columns(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
                    platinum.read_table_tags(spark, SOURCE_CATALOG, SOURCE_SCHEMA, p.table),
                    rules[p.table],
                    f"{TARGET_CATALOG}.{EXCEPTION_SCHEMA}.{p.table}_quarantine",
                    gate_log=dq_gate_log.DqGateLogger(
                        spark, TARGET_CATALOG, AUDIT_SCHEMA, "platinum", target_fqn, False, None
                    ),
                    treat=treats[p.table],
                    notify={"layer": "Platinum", "dbutils": dbutils},
                )
                log_ctx.set_count(n)
            print(f"  published {target_fqn} as a {PUBLISH_AS}: {n} row(s), {len(p.kept)} column(s)")
        except Exception as e:  # the audit context has already logged the failure
            failures.append((p.table, str(e)[:300]))
            print(f"  FAILED {target_fqn}: {e}")

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} table(s) failed to publish: {failures}")
print("Done.")
