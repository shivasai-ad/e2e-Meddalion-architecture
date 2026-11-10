# Databricks notebook source
# MAGIC %md
# MAGIC # Quarantine "latest run" views (ADP-1026)
# MAGIC
# MAGIC For every `<table>_quarantine` table that exists in the Silver (`formatted_exception`) and Gold
# MAGIC (`published_exception`) exception schemas, create or replace a view `<table>_quarantine_latest_run`
# MAGIC that shows only the rows of the most recent run (the largest `_dq_run_ts`, matched on
# MAGIC `_dq_job_run_id`). The DQ summary email links to these views, so a click shows that run's rows
# MAGIC and not every run's.
# MAGIC
# MAGIC "Latest" moves on with each run: a click long after the email shows a newer run.
# MAGIC
# MAGIC Silver and Gold are handled alike. A layer with no quarantine table yet (no row has failed) is
# MAGIC skipped, and its view is created on the first run after a row fails. A quarantine table without the
# MAGIC run-id column (written before the quarantine rows carried it) is skipped too.

# COMMAND ----------

import os
import sys

PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import get_catalog, get_schemas

CATALOG = get_catalog()
SCHEMAS = get_schemas()
LAYERS = {"Silver": SCHEMAS["formatted_exception"], "Gold": SCHEMAS["published_exception"]}

# COMMAND ----------

for layer, schema in LAYERS.items():
    tables = [
        r["table_name"]
        for r in spark.sql(
            f"SELECT table_name FROM {CATALOG}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type <> 'VIEW' AND table_name LIKE '%\\_quarantine' "
            f"ORDER BY table_name"
        ).collect()
    ]
    if not tables:
        print(f"{layer}: no quarantine table in {CATALOG}.{schema} yet, nothing to do.")
        continue
    for table in tables:
        cols = {
            r["column_name"]
            for r in spark.sql(
                f"SELECT column_name FROM {CATALOG}.information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
            ).collect()
        }
        if not {"_dq_run_ts", "_dq_job_run_id"} <= cols:
            print(f"{layer}: {table} has no _dq_job_run_id column, skipped.")
            continue
        src = f"{CATALOG}.{schema}.{table}"
        view = f"{src}_latest_run"
        spark.sql(
            f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {src} "
            f"WHERE _dq_job_run_id = (SELECT max_by(_dq_job_run_id, _dq_run_ts) FROM {src})"
        )
        n = spark.table(view).count()
        print(f"{layer}: {view} created, {n} row(s) from the latest run.")
