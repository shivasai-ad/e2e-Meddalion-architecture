"""Data-quality gate for every layer, built on DQX.

Rules are DQX checks (metadata format), kept in one file per promotion:
- config/dq_rules/bronze_to_silver.yml  (every Silver table)
- config/dq_rules/silver_to_gold.yml    (every Gold table)

Every check is pass/fail. A row that fails ANY check is written to the layer's
quarantine table (formatted_exception for Silver, published_exception for Gold);
a row that passes all checks continues. The run itself does not stop.
Which checks run is decided only by the YAML.
"""
import copy
import os
from typing import Dict, List, Optional

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .job_context import job_value

_ENGINE = None


def get_engine():
    """Create the DQEngine once, on first use (keeps DQX import lazy)."""
    global _ENGINE
    if _ENGINE is None:
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
        _ENGINE = DQEngine(WorkspaceClient())
    return _ENGINE


def load_table_rules(rules_file: str, entity: str, key: str = "checks") -> Optional[List[Dict]]:
    """Return the DQX checks for one table from a layer-level rules file.

    Every check is pass/fail, so each one runs with criticality "error" and a
    failing row is quarantined. A check that sets any other criticality is
    rejected, so a stray value cannot let a failing row continue.
    Returns None if the table has no entry, which skips the gate for that table.
    """
    with open(rules_file) as f:
        raw = yaml.safe_load(f) or {}
    table = (raw.get("tables") or {}).get(entity)
    if table is None:
        return None
    checks = table.get(key) or []
    if not checks:
        if key != "checks":
            return []  # optional rule list (for example Platinum's output_checks)
        raise ValueError(f"DQ rules for '{entity}' in '{rules_file}' have no checks.")
    for check in checks:
        level = check.get("criticality", "error")
        if level != "error":
            raise ValueError(
                f"Check '{check.get('name')}' for '{entity}' in '{rules_file}' sets "
                f"criticality '{level}'. Remove it: every failing row is quarantined."
            )
        check["criticality"] = "error"
    return checks


def scope_checks(checks: List[Dict], scope_filter: str) -> List[Dict]:
    """Limit every check to rows matching scope_filter (others pass)."""
    scoped = copy.deepcopy(checks)
    for check in scoped:
        existing = check.get("filter")
        check["filter"] = f"({scope_filter}) AND ({existing})" if existing else scope_filter
    return scoped


def quality_gate(
    spark,
    entity: str,
    df: DataFrame,
    checks: List[Dict],
    quarantine_fqn: str,
    label: str,
    gate_log=None,
    notify: Optional[Dict] = None,
) -> DataFrame:
    """Run checks on df. Returns the rows that passed every check.

    Rows that fail any check are appended to quarantine_fqn and are not returned.
    The run does not stop: the returned rows continue to the target.
    If gate_log (a dq_gate_log.DqGateLogger) is given, one row with the counts and the
    failed rules is written for this run, including a run where every row passed.
    """
    print(f"\n{'-'*70}")
    print(f"DQ GATE [{label}] — {entity}: {len(checks)} check(s)")
    print(f"{'-'*70}")

    engine = get_engine()
    # DQX split: good_df = rows with no failed check (result columns dropped);
    #            bad_df  = rows with at least one failed check (result columns kept)
    good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, checks)

    n_bad = bad_df.count()
    n_good = good_df.count()

    print(f"   Rows checked:                {n_good + n_bad}")
    print(f"   Passed (continue):           {n_good}")
    print(f"   Failed (quarantined):        {n_bad}")

    if n_bad == 0:
        print(f"   ✓ DQ passed for [{label}]\n")
        if gate_log:
            gate_log.log(entity, label, n_good, n_good, 0, {})
        _notify(spark, entity, label, n_good, n_bad, {}, bad_df, quarantine_fqn, _with_log(notify, gate_log))
        return good_df

    # Rows failed per rule: DQX keeps the failed checks of each bad row in `_errors`
    failed_rules = {
        r["rule"]: r["n"]
        for r in bad_df.select(F.explode("_errors").alias("e"))
        .groupBy(F.col("e.name").alias("rule"))
        .agg(F.count("*").alias("n"))
        .collect()
    }

    (
        bad_df.withColumn("_dq_label", F.lit(label))
        .withColumn("_dq_run_ts", F.current_timestamp())
        .withColumn("_dq_job_id", F.lit(job_value(spark, "spark.databricks.job.id", "job_id")).cast("string"))
        .withColumn("_dq_job_run_id", F.lit(job_value(spark, "spark.databricks.job.runId", "job_run_id")).cast("string"))
        .write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_fqn)
    )
    print(f"   ✗ {n_bad} row(s) quarantined to {quarantine_fqn}")
    print(f"   → {n_good} row(s) continue to the target\n")
    if gate_log:
        gate_log.log(entity, label, n_good + n_bad, n_good, n_bad, failed_rules, quarantine_fqn)
    _notify(spark, entity, label, n_good, n_bad, failed_rules, bad_df, quarantine_fqn, _with_log(notify, gate_log))
    return good_df


def _with_log(notify, gate_log):
    """Add the gate log's table name to the notify settings, for the email's summary link."""
    if not notify:
        return notify
    return {**notify, "gate_log_fqn": getattr(gate_log, "fqn", None)}


def _notify(spark, entity, label, n_good, n_bad, failed_rules, bad_df, quarantine_fqn, notify):
    """Send the run email when the caller asked for one. Never stops the data run."""
    if not notify:
        return
    from . import dq_email
    try:
        dq_email.notify_gate(
            spark, notify["dbutils"], layer=notify["layer"], entity=entity, label=label,
            gate_log_fqn=notify.get("gate_log_fqn"),
            job_id=job_value(spark, "spark.databricks.job.id", "job_id"),
            run_id=job_value(spark, "spark.databricks.job.runId", "job_run_id"),
            n_good=n_good, n_bad=n_bad, failed_rules=failed_rules,
            bad_df=bad_df, quarantine_fqn=quarantine_fqn,
        )
    except Exception as e:
        print(f"DQ email skipped ({type(e).__name__}): {str(e)[:200]}")
