"""Data-quality gate for Gold promotion, built on DQX.

Rules live in config/dq_rules/<entity>.yml (DQX metadata format).
- error-level failures: rows go to <entity>_quarantine in the gold schema,
  then the run stops before anything reaches Gold.
- warn-level failures: rows are loaded, and the warning count is printed.
Entities with no rules file skip this gate.
"""
import copy
import os
from typing import Dict, List, Optional

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_ENGINE = None


def get_engine():
    """Create the DQEngine once, on first use (keeps DQX import lazy)."""
    global _ENGINE
    if _ENGINE is None:
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
        _ENGINE = DQEngine(WorkspaceClient())
    return _ENGINE


def load_dq_rules(entity: str, rules_dir: str) -> Optional[List[Dict]]:
    """Return the DQX checks for an entity, or None if it has no rules file."""
    path = os.path.join(rules_dir, f"{entity}.yml")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    checks = raw.get("checks") or []
    if not checks:
        raise ValueError(f"DQ rules file '{path}' has no 'checks' entries.")
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
) -> DataFrame:
    """Run the checks on df. Returns the rows that may be loaded.

    If any error-level row fails, the failing rows are appended to
    quarantine_fqn and a ValueError is raised, so the caller stops
    before writing anything to Gold.
    """
    print(f"\n{'-'*70}")
    print(f"DQ GATE [{label}] — {entity}: {len(checks)} rule(s)")
    print(f"{'-'*70}")

    engine = get_engine()
    good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, checks)

    n_bad = bad_df.count()
    n_warn = good_df.filter(F.size("_warnings") > 0).count()
    good_df = good_df.drop("_errors", "_warnings")
    n_good = good_df.count()

    print(f"   Rows checked:                {n_good + n_bad}")
    print(f"   Passed (loaded):             {n_good}")
    print(f"   Passed with warnings:        {n_warn}")
    print(f"   Failed (error, quarantined): {n_bad}")

    if n_bad == 0:
        print(f"   ✓ DQ passed for [{label}]\n")
        return good_df

    (
        bad_df.withColumn("_dq_label", F.lit(label))
        .withColumn("_dq_run_ts", F.current_timestamp())
        .write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_fqn)
    )
    print(f"   ✗ {n_bad} row(s) written to quarantine: {quarantine_fqn}")
    raise ValueError(
        f"DQ FAILED for '{entity}' [{label}]: {n_bad} row(s) with error-level "
        f"failures. Quarantined to {quarantine_fqn}. Nothing was loaded to Gold."
    )
