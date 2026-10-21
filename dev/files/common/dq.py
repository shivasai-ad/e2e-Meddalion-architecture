"""Data-quality gate for every layer, built on DQX.

Rules are DQX checks (metadata format). Two sources:
- YAML files: config/dq_rules/<layer>/<entity>.yml (e.g. gold, silver)
- Generated: pk_not_null_checks() builds one not-null rule per primary-key
  column from silver_config.yml, so primary keys are never duplicated.

Behaviour (same in every layer):
- error-level failures: rows go to <target>_quarantine in the layer's schema,
  then the run stops before anything is written.
- warn-level failures: rows are kept and the warning count is printed.
- An entity with no rules file skips the YAML gate.
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


def load_dq_rules(layer: str, entity: str, rules_root: str) -> Optional[List[Dict]]:
    """Return the DQX checks in rules_root/<layer>/<entity>.yml, or None if absent."""
    path = os.path.join(rules_root, layer, f"{entity}.yml")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    checks = raw.get("checks") or []
    if not checks:
        raise ValueError(f"DQ rules file '{path}' has no 'checks' entries.")
    return checks


def pk_not_null_checks(primary_keys: List[str]) -> List[Dict]:
    """One error-level is_not_null check per primary-key column."""
    return [
        {
            "name": f"pk_{col}_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": col}},
        }
        for col in primary_keys
    ]


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
    """Run checks on df. Returns the rows that may be written.

    If any error-level row fails, the failing rows are appended to
    quarantine_fqn and a ValueError is raised, so the caller stops
    before writing anything.
    """
    print(f"\n{'-'*70}")
    print(f"DQ GATE [{label}] — {entity}: {len(checks)} rule(s)")
    print(f"{'-'*70}")

    engine = get_engine()
    # DQX (v0.16.0) split semantics:
    #   good_df = rows with no error-level failure (warn-only rows included),
    #             result columns already dropped
    #   bad_df  = rows with error OR warning failures, result columns kept
    # So error rows are the bad_df rows whose _errors is not null; warn-only
    # rows are already in good_df and must not stop the run.
    good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, checks)

    error_df = bad_df.where(F.col("_errors").isNotNull())
    n_err = error_df.count()
    n_warn = bad_df.where(F.col("_errors").isNull() & F.col("_warnings").isNotNull()).count()
    n_good = good_df.count()

    print(f"   Rows checked:                {n_good + n_err}")
    print(f"   Passed (loaded):             {n_good}")
    print(f"   Passed with warnings:        {n_warn}")
    print(f"   Failed (error, quarantined): {n_err}")

    if n_err == 0:
        print(f"   ✓ DQ passed for [{label}]\n")
        return good_df

    (
        error_df.withColumn("_dq_label", F.lit(label))
        .withColumn("_dq_run_ts", F.current_timestamp())
        .write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_fqn)
    )
    print(f"   ✗ {n_err} row(s) written to quarantine: {quarantine_fqn}")
    raise ValueError(
        f"DQ FAILED for '{entity}' [{label}]: {n_err} row(s) with error-level "
        f"failures. Quarantined to {quarantine_fqn}. Nothing was written."
    )
