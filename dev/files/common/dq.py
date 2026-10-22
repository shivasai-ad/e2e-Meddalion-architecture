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

_ENGINE = None


def get_engine():
    """Create the DQEngine once, on first use (keeps DQX import lazy)."""
    global _ENGINE
    if _ENGINE is None:
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
        _ENGINE = DQEngine(WorkspaceClient())
    return _ENGINE


def load_table_rules(rules_file: str, entity: str) -> Optional[List[Dict]]:
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
    checks = table.get("checks") or []
    if not checks:
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
) -> DataFrame:
    """Run checks on df. Returns the rows that passed every check.

    Rows that fail any check are appended to quarantine_fqn and are not returned.
    The run does not stop: the returned rows continue to the target.
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
        return good_df

    (
        bad_df.withColumn("_dq_label", F.lit(label))
        .withColumn("_dq_run_ts", F.current_timestamp())
        .write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_fqn)
    )
    print(f"   ✗ {n_bad} row(s) quarantined to {quarantine_fqn}")
    print(f"   → {n_good} row(s) continue to the target\n")
    return good_df
