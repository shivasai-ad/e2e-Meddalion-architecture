"""Platinum publish -- Gold -> Platinum, driven by the tags already on the Gold tables.

Platinum is the customer-facing layer (ADP-1026): a platform-owned subset of Gold.
Two tag-based decisions, nothing else:

  1. Table gate:   a Gold table is promoted only if its `table_type` tag is `platinum`
                   (set from `table_type: platinum` in gold_compliance_metadata.yml).
  2. Column rule:  each column's `sensitivity` tag decides what reaches Platinum,
                   following the "client, own legal entity" column of the access matrix.

        public        -> copied as is
        confidential  -> copied as is
        internal      -> left out (a client never sees it)
        restricted    -> last 4 characters only ('****' + last 4), as a STRING
        no tag        -> left out (default-deny; the PR check should make this impossible)
        pii = 'true'  -> left out (held back until someone signs it off)

The module has no Spark import at the top, so `decide_column` and `build_plan` can be
tested without a cluster. Spark is used only by the functions that read tags or write.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

TABLE_GATE_TAG = "table_type"
TABLE_GATE_VALUE = "platinum"
SENSITIVITY_TAG = "sensitivity"
PII_TAG = "pii"

INCLUDE = "include"
OMIT = "omit"
LAST4 = "last4"
FORCE_NULL = "null"


def decide_column(sensitivity: Optional[str], pii: Optional[str] = None) -> tuple:
    """Return (treatment, reason) for one Gold column."""
    if not sensitivity:
        return OMIT, "no sensitivity tag"
    sensitivity = sensitivity.strip().lower()
    if str(pii or "").strip().lower() == "true":
        return OMIT, "pii column, not signed off"
    if sensitivity in ("public", "confidential"):
        return INCLUDE, sensitivity
    if sensitivity == "internal":
        return OMIT, "internal is never exposed to clients"
    if sensitivity == "restricted":
        return LAST4, "restricted: last 4 characters only"
    return OMIT, f"unknown sensitivity '{sensitivity}'"


@dataclass
class ColumnPlan:
    name: str
    data_type: str
    sensitivity: Optional[str]
    treatment: str
    reason: str


@dataclass
class TablePlan:
    table: str
    promoted: bool
    reason: str
    columns: List[ColumnPlan] = field(default_factory=list)

    @property
    def kept(self) -> List[ColumnPlan]:
        return [c for c in self.columns if c.treatment != OMIT]


def build_plan(table: str, table_tags: Dict[str, str], columns: List[dict],
               column_tags: Dict[str, Dict[str, str]]) -> TablePlan:
    """Decide one table. `columns` is [{name, data_type}] in table order;
    `column_tags` is {column: {tag: value}}."""
    if table_tags.get(TABLE_GATE_TAG, "").strip().lower() != TABLE_GATE_VALUE:
        return TablePlan(table, False, f"not tagged {TABLE_GATE_TAG}={TABLE_GATE_VALUE}")
    plan = TablePlan(table, True, "tagged for Platinum")
    for col in columns:
        tags = column_tags.get(col["name"], {})
        treatment, reason = decide_column(tags.get(SENSITIVITY_TAG), tags.get(PII_TAG))
        plan.columns.append(
            ColumnPlan(col["name"], col["data_type"], tags.get(SENSITIVITY_TAG), treatment, reason)
        )
    if not plan.kept:
        plan.promoted = False
        plan.reason = "tagged for Platinum but no column is allowed out"
    return plan


def load_treat(rules_file: str, table: str) -> Dict[str, str]:
    """The `treat` map of one table in the rules file: {column: 'null'}. Empty if none.

    `treat` can only make a kept column stricter (force it to NULL); it never lets a column out.
    """
    import yaml

    with open(rules_file) as f:
        entry = ((yaml.safe_load(f) or {}).get("tables") or {}).get(table) or {}
    treat = entry.get("treat") or {}
    bad = {c: v for c, v in treat.items() if v != FORCE_NULL}
    if bad:
        raise ValueError(f"'treat' for '{table}' in '{rules_file}' allows only '{FORCE_NULL}', got {bad}.")
    return treat


def select_expr(col: ColumnPlan, treat: Optional[Dict[str, str]] = None) -> str:
    """One SELECT-list item for a kept column. `treat` can force a column to NULL."""
    quoted = f"`{col.name}`"
    if (treat or {}).get(col.name) == FORCE_NULL:
        return f"CAST(NULL AS {col.data_type}) AS {quoted}"
    if col.treatment == LAST4:
        # A value of 4 characters or fewer would show in full, so it is masked completely.
        text = f"cast({quoted} as string)"
        return (
            f"CASE WHEN {quoted} IS NULL THEN NULL "
            f"WHEN length({text}) > 4 THEN concat('****', right({text}, 4)) "
            f"ELSE '****' END AS {quoted}"
        )
    return quoted


# ── Spark-side helpers ─────────────────────────────────────────────────────

def _esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def list_tables(spark, catalog: str, schema: str) -> List[str]:
    rows = spark.sql(
        f"SELECT table_name FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name NOT LIKE '\\_%' ORDER BY table_name"
    ).collect()
    return [r["table_name"] for r in rows]


def read_table_tags(spark, catalog: str, schema: str, table: str) -> Dict[str, str]:
    rows = spark.sql(
        f"SELECT tag_name, tag_value FROM {catalog}.information_schema.table_tags "
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    ).collect()
    return {r["tag_name"]: r["tag_value"] for r in rows}


def read_columns(spark, catalog: str, schema: str, table: str) -> List[dict]:
    rows = spark.sql(
        f"SELECT column_name, data_type, comment FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"
    ).collect()
    return [{"name": r["column_name"], "data_type": r["data_type"], "comment": r["comment"]} for r in rows]


def read_column_tags(spark, catalog: str, schema: str, table: str) -> Dict[str, Dict[str, str]]:
    rows = spark.sql(
        f"SELECT column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags "
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    ).collect()
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        out.setdefault(r["column_name"], {})[r["tag_name"]] = r["tag_value"]
    return out


def plan_schema(spark, catalog: str, schema: str, only: Optional[str] = None) -> List[TablePlan]:
    plans = []
    for table in list_tables(spark, catalog, schema):
        if only and table.lower() != only.lower():
            continue
        plans.append(build_plan(
            table,
            read_table_tags(spark, catalog, schema, table),
            read_columns(spark, catalog, schema, table),
            read_column_tags(spark, catalog, schema, table),
        ))
    return plans


def publish_table(spark, plan: TablePlan, source_fqn: str, target_fqn: str,
                  source_columns: List[dict], source_table_tags: Dict[str, str],
                  checks: List[dict], quarantine_fqn: str, gate_log=None,
                  treat: Optional[Dict[str, str]] = None,
                  output_checks: Optional[List[dict]] = None) -> int:
    """Rebuild one Platinum table from Gold and copy its tags. Returns the row count.

    Two gates, both pass/fail, both quarantining to `quarantine_fqn`:
      1. Input gate (`checks`) on the full Gold rows, so a rule can use a column Platinum drops,
         such as the row-filter key.
      2. Output gate (`output_checks`) on the rows after the column handling (allowed columns kept,
         masks applied, `treat` columns forced to NULL). It verifies the handling worked, for example
         that a column meant to be NULL is NULL; a row that fails is not published.
    If no row passes a gate, the existing Platinum table is left as it is.
    """
    from . import dq  # imported here so the module can be tested without Spark

    good = dq.quality_gate(
        spark, plan.table, spark.table(source_fqn), checks, quarantine_fqn,
        "gold_to_platinum", gate_log=gate_log,
    )
    if good.isEmpty():
        raise ValueError(f"No row of {source_fqn} passed the DQ gate; {target_fqn} was left unchanged.")
    treat = treat or {}
    unknown = sorted(set(treat) - {c.name for c in plan.kept})
    if unknown:
        raise ValueError(f"'treat' for '{plan.table}' names column(s) {unknown} that are not kept in Platinum.")
    out = good.selectExpr(*[select_expr(c, treat) for c in plan.kept])
    if output_checks:
        out = dq.quality_gate(
            spark, plan.table, out, output_checks, quarantine_fqn,
            "gold_to_platinum_output", gate_log=gate_log,
        )
        if out.isEmpty():
            raise ValueError(f"No row of {source_fqn} passed the output gate; {target_fqn} was left unchanged.")
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_fqn)

    # Table tags: carry the Gold tags over, but not the gate tag itself, and record provenance.
    tags = {k: v for k, v in source_table_tags.items() if k != TABLE_GATE_TAG}
    tags["promoted_from"] = source_fqn
    pairs = ", ".join(f"'{_esc(k)}' = '{_esc(str(v))}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {target_fqn} SET TAGS ({pairs})")

    comments = {c["name"]: c["comment"] for c in source_columns}
    for col in plan.kept:
        if comments.get(col.name):
            spark.sql(f"ALTER TABLE {target_fqn} ALTER COLUMN `{col.name}` COMMENT '{_esc(comments[col.name])}'")
        # Same sensitivity tag as Gold, so masks and policies written by tag match Platinum too.
        if col.sensitivity:
            spark.sql(
                f"ALTER TABLE {target_fqn} ALTER COLUMN `{col.name}` "
                f"SET TAGS ('{SENSITIVITY_TAG}' = '{_esc(col.sensitivity)}')"
            )
    return spark.table(target_fqn).count()
