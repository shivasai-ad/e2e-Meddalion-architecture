"""Platinum publish -- a view over each tagged Gold table, driven by the tags on the Gold tables.

Platinum is the customer-facing layer (ADP-1026): a platform-owned subset of Gold. There is no data
filtering and no DQ here. Two tag-based decisions, nothing else:

  1. Table gate:   a Gold table is promoted only if its `table_type` tag is `platinum`
                   (set from `table_type: platinum` in gold_compliance_metadata.yml).
  2. Column rule:  each column's `sensitivity` tag decides whether the column is in the view.

        public        -> in the view
        confidential  -> in the view
        internal      -> left out (a client never sees it)
        restricted    -> left out (not exposed; no masking rule is agreed yet)
        no tag        -> left out (default-deny)
        pii = 'true'  -> left out (held back until someone signs it off)

The view is `SELECT <kept columns> FROM <gold table>`: every row of Gold, live. It is created the first
time. On every later run the job compares the `metadata_hash` tag of the Gold table with the one on the
Platinum view; when they differ it rebuilds the view and copies the tags again, and when they match it
does nothing.

The module has no Spark import at the top, so `decide_column` and `build_plan` can be tested without a
cluster. Spark is used only by the functions that read tags or write.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

TABLE_GATE_TAG = "table_type"
TABLE_GATE_VALUE = "platinum"
SENSITIVITY_TAG = "sensitivity"
PII_TAG = "pii"
HASH_TAG = "metadata_hash"

# Gold tags that are not copied to Platinum: the gate tag itself, and the hash written by the older
# gold-metadata-Standards-testing pipeline, which Platinum does not use.
NOT_COPIED_TAGS = {TABLE_GATE_TAG, "_compliance_config_hash"}

INCLUDE = "include"
OMIT = "omit"

CREATE = "create"
UPDATE = "update"
UNCHANGED = "unchanged"


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
        return OMIT, "restricted is not exposed"
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


def view_sql(plan: TablePlan, source_fqn: str, target_fqn: str) -> str:
    """The statement that creates or replaces the view: the kept columns, every row of Gold."""
    select_list = ", ".join(f"`{c.name}`" for c in plan.kept)
    return f"CREATE OR REPLACE VIEW {target_fqn} AS SELECT {select_list} FROM {source_fqn}"


def decide_action(view_exists: bool, gold_hash: Optional[str], view_hash: Optional[str]) -> str:
    """What to do with one Platinum view: create it, rebuild it, or leave it.

    No Gold hash means a change cannot be detected, so the view is rebuilt on every run.
    """
    if not view_exists:
        return CREATE
    if gold_hash and gold_hash == view_hash:
        return UNCHANGED
    return UPDATE


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


def view_exists(spark, target_fqn: str) -> bool:
    return spark.catalog.tableExists(target_fqn)


def view_action(spark, target_fqn: str, gold_tags: Dict[str, str]) -> str:
    """Compare the Gold table's metadata hash with the Platinum view's, without writing anything."""
    exists = view_exists(spark, target_fqn)
    view_hash = None
    if exists:
        catalog, schema, table = target_fqn.split(".")
        view_hash = read_table_tags(spark, catalog, schema, table).get(HASH_TAG)
    return decide_action(exists, gold_tags.get(HASH_TAG), view_hash)


def apply_view(spark, plan: TablePlan, source_fqn: str, target_fqn: str,
               source_columns: List[dict], source_table_tags: Dict[str, str]) -> str:
    """Create the Platinum view, rebuild it, or leave it, depending on the metadata hash.

    Returns `create`, `update` or `unchanged`. On create and update the view is replaced and the tags and
    column comments are copied from Gold again; tags that are no longer on the Gold table are removed.
    """
    action = view_action(spark, target_fqn, source_table_tags)
    if action == UNCHANGED:
        return action

    spark.sql(view_sql(plan, source_fqn, target_fqn))

    # Table tags: Gold's tags, except the ones not copied, plus where the view came from.
    tags = {k: v for k, v in source_table_tags.items() if k not in NOT_COPIED_TAGS}
    tags["promoted_from"] = source_fqn
    catalog, schema, table = target_fqn.split(".")
    stale = sorted(set(read_table_tags(spark, catalog, schema, table)) - set(tags))
    if stale:
        spark.sql(f"ALTER VIEW {target_fqn} UNSET TAGS ({', '.join(repr(k) for k in stale)})")
    pairs = ", ".join(f"'{_esc(k)}' = '{_esc(str(v))}'" for k, v in tags.items())
    spark.sql(f"ALTER VIEW {target_fqn} SET TAGS ({pairs})")

    # Column comments and the sensitivity tag (a view does not inherit them from Gold).
    comments = {c["name"]: c["comment"] for c in source_columns}
    for col in plan.kept:
        if comments.get(col.name):
            spark.sql(f"COMMENT ON COLUMN {target_fqn}.`{col.name}` IS '{_esc(comments[col.name])}'")
        if col.sensitivity:
            spark.sql(
                f"ALTER TABLE {target_fqn} ALTER COLUMN `{col.name}` "
                f"SET TAGS ('{SENSITIVITY_TAG}' = '{_esc(col.sensitivity)}')"
            )
    return action
