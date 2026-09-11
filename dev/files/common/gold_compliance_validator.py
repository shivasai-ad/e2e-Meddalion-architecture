"""Gold Compliance Validator

Provides functions to:
1. Load the YAML compliance config
2. Validate that a table's config entry has all required fields (pre-creation check)
3. Apply metadata from config to Unity Catalog (post-creation)
"""

import yaml
import os
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger("gold_compliance_validator")

# Required table-level tags — every compliant table MUST have these
REQUIRED_TABLE_TAGS = {"source_system", "source_squad", "dataset", "data_owner", "refresh_cadence", "client_id"}

# Required column-level tags — every column MUST have ALL of these
# NOTE: entity_id and client_id column tags are maintained by the governed tag custom_claim process
REQUIRED_COL_TAGS = {"sensitivity"}

# Allowed sensitivity values — exactly one from this set per column
ALLOWED_SENSITIVITY_VALUES = {"public", "internal", "confidential", "restricted"}

# Required top-level fields for a compliant table entry
REQUIRED_TABLE_FIELDS = {"description", "owner", "tags", "columns", "constraints"}


def load_compliance_config(config_path: str) -> dict:
    """
    Load the gold compliance YAML config file.

    Args:
        config_path: Absolute path to gold_compliance_metadata.yml on the workspace filesystem.

    Returns:
        dict of table entries (keyed by table name).

    Raises:
        FileNotFoundError: if config file does not exist.
        ValueError: if YAML is malformed or has no 'tables' key.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Compliance config not found at '{config_path}'. "
            f"Ensure gold_compliance_metadata.yml is deployed with the bundle."
        )

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not raw or "tables" not in raw:
        raise ValueError(f"Compliance config at '{config_path}' is empty or missing 'tables' key.")

    return raw["tables"]


def normalize_tag_value(value):
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)


def compute_config_hash(config_entry: dict) -> str:
    """
    Computes a stable hash fingerprint of a table's compliance config entry.
    Used to detect whether the YAML config for a table has changed since
    metadata was last applied to Unity Catalog.

    Args:
        config_entry: The config entry dict for a single table.

    Returns:
        A hex digest string uniquely identifying this config's content.
    """
    import hashlib
    import json

    config_str = json.dumps(config_entry, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()


def get_table_tag_value(catalog: str, schema: str, table_name: str, tag_name: str, spark=None) -> Optional[str]:
    """
    Reads the current value of a single table-level tag from Unity Catalog.
    Returns None if the table or tag doesn't exist.

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        tag_name: The tag key to look up.
        spark: SparkSession instance. If None, gets active session.

    Returns:
        The tag value as a string, or None if not found.
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    try:
        rows = spark.sql(f"""
            SELECT tag_value
            FROM system.information_schema.table_tags
            WHERE catalog_name = '{catalog}'
              AND schema_name = '{schema}'
              AND table_name = '{table_name}'
              AND tag_name = '{tag_name}'
        """).collect()
        return rows[0].tag_value if rows else None
    except Exception:
        return None


def set_table_tag(catalog: str, schema: str, table_name: str, tag_name: str, tag_value: str, spark=None) -> None:
    """
    Sets a single table-level tag on Unity Catalog (e.g. the compliance config hash).

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        tag_name: The tag key to set.
        tag_value: The tag value to set.
        spark: SparkSession instance. If None, gets active session.
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    full_name = f"{catalog}.{schema}.{table_name}"
    spark.sql(f"ALTER TABLE {full_name} SET TAGS ('{tag_name}' = '{tag_value}')")


def get_column_tag_value(catalog: str, schema: str, table_name: str, column_name: str, tag_name: str, spark=None) -> Optional[str]:
    """
    Reads the current value of a single column-level tag from Unity Catalog.
    Returns None if the column, tag, or table doesn't exist.

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        column_name: The column name.
        tag_name: The tag key to look up.
        spark: SparkSession instance. If None, gets active session.

    Returns:
        The tag value as a string, or None if not found.
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    try:
        rows = spark.sql(f"""
            SELECT tag_value
            FROM system.information_schema.column_tags
            WHERE catalog_name = '{catalog}'
              AND schema_name = '{schema}'
              AND table_name = '{table_name}'
              AND column_name = '{column_name}'
              AND tag_name = '{tag_name}'
        """).collect()
        return rows[0].tag_value if rows else None
    except Exception:
        return None


def get_constraint_info(catalog: str, schema: str, table_name: str, constraint_name: str, spark=None) -> Optional[dict]:
    """
    Reads the current definition of a constraint from Unity Catalog.
    Used to check if a constraint exists and what its definition is.

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        constraint_name: The constraint name to look up.
        spark: SparkSession instance. If None, gets active session.

    Returns:
        None if no constraint with this name exists. Otherwise a dict:
          - type: constraint type (PRIMARY_KEY or CHECK)
          - columns: list of key columns (for PRIMARY_KEY)
          - check_clause: SQL expression (for CHECK)
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    try:
        # PRIMARY_KEY / FOREIGN_KEY constraints live in Unity Catalog's own
        # constraint catalog, NOT in delta.constraints.* table properties.
        rows = spark.sql(f"""
            SELECT constraint_type
            FROM system.information_schema.table_constraints
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table_name}'
              AND constraint_name = '{constraint_name}'
        """).collect()

        if rows:
            constraint_type = rows[0].constraint_type  # e.g. 'PRIMARY KEY'

            if constraint_type == "PRIMARY KEY":
                col_rows = spark.sql(f"""
                    SELECT column_name
                    FROM system.information_schema.key_column_usage
                    WHERE table_catalog = '{catalog}'
                      AND table_schema = '{schema}'
                      AND table_name = '{table_name}'
                      AND constraint_name = '{constraint_name}'
                    ORDER BY ordinal_position
                """).collect()
                return {"type": "PRIMARY_KEY", "columns": [r.column_name for r in col_rows]}

            elif constraint_type == "CHECK":
                check_rows = spark.sql(f"""
                    SELECT check_clause
                    FROM system.information_schema.check_constraints
                    WHERE constraint_catalog = '{catalog}'
                      AND constraint_schema = '{schema}'
                      AND constraint_name = '{constraint_name}'
                """).collect()
                if check_rows:
                    return {"type": "CHECK", "check_clause": check_rows[0].check_clause}

        # CHECK constraints on Delta tables also appear as raw SQL text (not JSON)
        # under delta.constraints.<name> — fall back to that if info schema missed it.
        prop_rows = spark.sql(f"SHOW TBLPROPERTIES {catalog}.{schema}.{table_name}").collect()
        for row in prop_rows:
            if row[0] == f"delta.constraints.{constraint_name}":
                return {"type": "CHECK", "check_clause": row[1]}

        # Constraint not found
        return None
    except Exception as e:
        logger.warning(f"[{table_name}] get_constraint_info('{constraint_name}') raised {type(e).__name__}: {e}")
        return None



def validate_compliance_config(table_name: str, config: dict) -> dict:
    """
    Validates that the YAML config entry for a table has all required fields.
    Called BEFORE gold table creation to fail fast if metadata is incomplete.

    This function does NOT touch Unity Catalog — it only checks the config dict.
    It never raises — the caller decides what to do with the result.

    Failure messages follow the format:
        ERROR: <what is wrong>
        FIX:   <how to fix it in the YAML file>

    Args:
        table_name: Name of the table to validate.
        config: The loaded compliance config dict (output of load_compliance_config).

    Returns:
        dict with keys:
          - passed: bool
          - failures: list[str] (empty if passed)
    """

    # ── Table not in config ───────────────────────────────────────────────
    entry = config.get(table_name)
    if entry is None:
        not_found_msg = (
            f"ERROR: Table '{table_name}' has no entry in gold_compliance_metadata.yml.\n"
            f"  FIX: Add the following to notebooks/gold_metadata_standard/config/gold_compliance_metadata.yml\n"
            f"       (use the template at the bottom of the file):\n\n"
            f"         {table_name}:\n"
            f"           requires_compliance: true\n"
            f"           description: \"<table description>\"\n"
            f"           owner: \"<owner group or user>\"\n"
            f"           tags:\n"
            f"             source_system: \"sample_crm\"  # Single value\n"
            f"             # OR for multiple values:\n"
            f"             # source_system:\n"
            f"             #   - sample_crm\n"
            f"             #   - sample_analytics\n"
            f"             source_squad: \"<team name>\"\n"
            f"             dataset: \"<dataset name>\"\n"
            f"             data_owner: \"<business owner>\"\n"
            f"             refresh_cadence: \"daily|hourly|weekly|event-driven\"\n"
            f"             client_id: \"<client identifier>\"\n"
            f"           columns:\n"
            f"             <column_name>:\n"
            f"               description: \"<column description>\"\n"
            f"               tags:\n"
            f"                 sensitivity: \"public|internal|confidential|restricted\"\n"
            f"           constraints:\n"
            f"             - name: \"pk_<column>\"\n"
            f"               type: \"PRIMARY_KEY\"\n"
            f"               columns: [\"<primary_key_column>\"]\n"
            f"             - name: \"chk_<field>\"\n"
            f"               type: \"CHECK\"\n"
            f"               check: \"<SQL boolean expression>\"\n\n"
            f"       If this table is EXEMPT from compliance, add:\n\n"
            f"         {table_name}:\n"
            f"           requires_compliance: false\n"
            f"           reason: \"<why this table is exempt>\""
        )
        return {
            "passed": False,
            "failures": [not_found_msg],
        }

    # ── Validate requires_compliance field is present ────────────────────
    if "requires_compliance" not in entry:
        missing_field_msg = (
            f"ERROR: Required field 'requires_compliance' is missing for table '{table_name}'\n"
            f"  FIX: Add 'requires_compliance' field (true or false) to '{table_name}' in gold_compliance_metadata.yml:\n\n"
            f"       {table_name}:\n"
            f"         requires_compliance: true   # if table requires compliance checks\n"
            f"         # OR\n"
            f"         requires_compliance: false  # if table is exempt (provide reason)\n"
            f"         reason: \"<why this table is exempt>\""
        )
        return {
            "passed": False,
            "failures": [missing_field_msg],
        }

    # ── Table is exempt ───────────────────────────────────────────────────
    if not entry.get("requires_compliance", False):
        reason = entry.get("reason", "no reason provided")
        logger.info(f"[{table_name}] Compliance exempt: {reason}")
        return {
            "passed": True,
            "failures": [],
        }

    # ── Validate required fields ──────────────────────────────────────────
    failures = []

    # Get skip list (per-table override for which checks to skip)
    skip = set(entry.get("skip_validations", []))
    if skip:
        logger.info(f"[{table_name}] Skipping validations: {sorted(skip)}")

    # Check top-level required fields
    for field in REQUIRED_TABLE_FIELDS:
        if field in skip:
            continue
        if not entry.get(field):
            failures.append(
                f"ERROR: Missing required field: '{field}'\n"
                f"  FIX: Add '{field}' under the '{table_name}' entry in gold_compliance_metadata.yml\n"
                f"       Example:\n"
                f"         {table_name}:\n"
                f"           {field}: \"<value>\"\n"
                f"       Or add '{field}' to skip_validations if not applicable."
            )

    # Check required table tags
    tags = entry.get("tags", {})
    for tag in sorted(REQUIRED_TABLE_TAGS):
        if tag in skip:
            continue
        if tag not in tags or not tags[tag]:
            failures.append(
                f"ERROR: Missing required table tag: '{tag}'\n"
                f"  FIX: Add under '{table_name}' → 'tags' in gold_compliance_metadata.yml:\n"
                f"\n"
                f"       For single value:\n"
                f"         tags:\n"
                f"           {tag}: \"value\"\n"
                f"\n"
                f"       For multiple values:\n"
                f"         tags:\n"
                f"           {tag}:\n"
                f"             - value1\n"
                f"             - value2\n"
                f"\n"
                f"       Or add '{tag}' to skip_validations if not applicable."
            )

    # Check columns
    columns = entry.get("columns", {})
    if "columns" not in skip:
        if not columns:
            failures.append(
                f"ERROR: No columns defined in config\n"
                f"  FIX: Add at least one column under '{table_name}' → 'columns':\n"
                f"         columns:\n"
                f"           <column_name>:\n"
                f"             description: \"<description>\"\n"
                f"             tags:\n"
                f"               sensitivity: \"public|internal|confidential|restricted\""
            )
        else:
            for col_name, col_meta in columns.items():
                # Get column-level skip list (independent from table-level skip)
                col_skip = set(col_meta.get("skip_validations", []))

                # Column description
                if "column_description" not in skip and "description" not in col_skip:
                    if not col_meta.get("description"):
                        failures.append(
                            f"ERROR: Column '{col_name}' is missing 'description'\n"
                            f"  FIX: Add under '{table_name}' → 'columns' → '{col_name}':\n"
                            f"         {col_name}:\n"
                            f"           description: \"<what this column represents>\"\n"
                            f"       Or add 'description' to column-level skip_validations if not applicable."
                        )

                # Column tags
                col_tags = col_meta.get("tags", {})
                for tag in sorted(REQUIRED_COL_TAGS):
                    # Check both table-level and column-level skip lists
                    if tag in skip or tag in col_skip:
                        continue
                    if tag not in col_tags or not col_tags[tag]:
                        failures.append(
                            f"ERROR: Column '{col_name}' is missing required tag: '{tag}'\n"
                            f"  FIX: Add under '{table_name}' → 'columns' → '{col_name}' → 'tags':\n"
                            f"         tags:\n"
                            f"           {tag}: \"<value>\"\n"
                            f"       Or add '{tag}' to column-level skip_validations if not applicable."
                        )

                # Sensitivity value validation
                sensitivity = col_tags.get("sensitivity", "")
                if sensitivity and sensitivity not in ALLOWED_SENSITIVITY_VALUES:
                    failures.append(
                        f"ERROR: Column '{col_name}' has invalid sensitivity value: '{sensitivity}'\n"
                        f"  FIX: Change sensitivity to one of: {sorted(ALLOWED_SENSITIVITY_VALUES)}\n"
                        f"       In '{table_name}' → 'columns' → '{col_name}' → 'tags':\n"
                        f"         tags:\n"
                        f"           sensitivity: \"public|internal|confidential|restricted\""
                    )

    # Check constraints
    if "constraints" not in skip:
        constraints = entry.get("constraints", [])
        if not constraints:
            failures.append(
                f"ERROR: No constraints defined (data contract missing)\n"
                f"  FIX: Add at least one PRIMARY_KEY constraint under '{table_name}' → 'constraints':\n"
                f"         constraints:\n"
                f"           - name: \"pk_<column>\"\n"
                f"             type: \"PRIMARY_KEY\"\n"
                f"             columns: [\"<primary_key_column>\"]\n"
                f"           - name: \"chk_<field>\"\n"
                f"             type: \"CHECK\"\n"
                f"             check: \"<column> IS NOT NULL\"\n"
                f"       Or add 'constraints' to skip_validations if not applicable."
            )
        else:
            has_primary_key = False
            for i, constraint in enumerate(constraints):
                if not constraint.get("name"):
                    failures.append(
                        f"ERROR: Constraint #{i+1} is missing 'name'\n"
                        f"  FIX: Add 'name' to constraint #{i+1} in '{table_name}' → 'constraints':\n"
                        f"         - name: \"<constraint_name>\"\n"
                        f"           type: \"PRIMARY_KEY\" or \"CHECK\"\n"
                        f"           ..."
                    )
                if not constraint.get("type"):
                    failures.append(
                        f"ERROR: Constraint #{i+1} is missing 'type'\n"
                        f"  FIX: Add 'type' to constraint #{i+1} in '{table_name}' → 'constraints':\n"
                        f"         - name: \"{constraint.get('name', '<name>')}\"\n"
                        f"           type: \"PRIMARY_KEY\" or \"CHECK\""
                    )
                if constraint.get("type") == "PRIMARY_KEY":
                    has_primary_key = True
                    if not constraint.get("columns"):
                        failures.append(
                            f"ERROR: PRIMARY_KEY constraint '{constraint.get('name')}' is missing 'columns'\n"
                            f"  FIX: Add 'columns' list in '{table_name}' → 'constraints':\n"
                            f"         - name: \"{constraint.get('name')}\"\n"
                            f"           type: \"PRIMARY_KEY\"\n"
                            f"           columns: [\"<column_name>\"]\n"
                        )
                elif constraint.get("type") == "CHECK":
                    if not constraint.get("check"):
                        failures.append(
                            f"ERROR: CHECK constraint #{i+1} is missing 'check' expression\n"
                            f"  FIX: Add 'check' to constraint #{i+1} in '{table_name}' → 'constraints':\n"
                            f"         - name: \"{constraint.get('name', '<name>')}\"\n"
                            f"           type: \"CHECK\"\n"
                            f"           check: \"<SQL boolean expression>\""
                        )

            if not has_primary_key:
                failures.append(
                    f"ERROR: No PRIMARY_KEY constraint defined (every table must have at least one)\n"
                    f"  FIX: Add a PRIMARY_KEY constraint under '{table_name}' → 'constraints':\n"
                    f"         constraints:\n"
                    f"           - name: \"pk_<column>\"\n"
                    f"             type: \"PRIMARY_KEY\"\n"
                    f"             columns: [\"<primary_key_column>\"]\n"
                )

    # ── Return result ─────────────────────────────────────────────────────
    return {
        "passed": len(failures) == 0,
        "failures": failures,
    }


def apply_compliance_metadata(
    catalog: str,
    schema: str,
    table_name: str,
    config_entry: dict,
    spark=None,
) -> dict:
    """
    Applies compliance metadata from the YAML config entry to the gold table in Unity Catalog.
    Called AFTER the gold table is created/modified.

    Applies:
      - Table description (COMMENT ON TABLE)
      - Table owner (ALTER TABLE OWNER TO)
      - Table tags (ALTER TABLE SET TAGS)
      - Column descriptions (ALTER TABLE ALTER COLUMN COMMENT)
      - Column tags (ALTER TABLE ALTER COLUMN SET TAGS)
      - Constraints (ALTER TABLE ADD CONSTRAINT)

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        config_entry: The validated config entry dict (output of validate_compliance_config).
        spark: SparkSession instance. If None, gets active session.

    Returns:
        dict with summary of what was applied and any errors.

    Raises:
        ValueError: if any critical apply step fails.
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    full_name = f"{catalog}.{schema}.{table_name}"
    applied = []
    errors = []

    # ── Table description ────────────────────────────────────────────────
    description = config_entry.get("description", "")
    if description:
        try:
            safe_desc = description.replace("'", "\\'")
            spark.sql(f"COMMENT ON TABLE {full_name} IS '{safe_desc}'")
            applied.append("table_description")
        except Exception as e:
            errors.append(f"Failed to set table description: {e}")

    # ── Table owner ───────────────────────────────────────────────────────
    owner = config_entry.get("owner", "")
    if owner:
        try:
            spark.sql(f"ALTER TABLE {full_name} SET OWNER TO `{owner}`")
            applied.append("table_owner")
        except Exception as e:
            errors.append(f"Failed to set owner to '{owner}': {e}")

    # ── Table tags ────────────────────────────────────────────────────────
    tags = config_entry.get("tags", {})
    if tags:
        try:
            tag_pairs = ", ".join(f"'{k}' = '{normalize_tag_value(v)}'" for k, v in tags.items())
            spark.sql(f"ALTER TABLE {full_name} SET TAGS ({tag_pairs})")
            applied.append(f"table_tags ({len(tags)} tags)")
        except Exception as e:
            errors.append(f"Failed to set table tags: {e}")

    # ── Column descriptions + tags ────────────────────────────────────────
    columns = config_entry.get("columns", {})
    cols_applied = 0
    for col_name, col_meta in columns.items():
        # Column description
        col_desc = col_meta.get("description", "")
        if col_desc:
            try:
                safe_col_desc = col_desc.replace("'", "\\'")
                spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{col_name}` COMMENT '{safe_col_desc}'")
            except Exception as e:
                errors.append(f"Failed to set description on column '{col_name}': {e}")

        # Column tags
        col_tags = col_meta.get("tags", {})
        if col_tags:
            try:
                tag_pairs = ", ".join(f"'{k}' = '{normalize_tag_value(v)}'" for k, v in col_tags.items())
                spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{col_name}` SET TAGS ({tag_pairs})")
                cols_applied += 1
            except Exception as e:
                errors.append(f"Failed to set tags on column '{col_name}': {e}")

    if cols_applied:
        applied.append(f"column_metadata ({cols_applied} columns)")

    # ── Constraints ───────────────────────────────────────────────────────
    # Policy: if a constraint with this name already exists AND its definition
    # matches the YAML, skip it untouched. If it exists with a DIFFERENT
    # definition, drop it and recreate with the new definition. If it doesn't
    # exist, just create it.
    constraints = config_entry.get("constraints", [])
    constraints_applied = 0
    for constraint in constraints:
        name = constraint.get("name", "")
        constraint_type = constraint.get("type", "")

        if not name or not constraint_type:
            continue

        try:
            existing = get_constraint_info(catalog, schema, table_name, name, spark)
            logger.info(f"[{table_name}] get_constraint_info('{name}') -> {existing}")

            if constraint_type == "PRIMARY_KEY":
                columns = constraint.get("columns", [])
                if not columns:
                    continue

                desired_cols = [c.lower() for c in columns]

                if existing and existing.get("type") == "PRIMARY_KEY":
                    existing_cols = [c.lower() for c in existing.get("columns", [])]
                    if existing_cols == desired_cols:
                        logger.info(f"[{table_name}] PRIMARY_KEY '{name}' unchanged — skipped")
                        continue
                    logger.info(f"[{table_name}] PRIMARY_KEY '{name}' definition changed ({existing_cols} -> {desired_cols}) — dropping and recreating")
                    spark.sql(f"ALTER TABLE {full_name} DROP CONSTRAINT {name}")

                # Ensure PK columns are NOT NULL
                for col in columns:
                    try:
                        spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{col}` SET NOT NULL")
                    except Exception:
                        pass

                cols_str = ", ".join(f"`{col}`" for col in columns)
                spark.sql(f"ALTER TABLE {full_name} ADD CONSTRAINT {name} PRIMARY KEY ({cols_str})")
                constraints_applied += 1
                logger.info(f"[{table_name}] PRIMARY_KEY '{name}' applied")

            elif constraint_type == "CHECK":
                check = constraint.get("check", "")
                if not check:
                    continue

                if existing and existing.get("type") == "CHECK":
                    existing_check = (existing.get("check_clause") or "").strip().lower()
                    if existing_check == check.strip().lower():
                        logger.info(f"[{table_name}] CHECK '{name}' unchanged — skipped")
                        continue
                    logger.info(f"[{table_name}] CHECK '{name}' definition changed — dropping and recreating")
                    spark.sql(f"ALTER TABLE {full_name} DROP CONSTRAINT {name}")

                spark.sql(f"ALTER TABLE {full_name} ADD CONSTRAINT {name} CHECK ({check})")
                constraints_applied += 1
                logger.info(f"[{table_name}] CHECK '{name}' applied")

        except Exception as e:
            if "already exists" in str(e).lower():
                # Safety net: info-schema lookup missed it, but the DB knows it's there.
                logger.info(f"[{table_name}] Constraint '{name}' already exists — skipped")
            else:
                errors.append(f"Failed to process constraint '{name}': {e}")

    if constraints_applied:
        applied.append(f"constraints ({constraints_applied} added)")

    # ── Stamp config hash ─────────────────────────────────────────────────
    # Mark this table with a hash of its current config so we can skip
    # re-application on future runs if the config hasn't changed.
    try:
        config_hash = compute_config_hash(config_entry)
        set_table_tag(catalog, schema, table_name, "metadata_hash", config_hash, spark)
        applied.append(f"config_hash ({config_hash[:12]}...)")
    except Exception as e:
        errors.append(f"Failed to stamp config hash: {e}")

    # ── Summary ───────────────────────────────────────────────────────────
    if errors:
        error_msg = (
            f"apply_compliance_metadata for '{full_name}' completed with "
            f"{len(errors)} error(s):\n" +
            "\n".join(f"  - {e}" for e in errors)
        )
        logger.warning(error_msg)
        raise ValueError(f"[COMPLIANCE APPLY FAILED] {error_msg}")

    logger.info(f"[{table_name}] Metadata applied successfully: {applied}")
    return {
        "table": full_name,
        "applied": applied,
        "errors": errors,
    }



def validate_table_metadata(catalog: str, schema: str, table_name: str, spark=None) -> dict:
    """
    Validates all mandatory metadata items for an EXISTING table using information_schema.
    Checks the actual Unity Catalog state — tags, descriptions, constraints, ingestion column.

    Use this for existing gold tables that were created before the compliance flow existed.
    FIX messages point to the YAML config (source of truth).

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_name: The table name.
        spark: SparkSession instance. If None, gets active session.

    Returns:
        dict with keys:
          - table: fully qualified name
          - passed: bool
          - failures: list[str]
    """
    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()

    full_name = f"{catalog}.{schema}.{table_name}"
    failures = []

    # ── Table-level tags ──────────────────────────────────────────────────
    table_tags_df = spark.sql(f"""
        SELECT tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE catalog_name = '{catalog}'
          AND schema_name = '{schema}'
          AND table_name = '{table_name}'
    """)
    table_tags = {row.tag_name: row.tag_value for row in table_tags_df.collect()}

    for tag in REQUIRED_TABLE_TAGS:
        if tag not in table_tags:
            failures.append(
                f"ERROR: Missing table tag: '{tag}'\n"
                f"  FIX: Add '{tag}' under '{table_name}' → 'tags' in gold_compliance_metadata.yml:\n"
                f"         tags:\n"
                f"           {tag}: \"<value>\"\n"
                f"       Then run update_table_metadata() to apply."
            )

    # ── Technical owner + Table description ───────────────────────────────
    table_meta = spark.sql(f"""
        SELECT table_owner, comment
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
    """).collect()

    if table_meta:
        row = table_meta[0]
        if not row.table_owner:
            failures.append(
                f"ERROR: Missing technical owner (table_owner)\n"
                f"  FIX: Add 'owner' field under '{table_name}' in gold_compliance_metadata.yml:\n"
                f"         owner: \"<group_or_user>\"\n"
                f"       Then run update_table_metadata() to apply."
            )
        if not row.comment:
            failures.append(
                f"ERROR: Missing table description (comment)\n"
                f"  FIX: Add 'description' field under '{table_name}' in gold_compliance_metadata.yml:\n"
                f"         description: \"<what this table represents>\"\n"
                f"       Then run update_table_metadata() to apply."
            )
    else:
        failures.append(
            f"ERROR: Table '{full_name}' not found in information_schema.tables\n"
            f"  FIX: Ensure the table exists in catalog '{catalog}', schema '{schema}'."
        )
        return {"table": full_name, "passed": False, "failures": failures}

    # ── Get all columns ───────────────────────────────────────────────────
    all_columns = sorted(
        row.column_name for row in spark.sql(f"""
            SELECT column_name FROM system.information_schema.columns
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table_name}'
        """).collect()
    )

    # ── Column tags ───────────────────────────────────────────────────────
    col_tags_rows = spark.sql(f"""
        SELECT tag_name, column_name, tag_value
        FROM system.information_schema.column_tags
        WHERE catalog_name = '{catalog}'
          AND schema_name = '{schema}'
          AND table_name = '{table_name}'
    """).collect()

    col_tag_map = {}
    for row in col_tags_rows:
        col_tag_map.setdefault(row.column_name, {}).setdefault(row.tag_name, []).append(row.tag_value)

    cols_missing_sensitivity = []
    cols_multiple_sensitivity = []
    cols_invalid_sensitivity = []

    for col in all_columns:
        col_tags_dict = col_tag_map.get(col, {})

        sensitivity_values = col_tags_dict.get("sensitivity", [])
        if not sensitivity_values:
            cols_missing_sensitivity.append(col)
        elif len(sensitivity_values) > 1:
            cols_multiple_sensitivity.append((col, sensitivity_values))
        elif sensitivity_values[0] not in ALLOWED_SENSITIVITY_VALUES:
            cols_invalid_sensitivity.append((col, sensitivity_values[0]))

    # NOTE: entity_id and client_id column tags are maintained by the governed tag custom_claim process
    #       — they are NOT validated here.

    if cols_missing_sensitivity:
        failures.append(
            f"ERROR: 'sensitivity' tag missing on {len(cols_missing_sensitivity)}/{len(all_columns)} column(s): {cols_missing_sensitivity}\n"
            f"  FIX: Add 'sensitivity' tag for each column under '{table_name}' → 'columns' → '<col>' → 'tags' in gold_compliance_metadata.yml:\n"
            f"         tags:\n"
            f"           sensitivity: \"public|internal|confidential|restricted\"\n"
            f"       Then run update_table_metadata() to apply."
        )
    if cols_multiple_sensitivity:
        failures.append(
            f"ERROR: Multiple sensitivity values found (must be exactly one): {cols_multiple_sensitivity}\n"
            f"  FIX: Ensure only one sensitivity value per column in gold_compliance_metadata.yml.\n"
            f"       Then run update_table_metadata() to re-apply."
        )
    if cols_invalid_sensitivity:
        failures.append(
            f"ERROR: Invalid sensitivity values (allowed: {sorted(ALLOWED_SENSITIVITY_VALUES)}): {cols_invalid_sensitivity}\n"
            f"  FIX: Change sensitivity to one of: public, internal, confidential, restricted\n"
            f"       in gold_compliance_metadata.yml, then run update_table_metadata() to apply."
        )

    # ── Column descriptions ───────────────────────────────────────────────
    cols_without_desc = spark.sql(f"""
        SELECT column_name
        FROM system.information_schema.columns
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND (comment IS NULL OR comment = '')
    """).collect()

    if cols_without_desc:
        missing_desc_cols = [r.column_name for r in cols_without_desc]
        failures.append(
            f"ERROR: Column description missing on {len(missing_desc_cols)}/{len(all_columns)} column(s): {missing_desc_cols}\n"
            f"  FIX: Add 'description' for each column under '{table_name}' → 'columns' → '<col>' in gold_compliance_metadata.yml:\n"
            f"         <column_name>:\n"
            f"           description: \"<what this column represents>\"\n"
            f"       Then run update_table_metadata() to apply."
        )

    # ── Ingestion timestamp column ────────────────────────────────────────
    ingestion_patterns_sql = ["_gold_refreshed_at"]
    has_ingestion = any(
        any(pattern in col.lower() for pattern in ingestion_patterns_sql)
        for col in all_columns
    )

    if not has_ingestion:
        failures.append(
            f"ERROR: No _gold_refreshed_at column found\n"
            f"  FIX: The pipeline must add _gold_refreshed_at TIMESTAMP column before writing to gold.\n"
            f"       Example: silver_df = silver_df.withColumn('_gold_refreshed_at', F.current_timestamp())"
        )

    # ── Constraints ───────────────────────────────────────────────────────
    try:
        # Query Unity Catalog system views for constraint metadata (authoritative source)
        pk_result = spark.sql(f"""
            SELECT COUNT(*) as pk_count
            FROM system.information_schema.table_constraints
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table_name}'
              AND constraint_type = 'PRIMARY KEY'
        """).collect()

        # PRIMARY_KEY is REQUIRED
        pk_count = int(pk_result[0].pk_count) if pk_result else 0
        if pk_count == 0:
            failures.append(
                f"ERROR: No PRIMARY_KEY constraint defined (data contract missing)\n"
                f"  FIX: Add a PRIMARY_KEY constraint under '{table_name}' → 'constraints' in gold_compliance_metadata.yml:\n"
                f"         constraints:\n"
                f"           - name: \"pk_<column>\"\n"
                f"             type: \"PRIMARY_KEY\"\n"
                f"             columns: [\"<primary_key_column>\"]\n"
                f"       Then run update_table_metadata() to apply."
            )
    except Exception as e:
        failures.append(f"ERROR: Could not check constraints: {e}")

    # ── Return result ─────────────────────────────────────────────────────
    return {
        "table": full_name,
        "passed": len(failures) == 0,
        "failures": failures,
    }
