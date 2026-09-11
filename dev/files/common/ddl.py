"""
Simplified DDL generation for Medallion layers.

Takes table configuration (columns, properties, etc.) and generates
CREATE TABLE statements for Bronze, Silver, and Gold layers.

No entity-specific hardcoding. All config comes from external sources
(config_loader, YAML files).
"""

from typing import Dict, List, Tuple
from common.config_loader import get_s3_location


def _format_tblproperties(props: Dict[str, str]) -> str:
    """Format table properties for DDL."""
    if not props:
        return ""
    body = ",\n    ".join(f"'{k}' = '{v}'" for k, v in sorted(props.items()))
    return f"TBLPROPERTIES (\n    {body}\n)"


def _format_location(use_external: bool, entity: str, layer: str) -> str:
    """Generate LOCATION clause for external table, or empty string for managed."""
    if not use_external:
        return ""
    location = get_s3_location(entity, layer)
    return f"LOCATION '{location}'"


def _format_columns(columns: List[Tuple[str, str]]) -> str:
    """Format column definitions for DDL."""
    return ",\n    ".join(f"{name} {col_type}" for name, col_type in columns)


def _format_clustering(clustering_keys: List[str]) -> str:
    """Format liquid clustering clause for DDL, or empty string if no keys."""
    if not clustering_keys:
        return ""
    cols = ", ".join(clustering_keys)
    return f"CLUSTER BY ({cols})"


def generate_bronze_ddl(
    fqn: str,
    entity: str,
    columns: List[Tuple[str, str]],
    audit_columns: List[Tuple[str, str]],
    table_properties: Dict[str, str],
    use_external: bool = True,
    clustering_keys: List[str] = None,
    primary_keys: List[str] = None,
) -> str:
    """
    Generate CREATE TABLE DDL for Bronze layer.

    Args:
        fqn: Fully-qualified table name (catalog.schema.table)
        entity: Entity name
        columns: List of (column_name, sql_type) tuples (business columns)
        audit_columns: List of (column_name, sql_type) tuples (SCD2 audit columns from config)
        table_properties: TBLPROPERTIES dict from config
        use_external: Whether to create external table (True) or managed (False)
        clustering_keys: List of column names for liquid clustering (optional)
        primary_keys: List of primary key column names (optional)

    Returns:
        CREATE TABLE SQL statement with PRIMARY KEY constraint and liquid clustering if specified
    """
    all_columns = columns + audit_columns
    columns_clause = _format_columns(all_columns)

    # Add PRIMARY KEY constraint if specified
    pk_constraint = ""
    if primary_keys:
        pk_cols = ", ".join(f"`{col}`" for col in primary_keys)
        pk_constraint = f",\n    CONSTRAINT pk_{entity} PRIMARY KEY ({pk_cols})"

    location_clause = _format_location(use_external, entity, "bronze")
    clustering_clause = _format_clustering(clustering_keys or [])
    tblprops = _format_tblproperties(table_properties)

    # Build DDL with correct clause ordering (matches payments implementation):
    # CREATE TABLE ... (columns [, constraints]) USING DELTA
    # CLUSTER BY (...)          [if clustering keys specified]
    # LOCATION '...'            [if external]
    # TBLPROPERTIES (...)
    parts = [
        f"CREATE TABLE IF NOT EXISTS {fqn} (",
        f"    {columns_clause}{pk_constraint}",
        ") USING DELTA",
    ]
    if clustering_clause:
        parts.append(clustering_clause)
    if location_clause:
        parts.append(location_clause)
    if tblprops:
        parts.append(tblprops)

    return "\n".join(parts)


def generate_silver_ddl(
    fqn: str,
    entity: str,
    columns: List[Tuple[str, str]],
    audit_columns: List[Tuple[str, str]],
    table_properties: Dict[str, str],
    use_external: bool = True,
    primary_keys: List[str] = None,
) -> str:
    """
    Generate CREATE TABLE DDL for Silver layer.

    Args:
        fqn: Fully-qualified table name (catalog.schema.table)
        entity: Entity name
        columns: List of (column_name, sql_type) tuples (business columns)
        audit_columns: List of (column_name, sql_type) tuples (audit columns from config)
        table_properties: TBLPROPERTIES dict from config
        use_external: Whether to create external table (True) or managed (False)
        primary_keys: List of primary key column names (optional)

    Returns:
        CREATE TABLE SQL statement with PRIMARY KEY constraint if specified
    """
    all_columns = columns + audit_columns
    columns_clause = _format_columns(all_columns)

    # Add PRIMARY KEY constraint if specified
    pk_constraint = ""
    if primary_keys:
        pk_cols = ", ".join(f"`{col}`" for col in primary_keys)
        pk_constraint = f",\n    CONSTRAINT pk_{entity} PRIMARY KEY ({pk_cols})"

    location_clause = _format_location(use_external, entity, "silver")
    tblprops = _format_tblproperties(table_properties)

    ddl = f"""CREATE TABLE IF NOT EXISTS {fqn} (
    {columns_clause}{pk_constraint}
) USING DELTA
{location_clause}
COMMENT 'Silver conformed layer for {entity}'
{tblprops}"""

    return ddl.strip()


def generate_gold_ddl(
    fqn: str,
    entity: str,
    columns: List[Tuple[str, str]],
    audit_columns: List[Tuple[str, str]],
    table_properties: Dict[str, str],
    use_external: bool = True,
) -> str:
    """
    Generate CREATE OR REPLACE TABLE DDL for Gold layer.

    Gold columns come from config (business columns + calculated columns).

    Args:
        fqn: Fully-qualified table name (catalog.schema.table)
        entity: Entity name
        columns: List of (column_name, sql_type) tuples (from config)
        audit_columns: List of (column_name, sql_type) tuples (audit columns from config)
        table_properties: TBLPROPERTIES dict from config
        use_external: Whether to create external table (True) or managed (False)

    Returns:
        CREATE OR REPLACE TABLE SQL statement
    """
    all_columns = columns + audit_columns
    columns_clause = _format_columns(all_columns)
    location_clause = _format_location(use_external, entity, "gold")
    tblprops = _format_tblproperties(table_properties)

    ddl = f"""CREATE OR REPLACE TABLE {fqn} (
    {columns_clause}
) USING DELTA
{location_clause}
COMMENT 'Gold published layer for {entity}'
{tblprops}"""

    return ddl.strip()


def generate_staging_ddl(
    fqn: str,
    entity: str,
    columns: List[Tuple[str, str]],
    audit_columns: List[Tuple[str, str]],
    table_properties: Dict[str, str],
    use_external: bool = True,
    clustering_keys: List[str] = None,
) -> str:
    """
    Generate CREATE TABLE DDL for Staging (synthetic CDC source).

    Adds _rescued_data column (standard for Lakeflow).

    Args:
        fqn: Fully-qualified table name (catalog.schema.table)
        entity: Entity name
        columns: List of (column_name, sql_type) tuples (business columns)
        audit_columns: List of (column_name, sql_type) tuples (not used for staging, but kept for consistency)
        table_properties: TBLPROPERTIES dict from config
        use_external: Whether to create external table (True) or managed (False)
        clustering_keys: List of column names for liquid clustering (optional)

    Returns:
        CREATE TABLE SQL statement with liquid clustering if specified
    """
    # Add _rescued_data (standard for Lakeflow-managed tables)
    extra_columns = [
        ("_rescued_data", "STRING"),
    ]
    all_columns = columns + extra_columns

    columns_clause = _format_columns(all_columns)
    location_clause = _format_location(use_external, entity, "stg")
    clustering_clause = _format_clustering(clustering_keys or [])
    tblprops = _format_tblproperties(table_properties)

    # Build DDL with correct clause ordering:
    # CREATE TABLE ... (columns) USING DELTA
    # CLUSTER BY (...)          [if clustering keys specified]
    # LOCATION '...'            [if external]
    # TBLPROPERTIES (...)
    parts = [
        f"CREATE TABLE IF NOT EXISTS {fqn} (",
        f"    {columns_clause}",
        ") USING DELTA",
    ]
    if clustering_clause:
        parts.append(clustering_clause)
    if location_clause:
        parts.append(location_clause)
    if tblprops:
        parts.append(tblprops)

    return "\n".join(parts)
