"""
Configuration loader for the Medallion Framework.

Reads YAML config files (bronze_config.yml, silver_config.yml) and converts
them to the format consumed by the medallion pipeline functions. Gold has no
config file of its own -- it reads DDL columns/types and compliance metadata
both from `config/gold_compliance_metadata.yml` (see
`common/gold_compliance_validator.py`); audit columns and table properties
are hardcoded constants in `notebooks/published/gold.py`.

Usage:
    from common.config_loader import load_bronze_config, load_silver_config
    from common.config_loader import get_catalog, get_schemas, get_s3_bucket, get_fqn

    # Load configs
    bronze_tables = load_bronze_config("config/bronze_config.yml")
    silver_tables = load_silver_config("config/silver_config.yml")

    # Get defaults
    catalog = get_catalog()
    schemas = get_schemas()
    bucket = get_s3_bucket("formatted")
    fqn = get_fqn("orders", "bronze")

    # Use with pipeline
    from common import pipeline
    pipeline.run_bronze(spark, entity="orders", bronze_config=bronze_tables)
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional


class ConfigError(Exception):
    """Raised when config validation fails."""
    pass


def load_yaml(file_path: str) -> Dict[str, Any]:
    """Load and parse YAML config file."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {file_path}")
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {file_path}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# LOAD ALL DEFAULTS FROM databricks.yml
# ═══════════════════════════════════════════════════════════════════════════

def _load_databricks_config() -> Dict[str, Any]:
    """
    Load all variables from databricks.yml.

    Returns a dict with all variable defaults.
    """
    db_config_path = Path(__file__).parent.parent / "databricks.yml"
    config = load_yaml(str(db_config_path))
    return config.get("variables", {})


# Load ALL variables from databricks.yml
_DB_VARIABLES = _load_databricks_config()

# Extract defaults from databricks.yml variables section
CATALOG_DEFAULT = _DB_VARIABLES.get("catalog", {}).get("default", "rls_testing")
USE_EXTERNAL_TABLES_DEFAULT = _DB_VARIABLES.get("use_external_tables", {}).get("default", "true")
FORMATTED_BUCKET_DEFAULT = _DB_VARIABLES.get("formatted_bucket", {}).get("default", "ad-dna-eu-central-1-dev-datalake-formatted-156041432080")
PUBLISHED_BUCKET_DEFAULT = _DB_VARIABLES.get("published_bucket", {}).get("default", "ad-dna-eu-central-1-dev-datalake-published-156041432080")

# ═══════════════════════════════════════════════════════════════════════════
# DEFAULTS FROM common/config.py
# ═══════════════════════════════════════════════════════════════════════════
SCHEMA_PREFIX = "e2e_medallion_metadata_test"
HARNESS_NAMESPACE = "e2e_medallion_metadata_test"

# Schema names for each layer
SCHEMAS = {
    "stg": f"{SCHEMA_PREFIX}_stg",
    "bronze": f"{SCHEMA_PREFIX}_bronze",
    "silver": f"{SCHEMA_PREFIX}_silver",
    "gold": f"{SCHEMA_PREFIX}_gold",
    "audit": f"{SCHEMA_PREFIX}_audit",
}

# S3 Layer directory names (from medallion-cdc-fed-pattern.md)
S3_LAYER_DIRS = {
    "stg": "staging",
    "bronze": "formatted_stg",
    "silver": "formatted",
    "gold": "published",
    "audit": "audit",
}

# S3 Buckets mapped by layer
S3_BUCKETS = {
    "stg": FORMATTED_BUCKET_DEFAULT,     # Staging uses formatted bucket (formatted_stg prefix)
    "bronze": FORMATTED_BUCKET_DEFAULT,  # Bronze uses formatted bucket (formatted_stg prefix)
    "silver": FORMATTED_BUCKET_DEFAULT,  # Silver uses formatted bucket (formatted prefix)
    "gold": PUBLISHED_BUCKET_DEFAULT,    # Gold uses published bucket (published prefix)
    "audit": FORMATTED_BUCKET_DEFAULT,   # Audit uses formatted bucket (audit prefix)
}


# ═══════════════════════════════════════════════════════════════════════════
# ACCESSOR FUNCTIONS - ALL DATABRICKS.YML VARIABLES
# ═══════════════════════════════════════════════════════════════════════════

def get_catalog() -> str:
    """Get default catalog name from databricks.yml."""
    return CATALOG_DEFAULT


def get_use_external_tables() -> bool:
    """Get whether to use external tables from databricks.yml."""
    value = USE_EXTERNAL_TABLES_DEFAULT
    if isinstance(value, str):
        return value.lower() in ["true", "1", "yes"]
    return bool(value)


def get_formatted_bucket() -> str:
    """Get formatted bucket name from databricks.yml."""
    return FORMATTED_BUCKET_DEFAULT


def get_published_bucket() -> str:
    """Get published bucket name from databricks.yml."""
    return PUBLISHED_BUCKET_DEFAULT


def get_databricks_variables() -> Dict[str, Any]:
    """
    Get all raw variables from databricks.yml.

    Available variables:
    - catalog: Catalog name (default: rls_testing)
    - use_external_tables: Whether to use external tables (default: true)
    - formatted_bucket: S3 bucket for Staging/Bronze/Silver/Audit layers
    - published_bucket: S3 bucket for Gold layer

    Returns:
        Dict with all variable definitions including defaults and descriptions
    """
    return _DB_VARIABLES


def get_schemas() -> Dict[str, str]:
    """Get all schema names by layer."""
    return SCHEMAS


def get_schema(layer: str) -> str:
    """Get schema name for a specific layer (stg, bronze, silver, gold, audit)."""
    if layer not in SCHEMAS:
        raise ConfigError(f"Unknown layer: {layer}. Valid layers: {list(SCHEMAS.keys())}")
    return SCHEMAS[layer]


def get_s3_bucket(layer: str) -> str:
    """
    Get S3 bucket name for a specific layer.

    Args:
        layer: Layer (stg, bronze, silver, gold, audit)

    Returns:
        S3 bucket name
    """
    if layer not in S3_BUCKETS:
        raise ConfigError(f"Unknown layer: {layer}. Valid layers: {list(S3_BUCKETS.keys())}")
    return S3_BUCKETS[layer]


def get_s3_location(entity: str, layer: str) -> str:
    """
    Get S3 location for an entity at a specific layer.

    Follows pattern: s3://{bucket}/{layer_dir}/client_{HARNESS_NAMESPACE}/{entity}/

    Args:
        entity: Entity name (e.g., 'orders')
        layer: Layer (stg, bronze, silver, gold, audit)

    Returns:
        Full S3 path
    """
    if layer not in S3_LAYER_DIRS:
        raise ConfigError(f"Unknown layer: {layer}. Valid layers: {list(S3_LAYER_DIRS.keys())}")

    layer_dir = S3_LAYER_DIRS[layer]
    bucket = get_s3_bucket(layer)

    return f"s3://{bucket}/{layer_dir}/client_{HARNESS_NAMESPACE}/{entity}/"


def get_fqn(entity: str, layer: str) -> str:
    """
    Get fully-qualified table name for an entity in a specific layer.

    Args:
        entity: Entity name (e.g., 'orders')
        layer: Layer (stg, bronze, silver, gold, audit)

    Returns:
        FQN: catalog.schema.entity
    """
    if layer not in SCHEMAS:
        raise ConfigError(f"Unknown layer: {layer}. Valid layers: {list(SCHEMAS.keys())}")

    catalog = get_catalog()
    schema = get_schema(layer)
    return f"{catalog}.{schema}.{entity}"


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG LOADERS
# ═══════════════════════════════════════════════════════════════════════════


def load_staging_config(config_path: str = "config/staging_config.yml") -> Dict[str, Dict[str, Any]]:
    """
    Load staging_config.yml and convert to internal format.

    Returns:
        {
            "defaults": {"table_properties": {...}},
            "tables": {
                "orders": {
                    "columns": [{"name": "order_id", "type": "STRING", ...}, ...],
                    "primary_keys": ["order_id"],
                    "description": "..."
                },
                ...
            }
        }
    """
    config = load_yaml(config_path)

    if "tables" not in config:
        raise ConfigError("staging_config.yml must have 'tables' section")

    for table_name, table_cfg in config.get("tables", {}).items():
        _validate_table_config(table_name, table_cfg, layer="staging")

    return config


def load_bronze_config(config_path: str = "config/bronze_config.yml") -> Dict[str, Dict[str, Any]]:
    """
    Load bronze_config.yml and convert to internal format.

    Returns:
        {
            "defaults": {
                "table_properties": {...},
                "audit_columns": [...]
            },
            "tables": {
                "orders": {
                    "columns": [("order_id", "STRING NOT NULL"), ...],
                    "primary_keys": ["order_id"],
                    "description": "...",
                    "owner": "...",
                    "tags": {...}
                },
                ...
            }
        }
    """
    config = load_yaml(config_path)

    if "tables" not in config:
        raise ConfigError("bronze_config.yml must have 'tables' section")

    # Validate each table
    for table_name, table_cfg in config.get("tables", {}).items():
        _validate_table_config(table_name, table_cfg, layer="bronze")

    return config


def load_silver_config(config_path: str = "config/silver_config.yml") -> Dict[str, Dict[str, Any]]:
    """Load silver_config.yml and convert to internal format."""
    config = load_yaml(config_path)

    if "tables" not in config:
        raise ConfigError("silver_config.yml must have 'tables' section")

    for table_name, table_cfg in config.get("tables", {}).items():
        _validate_table_config(table_name, table_cfg, layer="silver")

    return config


# ═══════════════════════════════════════════════════════════════════════════
# CONVERSION HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def get_bronze_tables(config: Dict[str, Any]) -> Dict[str, List[tuple]]:
    """Extract tables from bronze config in the format used by existing code."""
    result = {}
    defaults = config.get("defaults", {})

    for table_name, table_cfg in config.get("tables", {}).items():
        columns = []

        # Add business columns
        for col in table_cfg.get("columns", []):
            col_name = col["name"]
            col_type = col["type"]
            columns.append((col_name, col_type))

        result[table_name] = {
            "columns": columns,
            "primary_keys": table_cfg.get("primary_keys", []),
            "description": table_cfg.get("description", ""),
            "owner": table_cfg.get("owner", ""),
            "tags": table_cfg.get("tags", {}),
            "table_properties": defaults.get("table_properties", {}),
            "audit_columns": defaults.get("audit_columns", [])
        }

    return result


def get_silver_columns_with_audit(config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """Get Silver columns including audit column mappings."""
    if table_name not in config.get("tables", {}):
        raise ConfigError(f"Table '{table_name}' not found in silver_config")

    table_cfg = config["tables"][table_name]
    defaults = config.get("defaults", {})

    # Build audit column mappings
    audit_mappings = {}
    framework_audit = []

    for audit_col in defaults.get("audit_columns", []):
        col_name = audit_col["name"]

        if "maps_from" in audit_col:
            audit_mappings[col_name] = audit_col["maps_from"]
        elif audit_col.get("generated_by") == "framework":
            framework_audit.append((col_name, audit_col["type"]))

    return {
        "columns": [(c["name"], c["type"]) for c in table_cfg.get("columns", [])],
        "primary_keys": table_cfg.get("primary_keys", []),
        "audit_column_mappings": audit_mappings,
        "framework_audit_columns": framework_audit,
        "table_properties": defaults.get("table_properties", {})
    }


# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════════════════


def _validate_table_config(table_name: str, config: Dict[str, Any], layer: str) -> None:
    """Validate table configuration for required fields."""
    if "columns" not in config:
        raise ConfigError(f"{layer}: table '{table_name}' missing 'columns'")

    if "primary_keys" not in config:
        raise ConfigError(f"{layer}: table '{table_name}' missing 'primary_keys'")

    if not config["columns"]:
        raise ConfigError(f"{layer}: table '{table_name}' has no columns")

    # Validate each column
    for col in config["columns"]:
        if "name" not in col or "type" not in col:
            raise ConfigError(f"{layer}: table '{table_name}' column missing 'name' or 'type': {col}")


