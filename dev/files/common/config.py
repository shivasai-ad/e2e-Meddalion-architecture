"""Configuration for the E2E medallion pattern test harness.

Single source of truth for schema names and every entity this harness
exercises. `orders` is deliberately unrelated to any real domain's tables
(Payments' `cases`/`payments`, FOF's tables, etc.) -- see README.md for why
this harness stays fully decoupled from real data.

To onboard a new entity, add an entry to TABLES below with its business
columns (name, SQL type), keys, and hash config -- common/ddl.py generates
staging/bronze DDL from this automatically. Layer-fixed metadata columns
(_rescued_data, the five _bronze_* columns) are NOT declared here -- they're
identical for every entity and are added by common/ddl.py itself. Gold's
aggregation logic is inherently entity-specific and still needs to be
hand-written per entity (see common/pipeline.py::run_gold).
"""

SCHEMA_PREFIX = "e2e_medallion_metadata_test"

TABLES = {
    "orders": {
        # Business columns for `orders`, in a fixed order. This list *is*
        # the frozen hash contract described in the golden path doc
        # (medallion-cdc-fed-pattern.md) -- reordering it would silently
        # change every existing row's hash.
        "columns": [
            ("order_id", "STRING NOT NULL"),
            ("customer_name", "STRING"),
            ("status", "STRING"),
            ("amount", "DOUBLE"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["order_id"],
        # updated_at is a source-side audit stamp, not a business fact --
        # excluded from the hash so re-stamping it without a real change is
        # a no-op. Same convention as Payments' notebooks/config/payments_tables.py.
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "asset": {
        "columns": [
            ("entity_master_asset_id", "STRING NOT NULL"),
            ("asset_name", "STRING"),
            ("asset_type", "STRING"),
            ("currency", "STRING"),
            ("status", "STRING"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["entity_master_asset_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "document": {
        "columns": [
            ("document_id", "STRING NOT NULL"),
            ("original_file_name", "STRING"),
            ("classification", "STRING"),
            ("document_received_date", "DATE"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["document_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "document_banking": {
        "columns": [
            ("document_id", "STRING NOT NULL"),
            ("beneficiary_bank_name", "STRING"),
            ("bank_location", "STRING"),
            ("beneficiary_account_name", "STRING"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["document_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "owner": {
        "columns": [
            ("entity_master_owner_id", "STRING NOT NULL"),
            ("owner_name", "STRING"),
            ("owner_type", "STRING"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["entity_master_owner_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "performance_metric": {
        "columns": [
            ("performance_metric_id", "STRING NOT NULL"),
            ("metric_name", "STRING"),
            ("period_type", "STRING"),
            ("metric_value", "DOUBLE"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["performance_metric_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "position": {
        "columns": [
            ("entity_master_position_id", "STRING NOT NULL"),
            ("owner_id", "STRING"),
            ("asset_id", "STRING"),
            ("status", "STRING"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["entity_master_position_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "ref_metric": {
        "columns": [
            ("metric_name", "STRING NOT NULL"),
            ("display_name", "STRING"),
            ("category", "STRING"),
            ("format_type", "STRING"),
            ("is_active", "BOOLEAN"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["metric_name"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "ref_transaction_type": {
        "columns": [
            ("transaction_type", "STRING NOT NULL"),
            ("display_name", "STRING"),
            ("category", "STRING"),
            ("subcategory", "STRING"),
            ("is_active", "BOOLEAN"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["transaction_type"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
    "transaction": {
        "columns": [
            ("transaction_id", "STRING NOT NULL"),
            ("position_id", "STRING"),
            ("transaction_type", "STRING"),
            ("amount", "DOUBLE"),
            ("currency", "STRING"),
            ("trade_date", "DATE"),
            ("updated_at", "TIMESTAMP"),
        ],
        "keys": ["transaction_id"],
        "exclude_from_hash": ["updated_at"],
        "sequence_by": "updated_at",
    },
}


def schema_names(prefix: str = SCHEMA_PREFIX) -> dict:
    """The 5 schemas this harness owns within its target catalog."""
    return {
        "stg": f"{prefix}_stg",
        "bronze": f"{prefix}_bronze",
        "silver": f"{prefix}_silver",
        "gold": f"{prefix}_gold",
        "audit": f"{prefix}_audit",
    }


def fqns(catalog: str, entity: str, prefix: str = SCHEMA_PREFIX) -> dict:
    """Fully-qualified name of `entity`'s table in each data layer (audit
    has no per-entity table -- it holds the watermark control table
    instead, see common/control.py)."""
    schemas = schema_names(prefix)
    return {
        layer: f"{catalog}.{schema}.{entity}"
        for layer, schema in schemas.items()
        if layer != "audit"
    }
