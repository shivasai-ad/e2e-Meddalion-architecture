# Databricks notebook source
# MAGIC %md
# MAGIC # STG - Staging table creation
# MAGIC
# MAGIC Creates the synthetic CDC source tables (a stand-in for a real Lakeflow
# MAGIC Connect source) that Bronze reads via Change Data Feed. All configuration
# MAGIC (columns, primary keys, table properties) comes from
# MAGIC `config/staging_config.yml`. Runs for all entities or a single entity
# MAGIC specified via widget.
# MAGIC
# MAGIC **No seed data is inserted here.** Bring your own rows: after running this
# MAGIC notebook, insert whatever you want directly into the staging table before
# MAGIC running Bronze. Every INSERT/UPDATE/DELETE against it produces genuine CDF
# MAGIC events, not simulated ones.
# MAGIC
# MAGIC Creates the staging schema on first run (idempotent), then creates all
# MAGIC requested entity tables within it.

# COMMAND ----------

import os
import sys

# This notebook lives at notebooks/stg/staging.py -- two levels below the
# project root (dev/files) where common/ and config/ live. Databricks sets a
# notebook's CWD to its own containing folder, so ".." would only reach
# notebooks/, not dev/files.
PROJECT_ROOT = os.path.abspath("../..")
sys.path.append(PROJECT_ROOT)

from common.config_loader import (
    load_staging_config,
    get_catalog,
    get_schemas,
    get_use_external_tables,
    get_fqn,
)
from common import ddl

# COMMAND ----------

dbutils.widgets.text("entity", "ALL", label="Entity (or ALL)")
ENTITY = dbutils.widgets.get("entity").strip()

CATALOG = get_catalog()
SCHEMAS = get_schemas()
USE_EXTERNAL = get_use_external_tables()

STAGING_CONFIG = load_staging_config(os.path.join(PROJECT_ROOT, "config/staging_config.yml"))
CONFIG_ENTITIES = list(STAGING_CONFIG.get("tables", {}).keys())

# Case-insensitive entity lookup: map lowercase keys to actual config keys
entity_map = {key.lower(): key for key in CONFIG_ENTITIES}
entity_normalized = ENTITY.lower()

if entity_normalized != "all":
    if entity_normalized not in entity_map:
        raise ValueError(f"Entity '{ENTITY}' not found in staging_config. Available: {CONFIG_ENTITIES}")
    ENTITIES = [entity_map[entity_normalized]]  # Use actual config key
else:
    ENTITIES = CONFIG_ENTITIES  # Use all entities as-is

print(f"Processing Staging for entities: {ENTITIES}")

# Create staging schema (idempotent)
stg_schema = SCHEMAS["stg"]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{stg_schema}")

# COMMAND ----------


def process_staging_entity(spark, entity: str, config: dict, table_properties: dict) -> str:
    """Create the staging table for a single entity if it doesn't already exist."""
    stg_fqn = get_fqn(entity, "stg")
    columns = [(col["name"], col["type"]) for col in config.get("columns", [])]

    if not spark.catalog.tableExists(stg_fqn):
        print(f"Creating staging table: {stg_fqn}")
        stg_ddl = ddl.generate_staging_ddl(stg_fqn, entity, columns, [], table_properties, USE_EXTERNAL)
        spark.sql(stg_ddl)
    else:
        print(f"Staging table already exists: {stg_fqn}")

    return stg_fqn

# COMMAND ----------

table_properties = STAGING_CONFIG.get("defaults", {}).get("table_properties", {})

results = {}
for entity in ENTITIES:
    entity_config = STAGING_CONFIG["tables"].get(entity)
    if not entity_config:
        print(f"WARNING: Entity '{entity}' has no config in staging_config.yml")
        continue

    fqn = process_staging_entity(spark, entity, entity_config, table_properties)
    results[entity] = fqn

print(f"\n{'='*70}")
print(f"Staging Table Creation Complete")
print(f"{'='*70}")
for entity, fqn in results.items():
    print(f"{entity}: {fqn} (rows: {spark.table(fqn).count()})")
