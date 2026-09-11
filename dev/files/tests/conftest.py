"""Shared pytest fixtures for the E2E medallion pattern test harness.

Meant to run inside a Databricks notebook (see notebooks/99_run_tests.py) --
the `spark` fixture picks up the live, already-active cluster session rather
than starting a new local one.
"""

import os
import sys
from pathlib import Path

# Make `common` importable regardless of where pytest is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from pyspark.sql import SparkSession

from common.pipeline import reset_environment

CATALOG = os.environ.get("E2E_TEST_CATALOG", "rls_testing")
USE_EXTERNAL_TABLES = os.environ.get("E2E_USE_EXTERNAL_TABLES", "true").strip().lower() == "true"
FORMATTED_BUCKET = os.environ.get("E2E_FORMATTED_BUCKET") or None
PUBLISHED_BUCKET = os.environ.get("E2E_PUBLISHED_BUCKET") or None

SEED_ROWS_SQL = """
    ('ORD-1001', 'Alice Smith',   'NEW',       120.50, CURRENT_TIMESTAMP()),
    ('ORD-1002', 'Bob Jones',     'PENDING',    75.00, CURRENT_TIMESTAMP()),
    ('ORD-1003', 'Carla Diaz',    'SHIPPED',   340.10, CURRENT_TIMESTAMP()),
    ('ORD-1004', 'Deepak Rao',    'NEW',        58.25, CURRENT_TIMESTAMP()),
    ('ORD-1005', 'Elena Petrova', 'CANCELLED',  99.99, CURRENT_TIMESTAMP())
"""


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def catalog():
    return CATALOG


@pytest.fixture(scope="session")
def pipeline_kwargs():
    """use_external/formatted_bucket/published_bucket, read from the same
    job parameters the pipeline job uses (via env vars 99_run_tests.py sets)
    -- never hardcoded, so the test suite exercises the exact same table
    mode (external vs managed) as whatever the pipeline is actually
    configured for."""
    return {
        "use_external": USE_EXTERNAL_TABLES,
        "formatted_bucket": FORMATTED_BUCKET,
        "published_bucket": PUBLISHED_BUCKET,
    }


@pytest.fixture
def fresh_environment(spark, catalog, pipeline_kwargs):
    """Reset to a deterministic clean slate before each test that needs full
    control over the CDC event sequence: TRUNCATEs every table this harness
    owns (not DROP SCHEMA CASCADE -- that only removes catalog metadata for
    an external table, the physical S3 data survives, so a later
    `CREATE TABLE IF NOT EXISTS` would silently resurrect old rows instead of
    starting fresh), then seeds the same 5 known rows every time.

    Function-scoped deliberately -- each test gets its own isolated
    environment so test order never matters and one test's mutations can't
    leak into another's assertions.
    """
    names, _ = reset_environment(spark, catalog, **pipeline_kwargs)
    spark.sql(
        f"INSERT INTO {names['stg']} (order_id, customer_name, status, amount, updated_at) "
        f"VALUES {SEED_ROWS_SQL}"
    )
    return names
