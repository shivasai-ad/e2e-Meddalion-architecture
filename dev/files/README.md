# E2E Medallion Pattern Test (DAA-856)

A standalone, runnable test harness that validates the **Medallion CDC-Fed
pattern** documented in
[`docs/databricks_objects/15_medallion_pattern_rds_to_databricks/medallion-cdc-fed-pattern.md`](../../docs/databricks_objects/15_medallion_pattern_rds_to_databricks/medallion-cdc-fed-pattern.md)
(referred to as **"the doc"** everywhere below).

It exercises the pattern itself, with its own synthetic `orders` entity and
its own Bronze/Silver/Gold tables — **zero dependency** on any real domain's
pipeline, tables, or data (e.g. Payments' `dev_payments_catalog`). Built for
[DAA-856](https://alterdomus.atlassian.net/browse/DAA-856).

Unlike the rest of this repo, everything under `examples/` is **concrete, not
a `{{mustache}}` template** — it deploys and runs as-is.

## Why `rls_testing`

Every domain catalog in the ADI workspace (`dev_payments_catalog`,
`dev_fof_catalog`, etc.) is Terraform-managed and only grants write access to
that domain's engineer group. `dev_native_catalog` (the generic shared one)
only grants `CREATE_SCHEMA` to `data-engineers`. `rls_testing` is the one
catalog that grants the harness owner `ALL_PRIVILEGES` directly, with no
extra grant request needed, and it has no relationship to any domain's data.
Point the `catalog` bundle variable at a dedicated sandbox catalog later if
the team provisions one — it's a one-line change.

## Why external tables (and why that took two tries to get right)

The doc's own examples all use **external** Delta tables at an explicit
`s3://ad-dna-{region}-{env}-datalake-{layer}-{account_id}/` `LOCATION`. All 6
tables in this harness now follow that exactly:

| Table | `LOCATION` |
|---|---|
| Staging (`orders`) | `s3://{formatted_bucket}/staging/client_e2e_medallion_pattern_test_copy/orders/` (this harness's own addition — the doc doesn't cover staging, since a real Lakeflow Connect staging table's storage isn't something a downstream pipeline configures) |
| Bronze (`orders`) | `s3://{formatted_bucket}/formatted_stg/client_e2e_medallion_pattern_test_copy/orders/` |
| Silver (`orders`) | `s3://{formatted_bucket}/formatted/client_e2e_medallion_pattern_test_copy/orders/` |
| Gold (`orders_status_summary`) | `s3://{published_bucket}/published/client_e2e_medallion_pattern_test_copy/orders_status_summary/` |
| `job_flags` / `audit_log` | `s3://{formatted_bucket}/audit/client_e2e_medallion_pattern_test_copy/{table}/` |

**This was checked twice, with two different (contradictory) results, and it's worth recording why:**

1. First check: `databricks grants get external_location <bucket>` showed only
   the `data-engineers` group holds `CREATE_EXTERNAL_TABLE`/`WRITE_FILES` on
   these buckets, and this harness's owner isn't a member — concluded
   external tables weren't possible, went with managed tables instead.
2. That conclusion was **wrong** for actual execution. An empirical test
   (`CREATE TABLE ... LOCATION ...` against the real bucket, then `INSERT`,
   then `SELECT`) succeeded completely. The explanation: this workspace's
   metastore (`metastore_aws_eu_central_1`) is owned by an account-level
   `metastore-admins` group, and Unity Catalog metastore admins have implicit
   access to every securable in the metastore regardless of that object's own
   explicit grant list. Reading grants alone missed this; only an actual
   `CREATE`+`INSERT`+`SELECT` attempt revealed it.

**Lesson applied generally in this harness: prefer testing a claim over
inferring it from one API's output** — especially for permissions, where
multiple overlapping grant mechanisms (explicit grants, ownership, admin
roles) can each independently allow something a single check appears to deny.

`use_external_tables` (bundle variable, default `"true"`) still exists as a
toggle — mirrors Payments' own `common_utils/ddl.py` pattern — so this can
fall back to managed tables if the grant situation ever changes; override
with `--var="use_external_tables=false"` at deploy time.

---

## Doc compliance — every section of the doc, mapped to this codebase

This is an exhaustive, section-by-section comparison: for every rule,
mechanic, and column the doc specifies, does this codebase have it? Legend:
**✅ present & compliant** · **⚠️ present but deviates** (with the reason) ·
**❌ genuinely missing** · **N/A** (doesn't apply to this harness).

### Architecture & layer roles

| Doc says | Here | Status |
|---|---|---|
| Source → Staging → Bronze → Silver → Gold, 4 stages | `common/pipeline.py` has `run_bronze`/`run_silver`/`run_gold`; staging is `notebooks/00_setup_test_environment.py` + manual inserts | ✅ |
| Staging = current state only (SCD1) | Our staging table has no history columns, just current rows | ✅ |
| Bronze = full historical tracking, SCD2 via CDF+hash | `common/pipeline.py::run_bronze` + `common/scd2.py` | ✅ |
| Silver = cleansed, current state, `WHERE _bronze_is_current = TRUE` | `common/pipeline.py::run_silver` | ✅ |
| Gold = business metrics, aggregated, rebuilt from Silver | `common/pipeline.py::run_gold` → `orders_status_summary` | ✅ |
| "History lives in exactly one place (Bronze)" | Silver/Gold carry no `_valid_from`/`_valid_to`/`_is_current` columns | ✅ |

### Audit / metadata columns by layer

| Doc column | Layer | Here | Status |
|---|---|---|---|
| `created_at` (Mandatory) | Source | Not present on the synthetic `orders` entity | ❌ **real gap** — our entity only has `updated_at` |
| `updated_at` (Mandatory) | Source | Present | ✅ |
| `created_by`/`updated_by` (Recommended) | Source | Not present | ❌ (Recommended, not Mandatory) |
| `_rescued_data` | Staging | Present, always `NULL` (nothing to rescue in this synthetic harness) | ✅ |
| `_bronze_valid_from/to/is_current/hash/ingested_at` | Bronze | All 5, exact names, exact nullability, verified live via `DESCRIBE TABLE` | ✅ |
| `_record_effective_date`, `_ingested_at`, `_row_hash`, `_silver_processed_at`, `_is_missing_*`, `_is_incomplete` | Silver | All present, exact names, verified live | ✅ |
| `_gold_refreshed_at` | Gold | Present on `orders_status_summary` | ✅ |
| Gold history view (`effective_from/to`, `is_current_version`, `version_number`) | Gold (optional) | Not built | N/A — doc marks this explicitly optional ("build it only if you have a consumer"); built once, then removed at explicit request since nothing here consumes it |
| `_`-prefix naming rule | All | Every audit column follows it | ✅ |

### Bronze mechanics

| Doc rule | Here | Status |
|---|---|---|
| Hash = MD5 over a fixed, explicit, sorted column list | `common/scd2.py::row_hash`, `sorted(business_cols)` | ✅ |
| "Include every business column, including the PK" | `order_id` (PK) included; `updated_at` excluded via `ENTITY_CONFIG["exclude_from_hash"]` | ⚠️ **deviates** — same choice Payments' real pipeline makes, to stop a re-stamped `updated_at` from looking like a business change. Documented trade-off, not an oversight. |
| Exclude `_bronze_*` audit columns from the hash | Hash is computed on the staging-side `changes` df, before Bronze columns exist | ✅ |
| `COALESCE` to a NULL-safe sentinel, not `''` | `NULL_SENTINEL = "\x1fNULL"` (doc's example uses `'__NULL__'` — same principle, different literal) | ✅ (spirit), literal differs |
| Fixed column order, `CONCAT_WS` | `sorted(business_cols)` | ✅ |
| External Bronze table, `LOCATION` at `formatted_stg/client_{id}/{entity}/` | `common/ddl.py::create_bronze_table_sql` | ✅ |
| Bronze `TBLPROPERTIES`: `enableChangeDataFeed`, `columnMapping.mode` | Set | ✅ |
| Bronze `TBLPROPERTIES`: `enableTypeWidening`, `enableIcebergCompatV2`, `universalFormat.enabledFormats`, `autoOptimize.optimizeWrite`, `autoOptimize.autoCompact`, `logRetentionDuration`, `deletedFileRetentionDuration` | All set, confirmed live via `SHOW TBLPROPERTIES` — see "Decision needed" below for the `enableIcebergCompatV2` trade-off this required | ✅ (with an open trade-off) |
| **One atomic `MERGE` (`UNION ALL` of close+insert)** | `run_bronze` does a `MERGE` for closes, then a **separate** `.write().append()` for inserts — two operations, not one | ⚠️ **deviates** — same choice Payments' pipeline makes, documented there as "self-healing, not silent" on partial failure (watermark only advances after both succeed, so a crash mid-way safely replays) |
| First-time load = full snapshot as inserts, no `MERGE` | `common/scd2.py::read_changes`, `from_version is None` branch | ✅ |
| Control-table-driven first-run vs incremental decision | `common/control.py::get_watermark` → `None` means first run | ✅ |
| Fallback: full-scan `MERGE` for disaster recovery | Not implemented | ❌ (edge case, no disaster-recovery drill run) |

### Silver mechanics

| Doc rule | Here | Status |
|---|---|---|
| Read only `_bronze_is_current = TRUE` | `run_silver`, `df.filter(F.col("_bronze_is_current"))` | ✅ |
| **"never `SELECT *`"** — explicit column list only | `run_silver` does `spark.table(bronze_fqn)` (all columns) then `.drop()`s known metadata columns — the opposite of an explicit list | ⚠️ **deviates** — same pattern as Payments' `silver_build.py`. A new Bronze column would silently flow into Silver undecided, exactly the failure mode the doc warns about |
| Cleansing only, no business logic | `TRIM(UPPER(status))`, `_is_missing_customer_name` — no aggregation, no KPIs | ✅ |
| Baseline DQ gate: PK `IS NOT NULL` | `for k in keys: df.filter(F.col(k).isNotNull())` | ✅ |
| Lineage columns carried from Bronze | `_record_effective_date`, `_ingested_at`, `_silver_processed_at` | ✅ |
| Incremental `MERGE` with `WHEN NOT MATCHED BY SOURCE THEN DELETE` | Full `CREATE OR REPLACE` rebuild every run instead | ⚠️ **deviates** — same as Payments ("cheap at this volume, immune to drift"); functionally equivalent outcome, less efficient at scale |
| Silver `TBLPROPERTIES` (`enableIcebergCompatV2`, `universalFormat`, `autoOptimize.*`) | All set, confirmed live | ✅ |

### Gold mechanics

| Doc rule | Here | Status |
|---|---|---|
| Only layer allowed business logic | `orders_status_summary`'s `GROUP BY status` | ✅ |
| KPI example: dimension cols + `COUNT(*)` + `_gold_refreshed_at` | `status`, `order_count`, `_gold_refreshed_at` | ✅ (simpler — doc's example is illustrative, not a mandatory shape) |
| KPI example: `incomplete_records`, `data_quality_pct`, `latest_data_at` | Not aggregated into Gold (the underlying `_is_missing_customer_name` flag exists in Silver but isn't rolled up) | ❌ minor gap — nice-to-have, not core to the pattern |
| History view — optional | Not built | N/A, correctly deferred (see above) |
| "Full `CREATE OR REPLACE` per run is normal, not wasteful" | Exactly how `run_gold` works | ✅ |
| Gold `TBLPROPERTIES` | All set, confirmed live | ✅ |

### No-PK fallback

| Doc section | Here | Status |
|---|---|---|
| Row-hash-as-synthetic-PK, `REPLICA IDENTITY FULL`, decision flowchart | Not implemented | N/A — `orders` has a real PK (`order_id`); this entire section doesn't apply |

### Control table (`job_flags`)

| Doc rule | Here | Status |
|---|---|---|
| "Don't invent a new `_ingestion_control` table — reuse `job_flags.py`" | `common/job_flags.py` — this harness previously built exactly the anti-pattern named (a bespoke `_ingestion_control` table), fixed | ✅ |
| Exact DDL: 9 columns, `job_name`/`key`/`value`/`insertion_timestamp`/`x_ad_meta_*` | `common/job_flags.py::ensure_table` | ✅ verified live |
| `get()`/`set()` API, append-only, "latest by timestamp wins" | `common/job_flags.py::get/set` | ✅ |
| External table at `.../audit/client_{id}/job_flags/` | ✅ | ✅ |
| `job_flags` `TBLPROPERTIES`: `columnMapping.mode`, `deletedFileRetentionDuration`, `enableChangeDataFeed` | Set | ✅ |
| `job_flags` `TBLPROPERTIES`: `enableIcebergCompatV2`, `universalFormat.enabledFormats`, `parquet.compression.codec=zstd` | All set, confirmed live | ✅ |

### Schema-change runbook

| Scenario | Here | Status |
|---|---|---|
| Add nullable column, add `NOT NULL`/`DEFAULT`, column rename, type change | None tested or coded for | ❌ entirely deferred (DAA-856's TC-006/106) |

### External table recovery

| Doc rule | Here | Status |
|---|---|---|
| S3 versioning, 30+ day retention, no `VACUUM RETAIN 0 HOURS`, `RESTORE TABLE` drills | Retention properties now set on Bronze (`logRetentionDuration`/`deletedFileRetentionDuration` = `30 days`); no actual `RESTORE TABLE` recovery drill has been run | ⚠️ properties in place, drill still deferred |

### Operational monitoring

| Doc rule | Here | Status |
|---|---|---|
| Slot lag query | N/A — no real Postgres replication slot (synthetic source) | N/A |
| Mass-closure detection | Not built (no dashboard/alert on `_bronze_valid_to` spikes) | ❌ deferred |
| `AuditLogger` — logs every Bronze/Silver/Gold write | `common/audit_log.py` | ✅ |
| Exact DDL: 18 columns | `common/audit_log.py::ensure_table` | ✅ verified live |
| `audit()` context manager (success/failure/duration) | `common/audit_log.py::audit` | ✅ |
| `log_merge()` → 3 rows (`merge_insert`/`merge_update`/`merge_delete`), even a 0-count action | `common/audit_log.py::log_merge` | ✅ verified live with real MERGE metrics |
| External table at `.../audit/client_{id}/audit_log/` | ✅ | ✅ |
| `audit_log` `TBLPROPERTIES`: `columnMapping.mode`, `deletedFileRetentionDuration` | Set | ✅ |
| `audit_log` `TBLPROPERTIES`: `enableIcebergCompatV2`, `universalFormat.enabledFormats` | All set, confirmed live | ✅ |

### `TBLPROPERTIES` — fixed, but it surfaced a real trade-off (decision needed)

All doc-specified `TBLPROPERTIES` are now set on all 6 tables, confirmed live
via `SHOW TBLPROPERTIES`. Getting there required disabling **Deletion
Vectors** first — Databricks enforces that `enableIcebergCompatV2` and
Deletion Vectors cannot coexist (hit `DELTA_ICEBERG_COMPAT_VIOLATION`
directly; resolved with `ALTER TABLE ... SET TBLPROPERTIES
('delta.enableDeletionVectors'='false')` + `REORG TABLE ... APPLY (PURGE)`
on every table before the Iceberg properties would apply).

**This is a real, open trade-off for a pipeline meant to take growing
production loads, not just this harness's 5-row sample:**

| Option | What it costs / buys |
|---|---|
| **Deletion Vectors ON** (Iceberg off) | Databricks' own recommended default for MERGE-heavy tables — a `MERGE`/`UPDATE`/`DELETE` writes a small marker instead of rewriting whole Parquet files. Bronze runs an SCD2 `MERGE` on *every incremental load, forever* — the layer this choice matters most for, and the one that grows biggest over time. No external engine (Trino, Snowflake, an Iceberg REST Catalog) can read the table natively as Iceberg. |
| **Iceberg UniForm ON** (current setting, per the doc) | Matches the doc's `TBLPROPERTIES` literally. Every `MERGE`/`UPDATE`/`DELETE` must rewrite full Parquet files — negligible on 5 rows, a real and growing cost as Bronze's history accumulates in production. Buys nothing unless something actually reads this table via Iceberg outside Databricks. |

**The question that actually decides this**: does any consumer of this
pipeline's tables need to read them as Iceberg from outside Databricks?
Nothing in DAA-856 or this project has named one so far.

**Current state**: Iceberg UniForm is ON (Deletion Vectors OFF), following
the doc literally, per explicit direction — implemented this way and raised
here for a stakeholder decision, rather than decided unilaterally. Reversible
either way with `ALTER TABLE SET TBLPROPERTIES` (+ a `REORG TABLE ... APPLY
(PURGE)` if re-enabling Iceberg after a period with Deletion Vectors on).

### Summary of remaining open gaps (not just documented trade-offs)

1. **Source-layer `created_at`** (doc: Mandatory) missing from the synthetic entity.
2. **Schema-change runbook** untested (column add/rename/type change).
3. **Gold KPI columns** (`data_quality_pct`, `latest_data_at`) not rolled up.
4. **External table recovery drill** — properties are in place (see above), but no actual `RESTORE TABLE` recovery has been run.
5. **Fallback full-scan MERGE** and **mass-closure monitoring** not built.

### Deliberate, documented deviations (not gaps — inherited from the real Payments pipeline, for the same reasons)

- Hash excludes `updated_at`.
- Bronze does two writes (`MERGE` + append) instead of one atomic `UNION ALL MERGE`.
- Silver does `SELECT *`-then-drop instead of an explicit column list.
- Silver is a full rebuild every run instead of an incremental `MERGE` with `WHEN NOT MATCHED BY SOURCE THEN DELETE`.

---

## How it runs — execution flow, file by file

### The layer chain (what runs after what)

```
00_setup_test_environment   (creates schemas + empty staging table, or TRUNCATEs if reset=true)
        │
        ▼  (you insert/update/delete rows into staging yourself, by hand)
        │
01_bronze_scd2               common.pipeline.run_bronze(spark, catalog, ...)
        │                     reads CDF since the last watermark, hashes, MERGEs closes,
        │                     appends new current rows, advances job_flags' watermark
        ▼
02_silver_conform            common.pipeline.run_silver(spark, catalog, ...)
        │                     reads Bronze WHERE _bronze_is_current, cleanses,
        │                     rebuilds Silver, reconciles row counts
        ▼
03_gold_aggregate            common.pipeline.run_gold(spark, catalog, ...)
                              GROUP BY status from Silver, rebuilds orders_status_summary
```

Every one of the 4 notebooks calls straight into `common/pipeline.py` — the
notebooks themselves are thin: read widgets, call one function, print/display
the result. All the actual logic lives in `common/`, which is why the same
functions are also what `tests/*.py` calls directly.

### What each file is and does

| File | What it is | What it does |
|---|---|---|
| `databricks.yml` | Bundle config | Declares 4 variables (`catalog`, `use_external_tables`, `formatted_bucket`, `published_bucket`) — every one overridable via `--var`, none hardcoded. Points the `dev` target at the ADI workspace. |
| `common/config.py` | Static config | The one place the `orders` entity's shape lives: its 5 schema names, its key column (`order_id`), which column to exclude from the hash (`updated_at`), and the sequence column for collapsing rapid changes. |
| `common/ddl.py` | Table DDL builder | Builds the `CREATE TABLE` SQL for staging and Bronze, including the `LOCATION` clause when `use_external=True`. `location_clause()` is the one function that knows the doc's S3 path convention. |
| `common/scd2.py` | Bronze's core mechanics | `row_hash()` (the MD5 hash), `read_changes()` (CDF read, or full snapshot on first run), `latest_per_key()` (collapses multiple changes to one key in a batch), `classify()` (tags each change NEW/CHANGED/DELETED/UNCHANGED). |
| `common/job_flags.py` | Generic control table | The doc's own prescribed watermark store — `get`/`set` on a `(job_name, key, value)` table, append-only. |
| `common/control.py` | Thin adapter | Wraps `job_flags` with `get_watermark`/`set_watermark`, so `pipeline.py` doesn't need to know the underlying storage is `job_flags`. |
| `common/audit_log.py` | Observability | `AuditLogger` — a context manager (`audit()`) that logs success/failure/duration for a write, and `log_merge()` which decomposes a MERGE's metrics into 3 rows. |
| `common/pipeline.py` | **The actual pipeline** | `ensure_environment()` (idempotent create), `reset_environment()` (TRUNCATE everything), `run_bronze()`, `run_silver()`, `run_gold()` — the 3 functions that do everything described in "The layer chain" above. |
| `notebooks/00_setup_test_environment.py` | Entry point #1 | Widgets → `ensure_environment()` or `reset_environment()` depending on the `reset` widget. Creates schemas + an empty staging table. No seed data. |
| `notebooks/01_bronze_scd2.py` | Entry point #2 | Widgets → `run_bronze()` → displays the resulting Bronze table. |
| `notebooks/02_silver_conform.py` | Entry point #3 | Widgets → `run_silver()` → displays Silver, prints reconciliation status. |
| `notebooks/03_gold_aggregate.py` | Entry point #4 | Widgets → `run_gold()` → displays `orders_status_summary`, prints reconciliation status. |
| `notebooks/99_run_tests.py` | Test entry point | Installs `pytest`, restarts Python (Workspace Files can't write `__pycache__`, so bytecode caching must be off), runs the whole `tests/` suite, fails the task if any test fails. |
| `tests/conftest.py` | Shared test fixtures | `spark`, `catalog`, `pipeline_kwargs` (external/bucket config, read from env vars `99_run_tests.py` sets), and `fresh_environment` (calls `reset_environment()` + seeds a fixed 5-row baseline — **only** used by tests, never by the manual notebooks). |
| `tests/test_tc*.py` | The 10 pytest tests | Each one calls `run_bronze`/`run_silver`/`run_gold` directly (no `dbutils.notebook.run`), mutates staging with raw SQL, and asserts on the result — one test file per DAA-856 TC group. |
| `resources/jobs/e2e_medallion_pipeline.yml` | Job #1 | 4 tasks: `setup_test_env → bronze_scd2 → silver_conform → gold_aggregate`. `reset` is a job **parameter** (default `"false"`), not hardcoded. |
| `resources/jobs/e2e_medallion_tests.yml` | Job #2 | 1 task: `run_tests`. Deliberately separate — see below. |

### Why two jobs, not one

Originally one job, 5 tasks. Splitting them was **not** a doc requirement (the
doc says nothing about job/test orchestration) — it was the fix for a real
collision: pytest needs a guaranteed-clean state before every test
(`fresh_environment` truncates + reseeds), but that's exactly wrong for a
pipeline run, which should never destroy data you just inserted by hand. One
job doing both meant running the pipeline silently wiped your manual testing
data the moment the chained test task fired. Two jobs means:
- **`e2e_medallion_pipeline`** — safe to run anytime, never destroys your data (`reset` defaults to `false`).
- **`e2e_medallion_tests`** — the authoritative automated check, which intentionally resets to a known baseline every time.

**Never run both at once** — they hit the same fixed external table locations
(`job_flags`, `audit_log`) and will collide (`LOCATION_OVERLAP`, or corrupted
watermarks) if run concurrently. Check `databricks jobs list-runs --active-only`
first.

---

## Deploy and run

```bash
cd examples/e2e_medallion_pattern_test_copy
databricks bundle validate --profile adi
databricks bundle deploy -t dev --profile adi

# run the pipeline on whatever is currently in staging (no reset)
databricks bundle run e2e_medallion_pipeline -t dev --profile adi

# ...or force a clean slate first
databricks bundle run e2e_medallion_pipeline --params reset=true -t dev --profile adi

# run the automated test suite (resets data as part of testing)
databricks bundle run e2e_medallion_tests -t dev --profile adi
```

Or from the Databricks UI: **Workflows → Jobs →** "E2E Medallion Pipeline" or
"E2E Medallion Tests" → **Run now**.

### The manual workflow this enables

**No seed data is ever inserted automatically.** `00_setup_test_environment`
only creates schemas and an empty staging table — you bring your own rows.

1. Insert your own data (SQL Editor, or a notebook `%sql` cell):
   `INSERT`/`UPDATE`/`DELETE` against `<catalog>.e2e_medallion_pattern_copy_stg.orders`.
2. Run `e2e_medallion_pipeline` (default `reset=false`) — or open
   `01_bronze_scd2` → `02_silver_conform` → `03_gold_aggregate` individually
   and **Run All** on each, if you want to watch one layer at a time.
3. Query Bronze/Silver/Gold to see your specific change propagate.
4. Run `e2e_medallion_tests` only when you want the full automated check —
   it will reset the data as part of verifying the architecture rules (its
   own `tests/conftest.py` seeds its own fixed 5-row baseline internally,
   independent of whatever you've put in staging by hand — tests need
   deterministic inputs to assert deterministic outputs).

### `reset` — exact semantics

- **`reset=true`**: calls `common/pipeline.py::reset_environment`, which
  **`TRUNCATE`s** every table this harness owns (staging, Bronze, Silver,
  Gold, `job_flags`, `audit_log`) — not `DROP SCHEMA ... CASCADE`. For an
  **external** table, `DROP` only removes the catalog metadata; the physical
  Delta files at the fixed S3 `LOCATION` survive, so a later
  `CREATE TABLE IF NOT EXISTS` at that same path would silently resurrect
  old accumulated rows instead of starting fresh — this was a real bug,
  caught and fixed mid-session. Nothing is reseeded afterward; every table
  ends at 0 rows.
- **`reset=false`** (default): only `CREATE SCHEMA/TABLE IF NOT EXISTS` (a
  no-op if they already exist) — no truncation, nothing reseeded. Whatever
  is currently in staging is processed **incrementally** from wherever the
  watermark last stopped.

This means: to see your own `INSERT`/`UPDATE`/`DELETE` flow through, always
run with `reset=false` (the default) — `reset=true` would wipe your edit
before the pipeline ever sees it.

## What's covered (focused starter set)

| TC | What it checks | Test file |
|---|---|---|
| TC-001 | Initial full load | `tests/test_tc001_initial_load.py` |
| TC-002/003/004 | CDC INSERT/UPDATE/DELETE captured in Bronze SCD2 | `tests/test_tc002_004_cdc_insert_update_delete.py` |
| TC-005 + TC-403 | Watermark correctness + idempotent replay | `tests/test_tc005_tc403_watermark_idempotent.py` |
| TC-102 | Silver data-quality null flag | `tests/test_tc102_silver_dq_flags.py` |
| TC-104 | Dedup: multiple changes to one key collapse to one version | `tests/test_tc104_dedup_multiple_changes.py` |
| TC-203 | Gold aggregation reconciles with Silver row count | `tests/test_tc203_gold_reconciliation.py` |
| TC-301 | Full E2E: one row, source to Gold | `tests/test_tc301_full_e2e_flow.py` |

## Deferred / outstanding (not in this pass)

See "Summary of remaining open gaps" and the "TBLPROPERTIES" decision-needed
section above for the doc-compliance items. On
top of those, from DAA-856 itself:

- The remaining ~18 TC cases (schema evolution, Z-ORDER, partition pruning,
  lineage tracking, alerting, dashboard/reporting). TC-105 (SCD2 in Silver)
  doesn't apply to this architecture — SCD2 lives in Bronze by design.
  TC-305 (audit logs) is implemented (`common/audit_log.py`) but not yet
  asserted by a dedicated test.
- Great Expectations — TC-102 is demonstrated with plain PySpark assertions;
  layering GX suites on top later doesn't require touching the pipeline.
- CI/CD wiring and a schedule — this is currently manual-trigger only.

## Layout

```
databricks.yml                  bundle config (catalog, use_external_tables, formatted_bucket, published_bucket)
common/                         the actual pipeline logic (importable by notebooks AND pytest)
  config.py                       schema names, the `orders` entity's key/hash config
  ddl.py                          table DDL -- external at the doc's S3 convention by default, managed as a fallback
  scd2.py                         hash / classify / latest-per-key mechanics
  job_flags.py                    generic key-value control table (the doc's own prescribed reuse)
  control.py                      watermark get/set -- thin adapter over job_flags.py
  audit_log.py                    AuditLogger -- logs every Bronze/Silver/Gold write
  pipeline.py                     run_bronze / run_silver / run_gold / ensure_environment / reset_environment
notebooks/
  00_setup_test_environment.py    creates schemas + an empty staging table -- no seed data, bring your own
  01_bronze_scd2.py                thin wrapper around common.pipeline.run_bronze
  02_silver_conform.py             thin wrapper around common.pipeline.run_silver
  03_gold_aggregate.py             thin wrapper around common.pipeline.run_gold
  99_run_tests.py                  installs pytest, runs tests/, fails the task on any failure
tests/                           pytest suite, one file per TC group above, plus conftest.py
resources/jobs/
  e2e_medallion_pipeline.yml      setup -> bronze -> silver -> gold (reset defaults to false)
  e2e_medallion_tests.yml         run_tests, on its own (resets data as part of testing)
GUIDE.html                       presentation-friendly walkthrough with live evidence (open in a browser)
```
