# DQX checks: what we can use, and where

This guide lists the data-quality checks available in DQX (`databricks-labs-dqx==0.16.0`, the version the pipeline installs), and says where each one fits in our Bronze → Silver → Gold flow.

Each check is written in a rules file (`config/dq_rules/bronze_to_silver.yml` or `config/dq_rules/silver_to_gold.yml`). The code reads the file and runs the checks. The code does not decide which checks run.

## 1. How a check behaves in our pipeline

- **Every check is pass/fail.** A row that fails any check is written to the quarantine table. A row that passes every check continues to the target. Nothing is marked as a warning.
- **Quarantine tables:**
  - Silver failures → `<catalog>.e2e_medallion_metadata_v2_formatted_exception.<entity>_quarantine`
  - Gold failures → `<catalog>.e2e_medallion_metadata_v2_published_exception.<entity>_quarantine`
- **Deleted rows are not checked.** On Silver, rows with `_bronze_is_current = FALSE` are skipped. On Gold, rows with `_change_type = 'delete'` are skipped.
- **Literal values need quotes.** In list arguments such as `allowed`, a bare word is read as a column name. Write the literal with quotes: `allowed: ["'pending'", "'completed'"]`.
- **Two kinds of check:**
  - **Row-level** checks look at one row at a time. They run inside the gate.
  - **Dataset-level** checks compare rows with each other or with another table. They need the whole table, so they do not run inside the gate on an incremental batch (see section 4).

## 2. The checks, by group

Status key:
- **In use**: already in the rules files.
- **Recommended**: fits our tables and should be added.
- **Later**: useful, but needs more than the current design.
- **Not needed**: does not fit our tables.

### 2.1 Null and empty checks (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_not_null` | value is not null | `column` | primary keys, required dates, numbers, booleans | In use |
| `is_not_null_and_not_empty` | value is not null and not an empty string | `column`, `trim_strings` | required text columns | In use |
| `is_not_empty` | value is not an empty string (null is allowed) | `column`, `trim_strings` | optional text | Recommended |
| `is_null` | value is null | `column` | a column that must stay empty | Not needed |
| `is_empty` | value is an empty string (null is allowed) | `column`, `trim_strings` | — | Not needed |
| `is_null_or_empty` | value is null or empty | `column`, `trim_strings` | — | Not needed |
| `is_not_null_and_not_empty_array` | array is not null and not empty | `column` | array columns (none yet) | Later |

### 2.2 Allowed values (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_in_list` | value is in the list; null passes | `column`, `allowed`, `case_sensitive` | `orders.status`; `asset.status`, `position.status`, `asset_type`, `period_type`, `category`, `currency` once the owners give the values | In use (orders); Recommended (others) |
| `is_not_in_list` | value is not in a forbidden list; null passes | `column`, `forbidden`, `case_sensitive` | banned values | Later |
| `is_not_null_and_is_in_list` | value is not null and is in the list | `column`, `allowed`, `case_sensitive` | required status-type columns | Recommended |
| `has_valid_string_case` | text is upper, lower, title, or sentence case | `column`, `case` | codes that must be upper case, such as currency | Later |

Note: `is_in_list` lets nulls pass. For a required column, use `is_not_null_and_is_in_list` instead.

### 2.3 Numbers and ranges (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `sql_expression` | any SQL condition is true | `expression`, `msg`, `name`, `negate`, `columns` | `amount > 0` (in use); any rule that isn't covered by a built-in | In use |
| `is_in_range` | value is between two limits, inclusive | `column`, `min_limit`, `max_limit` | amounts, metric values, percentages | Recommended |
| `is_not_in_range` | value is outside two limits | `column`, `min_limit`, `max_limit` | — | Later |
| `is_not_less_than` | value is at least a limit | `column`, `limit` | amounts that must not be negative | Recommended |
| `is_not_greater_than` | value is at most a limit | `column`, `limit` | — | Later |
| `is_equal_to` | value equals a given value | `column`, `value`, `abs_tolerance`, `rel_tolerance` | fixed values, such as a version | Later |
| `is_not_equal_to` | value does not equal a given value | `column`, `value`, `abs_tolerance`, `rel_tolerance` | — | Later |

### 2.4 Dates and times (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_valid_date` | value parses as a date | `column`, `date_format` | text dates (not needed for typed DATE columns) | Later |
| `is_valid_timestamp` | value parses as a timestamp | `column`, `timestamp_format` | text timestamps | Later |
| `is_not_in_future` | timestamp is not after now + offset | `column`, `offset` (seconds), `curr_timestamp` | `trade_date`, `document_received_date`, load timestamps | Recommended |
| `is_not_in_near_future` | timestamp is not within the next N seconds | `column`, `offset`, `curr_timestamp` | — | Later |
| `is_older_than_n_days` | date is at least N days before today | `column`, `days`, `curr_date`, `negate` | stale records | Later |
| `is_older_than_col2_for_n_days` | column1 is at least N days older than column2 | `column1`, `column2`, `days`, `negate` | a date that must come before another date | Later |
| `is_data_fresh` | timestamp is not older than N minutes from a base time | `column`, `max_age_minutes`, `base_timestamp` | `_ingested_at`, `_silver_processed_at` freshness | Later |

### 2.5 Text format (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `regex_match` | text matches a regular expression | `column`, `regex`, `negate` | IDs with a fixed pattern, codes | Recommended |
| `is_valid_email` | text is a valid email address | `column` | owner or client email columns, if any | Later |
| `is_valid_uuid` | text is a valid UUID | `column`, `strict` | UUID keys | Later |
| `is_valid_national_id` | text is a valid national ID for a country | `column`, `country` (default `US`) | personal identifiers, with `restricted` sensitivity | Later |

### 2.6 Standard codes (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_valid_currency_code` | value is an ISO 4217 currency code | `column`, `code_format` (default `alphabetic`), `case_sensitive` | `currency` in `asset` and `transaction` | Recommended |
| `is_valid_country_code` | value is an ISO 3166-1 country code | `column`, `code_format` (default `alpha-2`), `case_sensitive` | country columns, if any | Later |
| `is_valid_language_code` | value is an ISO 639 language code | `column`, `code_format` (default `alpha-2`), `case_sensitive` | language columns, if any | Later |
| `is_valid_subdivision_code` | value is an ISO 3166-2 subdivision code | `column`, `case_sensitive`, `country_column` | — | Later |

### 2.7 Network and JSON (row-level)

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_valid_ipv4_address` | value is an IPv4 address | `column` | — | Not needed |
| `is_valid_ipv6_address` | value is an IPv6 address | `column` | — | Not needed |
| `is_ipv4_address_in_cidr` | IPv4 address is in a CIDR block | `column`, `cidr_block` | — | Not needed |
| `is_ipv6_address_in_cidr` | IPv6 address is in a CIDR block | `column`, `cidr_block` | — | Not needed |
| `is_valid_json` | text is valid JSON | `column` | JSON payload columns, if any | Later |
| `has_json_keys` | JSON object has the given keys | `column`, `keys`, `require_all` | — | Later |
| `has_valid_json_schema` | JSON matches a schema | `column`, `schema` | — | Later |

### 2.8 Dataset-level checks

These look at more than one row, or at another table. They are not row-level, so they need the whole table (section 4).

| Check | What it tests | Key arguments | Where we use it | Status |
|---|---|---|---|---|
| `is_unique` | the column, or combination of columns, has no duplicates | `columns`, `nulls_distinct` (default true) | primary keys on every table | Recommended |
| `foreign_key` | each value exists in another table's column (or does not, with `negate`) | `columns`, `ref_columns`, `ref_table`, `negate`, `null_safe` | `position.owner_id` → `owner`; `transaction.position_id` → `position` | Recommended |
| `compare_datasets` | two tables agree row by row, and reports what differs | `columns`, `ref_columns`, `ref_table`, `check_missing_records`, `exclude_columns`, tolerances | reconciliation of Silver against Bronze or a source extract | Later |
| `aggr_matches_dataset` | an aggregate (count, sum, …) matches the same aggregate in another table | `column`, `aggr_type`, `ref_table`, `ref_column`, `group_by`, tolerances | row counts and totals between layers | Later |
| `is_aggr_not_greater_than` | an aggregate is at most a limit | `column`, `limit`, `aggr_type`, `group_by` | totals that must stay under a cap | Later |
| `is_aggr_not_less_than` | an aggregate is at least a limit | `column`, `limit`, `aggr_type`, `group_by` | minimum row counts | Later |
| `is_aggr_equal` | an aggregate equals a value | `column`, `limit`, `aggr_type`, `group_by`, tolerances | — | Later |
| `is_aggr_not_equal` | an aggregate does not equal a value | `column`, `limit`, `aggr_type`, `group_by`, tolerances | — | Later |
| `has_no_outliers` | values are not extreme outliers in the column | `column` | amounts and metrics | Later |
| `has_no_aggr_outliers` | an aggregate over time is not a sigma outlier | `column`, `time_column` | daily totals | Later |
| `is_data_fresh_per_time_window` | records arrive at least every X minutes | `column`, `window_minutes`, `min_records_per_window`, `lookback_windows` | feed freshness | Later |
| `has_no_gaps_per_time_window` | no time window is empty between windows that have data | `column`, `window_minutes`, `group_by`, `trailing_gap` | feed gaps | Later |
| `has_valid_schema` | the table has the expected columns and types | `expected_schema` or `ref_table`, `strict`, `exclude_columns` | schema contract | Later |
| `sql_query` | a custom SQL query returns the rows that fail | `query`, `merge_columns`, `msg`, `name`, `negate` | rules that need a join or a subquery | Later |

## 3. Recommended checks for each table

The starting point is what is already in the rules files: a not-null or not-empty check on every required column, and the business rules for `orders`. These are the next additions, in priority order:

1. **Primary keys unique** (`is_unique`) on every table. This is the largest gap: a duplicate key can currently get into Silver or Gold.
2. **Parent keys exist** (`foreign_key`):
   - `position.owner_id` → `owner.entity_master_owner_id`
   - `transaction.position_id` → `position.entity_master_position_id`
   - Only add these once the parent-first load order is confirmed (see section 4).
3. **Allowed values** (`is_in_list` or `is_not_null_and_is_in_list`) on status and category columns. The allowed values must come from the table owners.
4. **Currency codes** (`is_valid_currency_code`) on `currency` in `asset` and `transaction`.
5. **Ranges and signs** (`is_in_range`, `is_not_less_than`) on amounts and metric values, once the owners give the valid range.
6. **Future dates** (`is_not_in_future`) on trade and received dates.
7. **Text patterns** (`regex_match`) on IDs that have a fixed format.

## 4. Caveats for dataset-level checks

- **They need the whole table, not one batch.** On an incremental run the gate sees only the new changes. A uniqueness or total check on that batch gives wrong results. Run these checks on the full target table, as a separate step after each load.
- **Quarantine changes the totals.** A quarantined row is not in the target, so counts in Silver will be lower than in Bronze. `aggr_matches_dataset` and `compare_datasets` will report that difference. To reconcile correctly, compare Silver plus quarantine with Bronze.
- **Load order matters for `foreign_key`.** A child row that arrives before its parent fails. It goes to quarantine and is not retried automatically. So parents must load first.
- **Reference tables must be readable by the job.** `ref_table` names a full table, such as `rls_testing.e2e_medallion_metadata_v2_silver.owner`. The job needs access to that table in Unity Catalog. The schema name is written into the YAML, so a namespace change means editing those files.
- **Version note.** The DQX documentation website lists some geospatial checks (for example `is_geo_within`) that are not in the v0.16.0 source. Check the source before using a check that is not in this guide.

## 5. Open decisions

- **Which checks each owner signs off**, especially the allowed values and ranges.
- **Whether a child row with a missing parent should be quarantined**, as the gate does now, or held until the parent arrives.
- **Where reconciliation runs** (`compare_datasets`, `aggr_matches_dataset`): as a separate job after each load, or as a scheduled check.

## 6. Source references

- Check definitions: `databricks/labs/dqx/check_funcs.py` in the DQX repository, tag `v0.16.0`
- Our rules: `config/dq_rules/bronze_to_silver.yml`, `config/dq_rules/silver_to_gold.yml`
- The gate: `common/dq.py`
