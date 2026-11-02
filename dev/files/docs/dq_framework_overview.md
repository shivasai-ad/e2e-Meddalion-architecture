# Data Quality Framework

How the platform checks data on its way from Bronze to Silver and from Silver to Gold, what happens to rows that fail, and how failed rows are fixed.

Built on [Databricks Labs DQX](https://databrickslabs.github.io/dqx/docs/guide/).

## 1. What it does

- Every Silver and Gold table has a set of **rules**, written in a YAML file.
- The rules are applied to the data that is about to be written to the target table, **before** it is written.
- Each rule is **pass or fail**. A row that passes every rule continues to the next layer.
- A row that fails any rule is **moved to a quarantine table** with the reason it failed. The load does not stop; the good rows keep flowing.
- Rules are configuration. Adding or changing a rule does not need a code change.

## 2. Where the checks run

```
Source -> Staging -> Bronze -> [ DQ gate ] -> Silver -> [ DQ gate ] -> Gold
                                  rules:                   rules:
                           bronze_to_silver.yml      silver_to_gold.yml
                                  |                          |
                          formatted_exception       published_exception
                          (quarantine tables)       (quarantine tables)
```

- **Bronze** keeps the full history of each record and audits schema changes. It does not apply business rules.
- **Silver gate** applies the Bronze to Silver rules to the data that is about to be written to Silver, before it is written. Rows that fail go to the `formatted_exception` schema. Rows that pass are written to Silver.
- **Gold gate** applies the Silver to Gold rules to the data that is about to be written to Gold, before it is written. Rows that fail go to the `published_exception` schema. Rows that pass are written to Gold.

In both layers the rules are checked against what the target table is going to receive, so a rule can be written for any column that will be stored there.

## 3. How rules are defined

One rules file per layer, one section per table:

```yaml
tables:
  orders:
    checks:
      - name: orders_order_id_not_empty
        check:
          function: is_not_null_and_not_empty
          arguments:
            column: order_id
      - name: status_allowed_values
        check:
          function: is_in_list
          arguments:
            column: status
            allowed: ["'pending'", "'completed'", "'refunded'"]
      - name: amount_positive
        check:
          function: sql_expression
          arguments:
            expression: amount IS NOT NULL AND amount > 0
            msg: amount must be present and greater than 0
            columns: ["amount"]
```

- A table with no entry in the file is not checked, and the run logs that the gate was skipped.
- Every check is forced to pass/fail. A rule that tries to set a different severity is rejected when the file loads, so a failing row can never slip through by mistake.
- Rule files live in Git, so every change is reviewed and versioned.

## 4. What happens to each row

| Row | Result |
|---|---|
| Passes every rule | Continues to the target table |
| Fails any rule | Written to the quarantine table, not to the target |
| A delete (record removed upstream) | Not checked. It only removes a key |

The gate runs in both load types:

- **First-time load:** all current rows of the source are checked, then inserted.
- **Incremental load:** only the rows that changed since the last run are checked, then merged (insert, update or delete).

## 5. The quarantine tables

Silver failures go to the `formatted_exception` schema, and Gold failures to the `published_exception` schema. There is one table per entity, named `<entity>_quarantine`.

Each quarantined row keeps the original values and adds:

- `_errors`: one entry per failed rule, with the rule name, a message, the column(s), the check function, the run id and the time.
- `_dq_label`: which load produced it (for example "incremental (Bronze CDF)").
- `_dq_run_ts`: when it was quarantined.

To see which rules fail most, expand `_errors` and group by rule name.

## 6. The check library

DQX provides built-in checks, and any other condition can be written as a SQL expression:

- Null and empty values
- Allowed values (lists)
- Numeric ranges and limits
- Dates and timestamps (not in the future, valid format, freshness)
- Text format (regular expressions, email, UUID)
- Custom SQL conditions

Checks that compare rows with each other or with another table need the whole table, so they are handled outside the row-level gate.

## 7. Derived and calculated columns

The gate checks the columns that exist on the DataFrame it receives. To protect a calculated column, the calculation is added to the DataFrame **before** the gate, in both the first-time and incremental paths. The rules file can then name the new column like any other.

- Rules on the source columns (for example `amount`) already protect a column calculated from them.
- Rules on the calculated column itself (for example "not null" or "equals 18% of amount") also catch calculation faults.
- A rule that names a column that is not on the DataFrame is skipped by DQX, so rule names and column names must match.

## 8. Fixing quarantined records

| Situation | What to do |
|---|---|
| The source data is wrong | Correct it in the source system or re-load it into Staging. The correction moves through Bronze, Silver and Gold automatically |
| A rule was too strict | Fix the rule in YAML. Because the source did not change, the quarantined rows need a replay (see below) |
| The data is permanently invalid | Mark it rejected and archive it |
| The same bad record keeps arriving | Track it by key, so the quarantine table does not grow with duplicates |

How a corrected record flows:

1. The corrected record is loaded into Staging.
2. Bronze compares it with the current record. If a business column changed, Bronze closes the old version and adds the new one.
3. The next Silver run reads that change, applies the gate again, and merges it if it passes.
4. Gold follows from Silver in the same way.

Things to know:

- A quarantined change is **not retried automatically**. Silver keeps the previous version of that record, or has no row if the key is new, until a corrected version arrives.
- A correction that leaves the business columns unchanged is treated as "no change" and does not flow.
- Fix data through Staging or the source. Editing Silver or Bronze directly bypasses the gate and the history.

## 9. Using the framework in your pipeline

The snippets below show the pattern the pipeline uses. Any team with its own Bronze, Silver and Gold notebooks can copy the module and add the same calls before each write. The framework only needs a Spark DataFrame, a list of checks and a table name for failures. It does not depend on how the DataFrame was built.

### 9.1 Install and pin DQX

Add DQX to the job environment, pinned to one version, and review release notes before upgrading:

```
databricks-labs-dqx==0.16.0
```

### 9.2 The DQ module

The module is one file, `common/dq.py`. Copy it into your shared code as it is. It has four functions. The notebooks call only the last three.

| Function | What it does | Inputs | Returns |
|---|---|---|---|
| `get_engine()` | Creates the DQX engine once. The gate calls it, so you do not | none | the DQX engine |
| `load_table_rules(rules_file, entity)` | Reads one table's checks from the layer's rules file | `rules_file`: path to the YAML. `entity`: table name | a list of checks, or `None` if the table has no entry |
| `scope_checks(checks, scope_filter)` | Limits the checks to rows that match a SQL condition. Other rows pass | `checks`: the list from `load_table_rules`. `scope_filter`: a SQL condition as text | a new list of checks |
| `quality_gate(spark, entity, df, checks, quarantine_fqn, label)` | Runs the checks, quarantines the failing rows, returns the rest | see the table below | a DataFrame of the rows that passed |

The inputs of `quality_gate`, in detail:

| Input | Type | What it is and where it comes from |
|---|---|---|
| `spark` | SparkSession | The Spark session. In a Databricks notebook it is the built-in `spark`. Pass it through |
| `entity` | text | The table name, for example `orders`. It is a key from the layer's configuration: Silver reads the keys under `tables:` in `config/silver_config.yml`, and Gold reads the keys of `config/gold_compliance_metadata.yml`. A notebook widget named `entity` picks one table or `ALL`, and the notebook loops `for entity in ENTITIES`. The same name must be a key under `tables:` in the DQ rules file, or the gate is skipped |
| `df` | DataFrame | The data about to be written to the target. What it holds depends on the load, see the next table. It must contain every column that the rules name |
| `checks` | list of dicts | The rules for this table. Get them with `dq.load_table_rules(rules_file, entity)`. Each item is one DQX check: a `name` and a `check` with a `function` and its `arguments`. To check only some rows, wrap the list in `dq.scope_checks(checks, "<SQL condition>")` |
| `quarantine_fqn` | text | The full name of the table that receives failing rows, as `catalog.schema.table`. Build it from three values: `catalog` from `get_catalog()`, which reads the `catalog` variable in `databricks.yml`; the schema from `get_schemas()`, using the key `formatted_exception` for Silver or `published_exception` for Gold; and the table name `<entity>_quarantine`. Both functions are in `common/config_loader.py` |
| `label` | text | A short description of the load, for example `bootstrap (Bronze)`. It is printed in the log and saved on every quarantined row in `_dq_label` |

What `df` holds in each load:

| Load | `df` is | Built from |
|---|---|---|
| Silver, first-time | All current Bronze rows, as they were at one pinned Bronze version | The Bronze table, filtered to current rows, then selected into Silver's columns plus the audit columns |
| Silver, incremental | The rows to merge into Silver: the changed rows, one per key, selected into Silver's columns plus the audit columns | Bronze's change data feed, read from Silver's watermark to Bronze's latest version |
| Gold, first-time | The finished Gold DataFrame, after any joins and aggregations, with every column that will be stored | Built from the Silver table or tables |
| Gold, incremental | The Gold rows recomputed for the groups that changed in Silver | The Silver tables, limited to the changed groups, then joined and aggregated again |

`rules_file` is the YAML for the layer. Build it from your project root: `config/dq_rules/bronze_to_silver.yml` for Silver and `config/dq_rules/silver_to_gold.yml` for Gold.

Before the first run, the exception schemas must exist. In our project `00_setup_test_environment.py` creates every schema in the schema list, including `formatted_exception` and `published_exception`. The quarantine table itself is created by the gate's write the first time a row fails.

The gate returns a DataFrame of the rows that passed, with the same columns as `df`. Write that DataFrame to the target, not the one you passed in.

### 9.3 Silver: first-time load of a new table

Build the DataFrame that will be written, call the gate, then write what comes back.

```python
SILVER_DQ_RULES = os.path.join(PROJECT_ROOT, "config/dq_rules/bronze_to_silver.yml")

# new_rows is the DataFrame that will be inserted into Silver
# (business columns, renamed audit columns, framework audit columns)
silver_rules = dq.load_table_rules(SILVER_DQ_RULES, entity)
if silver_rules:
    new_rows = dq.quality_gate(
        spark, entity, new_rows, silver_rules,
        quarantine_fqn=f"{catalog}.{schemas['formatted_exception']}.{entity}_quarantine",
        label="bootstrap (Bronze)",
    )
else:
    print(f"No DQ rules for '{entity}', skipping DQ gate")

new_rows.write.format("delta").mode("append").saveAsTable(silver_fqn)
```

### 9.4 Silver: incremental load

The input is the set of changed rows read from Bronze's change data feed, reduced to one row per key. Build the DataFrame that will be merged, then run the gate on it. Deleted rows are not checked, because they only remove a key.

```python
changes = cdf.latest_per_key(changes, primary_keys)

# 1. Build the DataFrame that will be merged into Silver: business columns,
#    renamed audit columns, framework audit columns (and any calculated columns)
source = changes.select(*select_exprs)
for fw_col in framework_audit:
    source = source.withColumn(fw_col, F.lit(atomic_ts).cast(audit_col_types[fw_col]))

# 2. Run the gate on that DataFrame. Deleted rows are not checked
silver_rules = dq.load_table_rules(SILVER_DQ_RULES, entity)
if silver_rules:
    source = dq.quality_gate(
        spark, entity, source,
        dq.scope_checks(silver_rules, "_bronze_is_current = TRUE"),
        quarantine_fqn=f"{catalog}.{schemas['formatted_exception']}.{entity}_quarantine",
        label="incremental (Bronze CDF)",
    )

# 3. Merge only the rows that passed
source.createOrReplaceTempView("_silver_changes")

spark.sql(f"""
    MERGE INTO {silver_fqn} AS t
    USING _silver_changes AS s
    ON {on}
    WHEN MATCHED AND s._bronze_is_current = FALSE THEN DELETE
    WHEN MATCHED AND s._bronze_is_current = TRUE THEN UPDATE SET {update_set}
    WHEN NOT MATCHED AND s._bronze_is_current = TRUE
        THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""")
```

### 9.5 Gold: first-time load of a new table

A Gold table is often built from several Silver tables, joined and aggregated. The gate runs on the **finished Gold DataFrame**, after the join and the aggregation, so the rules can name any column that will be stored, including the aggregated ones. Only the rows that pass are written.

The table and column names below are an example.

```python
GOLD_DQ_RULES = os.path.join(PROJECT_ROOT, "config/dq_rules/silver_to_gold.yml")
entity = "customer_sales"

# 1. Read the Silver tables
orders = spark.table(silver_orders_fqn)
customers = spark.table(silver_customers_fqn)
products = spark.table(silver_products_fqn)

# 2. Join and aggregate. This is the DataFrame that will be written to Gold
gold_df = (
    orders.join(customers, "customer_id")
    .join(products, "product_id")
    .groupBy("customer_id", "region")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("total_amount"),
    )
    .withColumn("_gold_refreshed_at", F.current_timestamp())
)

# 3. Run the gate on the finished Gold DataFrame
dq_rules = dq.load_table_rules(GOLD_DQ_RULES, entity)
if dq_rules:
    gold_df = dq.quality_gate(
        spark, entity, gold_df, dq_rules,
        quarantine_fqn=f"{catalog}.{schemas['published_exception']}.{entity}_quarantine",
        label="bootstrap (Silver join + aggregate)",
    )

# 4. Write only the rows that passed
gold_df.write.mode("append").saveAsTable(gold_fqn)
```

The rules for this table are written against the Gold columns:

```yaml
tables:
  customer_sales:
    checks:
      - name: customer_sales_customer_id_not_null
        check:
          function: is_not_null
          arguments:
            column: customer_id
      - name: customer_sales_order_count_positive
        check:
          function: sql_expression
          arguments:
            expression: order_count > 0
            msg: order_count must be greater than 0
            columns: ["order_count"]
      - name: customer_sales_total_amount_not_negative
        check:
          function: sql_expression
          arguments:
            expression: total_amount IS NOT NULL AND total_amount >= 0
            msg: total_amount must be present and not negative
            columns: ["total_amount"]
```

### 9.6 Gold: incremental load

Recompute the aggregate only for the groups that changed in Silver, run the gate on that result, then merge the rows that passed.

```python
# 1. Find the groups affected by changes in any of the Silver tables.
#    changed_keys is a DataFrame with the group key (here, customer_id).
changed_keys = ...   # your own logic, for example from each Silver table's change data feed

# 2. Recompute the aggregate for those groups only
gold_df = (
    orders.join(changed_keys, "customer_id")
    .join(customers, "customer_id")
    .join(products, "product_id")
    .groupBy("customer_id", "region")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("total_amount"),
    )
    .withColumn("_gold_refreshed_at", F.current_timestamp())
)

# 3. Run the gate on the recomputed Gold rows
dq_rules = dq.load_table_rules(GOLD_DQ_RULES, entity)
if dq_rules:
    gold_df = dq.quality_gate(
        spark, entity, gold_df, dq_rules,
        quarantine_fqn=f"{catalog}.{schemas['published_exception']}.{entity}_quarantine",
        label="incremental (Silver join + aggregate)",
    )

# 4. Merge only the rows that passed, on the group key
gold_df.createOrReplaceTempView("_gold_merge_source")
spark.sql(f"""
    MERGE INTO {gold_fqn} AS t
    USING _gold_merge_source AS s
    ON t.customer_id <=> s.customer_id AND t.region <=> s.region
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

What to know about aggregated Gold tables:

- A quarantined row is a Gold row, one per group, not a Silver row. Keep the group key in the output so a failed row can be traced back to its Silver data.
- If a group fails, Gold keeps the previous value for that group, or has no row for it, until the problem is fixed.
- To fix it, correct the Silver data through Staging or the source. The next run recomputes the group from Silver and the gate checks it again.
- A group whose input rows were all deleted needs its own handling, because the gate only sees rows that exist.

### 9.7 Adding a new table

1. Add the table to the layer's rules file, under `tables:`, using exactly the same name as the entity in your configuration.
2. Write each rule against a column that exists on the DataFrame at the gate. A rule that names a missing column is skipped by DQX.
3. Run the load. The notebook code does not change: it loops over entities and calls the gate for each.
4. Failing rows appear in `<entity>_quarantine` in the exception schema. The table is created on the first failure. The exception schema must already exist.

### 9.8 Adapting it to your platform

- Change the rules file paths, the exception schema names and the label text.
- Keep one call to `quality_gate` before each write to a layer. Place it after the DataFrame is complete, so every column that will be stored can have a rule.
- Use `scope_checks` for any rows that should not be checked, such as deletes.
- Always continue with the DataFrame the gate returns.
- The engine needs a Databricks `WorkspaceClient`, so the code must run with workspace credentials, as it does in a job.

## 10. Governance and monitoring

- Rules are reviewed and versioned in Git.
- Each quarantined row is traceable to the rule that rejected it and the load that produced it.
- DQX adds run-level summary metrics (input, valid and error row counts), alerts (Slack, Teams, webhook) and a dashboard. See the status table below.

## 11. Status

| Capability | Status |
|---|---|
| Rule gate at Bronze to Silver (first-time and incremental) | In the pipeline |
| Rule gate at Silver to Gold (first-time and incremental) | In the pipeline |
| Rules defined per layer and per table in YAML | In the pipeline |
| Quarantine tables with failure reasons | In the pipeline |
| Calculated-column checks before the gate | Designed, not yet built |
| Replay of quarantined rows after a rule or data fix | Planned |
| Status on quarantined rows (open, resolved, rejected) | Planned |
| Run metrics, alerts and a failure threshold | Planned |
| Automatic check that rule column names match the tables | Planned |
