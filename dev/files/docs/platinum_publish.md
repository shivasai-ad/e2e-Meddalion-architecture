# Platinum publish (ADP-1026)

Platinum is the customer-facing layer. The platform team owns it. It holds the Gold tables that are
approved for customers, with only the columns customers may see, after a data-quality (DQ) gate.

Written for: engineers and reviewers on this repo. Status is as of branch `ADP-1026-platinum-publish`.

## 1. The flow at a glance

```
 Gold schema (squad-owned, many tables)
        |
        |  1. discover   list every table in the Gold schema
        |  2. table gate keep only tables whose Gold tag  table_type = platinum
        |  3. columns    decide each column from its  sensitivity  tag: keep / mask / drop
        |  4. plan       print the plan (dry run stops here)
        |  5. rules      load the DQ rules for each promoted table; no rules = not published
        |  6. handle     build the SELECT: kept columns, masks applied, `treat` columns nulled
        |  7. DQ gate    run the rules on those rows; failing rows -> quarantine
        |  8. publish    passing rows overwrite the Platinum table; copy tags; write audit rows
        v
 Platinum schema (platform-owned)  +  quarantine  +  DQ gate log  +  audit log
```

Nothing in this flow is specific to one table. Any Gold table follows it, as long as it carries the tags and has an
entry in the rules file. Gold is only read, never changed.

## 2. The run, step by step

### Step 0: the job starts
- **What:** the job `E2E Platinum Publish` runs one notebook, `platinum_publish`, on serverless compute with DQX installed.
- **Inputs (widgets):** `source_catalog`, `source_schema` (where Gold is), `target_catalog`, `target_schema` (where Platinum goes),
  `entity` (one table or `ALL`), `dry_run` (`true` = plan only). The catalogs and schemas come from bundle variables in
  `databricks.yml`, fixed at deploy time. `entity` and `dry_run` are job parameters, set per run.
- **Also passed in:** `job_id`, `job_run_id`, `task_run_id`, `task_key`, so the logs can say which run wrote what. On serverless
  the notebook cannot read these from Spark, so the job passes them in.
- **Files:** `resources/jobs/e2e_platinum_publish.yml`, `databricks.yml`, `notebooks/published/platinum_publish.py`, `common/job_context.py`.

### Step 1: discover the Gold tables
- **What:** list every table in `<source_catalog>.<source_schema>` (tables starting with `_`, such as refresh logs, are ignored).
- **How:** a query on `information_schema.tables`.
- **Code:** `platinum.list_tables`, called from `platinum.plan_schema`.

### Step 2: the table gate
- **What:** a table is promoted only if its Gold table tag `table_type` equals `platinum`. Every other table is skipped, and the plan says why.
- **How:** the table's tags are read from `information_schema.table_tags`.
- **Where the tag comes from:** the squad sets `table_type: platinum` in its Gold config (`gold_compliance_metadata.yml`); the Gold
  compliance validator applies it as a Unity Catalog tag.
- **Code:** `platinum.read_table_tags`, then `platinum.build_plan`.

### Step 3: decide each column
- **What:** for a promoted table, every Gold column gets one outcome, from its `sensitivity` tag (and `pii` tag when present):

  | Gold tag | Outcome in Platinum |
  |---|---|
  | `public` | kept as is |
  | `confidential` | kept as is |
  | `internal` | dropped |
  | `restricted` | masked: `****` plus the last 4 characters; a value of 4 characters or fewer becomes `****`; NULL stays NULL |
  | no `sensitivity` tag | dropped (default deny) |
  | `pii = true` (any level) | dropped (the `pii` tag is not in use yet) |

  A table where no column survives is skipped ("tagged for Platinum but no column is allowed out").
- **How:** columns come from `information_schema.columns`, their tags from `information_schema.column_tags`.
- **Code:** `platinum.read_columns`, `platinum.read_column_tags`, `platinum.decide_column`, `platinum.build_plan`.
- **New columns:** the plan is rebuilt on every run, so a column added to Gold is picked up on the next run. If it is tagged
  `public` or `confidential` it flows in with no further approval; if it is untagged or `internal` it stays out.

### Step 4: the plan (and the dry run)
- **What:** the notebook prints a report: for each Gold table, promoted or skipped and why, and for each promoted table which
  columns are kept, masked or left out, and how many DQ rules it has.
- **Dry run:** with `dry_run = true` the job stops here. Nothing is written.
- **Code:** the "Plan" section of `notebooks/published/platinum_publish.py`.

### Step 5: load the DQ rules
- **What:** for each promoted table, the notebook loads its entry from `config/dq_rules/gold_to_platinum.yml`: the `checks` list and the
  optional `treat` map.
- **No rules, no publish:** a promoted table with no entry (or no checks) is not published, and the run fails with a message at the end.
- **Code:** `dq.load_table_rules`, `platinum.load_treat`.

### Step 6: prepare the targets (real run only)
- **What:** create the Platinum schema, `<target_schema>_audit` and `<target_schema>_exception` if they do not exist, and make sure the
  `audit_log` table exists. All are in the Platinum catalog.
- **Code:** `notebooks/published/platinum_publish.py`, `common/audit_log.py`.

### Step 7: publish each promoted table
Each table is processed on its own inside the audit log context, so one table failing does not stop the others.
This is `platinum.publish_table`. It does the following, in order:

1. **Checks before reading any data**
   - Every `treat` column must be a column Platinum keeps.
   - Every column named by a DQ rule must be a column Platinum keeps. The gate runs after the columns are dropped, so a rule on a dropped
     column (for example a key) cannot run. If one is found, the table fails here and nothing is written.
2. **Column handling.** Read the Gold table and build the SELECT: kept columns only, `restricted` columns masked, `treat` columns replaced by
   `CAST(NULL AS <type>)`. (`platinum.select_expr`.)
3. **DQ gate.** Run the rules on that result with DQX (`dq.quality_gate`):
   - Rows are split into passing and failing. A row that fails any check is failing.
   - Failing rows are appended to `<target_catalog>.<target_schema>_exception.<table>_quarantine`, with the failed rules and a run time.
   - One row is written to `dq_gate_log` (layer `platinum`): rows checked, passed, quarantined, and the count per failed rule. A clean run also writes a row.
   - The run does not stop for failing rows.
4. **Empty guard.** If no row passes, the existing Platinum table is left as it is and the table is reported as failed.
5. **Write.** The passing rows overwrite `<target_catalog>.<target_schema>.<table>`. Platinum tables are managed tables, rebuilt in full on every run.
6. **Copy metadata.** Table tags are copied from Gold, except `table_type`, and `promoted_from = <gold table>` is added. For each kept column the Gold
   comment and the `sensitivity` tag are copied, so masks and policies that work by tag can match Platinum too.
7. **Audit.** The audit log context writes one `overwrite` row: success or failure, the row count, and the job and run IDs.

### Step 8: end of run
- If any table failed (no rules, rule on a dropped column, no row passed, or an error), the run fails at the end and lists them.
  Tables that succeeded stay published.

## 3. Which file does what

| File | Role |
|---|---|
| `resources/jobs/e2e_platinum_publish.yml` | The job: one notebook task, serverless, DQX installed, parameters and run IDs passed in |
| `databricks.yml` | Bundle variables: source and target catalog and schema |
| `notebooks/published/platinum_publish.py` | The runner: widgets, plan, dry run, loop over promoted tables, failure summary |
| `common/platinum.py` | The Platinum logic: table gate, column decisions, SELECT building, `publish_table`, rule-column guard |
| `common/dq.py` | The shared DQX gate (`quality_gate`) and rule loading (`load_table_rules`) |
| `common/dq_gate_log.py` | Writes the `dq_gate_log` row for each gate run |
| `common/audit_log.py` | Writes the `audit_log` row for each write |
| `common/job_context.py` | Holds the job and run IDs passed in by the job |
| `config/dq_rules/gold_to_platinum.yml` | The DQ rules and `treat` for each promoted table |

## 4. Writing the rules file for any table

The file does not choose tables or columns. Tags do. It only holds the quality rules and forced-NULL columns.
One entry per promoted table, named exactly like the Gold table:

```yaml
tables:
  <gold_table_name>:
    checks:                       # REQUIRED. DQX checks, all pass/fail
      - name: <unique_rule_name>
        check:
          function: <DQX function>
          arguments:
            column: <a column Platinum keeps>
    treat:                        # OPTIONAL. Force a kept column to NULL
      <column>: "null"
```

Rules of the file:
- Every check is pass/fail. A failing row is quarantined; do not set a criticality.
- A rule may only name a column Platinum keeps (a kept column is public or confidential, or restricted and masked). The job fails before writing if a rule names any other column.
- A `sql_expression` passes when it evaluates to true.
- To confirm a masked column, write a check on the masked value, for example `vendor_iban IS NULL OR left(vendor_iban, 4) = '****'`. The gate does not add this by itself.
- `treat` can only make a column stricter. It cannot add a column.

To add a table: tag the Gold table `table_type: platinum`, make sure every Gold column has a `sensitivity` tag, add an entry with at least a few checks, run a dry run, then a real run.

## 5. What a run produces

For each promoted table `<table>`:

| Object | Location |
|---|---|
| Platinum table | `<target_catalog>.<target_schema>.<table>` (overwritten each run) |
| Quarantine | `<target_catalog>.<target_schema>_exception.<table>_quarantine` (appended each run) |
| DQ gate log | `<target_catalog>.<target_schema>_audit.dq_gate_log` (one row per gate run, `layer = platinum`) |
| Audit log | `<target_catalog>.<target_schema>_audit.audit_log` (one row per write) |

Dev setup today: source `rls_testing.ad_published`, target `dev_platinum_catalog.ad_rls_testing_platinum`. The catalog was created by Terraform in
`adi-databricks-infra-dataplatform` (entry `ad_platinum`, owner group `ad-platinum-engineers`); the Platinum schemas were created by hand as test schemas.
The quarantine, audit and gate log live in the Platinum catalog so the platform team owns them; the Datahub principal should get no access to them.

## 6. What happens when

| Situation | Result |
|---|---|
| Gold table not tagged `table_type = platinum` | Skipped, reason in the plan |
| Tagged, but no column is allowed out | Skipped |
| Column has no `sensitivity` tag | Column dropped |
| Promoted table has no DQ rules | Not published; run fails at the end |
| A rule names a column Platinum does not keep | That table fails before any write; others continue |
| Some rows fail a rule | Those rows go to quarantine; the rest are published |
| No row passes | Existing Platinum table left unchanged; that table is reported failed |
| One table errors | Others still publish; the run fails at the end |
| `dry_run = true` | Plan printed, nothing written |
| Column added to Gold | Picked up on the next run if tagged `public`/`confidential` |
| Table loses its `table_type` tag | Skipped from then on; its old Platinum copy stays (no cleanup yet) |

## 7. Running it

```
databricks bundle deploy -t dev -p dev-adi
databricks bundle run e2e_platinum_publish --params dry_run=true -t dev -p dev-adi   # plan only
databricks bundle run e2e_platinum_publish -t dev -p dev-adi                          # publish all tagged tables
databricks bundle run e2e_platinum_publish --params entity=<table> -t dev -p dev-adi  # one table
```

To point at another source or target, change the bundle variables (or pass `--var`) and redeploy.

## 8. Example run (`asset`, the only tagged table in the dev Gold schema)

This section is an example of the flow above, not part of the design.

On ADI, with commit `765b252` (two gates; the single-gate commit `52fa457` has not been run):
- Tables: `asset` was the only table tagged and the only one in Platinum. The other eight Gold tables were not.
- Columns: 11 of 26 kept (all `public` or `confidential`); no `internal` column in Platinum.
- Data: Gold rows passing the rules, projected to those 11 columns, equalled the Platinum rows, compared both ways.
- Reconciliation: Gold 167 = Platinum 4 + quarantine 163. 163 rows failed because `status` is empty in the Gold test data.
- Tags and logs: Platinum column tags matched Gold; the audit row (4 rows) and gate log (4 passed, 163 quarantined) agreed with the table.

## 9. Not verified, and known gaps

**Not yet run**
- `52fa457` (single gate): compiled and the rule-column guard tested locally, but not deployed or run.
- `treat` / forced NULL and masked-value checks on real data: the example table has no `restricted` column.
- A failing row through the gate on a masked column; the "no rules" and "no row passes" guards; a second run (quarantine grows).

**Known gaps**
- **Keys not checked.** Keys are usually `internal` and dropped (for example `org_id`, the row-filter key, and a primary key), so the gate cannot check that rows have a key or that it is unique. A row without a row-filter key could be published and could not be filtered per customer.
- **Quarantine is append-only.**
- **No atomic swap.** A failure part-way through a write can leave a table with data but no tags.
- **No cleanup.** A table that loses its tag keeps its old Platinum copy.
- **Gold operational tags are copied** (`metadata_hash`, `_compliance_config_hash`) and describe Gold, not Platinum.
- **Rows can look identical.** With the identifying columns dropped, different Gold rows can look the same to a customer.
- **Test data.** The dev Gold source is a dummy copy.

## 10. Open decisions

1. **PII.** Who signs off per column? No `pii` tag exists yet and the OpenFGA compliance review is not done. Today a personal column tagged `confidential` is copied in full.
2. **New columns.** Tagged `public` or `confidential`, they flow in automatically. Accept that, or require approval?
3. **Keys.** Keep the row-filter key and primary key in Platinum, or find another way to check them?
4. **Failing rows.** Vanish from Platinum (current), or keep the last good version?
5. **Masked-value checks.** Written by hand per restricted column (current), or added by the job?
6. **Catalog layout.** One shared Platinum catalog, or one per squad? Confirm with the platform team.
7. **Job identity.** The job runs as a person. Should it run as a service principal?
8. **One job or many.** One job per squad (source and target set at deploy), or one job that loops over squads?
9. **Golden Path.** Which parts go into the squad template (tagging), and which stay in a platform repo (the publish job)?
10. **Overlap with ADP-1073.** Governed tags and policies are being built in parallel; avoid building them twice.

## 11. Datahub (next)

Not started. `datahub-api` code is not available to us. Open questions: who owns it, how it names its tables, whether it expects a fixed column list,
which principal reads Platinum, and how the `gsp_id` row filter (`entity_access_filter`) gets attached to Platinum tables.
