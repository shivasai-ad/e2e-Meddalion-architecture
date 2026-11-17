# Platinum publish (ADP-1026)

Platinum is the customer-facing layer. The platform team owns it. For each approved Gold table it holds a
**view** that shows only the columns customers may see. There is no data filtering and no DQ in Platinum:
the view shows every row of Gold, live.

Written for: engineers and reviewers on this repo.

## 1. The flow

```
Gold schema (squad-owned, many tables)
   |
   |  1. table filter   keep only tables whose Gold tag  table_type = platinum
   |  2. column filter  keep only columns whose Gold tag sensitivity allows it
   |  3. view           first run: create the view   |   later runs: compare metadata_hash, update if changed
   v
Platinum schema (platform-owned): one view per tagged Gold table
```

Gold is only read, never changed.

## 2. What decides what

| Decision | Made by |
|---|---|
| Which tables get a view | The Gold table tag `table_type = platinum` |
| Which columns are in the view | Each column's `sensitivity` tag |
| Whether the view needs rebuilding | The `metadata_hash` tag: Gold's against the Platinum view's |

Column rule (`decide_column` in `common/platinum.py`):

| Sensitivity (Gold tag) | In the Platinum view |
|---|---|
| `public` | yes |
| `confidential` | yes |
| `internal` | no |
| `restricted` | no (not exposed; no masking rule is agreed) |
| no tag, or `pii = true` | no (default deny) |

## 3. The run, step by step

1. **Discover.** List the tables in the Gold schema (tables starting with `_` are ignored).
2. **Table filter.** Read each table's tags. A table without `table_type = platinum` is skipped, and the plan says why.
3. **Column filter.** Read each column's `sensitivity` tag and decide include or omit. A table with no column left is skipped.
4. **Decide the action** for each promoted table by comparing the `metadata_hash` tag on the Gold table with the one on
   the Platinum view:

   | Situation | Action |
   |---|---|
   | The view does not exist | **create** |
   | Both hashes are the same | **unchanged**: nothing is written, nothing is logged |
   | The hashes differ | **update**: rebuild the view and copy the tags again |
   | Gold has no `metadata_hash` | **update** on every run, since a change cannot be detected |

5. **Plan.** The notebook prints the plan: for each table, the action, how many columns are in the view, and the Gold hash.
   With `dry_run = true` the job stops here and writes nothing.
6. **Apply** (create and update). Run `CREATE OR REPLACE VIEW <platinum> AS SELECT <kept columns> FROM <gold>`, then copy the tags:
   - table tags: all Gold table tags except `table_type` and the older `_compliance_config_hash`, plus `promoted_from`;
     tags that are no longer on the Gold table are removed from the view;
   - column comments, and each column's `sensitivity` tag.
7. **Audit.** Each create or update writes one row to `audit_log` in `<target_schema>_audit`. An unchanged view writes none.
8. **End of run.** A table that failed is reported and the run carries on. It fails at the end if any table failed, and the
   notebook returns a summary such as `{"views": {"asset": "unchanged"}}`.

## 4. Which file does what

| File | Role |
|---|---|
| `common/platinum.py` | The logic: table gate, column rule, the view statement, the hash decision, create/update |
| `notebooks/published/platinum_publish.py` | The runner: widgets, plan, dry run, loop over tables, summary |
| `resources/jobs/e2e_platinum_publish.yml` | The job and its parameters (`entity`, `dry_run`) |
| `databricks.yml` | Variables: Gold source and Platinum target catalog and schema |
| `common/audit_log.py` | Writes the audit row |

Run it:

```
databricks bundle deploy -t dev -p dev-adi
databricks bundle run e2e_platinum_publish --params dry_run=true -t dev -p dev-adi   # plan only
databricks bundle run e2e_platinum_publish -t dev -p dev-adi                          # create or update the views
databricks bundle run e2e_platinum_publish --params entity=<table> -t dev -p dev-adi  # one table
```

## 5. What happens when

| Situation | Result |
|---|---|
| New rows in Gold | Visible in the view straight away: it reads Gold live |
| A column is added to Gold | Not in the view until the job rebuilds it. The `sensitivity` tag then decides if it is included |
| A Gold tag changes through the Gold pipeline | `metadata_hash` changes, so the next run rebuilds the view and copies the tags |
| A Gold tag is edited by hand in Unity Catalog | `metadata_hash` does not change, so the run sees **unchanged**. The view stays as it was until the hash changes |
| A table loses its `table_type` tag | It is skipped from then on. Its old Platinum view is **not** removed |
| A table is tagged but no column is allowed | Skipped |
| One table fails | The others still run. The run fails at the end |

## 6. Limits and open points

- **Hand edits to tags are not detected.** The hash only changes when the Gold pipeline writes it. A manual change to a
  `sensitivity` tag, for example `public` to `internal`, leaves the old column in the view until the hash changes.
- **No cleanup.** A view whose Gold table is no longer tagged stays and keeps serving data.
- **No data filtering.** Every Gold row is visible, including rows that fail Gold's own rules.
- **`restricted` columns are never shown.** Masking them (last 4 characters) was removed to keep this simple.
- **PII.** Nobody signs off per column yet, and no `pii` tag exists in Gold. A personal column tagged `confidential` is in the view.
- **Live reads.** Each query on a view reads Gold, so cost grows with Gold's size and the number of queries. A view also
  shows Gold as it is during a Gold refresh.
- **Not tested:** reading the view from an account that has access to the view but not to Gold, and attaching row filters or
  masks to a view.
- **Datahub** is not connected yet. `datahub-api` code is not available to us.
