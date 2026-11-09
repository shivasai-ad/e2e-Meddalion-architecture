# Platinum publish (ADP-1026)

Platinum is the customer-facing layer. The platform team owns it. It holds the Gold tables that are
approved for customers, with only the columns customers may see, after a DQ gate.

Written for: engineers and reviewers on this repo. Status is as of the branch `ADP-1026-platinum-publish`.

## 1. Flow

```
Gold table (squad)
  -> select   a Gold table is promoted only if its Gold tag  table_type = platinum
  -> handle   each column is kept, masked, forced to NULL, or dropped by its sensitivity tag
  -> DQ gate  rules in config/dq_rules/gold_to_platinum.yml, on the rows that will be published
  -> publish  passing rows overwrite the Platinum table; failing rows go to quarantine
```

Only the job writes Platinum. Gold is read, never changed.

## 2. What decides what

| Decision | Made by | Where |
|---|---|---|
| Which tables are promoted | Gold tag `table_type = platinum` | Gold config, applied by the validator |
| Which columns are kept | Gold tag `sensitivity` on each column | Gold config |
| How a kept column looks | `sensitivity` (and `pii`, not yet used) | `common/platinum.py` |
| Which rows are published | DQ rules | `config/dq_rules/gold_to_platinum.yml` |
| Which kept columns are forced to NULL | `treat` in the rules file | `config/dq_rules/gold_to_platinum.yml` |

Column rules, in `common/platinum.py` (`decide_column`):

| Sensitivity (Gold tag) | In Platinum |
|---|---|
| `public` | copied as is |
| `confidential` | copied as is |
| `internal` | dropped |
| `restricted` | masked: `****` plus the last 4 characters; a value of 4 characters or fewer becomes `****` |
| no tag, or `pii = true` | dropped (default deny; `pii` is not in use yet) |

`treat: {column: null}` forces a kept column to NULL. It can only make a column stricter.

## 3. The DQ gate

- One gate, after the column handling. Rules use DQX functions and are pass/fail: every failing row is quarantined.
- A rule may only name a column Platinum keeps. The job checks this before writing; if a rule names any other column, the table fails and nothing is written.
- Rules are written by hand, per table, in `config/dq_rules/gold_to_platinum.yml`.
- A promoted table with no rules is not published (the run fails with a message).
- If no row passes, the existing Platinum table is left as it is.

Consequence: keys that Platinum drops (`org_id`, the row-filter key; `asset_id`, the primary key) cannot be checked by this gate. The `asset` rules for them were removed. This is a known gap; see section 7.

## 4. Where things live

| Object | Location |
|---|---|
| Platinum tables | `dev_platinum_catalog.ad_rls_testing_platinum` (test; target is a setting) |
| Quarantine | `dev_platinum_catalog.ad_rls_testing_platinum_exception.<table>_quarantine` |
| Audit log | `dev_platinum_catalog.ad_rls_testing_platinum_audit.audit_log` |
| DQ gate log | `dev_platinum_catalog.ad_rls_testing_platinum_audit.dq_gate_log` (`layer = platinum`) |
| Source (Gold) | `rls_testing.ad_published` (test copy; source is a setting) |

The Platinum catalog was created by Terraform in `adi-databricks-infra-dataplatform` (entry `ad_platinum`, owner group
`ad-platinum-engineers`). The Platinum schemas were created by hand, as test schemas.

## 5. Code map

| File | What it does |
|---|---|
| `common/platinum.py` | Table gate, column rules, SQL for each column, `publish_table`, the column guard |
| `common/dq.py` | Shared DQX gate (`quality_gate`), rule loading (`load_table_rules`) |
| `common/dq_gate_log.py` | Writes one row per gate run |
| `notebooks/published/platinum_publish.py` | The job's notebook: plan, dry run, publish |
| `resources/jobs/e2e_platinum_publish.yml` | The job (serverless, DQX installed) |
| `databricks.yml` | Source and target catalog and schema as variables |
| `config/dq_rules/gold_to_platinum.yml` | DQ rules and `treat` per table |

Run it:

```
databricks bundle deploy -t dev -p dev-adi
databricks bundle run e2e_platinum_publish --params dry_run=true -t dev -p dev-adi   # plan only
databricks bundle run e2e_platinum_publish -t dev -p dev-adi                          # publish
```

## 6. What has been verified

On ADI, with commit `765b252` (two gates; the single-gate commit `52fa457` has not been run):

- Tables: `asset` is the only table tagged `platinum` and the only one in Platinum. The other eight Gold tables are not in Platinum.
- Columns: 11 expected, 11 present; no `internal` column present.
- Data: Gold rows that pass the rules, projected to the 11 columns, equal the Platinum rows (no difference either way).
- Reconciliation: Gold 167 = Platinum 4 + quarantine 163; quarantined `asset_id`s match the failing rows.
- Tags: Platinum column tags match Gold; `promoted_from` is set; `table_type` is absent.
- Logs: the audit row (4 rows) and the gate log (4 passed, 163 quarantined) agree with the table.

## 7. Not verified, and known gaps

**Not yet run**
- `52fa457` (single gate): compiled and the column guard is tested locally, but not deployed or run.
- `treat` / forced NULL, and masked-value checks: `asset` has no `restricted` column, so these have not run on real data.
- A failing row through the gate on a masked column.
- The "no rules" and "no row passes" guards.
- A second run: quarantine appends, so it grows.

**Known gaps**
- **Row-filter key not checked.** `org_id` is dropped from Platinum, so nothing checks that rows have it. A row without it could be published and could not be filtered per customer.
- **Primary key not checked.** `asset_id` is dropped, so uniqueness cannot be checked. The four published `asset` rows look identical to a customer.
- **Quarantine is append-only.**
- **No atomic swap.** A failure part-way through a write can leave a table with data but no tags.
- **No cleanup.** A table that loses its `table_type` tag keeps its old Platinum copy.
- **Gold operational tags are copied** (`metadata_hash`, `_compliance_config_hash`) and describe Gold, not Platinum.
- **Test data.** The Gold source is a dummy copy. 163 of 167 rows have no `status`, so almost every row is quarantined.

## 8. Open decisions

1. **PII.** Who signs off per column? No `pii` tag exists yet; the OpenFGA compliance review is not done. Today a personal column tagged `confidential` is copied in full.
2. **New columns.** Tagged `public` or `confidential`, they flow in automatically. Accept that, or require approval?
3. **Keys.** Keep the row-filter key and primary key in Platinum, or find another way to check them?
4. **Failing rows.** Vanish from Platinum, or keep the last good version? The first is current behaviour.
5. **Masked-value checks.** Written by hand per restricted column, or added automatically by the job?
6. **Catalog layout.** One shared Platinum catalog, or one per squad? Confirm with the platform team.
7. **Job identity.** The job runs as a person. Should it run as a service principal?
8. **One job or many.** One job per squad, or one job that loops over squads?
9. **Golden Path.** Which parts go into the squad template (tagging), and which stay in a platform repo (the publish job)?
10. **Overlap with ADP-1073.** Governed tags and policies are being built in parallel; avoid building them twice.

## 9. Datahub (next)

Not started. `datahub-api` code is not available to us. Open questions: who owns it, how it names its tables,
whether it expects a fixed column list, which principal reads Platinum, and how the `gsp_id` row filter
(`entity_access_filter`) gets attached to Platinum tables.
