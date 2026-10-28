# Progress and lessons learned

Status on 28 October 2026. Everything below was run in my own Azure subscription. Items marked "not tested" or "not recorded" are stated as such on purpose.

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| 0 Foundations | Done | Resource group, ADLS Gen2 (hierarchical namespace on), free Azure SQL Database, Data Factory, Databricks workspace |
| 1 Ingestion with Data Factory | Done | Two pipelines, `ctl.load_log` control table, nine monthly transaction files copied, failure path tested |
| 2 Bronze and silver | Done, one deviation (see below) | Five bronze and five silver Delta tables, counts reconciled, injected data quality problems found and matched to the answer key |
| 3 Incremental loads and SCD2 | Done, one gap (see below) | `MERGE` rerun proof, late-arriving row proof, SCD2 customers (5,750 rows, 5,000 current, 750 customers with two rows) |
| 4 Gold, Azure SQL, Power BI | Not started | |
| 5 Orchestration and security | Not started | |
| 6 Real-time on Azure Data Explorer | Not started, optional | |
| 7 Monitoring, export, tear-down | Not started | |

## Deviations and gaps (stated honestly)

- **Phase 2: flag, don't quarantine.** The plan said to quarantine rejected rows into a separate table. In silver I kept every row and added `dq_*` flag columns instead, so nothing is silently deleted. Reconciliation still works: bronze rows minus removed duplicates equals silver rows.
- **Phase 3: the `load` route of the new `pl_ingest_transactions` is not tested.** All nine months were already loaded, so the Switch only took the skip route (January) and the missing-file route (October). The copy and log steps inside the `load` case are the same activities as before, moved inside it.
- **SCD2 start date.** Every customer's first row starts on `2026-01-01`, which is my assumption based on the January snapshot.
- **SCD2 source.** The history table was built from the bronze customers table, not from silver.
- **Log gap.** `rows_copied` in `ctl.load_log` is NULL for the transaction loads, so row counts are checked in Databricks, not from the log.
- **File events** were skipped for the Unity Catalog external locations (see ADR-005).

## Environment record

| Item | Value |
|---|---|
| Credit at start | ₹19,197.88, expires 10 November 2026 |
| Storage account | `stuaebankdev417` (LRS, hierarchical namespace on, East US). Containers: `landing`, `bronze`, `silver`, `gold` |
| Data Factory | `adf-uae-banking-dev`, connected to this GitHub repo (`main`, root folder `/adf`) |
| Databricks workspace | `dbw-uae-banking-dev`, Premium tier, East US |
| Access Connector | `ac-uae-banking-dev` (system-assigned managed identity, Storage Blob Data Contributor on the storage account) |
| Cluster | `cl-single-dev`, single node, `Standard_D4ds_v4` (4 cores), runtime 18 LTS, auto-terminate 10 minutes |
| SQL server and database | `sql-uae-banking-dev-417` (Central India), `sqldb-banking-serving` |
| Region note | The SQL server is in Central India, the rest is in East US. This works but crosses regions. |
| Data Factory region, budget alert, Phase 0 completion date | Not recorded here |

## Cost record (Azure portal, "costs incurred this month")

| Date | Costs incurred |
|---|---|
| 24 Oct, 17:08 | ₹156.77 |
| 25 Oct | ₹273.11 |
| 28 Oct | ₹383.31 |

The "available credits" figure lags behind the real spend.

## Lessons learned

1. **The free serverless SQL database auto-pauses.** The first connection after a pause can time out ("Server provided routing information, but timeout already expired"). Fix: Retry 3 with 90 seconds on the Script activities that write to `ctl.load_log`.
2. **A free-trial subscription can't use the Databricks Trial tier.** The workspace is Premium and bills from credit.
3. **Only one 4-core cluster can run at a time** on this subscription's quota. `Standard_DS3_v2` wasn't available, `Standard_D4ds_v4` was.
4. **Check row counts after every copy.** A copy activity named for the changes file copied the customers file (5,000 rows instead of 750) because the Source tab still had the old file name. The pipeline succeeded, and only a count check caught it.
5. **Data Factory doesn't allow an If Condition inside another If Condition.** The skip-if-loaded logic uses one Switch with three routes (`skip`, `load`, `missing`) instead.
6. **Cutting and pasting activities can drop the arrows between them.** Redraw them and validate.
7. **Debug runs write to the log too.** Every full Debug run added a `SUCCEEDED` row, so count `DISTINCT file_name` and filter on status.
8. **In Data Factory with Git connected, Save commits to `main`.** Run `git pull` locally before committing anything else.
9. **A notebook or cluster restart loses session variables.** Re-run the definitions cell first.
10. **Avoid Databricks serverless on this subscription.** Its price is unverified, so classic single-node compute is used.

## Open items

- Export the Databricks notebooks to `databricks/`.
- Phase 4 onward as listed in [`build-plan.md`](build-plan.md).
- Test the `load` route when a new month is available, or in the Phase 5 master pipeline.
