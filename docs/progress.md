# Progress and lessons learned

Status on 28 October 2026. Everything below was run in my own Azure subscription. Items marked "not tested" or "not recorded" are stated as such on purpose.

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| 0 Foundations | Done | Resource group, ADLS Gen2 (hierarchical namespace on), free Azure SQL Database, Data Factory, Databricks workspace |
| 1 Ingestion with Data Factory | Done | Two pipelines, `ctl.load_log` control table, nine monthly transaction files copied, failure path tested |
| 2 Bronze and silver | Done, one deviation (see below) | Five bronze and five silver Delta tables, counts reconciled, injected data quality problems found and matched to the answer key |
| 3 Incremental loads and SCD2 | Done, one gap (see below) | `MERGE` rerun proof, late-arriving row proof, SCD2 customers (5,750 rows, 5,000 current, 750 customers with two rows) |
| 4 Gold, Azure SQL, Power BI | Done, see gaps below | Gold Delta tables (dim_date 273, dim_merchant 602, dim_account 7,835, dim_customer 5,751, fact_transactions 221,759) loaded to Azure SQL with indexes, two views and `gold.usp_monthly_kpi`; AED totals reconciled between silver and gold; Power BI report committed in `PowerBI/` |
| 5 Orchestration and security | Done, one gap (see below) | `pl_master_daily` (reference ingest, transactions ingest, `20_gold_build` notebook, SQL load) ran end to end on 6 Nov 2026 and failed cleanly on three forced missing-month runs; access documented in `security-notes.md` |
| 6 Real-time on Azure Data Explorer | Not started, optional | |
| 7 Monitoring, export, tear-down | In progress | Notebooks exported and committed 6 Nov 2026 |

## Deviations and gaps (stated honestly)

- **Phase 2: flag, don't quarantine.** The plan said to quarantine rejected rows into a separate table. In silver I kept every row and added `dq_*` flag columns instead, so nothing is silently deleted. Reconciliation still works: bronze rows minus removed duplicates equals silver rows.
- **Phase 3: the `load` route of the new `pl_ingest_transactions` is not tested.** All nine months were already loaded, so the Switch only took the skip route (January) and the missing-file route (October). The copy and log steps inside the `load` case are the same activities as before, moved inside it.
- **SCD2 start date.** Every customer's first row starts on `2026-01-01`, which is my assumption based on the January snapshot.
- **SCD2 source.** The history table was built from the bronze customers table, not from silver.
- **Log gap.** `rows_copied` in `ctl.load_log` is NULL for the transaction loads, so row counts are checked in Databricks, not from the log.
- **File events** were skipped for the Unity Catalog external locations (see ADR-005).
- **Phase 5: failure alert not verified.** The Azure Monitor rule `alert-master-failed` exists (metric `PipelineFailedRuns` greater than 0, four pipelines, 5-minute lookback, email action group). The failures were recorded in the metric, but no alert instance fired in three tests on 6 Nov 2026 and no email arrived. The cause was not found.
- **Phase 5: the master pipeline does not run bronze and silver.** It runs ingestion, then the gold build, then the SQL load. A new month would reach bronze/raw but not silver or gold until the bronze and silver notebook cells are run by hand.
- **Phase 5: the master pipeline writes no result row to `ctl.load_log`.** The skip route of `pl_ingest_transactions` logs nothing, so a skipped month leaves no row.
- **Phase 5: Key Vault not used**, because no secret exists (see `security-notes.md`).
- **Phase 4: Power BI totals differ slightly by design.** Report filters exclude merchant emirate Unknown, so the cards differ a little from the monthly chart total (for example 23.56M against 23.63M). A footnote on the report explains it.

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

Later readings of "costs incurred this month" (a new billing month started in November): 4 Nov INR 131.48, 5 Nov (12:43) INR 332.67, 6 Nov (22:13) INR 343.20. "Available credits" still showed INR 19,043.85 on 6 Nov, so it was not updated. The credit expires on 10 November 2026.

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
11. **Data Factory calling Databricks with managed identity needs two things:** Contributor for the Data Factory identity on the workspace, and Unity Catalog grants on the external locations for that identity. The cluster ID has to be typed in, because the dropdown stays empty.
12. **Trigger now runs the published version.** An unpublished pipeline fails with "Pipeline is not found, please publish first". Debug runs do not appear in the alert metrics list.
13. **Azure Monitor alert forms need every dimension filled.** A blank FailureType gave "Dimension list contains nulls or empty dimension values". Creating the action group first failed with a missing subscription registration message.
14. **Check the notebook compute selector before pressing Run.** It can show Serverless, and one 30-second gold run happened on serverless on 4 Nov before it was noticed. The cost is unverified.

## Open items

- Find out why the failure alert does not fire (rule exists, firing not verified).
- Finish README and ADR updates and screenshots, final export by 7 Nov, tear-down by 9 Nov.
- Test the `load` route when a new month is available, or in the Phase 5 master pipeline.
