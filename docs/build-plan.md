# Build plan

Window: **17 October to 9 November 2026** (24 days), because the Azure credit expires on 10 November 2026. Day numbers assume you work most days for about 2 to 3 hours. If you work less, drop the stretch items first (marked **stretch**), never the export step.

Tick a phase only when its "Done when" list is true and the evidence is committed.

## How the phases fit the timeline

| Phase | Dates (target) | Focus |
|---|---|---|
| 0 | 16 to 17 Oct | Foundations |
| 1 | 18 to 19 Oct | Data Factory ingestion |
| 2 | 20 to 23 Oct | Bronze and silver in Databricks |
| 3 | 24 to 27 Oct | Incremental loads and SCD2 |
| 4 | 28 to 31 Oct | Gold star schema, Azure SQL, Power BI |
| 5 | 1 to 3 Nov | Orchestration, security, secrets |
| 6 | 4 to 6 Nov | Real-time branch on Azure Data Explorer |
| 7 | 7 to 9 Nov | Monitoring, documentation, export, tear-down |

## Phase 0: Foundations

Follow [`azure-setup.md`](azure-setup.md).

Done when: resource group, budget alert, ADLS Gen2 account with four containers, January data in `landing`, free Azure SQL Database, ADF and Databricks workspaces created.

## Phase 1: Ingestion with Data Factory

1. Connect ADF to this GitHub repo (collaboration branch `main`, root folder `adf`).
2. Create linked services to the storage account (managed identity) and to Azure SQL Database.
3. Create a small **control table** in Azure SQL (`ctl.load_log`: dataset, file name, load date, rows copied, status, run id).
4. Build `pl_ingest_reference_data`: copy customers, accounts, merchants and FX files from `landing` to `bronze/raw/<dataset>/load_date=<date>/`.
5. Build `pl_ingest_transactions` with a `month` parameter: Get Metadata to check the file exists, Copy, then log the result to the control table.
6. Test failure: run it for a month that doesn't exist and confirm the failure is logged.

Done when: both pipelines run from a manual trigger, the files are in `bronze/raw`, the control table has rows, and the pipeline JSON is committed under `adf/`.

## Phase 2: Bronze and silver in Databricks

1. Create a single-node cluster with a short auto-terminate. Record any quota error exactly.
2. Choose and document the method for reading the lake from Databricks (see ADR-002 open question). Don't hard-code keys.
3. Notebook `01_bronze`: read raw files as strings, add `_source_file` and `_ingested_at`, write Delta tables.
4. Notebook `02_silver_transactions`: cast types, parse both date formats, standardise currency, handle missing and negative amounts, remove duplicates, quarantine rejects with a reason.
5. Compare your findings with `docs/dq-answer-key.json`.
6. Flatten merchants (the nested `location` object).

Done when: five bronze tables and cleaned silver tables exist, counts reconcile (bronze minus rejects minus duplicates equals silver), and the notebooks are exported to `databricks/`.

**Fallback if the Azure Databricks quota blocks you:** use Databricks Free Edition (serverless, non-commercial, separate from your Azure subscription) for the Spark work and keep ADF and storage on Azure. Document that clearly in the README. Don't claim the Spark work ran on Azure Databricks if it didn't.

## Phase 3: Incremental loads and SCD2

1. Upload February to September transactions one month at a time.
2. Make the pipeline load only months not yet in the control table (watermark).
3. Silver transactions: `MERGE` on `transaction_id` so a rerun adds nothing twice.
4. Late-arriving rows: show that a late row for an earlier month is picked up.
5. Customers as SCD Type 2 using `customer_changes.csv` (`valid_from`, `valid_to`, `is_current`).

Done when: reruns are proven safe (run the same month twice and show the row count doesn't change), and an SCD2 customer shows two rows with correct dates.

## Phase 4: Gold star schema and Azure SQL serving

1. Build `dim_customer` (SCD2), `dim_account`, `dim_merchant`, `dim_date` and `fact_transactions` in Delta.
2. Convert amounts to AED using the FX table.
3. Load the gold tables into Azure SQL Database. Add indexes and reporting views.
4. Build a Power BI report: spend by category and emirate, decline rate, fraud-flag rate, monthly trend, top merchants.

Done when: the SQL views return correct totals (check one total by hand against silver), and the report screenshots are committed.

## Phase 5: Orchestration, security, secrets

1. Parent pipeline `pl_master_daily`: ingest, then run the Databricks notebooks, then load SQL, then log the result.
2. Failure alert (email or an ADF alert rule).
3. Move any secret into Key Vault and read it from ADF and Databricks. Remove any password from notebooks and pipeline JSON.
4. Review role assignments and write them in `docs/security-notes.md`.
5. **Stretch:** a storage-event trigger that starts the pipeline when a new month file lands.

Done when: one command (a trigger run) refreshes everything end to end, a forced failure sends an alert, and a search of the repo for passwords and keys finds nothing.

## Phase 6: Real-time branch with Azure Data Explorer

1. Write `scripts/generate_payment_events.py` to create a file of payment events (timestamp, card-hash, merchant, amount, status, device, country).
2. Create the free Azure Data Explorer cluster (separate sign-up, no subscription needed) and ingest the events.
3. KQL: events per 5-minute window, decline spike detection, impossible-travel flag (same card in two countries within an hour), top merchants by minute.
4. **Stretch, only after checking current prices:** send the events through Event Hubs.

Done when: three saved KQL queries return sensible results and a screenshot shows the spike detection.

## Phase 7: Monitoring, documentation, export, tear-down

1. Review ADF run history and write a short "common failures" table from what actually went wrong.
2. Complete [`cost-guardrails.md`](cost-guardrails.md) export checklist.
3. Update the README with a real architecture screenshot, actual cost, and lessons learned.
4. Write the project summary for your CV and LinkedIn using only what you built (see [`skills-tracker.md`](skills-tracker.md)).
5. Delete the resource group.

Done when: everything in the export checklist is ticked and the resource group is deleted.

## Risks

| Risk | Mitigation |
|---|---|
| Databricks quota blocks multi-node clusters | Single-node cluster, or Databricks Free Edition fallback |
| Credit runs out early | Budget alert, daily credit check, stop clusters and triggers |
| Credit expires before work is exported | Hard stop on 7 November to export; tear-down on 9 November |
| Time slips | Cut stretch items first, then Phase 6 scope, never Phases 1 to 4 |
| Cost of ADF runs unknown | Measure after the first pipeline run and adjust |

## Session log

Add one line after every session: date, phase, what worked, what broke, credit balance.
