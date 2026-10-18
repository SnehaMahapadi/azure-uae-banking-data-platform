# Skills tracker

Tick an item only after you have built it **and** can explain it without notes. Add the evidence (file path or screenshot) so every claim on your CV or LinkedIn can be backed up in an interview. Confidence: 1 (shaky) to 5 (could teach it).

This tracker is based on the services and tasks a data engineer is expected to handle on Azure. It is not an exam outline. Microsoft Fabric (the DP-700 certification track) is **not** covered; see the last section.

## Progress

| Area | Items | Done |
|---|---|---|
| A. Azure foundations and security | 7 | 0 |
| B. Storage and lake design | 5 | 0 |
| C. Ingestion and orchestration (ADF) | 9 | 0 |
| D. Transformation (Databricks, Spark, Delta) | 11 | 0 |
| E. Modelling and serving | 6 | 0 |
| F. Streaming and KQL | 5 | 0 |
| G. Monitoring, cost and DevOps | 7 | 0 |
| **Total** | **50** | **0** |

## A. Azure foundations and security (Phase 0, 5)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Explain tenant, subscription, resource group, resource, region | 0 | _/5 | |
| [ ] | Create resources with a naming convention and tags | 0 | _/5 | |
| [ ] | Assign RBAC roles (Storage Blob Data Contributor) | 0 | _/5 | |
| [ ] | Use a managed identity instead of a stored secret | 1 | _/5 | |
| [ ] | Store a secret in Key Vault and read it from ADF | 5 | _/5 | |
| [ ] | Configure SQL firewall rules and Entra authentication | 0 | _/5 | |
| [ ] | Explain what changes for a production network (private endpoints) | 5 | _/5 | |

## B. Storage and lake design (Phase 0, 2)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Create ADLS Gen2 (hierarchical namespace) and explain why | 0 | _/5 | |
| [ ] | Design a landing/bronze/silver/gold container layout | 0 | _/5 | |
| [ ] | Choose file formats (CSV vs JSON vs Parquet vs Delta) with reasons | 2 | _/5 | |
| [ ] | Partition data sensibly (by load date or month) | 1 | _/5 | |
| [ ] | Explain storage redundancy and access tiers | 0 | _/5 | |

## C. Ingestion and orchestration (Phase 1, 3, 5)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Create linked services and datasets | 1 | _/5 | |
| [ ] | Build a parameterised Copy pipeline | 1 | _/5 | |
| [ ] | Use Get Metadata and conditional logic | 1 | _/5 | |
| [ ] | Keep a control/log table for runs | 1 | _/5 | |
| [ ] | Implement a watermark incremental load | 3 | _/5 | |
| [ ] | Run Databricks notebooks from ADF with parameters | 5 | _/5 | |
| [ ] | Schedule with a trigger (and know when to use event triggers) | 5 | _/5 | |
| [ ] | Handle retries, timeouts and failure paths | 5 | _/5 | |
| [ ] | Connect ADF to Git and explain branch/publish flow | 1 | _/5 | |

## D. Transformation: Databricks, Spark, Delta (Phase 2, 3, 4)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Create and size a cluster; control cost (auto-terminate, single node) | 2 | _/5 | |
| [ ] | Read lake data securely from Databricks | 2 | _/5 | |
| [ ] | Read CSV and nested JSON in PySpark with explicit options | 2 | _/5 | |
| [ ] | Write and query Delta tables | 2 | _/5 | |
| [ ] | Clean data: types, dates, currency, missing and negative values | 2 | _/5 | |
| [ ] | Remove duplicates with a window function | 2 | _/5 | |
| [ ] | Quarantine rejected rows with a reason | 2 | _/5 | |
| [ ] | Use Delta `MERGE` for idempotent loads | 3 | _/5 | |
| [ ] | Handle late-arriving data | 3 | _/5 | |
| [ ] | Build SCD Type 2 | 3 | _/5 | |
| [ ] | Use Delta history, time travel and `OPTIMIZE`/`VACUUM` | 4 | _/5 | |

## E. Modelling and serving (Phase 4)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Design a star schema (grain, facts, dimensions) | 4 | _/5 | |
| [ ] | Build a date dimension | 4 | _/5 | |
| [ ] | Convert currencies with an FX table | 4 | _/5 | |
| [ ] | Load Azure SQL Database from the lake | 4 | _/5 | |
| [ ] | Write T-SQL views, indexes and a stored procedure | 4 | _/5 | |
| [ ] | Build a Power BI report on the serving layer | 4 | _/5 | |

## F. Streaming and KQL (Phase 6)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Explain batch vs streaming and when to use each | 6 | _/5 | |
| [ ] | Ingest events into Azure Data Explorer | 6 | _/5 | |
| [ ] | Write KQL: filter, summarize, `bin()` time windows | 6 | _/5 | |
| [ ] | Detect a decline spike and a suspicious pattern | 6 | _/5 | |
| [ ] | Explain Event Hubs and Stream Analytics (built only if cost checked) | 6 | _/5 | |

## G. Monitoring, cost and DevOps (Phase 0, 5, 7)

| Done | Skill | Phase | Confidence | Evidence |
|---|---|---|---|---|
| [ ] | Set a budget alert and track credit use | 0 | _/5 | |
| [ ] | Read ADF run history and diagnose a failed run | 1 | _/5 | |
| [ ] | Create an alert for a failed pipeline | 5 | _/5 | |
| [ ] | Read Databricks job/run logs | 2 | _/5 | |
| [ ] | Commit pipelines and notebooks to Git with clear history | 1 | _/5 | |
| [ ] | Write data-quality checks with row-count reconciliation | 2 | _/5 | |
| [ ] | Document runbook-style "common failures" from real incidents | 7 | _/5 | |

## What this project does not cover

Be clear about these on a CV and in interviews so nothing is overclaimed.

- **Microsoft Fabric items** (Lakehouse, Warehouse, Eventhouse, Eventstream, Dataflow Gen2, OneLake shortcuts and mirroring, Fabric deployment pipelines). The Fabric trial was not available on this account, so none of it was built or tested. The concepts overlap heavily with Databricks, Delta and KQL, which this project does cover.
- **DP-700 exam preparation.** This tracker doesn't map to the exam outline.
- **Azure Synapse Analytics**, **Microsoft Purview**, **Cosmos DB**, **HDInsight**, **infrastructure as code** and **CI/CD release pipelines.** Understand them at concept level; add later if time and credit allow.
- **Production-scale concerns**: real data volumes, private networking, multi-environment promotion, disaster recovery.
