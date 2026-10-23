# ADR-005: How Databricks reads the data lake

Date: 2026-10-22
Status: Accepted (verified by a notebook run on 2026-10-22)

## Context
Databricks (classic compute, single node) must read and write the ADLS Gen2 account.
Secrets must not appear in notebooks or in Git.

## Decision
- Create an Access Connector for Azure Databricks (`ac-uae-banking-dev`) with a system-assigned managed identity.
- Give that identity the role Storage Blob Data Contributor on the storage account.
- In Unity Catalog, create one storage credential (`cred_uae_banking`) and three external locations: `loc_bronze`, `loc_silver`, `loc_gold` (one per container).
- Skip file events (optional). Databricks lists the folder to find files.

## Alternatives not chosen
- Storage account key: a secret that would need a secret scope and rotation.
- SAS token or service principal secret: also secrets, with expiry to manage.

## Consequences
- No keys or passwords anywhere in the repo or notebooks.
- Role is scoped to the whole storage account, which is wider than strictly needed. Acceptable for a dev project; production would scope it to containers.
- File events were not verified: the setup check showed the file-events permissions failing. I created the locations anyway ("Force create") because that feature is optional here. Listing mode may be slower and may cost more at large scale.
- The `landing` container is not registered, because only ADF reads it.

## Evidence
A notebook ran `dbutils.fs.ls` on `bronze/raw/` and returned five folders: accounts, customers, fx_rates, merchants, transactions.



