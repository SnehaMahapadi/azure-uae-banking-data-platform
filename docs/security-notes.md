# Security notes

Written 6 November 2026 from what was checked in the Azure portal during the build. Anything not checked is marked **unverified**. This is a learning build on a free-trial subscription, not a production design.

## Who can access what

| Resource | Identity | Access | How it was checked |
|---|---|---|---|
| Subscription | Project owner (user) | Owner | Storage IAM page (inherited) |
| Storage account `stuaebankdev417` | Data Factory managed identity `adf-uae-banking-dev` | Storage Blob Data Contributor | IAM role assignments, 6 Nov 2026 |
| Storage account `stuaebankdev417` | Databricks access connector `ac-uae-banking-dev` (managed identity) | Storage Blob Data Contributor | IAM role assignments, 6 Nov 2026 |
| Storage account `stuaebankdev417` | Project owner (user) | Storage Blob Data Contributor | IAM role assignments, 6 Nov 2026 |
| Databricks workspace `dbw-uae-banking-dev` | Data Factory managed identity | Contributor | Needed for the ADF to Databricks linked service to work; confirmed by a successful run |
| Unity Catalog external locations | Data Factory principal | `loc_gold`: READ FILES and WRITE FILES; `loc_silver`: READ FILES | Grants screen |
| Azure SQL database `sqldb-banking-serving` | Data Factory managed identity | `db_ddladmin`, `db_datareader`, `db_datawriter` | Granted with T-SQL during Phase 4 |

Databricks reads the lake through Unity Catalog: storage credential `cred_uae_banking` (backed by the access connector) and the external locations `loc_bronze`, `loc_silver`, `loc_gold`. No storage keys are used by the notebooks.

## Secrets

- No passwords, account keys, SAS tokens or personal access tokens are in the notebooks, the Data Factory JSON or the repo. The linked services use managed identity.
- A search of tracked files for `password`, `accountkey`, `sharedaccesssignature`, `accesstoken`, `dapi` and `gmail` (5 Nov 2026) matched only documentation text that says not to commit secrets. The exported notebooks were scanned the same way on 6 Nov 2026 and returned nothing.
- **Key Vault was not used.** There is no secret to store, because every connection uses an identity. Adding a vault with nothing in it would not demonstrate anything. If a secret ever became necessary, it would go into Key Vault and be read through managed identity.
- Notebooks used by pipelines live under `/Shared/...` so that no personal email address appears in pipeline JSON.
- Whether the Power BI file stores any credential is **unverified**. Power BI connects with an interactive Microsoft Entra sign-in.

## Network exposure

- The storage account has public network access enabled from all networks, with no network security perimeter and no firewall rules in effect (checked 6 Nov 2026). Access is controlled by Entra ID role assignments only.
- Whether shared-key access to the storage account is disabled is **unverified**.
- This was left open on purpose. The classic Databricks cluster and the Data Factory auto-resolve runtime reach storage over public endpoints. Closing it properly needs private endpoints or VNet injection, which is beyond this project's scope and credit.
- The SQL server also has a public endpoint. On 6 Nov 2026 it had 8 client-IP firewall rules created from the portal (one `ClientIp` rule and seven `QueryEditorClientIPAddress` rules), and the exception "Allow Azure services and resources to access this server" was ticked, which Data Factory relies on. The seven older rules were removed the same day, leaving only the current client address. The addresses are not recorded here.
- The SQL server accepts SQL authentication as well as Microsoft Entra: the setting "Support only Microsoft Entra authentication for this server" is switched off (checked 6 Nov 2026), and a Microsoft Entra admin is set. Which accounts actually sign in with SQL authentication was not checked. A production setup would turn Entra-only authentication on.

## Known trade-offs

- **Contributor on the Databricks workspace** is broader than the job needs. A narrower custom role was not tested.
- The database holds both the `ctl` control tables and the serving tables. A production design would separate them.
- The project owner holds Owner and a data role at the same time. A production design would separate duties.

## What I would change for production

1. Private endpoints and disabled public access on storage and SQL.
2. Disable shared-key access on the storage account.
3. Least-privilege custom roles for Data Factory and Databricks.
4. Key Vault for any secret, read by managed identity.
5. Separate subscriptions or resource groups for dev and prod, with just-in-time elevation for owners.

## Clean-up

The whole environment is deleted with the resource group `rg-uae-banking-dev` before the credit expires on 10 November 2026.
