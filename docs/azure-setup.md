# Azure setup guide (Phase 0)

Follow this once. Use **one region** for every resource and record your real names in the table at the bottom.

> Azure resource names are mostly permanent. If you already created something with a different name, keep it and write the real name in the table. Do not delete and recreate just to match this page.

## 0. Before you start

- Sign in to https://portal.azure.com with the account that owns the subscription (the one that shows your free-trial credit). A second user in the same directory sees no subscription until it is given a role.
- Check **Subscriptions** shows an active subscription and **Cost Management > Credits** (or the credits banner) shows your balance and expiry date. Write the expiry date in the table below.
- Free-trial subscriptions have limits (for example, Databricks core quota). If something fails with a quota or "not available" message, record the exact message and stop. Don't keep retrying.

## 1. Naming convention

Pattern: `<type>-<workload>-<env>[-<suffix>]`

| Resource | Proposed name | Notes |
|---|---|---|
| Resource group | `rg-uae-banking-dev` | Everything lives here, so one delete cleans up |
| Storage account | `stuaebankdev<3 digits>` | 3 to 24 characters, lowercase letters and digits only, globally unique |
| Data Factory | `adf-uae-banking-dev` | |
| Databricks workspace | `dbw-uae-banking-dev` | |
| SQL server | `sql-uae-banking-dev-<suffix>` | Globally unique |
| SQL database | `sqldb-banking-serving` | Use the free offer (see step 6) |
| Key Vault | `kv-uae-bank-dev-<suffix>` | 3 to 24 characters, globally unique |

Tags on every resource (set on the resource group and apply to resources if offered):

| Tag | Value |
|---|---|
| project | uae-banking |
| env | dev |
| owner | sneha |
| delete-by | the day before your credit expires |

## 2. Resource group

1. Search **Resource groups**, then **Create**.
2. Subscription: your free-trial subscription. Name: `rg-uae-banking-dev`. Pick your region.
3. Add the tags above. **Review + create**.

## 3. Budget alert (do this before creating anything that costs money)

1. Search **Cost Management**, then **Budgets**, then **Add**.
2. Scope: the subscription (or the resource group). Amount: a figure well under your remaining credit that you'd be uncomfortable exceeding (for example ₹10,000).
3. Add alert conditions at 50%, 80% and 100% with your email.
4. If your subscription type doesn't offer budgets, note that in the table and instead check **Credits** at the start and end of every working session.

## 4. Storage account (the data lake)

1. **Create a resource**, then **Storage account**. Resource group `rg-uae-banking-dev`, your name, same region.
2. Performance: Standard. Redundancy: **LRS** (cheapest, fine for a learning project).
3. **Advanced** tab: tick **Enable hierarchical namespace**. This is what makes it ADLS Gen2, and it can't be switched on later without a migration.
4. Leave public network access as it is for now. Review + create.
5. Open the account, then **Data storage > Containers**, and create four containers: `landing`, `bronze`, `silver`, `gold`.
6. Give yourself data access: **Access control (IAM) > Add role assignment > Storage Blob Data Contributor > your user**. Owner on the subscription does not by itself grant data-plane access.

## 5. Upload the first data

1. Generate the data (see the README) and open the `landing` container.
2. Upload the `raw` folder so the structure is `landing/raw/customers/...`, `landing/raw/transactions/2026/01/...` and so on. Storage Explorer in the portal, or the Azure Storage Explorer desktop app, both work for folders.
3. For Phase 1, upload only **January** transactions plus the reference files (customers, accounts, merchants, FX). The remaining months arrive in Phase 3 to practise incremental loads.
4. Screenshot the container into `docs/screenshots/phase0-landing.png`.

## 6. Azure SQL Database (free offer)

1. **Create a resource**, then **Azure SQL**, then **SQL databases**, **Single database**.
2. Look for the banner or option to **apply the free offer** and accept it. Check the summary page says the database is free before you click Create. If no free option appears for your subscription or region, stop and check the exact message before going on. Don't continue with a paid tier by accident.
3. Create a new server. Authentication: use Microsoft Entra authentication if offered, otherwise SQL authentication with a strong password stored in a password manager. **Never put it in Git.**
4. Networking: allow Azure services, and add your own client IP only.
5. In the database settings, choose the **auto-pause** behaviour if offered.

## 7. Data Factory

1. **Create a resource**, then **Data Factory (V2)**. Resource group, name `adf-uae-banking-dev`, same region.
2. On the Git tab, choose **configure later** for now (Phase 1 connects it to this GitHub repo).
3. After creation, open **Launch studio**.
4. Give ADF access to the lake: storage account > **Access control (IAM) > Add role assignment > Storage Blob Data Contributor > Managed identity > your Data Factory**.

## 8. Databricks workspace

1. **Create a resource**, then **Azure Databricks**. Resource group, name `dbw-uae-banking-dev`, same region.
2. Pricing tier: pick the one the portal offers for your subscription and write it down.
3. Free-trial subscriptions have a small core quota. Plan for **one single-node cluster** with **terminate after 10 to 15 minutes of inactivity**. If cluster creation fails on quota, record the exact message. Databricks Free Edition (separate, non-commercial, not in your Azure subscription) is the fallback for the Spark phases and is described in [`build-plan.md`](build-plan.md).

## 9. Key Vault (Phase 5)

Created later. Listed here so the names stay consistent.

## 10. Environment record (fill this in)

| Item | Value |
|---|---|
| Subscription name |Azure subscription 1|
| Credit balance at start | ₹19,197.88 |
| Credit expiry date | 10 November 2026 |
| Region |East US (SQL server in Central India, because East US was refused for this subscription)|
| Resource group | rg-uae-banking-dev|
| Storage account |stuaebankdev417 (LRS, hierarchical namespace on)|
| ADF name |adf-uae-banking-dev|
| Databricks workspace and tier |not created yet|
| SQL server and database |sql-uae-banking-dev-417 / sqldb-banking-serving (free offer)|
| Budget alert created (yes/no, amount) |Yes, ₹10,000 (50%, 80%, 100%)|
| Date Phase 0 completed |leave blank until Databricks is done|

Do not put passwords, keys, tokens or connection strings in this table.

## Check that Phase 0 is done

- [ ] Resource group exists with tags
- [ ] Budget alert exists, or daily credit checks are noted
- [ ] Storage account has hierarchical namespace on, and four containers
- [ ] You can see files in `landing` from the portal
- [ ] Azure SQL Database shows as free
- [ ] ADF launches and its identity has the storage role
- [ ] Databricks workspace opens (cluster creation can wait for Phase 2)
- [ ] Screenshots saved under `docs/screenshots/`
