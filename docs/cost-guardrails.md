# Cost guardrails

The project runs on a free-trial Azure subscription with a fixed credit (₹19,197.88 at the start, expiring **10 November 2026**). Controlling spend is part of the exercise.

## What is verified and what is not

| Item | Status | Source or note |
|---|---|---|
| Free account: USD 200 credit for 30 days, then services are disabled until upgrade to pay-as-you-go | Verified | Microsoft's free-account page. Your own credit shows an expiry of 10 November 2026, so check the Credits page for the real rule on your subscription |
| Credit can't be used on Azure Marketplace items | Verified | Microsoft's free-account page |
| Azure SQL Database free offer: 100,000 vCore-seconds and 32 GB per month, up to 10 databases per subscription | Verified | Microsoft's Azure SQL free-offer documentation |
| Free Azure Data Explorer cluster (about 100 GB, KQL, one year, no card needed) | Verified | Microsoft's free-cluster page |
| Databricks on free-trial subscriptions: very small core quota, no quota increase, single-node cluster as workaround | Partly verified | A Microsoft Q&A answer by a moderator, not official documentation. Test it in your own workspace |
| Data Factory run prices | **Not verified** | The pricing page gave no figures when checked. Measure real cost on the Credits page after your first runs |
| Event Hubs and Stream Analytics prices | **Not verified** | Don't build these until you've checked the current price for your region |
| What happens to data when the credit expires | **Not verified** | Assume everything may become unavailable. Export your work first |

## Rules

1. **Create the budget alert first** ([`azure-setup.md`](azure-setup.md), step 3).
2. **Check the Credits page at the start and end of every session.** Write the balance in your session log.
3. **Databricks clusters:** single node, auto-terminate at 10 to 15 minutes, and terminate manually when you stop. A forgotten cluster is the most likely way to burn credit.
4. **Data Factory:** don't leave a recurring schedule trigger enabled between working sessions. Publish it, test it, then stop the trigger.
5. **Azure SQL Database:** confirm "free offer applied" before creating it. Keep to one free database for serving.
6. **Storage:** LRS, hot tier, and small files only. Don't upload anything beyond the generated dataset.
7. **Don't create** Event Hubs, Stream Analytics, Synapse dedicated pools, Purview or Cosmos DB unless you have checked the current price and decided they are worth the credit.
8. **Tear-down:** on the last working day, export everything (see below), then delete the resource group so nothing keeps billing after the credit ends.

## Session log (fill in)

| Date | Credit at start | Credit at end | What ran | Anything surprising |
|---|---|---|---|---|
| | | | | |

## Before the credit expires: export checklist

Do this at least 2 days before 10 November 2026.

- [ ] ADF pipelines exported (Git integration, or **Export ARM template**) into `adf/`
- [ ] Databricks notebooks exported as `.py` source files into `databricks/`
- [ ] SQL scripts (tables, views, procedures) saved in `sql/`
- [ ] KQL queries saved in `adx/` (the free cluster doesn't use the subscription credit, so it can outlive it, but still save the queries)
- [ ] Screenshots of every working phase in `docs/screenshots/`
- [ ] Power BI report saved (`.pbix` kept outside Git if it contains data, or screenshots only)
- [ ] Final cost summary written in the README
- [ ] Resource group deleted, then Credits page checked for zero running spend
