# ADR-004: KQL analysis on Azure Data Explorer (batch-loaded, no streaming)

Status: **Accepted, with a change from the original proposal**

## Context

Payments have a real-time side: spotting decline spikes and suspicious patterns within minutes. Event Hubs and Stream Analytics are the usual Azure streaming services, but their cost on the trial subscription was never verified. The original proposal was a free Azure Data Explorer cluster fed by a simulated payment-event file.

## Decision

Use a **free Azure Data Explorer cluster** and KQL, but load the **real transaction CSV files in batch** (nine monthly files, 222,642 rows) instead of a simulated event feed. `scripts/generate_payment_events.py` was never written. Event Hubs was not built and its price was not checked. The work is in [`adx/`](../../adx/).

## Why

- The free cluster needs a Microsoft account but no Azure subscription or card, so it does not draw on the trial credit. It also stayed available after the Azure resource groups were deleted.
- KQL is a core skill for time-window and anomaly queries, and for Azure Monitor and Log Analytics.
- Loading the existing transactions let the analysis reuse data already known to contain realistic problems (duplicates, two timestamp formats, mixed currency codes, missing and negative amounts).

## Consequences

- This is **not streaming**. Data was loaded from files, and the README says so plainly.
- Anomaly detection ran on a daily series (`series_decompose_anomalies`), not on minute-level windows. No impossible-travel or per-minute queries were written.
- The free cluster has size and time limits (about 100 GB uncompressed, one year, no SLA, and Microsoft may change or suspend it, per Microsoft's free-cluster page). The scripts in `adx/` and the screenshots are the lasting record.
- Merchants and accounts were not loaded into ADX, so orphan-key checks and late-arriving-row detection were not done there.
- Stream Analytics, Event Hubs and Spark Structured Streaming remain concept-level knowledge.

## Alternatives considered

- Event Hubs plus Stream Analytics: closest to a typical production design, but unverified cost.
- Spark Structured Streaming on Databricks: ties up the limited cluster quota, and the Azure resources were deleted before this phase.
- A simulated event feed pushed with the Kusto Python SDK: considered as a stretch step, not built.
