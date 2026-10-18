# ADR-004: Streaming branch on Azure Data Explorer

Status: **Proposed** (confirm in Phase 6)

## Context

Payments have a real-time side: spotting decline spikes and suspicious patterns within minutes. Event Hubs and Stream Analytics are the usual Azure streaming services, but their cost on this trial subscription hasn't been verified.

## Decision

Build the real-time branch on a **free Azure Data Explorer cluster** using KQL. Generate a simulated payment-event file with `scripts/generate_payment_events.py` and ingest it. Add Event Hubs as a **stretch** step only after checking its current price.

## Why

- The free cluster needs a Microsoft account but no Azure subscription or card, so it doesn't draw on the trial credit.
- KQL is a core skill for time-window and anomaly queries, and for Azure Monitor and Log Analytics.
- It lets the project show windowing and spike detection without risking cost.

## Consequences

- Events are ingested from a file, not from a live stream, until Event Hubs is added. The README must say so plainly.
- The free cluster has size and time limits (about 100 GB uncompressed and one year, per Microsoft's free-cluster page). Re-check before relying on it.
- Stream Analytics and Spark Structured Streaming remain concept-level knowledge unless built.

## Alternatives considered

- Event Hubs plus Stream Analytics: closest to a typical production design, but unverified cost.
- Spark Structured Streaming on Databricks: ties up the limited cluster quota.
