# ADR-001: ADLS Gen2 with a Delta medallion layout

Status: **Proposed** (confirm in Phase 2 once Delta tables are written)

## Context

The project needs a place to keep raw files forever, clean data in stages, and serve analytics. Source data arrives as CSV and nested JSON. Reruns must not create duplicates, and bad rows must be traceable.

## Decision

Use Azure Data Lake Storage Gen2 (hierarchical namespace on) with four containers: `landing`, `bronze`, `silver`, `gold`. Store bronze, silver and gold as **Delta tables**. Bronze keeps all values as strings plus lineage columns. Cleaning starts in silver.

## Why

- Delta gives transactions, `MERGE` for idempotent loads, history and time travel.
- Keeping bronze raw means any silver result can be rebuilt and any complaint about a number can be traced to the original file.
- Hierarchical namespace gives real folders and folder-level permissions, and works well with Spark.

## Consequences

- Types must be cast deliberately in silver. Dates arrive in two formats and amounts can be missing.
- Delta tables need maintenance (`OPTIMIZE`, `VACUUM`) as they grow. At this data size it's a learning exercise, not a need.
- Hierarchical namespace must be chosen when the storage account is created.

## Alternatives considered

- Plain Parquet without Delta: no `MERGE` or history, so incremental loads are harder.
- Loading straight into a SQL database: loses the raw record and doesn't show lake skills.
