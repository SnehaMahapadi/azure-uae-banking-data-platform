# ADR-003: Azure SQL Database as the serving layer

Status: **Proposed** (confirm in Phase 4)

## Context

Reports need a fast, familiar SQL endpoint over the gold star schema. Many Azure designs use a dedicated warehouse engine for this, which can cost much more than a trial credit comfortably allows (not price-checked here).

## Decision

Load the gold tables into **Azure SQL Database** (using the free offer) and let Power BI read from there. Reporting views and one stored procedure live in `sql/`. The same database also holds the small `ctl` control tables used by Data Factory.

## Why

- The free offer (100,000 vCore-seconds and 32 GB per month) is expected to be enough for about 220,000 transaction rows (to be confirmed by watching usage in Phase 4).
- T-SQL star-schema work is directly relevant to the target roles.
- It keeps the BI half of the project close to the existing Power BI experience.

## Consequences

- It is a row-store database, not a scale-out warehouse. Design choices here don't prove large-scale warehouse tuning.
- The free vCore allowance is limited. Use auto-pause and avoid heavy repeated reloads.
- Using one database for both control tables and serving tables is fine for a learning build. A production design would separate them.

## Alternatives considered

- Synapse dedicated SQL pool: too costly for the credit, and its lifecycle position is unverified.
- Query Delta directly from Power BI: possible, but hides the T-SQL modelling skills this project is meant to show.
