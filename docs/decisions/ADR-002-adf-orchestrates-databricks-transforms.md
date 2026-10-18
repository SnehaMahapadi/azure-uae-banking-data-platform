# ADR-002: Data Factory orchestrates, Databricks transforms

Status: **Proposed** (confirm in Phases 1 and 2)

## Context

Both Azure Data Factory and Azure Databricks can move and transform data. The project needs one clear split of responsibilities, and it must work within the limits of a free-trial subscription.

## Decision

- **Azure Data Factory** moves files, controls the order of steps, keeps the run log and sends alerts.
- **Azure Databricks** does all cleaning, deduplication, SCD2 and modelling in PySpark and Delta.
- ADF mapping data flows are **not** used for the core pipeline.

## Why

- This is the most common split in job descriptions for Azure data engineers.
- Business logic stays in version-controlled notebooks that can be read and tested, not hidden in a visual designer.
- ADF's strength is scheduling, parameters, retries and monitoring.

## Open question to settle in Phase 2

How Databricks reads the lake. Options include a Unity Catalog external location with a managed identity, a service principal with a secret kept in Key Vault, or an access key. Prefer the identity-based options. Don't put a key in a notebook or in Git. Record the choice and the reason here once tested.

## Consequences

- Two tools to learn and to debug across.
- If Azure Databricks quota blocks cluster creation on the trial subscription, the Spark work moves to Databricks Free Edition and this ADR must be updated to say so.

## Alternatives considered

- All transformations in ADF data flows: simpler to start, harder to test and review.
- Synapse pipelines and Spark: an option for existing Azure estates; its current lifecycle position hasn't been verified from Microsoft documentation, so it is not used here.
