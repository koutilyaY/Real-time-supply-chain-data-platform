# Governance, Security & Compliance

This directory holds the platform's governance artifacts. In the vertical slice
these are **policy-as-documentation + config templates**; the enforcement points
are noted so they can be wired in as the platform hardens.

## Layers of control

| Concern | Where enforced | Status in slice |
|---|---|---|
| Authentication | Trino password file / OAuth2; API OAuth2/JWT | documented (`policies/rbac.md`) |
| Authorization (RBAC) | Trino access-control, Iceberg schema grants, API scopes | template |
| Column masking / tokenization | dbt masking macros + Trino column masks | `policies/masking.yml` + `transformations/dbt/macros` |
| PII handling | mask at ingestion (generator emits none); contracts mark sensitive fields | contracts |
| Audit logging | Trino query events, API access logs, Kafka audit topic | documented |
| Data contracts | `ingestion/generator/contracts/*.schema.json` | enforced shape |
| Lineage / catalog | DataHub (see `docs/architecture.md` → Metadata) | roadmap |

## Principles
- **Least privilege:** every service connects with a named, scoped principal
  (`api`, `dbt`, `ml`, `dagster`) rather than a shared root.
- **Secrets:** local dev uses `.env`; production should use a secret manager
  (Vault / cloud KMS) and never commit `.env`.
- **Defense in depth:** network isolation (compose `platform` network), per-layer
  auth, and masking at the query layer.
