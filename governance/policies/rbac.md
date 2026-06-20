# RBAC Model

## Principals (service accounts)
| Principal | Used by | Grants |
|---|---|---|
| `api`     | FastAPI serving | SELECT on `iceberg.gold.*`, `iceberg.silver.*` |
| `dbt`     | transformations | SELECT on silver/bronze, CREATE/INSERT on gold/silver |
| `ml`      | training scripts | SELECT on gold |
| `dagster` | orchestration | SELECT all, trigger dbt |
| `analyst` | Superset users | SELECT on `iceberg.gold.*` only |

## Roles
- **reader** → SELECT on `gold`
- **curator** → reader + write on `silver`/`gold`
- **admin** → all + schema DDL

## Enforcement points
1. **Trino** — file-based access control (`etc/access-control.properties` +
   rules JSON) maps users→catalogs/schemas/tables. Add in `infra/trino/`.
2. **Iceberg/REST catalog** — namespace-level grants when backed by a catalog
   that supports them (e.g. Polaris/Nessie); the dev REST fixture is open.
3. **API** — OAuth2 bearer tokens; scope claims map to the endpoints above.
   "Capability tokens" limit any future autonomous-agent actions.
