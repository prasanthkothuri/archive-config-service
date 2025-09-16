# Retention Policy Service (SQLite Starter)

Minimal FastAPI + SQLite service for connections, warehouses, policies, sources, and rules.

By default the service uses a local SQLite file (`app.db`). Set `DATABASE_URL` to a PostgreSQL
connection string (e.g. `postgresql+psycopg://…`) to run against Postgres instead.

## Quickstart
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# optional: point to a PostgreSQL instance
# export DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/archive_config
uvicorn app.main:app --reload
```
Open docs: http://127.0.0.1:8000/docs

### Seed sample data
```bash
python -m app.seed
```

### Export a source config (for Airflow)
```bash
curl -s http://127.0.0.1:8000/v1/sources/1:export | jq .
```
Returns a JSON object with source name/env, connection/warehouse/policy names, and include/exclude rules grouped by schema.

## API Overview

- Connections
  - POST `/v1/connections` — create (name unique; driver/jdbc_url optional)
  - GET `/v1/connections` — list
  - GET `/v1/connections/{id}` — get by id
  - PATCH `/v1/connections/{id}` — update (name uniqueness enforced)
  - DELETE `/v1/connections/{id}` — delete (blocked if in use by any source)

- Warehouses
  - POST `/v1/warehouses` — create (name unique)
  - GET `/v1/warehouses` — list
  - GET `/v1/warehouses/{id}` — get by id
  - PATCH `/v1/warehouses/{id}` — update (name uniqueness enforced)
  - DELETE `/v1/warehouses/{id}` — delete (blocked if in use by any source)

- Policies
  - POST `/v1/policies` — create
  - GET `/v1/policies` — list
  - GET `/v1/policies/{id}` — get by id
  - PATCH `/v1/policies/{id}` — update (name uniqueness enforced)
  - DELETE `/v1/policies/{id}` — delete (blocked if referenced by any source or rule)

- Sources
  - POST `/v1/sources` — create (validates connection/warehouse/policy IDs)
  - GET `/v1/sources` — list
  - GET `/v1/sources/{id}` — get by id
  - PATCH `/v1/sources/{id}` — update (name uniqueness + FK checks)
  - DELETE `/v1/sources/{id}` — delete (blocked if rules exist)
  - GET `/v1/sources/{source_id}:export` — export Airflow-friendly config

- Rules (scoped to a source)
  - GET `/v1/sources/{source_id}/rules` — list
  - POST `/v1/sources/{source_id}/rules` — create (override_policy requires policy_id)
  - GET `/v1/sources/{source_id}/rules/{rule_id}` — get
  - PATCH `/v1/sources/{source_id}/rules/{rule_id}` — update
  - DELETE `/v1/sources/{source_id}/rules/{rule_id}` — delete

- Glue helper
  - GET `/v1/sources/{source_id}/policy:effective?schema={schema}&table={table}`
    - Returns the effective policy for a specific table and resolved legal_hold.
    - Resolution: table override > schema override > source default.

## Examples

Create a simple policy
```bash
curl -s -X POST http://127.0.0.1:8000/v1/policies \
  -H 'content-type: application/json' \
  -d '{
    "name":"default_6m",
    "retention_value":"6m",
    "rules_json":"{\"spec_version\":\"1\",\"name\":\"default_6m\",\"description\":\"Keep everything 6 months\",\"application\":\"finance_app\"}"
  }' | jq .
```

Update a policy
```bash
curl -s -X PATCH http://127.0.0.1:8000/v1/policies/1 \
  -H 'content-type: application/json' \
  -d '{"retention_value":"12m"}' | jq .
```

Effective policy for a table
```bash
curl -s 'http://127.0.0.1:8000/v1/sources/1/policy:effective?schema=doc_sup_owner&table=feed' | jq .
```
