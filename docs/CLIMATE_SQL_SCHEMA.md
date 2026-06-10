# Climate SQL schema (ACES-owned)

**Purpose:** Store Prograde integration data in **new tables** linked to `clients.id` — no changes to existing CRM offer/task/strategy tables.

**Database:** Same Cloud SQL / SQLite instance as the CRM (`init_db()` creates tables on startup).

## Tables

### `climate_drift_events`

Inbound Prograde DRIFT_EVENT v1 webhooks (replaces in-memory-only log).

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | VARCHAR(128) UNIQUE | Idempotency key |
| `severity` | VARCHAR(32) | info … critical |
| `affected_scope` | VARCHAR(32) | global, entity, site, … |
| `payload_json` | TEXT | Full signed payload |
| `acknowledged_at` | DATETIME | G1 consultant ack (future) |

**Written by:** `POST /api/webhooks/prograde-drift`  
**Read by:** `GET /api/climate/drift-events`, `GET /api/clients/{id}/climate/drift-events`

### `climate_activity_records`

B4-boundary ActivityRecord v1 JSON staged before Prograde ingest (post-Tuesday).

| Column | Type | Notes |
|--------|------|-------|
| `record_id` | VARCHAR(128) UNIQUE | `act_*` from ETL |
| `client_id` | FK → `clients.id` | CRM link only |
| `entity_id` | VARCHAR(128) | A1 slug |
| `site_id` | VARCHAR(128) | NMI / MRIN |
| `body_json` | TEXT | Full activity_record.v1 |
| `status` | VARCHAR(32) | draft → submitted → locked |
| `source_row_id` | VARCHAR(64) | Airtable invoice `rec…` |

**Written by:** `POST /api/clients/{id}/climate/etl/sync`  
**Read by:** `GET /api/clients/{id}/climate/activity-records`

### `climate_ingest_runs`

ETL batch audit log (utility type, identifier, counts, diagnostics JSON).

## ETL flow

```
Airtable invoice rows
  → services/climate_activity_etl.py (transform)
  → climate_activity_records (upsert)
  → climate_ingest_runs (log)
  → [post-Tuesday] aces-climate-api POST /api/climate/activity → Prograde PC1
```

## What we deliberately did NOT do

- No new columns on `offers`, `strategy_items`, or utility Airtable mirrors in SQL
- No writes to Prograde `platform_live/` from ACES yet
- `clients.reporting_entity` remains the only CRM touchpoint (already migrated)

## Ops

Tables are created automatically via SQLAlchemy `Base.metadata.create_all` on backend startup. No manual migration required for first deploy; Cloud Run restart after deploy is sufficient.
