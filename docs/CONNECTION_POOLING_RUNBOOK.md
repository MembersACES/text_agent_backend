# Cloud SQL connection pooling — ops runbook

## Problem signature

```
FATAL: remaining connection slots are reserved for non-replication superuser connections
```

All CRM API endpoints return 500 when Cloud SQL `max_connections` is exhausted.

## Connection budget

```
effective_max_backends >= Σ (service_max_instances × app_connections_per_container) + ~3 admin
```

App connections per container = `DB_POOL_SIZE + DB_MAX_OVERFLOW` (SQLAlchemy).

## Services and databases (as of 2026-06-10)

| Cloud Run service | Cloud SQL instance | max-instances | concurrency | App pool (env) |
|-------------------|-------------------|---------------|-------------|----------------|
| `text-agent-backend` (prod) | `aces-tasks-db` | 8 | 20 | `DB_POOL_SIZE=2`, `DB_MAX_OVERFLOW=2` |
| `text-agent-backend-dev` | `aces-tasks-db-dev` | 5 | 20 | `DB_POOL_SIZE=1`, `DB_MAX_OVERFLOW=1` |
| `autonomous-agent-backend-git` | `aces-tasks-db` | 3 | 20 | (separate codebase — audit pool settings) |
| `autonomous-agent-backend-dev` | `aces-tasks-db-dev` | 20 | 80 | secret `DATABASE_URL_AUTONOMOUS_DEV` |

**Dev CRM backend must not use prod DB.** Dev is on `aces-tasks-db-dev`.

## App-side pool (`database.py`)

PostgreSQL only. Env vars:

| Variable | Default | Notes |
|----------|---------|-------|
| `DB_POOL_SIZE` | `1` | Warm connections per container |
| `DB_MAX_OVERFLOW` | `1` | Burst above pool_size |
| `DB_POOL_TIMEOUT` | `30` | Seconds; fail fast on exhaustion |
| `DB_POOL_RECYCLE` | `3600` | Do not lower without stale-connection evidence |

**Pair pool limits with `containerConcurrency`.** Do not use `pool_size=1` with `concurrency=80` unless using a managed pooler.

## Managed Connection Pooling (PgBouncer)

**Status:** Not enabled. Requires **Cloud SQL Enterprise Plus** edition.

Attempted on `aces-tasks-db-dev` (f1-micro):

```
Connection pool is only supported for Enterprise Plus edition.
```

When upgrading to Enterprise Plus:

1. Enable on **dev** first: `gcloud sql instances patch aces-tasks-db-dev --enable-connection-pooling`
2. Point app at pooler port **6432** (direct) or **3307** (Cloud SQL Auth Proxy / Cloud Run connector)
3. Run transaction-mode compatibility checklist (below)
4. Soak 24–48h on dev, then enable on prod

### Transaction-mode compatibility (monolith audit 2026-06-10)

No blockers found in `text_agent_backend`:

- Single module-level `create_engine` in `database.py`
- FastAPI `get_db()` closes sessions in `finally`
- No advisory locks, LISTEN/NOTIFY, or session-persisted GUCs in app code
- `engine.begin()` usage is transaction-scoped (migrations only)

**Still audit** `autonomous-agent-backend-git` separately (different repo).

## Emergency procedures

### 1. Restart Cloud SQL (clears stale connections)

```bash
gcloud sql instances restart aces-tasks-db --project=aces-ai --quiet
```

### 2. Cap Cloud Run scale (stop fan-out)

```bash
gcloud run services update text-agent-backend-dev \
  --project=aces-ai --region=australia-southeast2 \
  --max-instances=5 --concurrency=20

gcloud run services update text-agent-backend \
  --project=aces-ai --region=australia-southeast2 \
  --max-instances=8 --concurrency=20

gcloud run services update autonomous-agent-backend-git \
  --project=aces-ai --region=australia-southeast2 \
  --max-instances=3 --concurrency=20
```

### 3. Inspect live connections (before enabling pooler)

```sql
SELECT application_name, state, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY 3 DESC;
```

## Tier headroom

`db-f1-micro` (~25 connections, RAM-limited) is insufficient for multi-service Cloud Run.

Prod `aces-tasks-db` upgraded to **`db-g1-small`** (~100 connections) as interim headroom until Enterprise Plus + managed pooling.

## Alerts (recommended)

- Log match: `remaining connection slots` → page immediately
- Cloud SQL `num_backends` > 80% of max for 5 min
- Cloud Run 5xx rate > 5% per service (include `autonomous-agent-backend-git`)

## Deploy checklist (new services)

- [ ] Which Cloud SQL instance?
- [ ] `max-instances` × (`DB_POOL_SIZE` + `DB_MAX_OVERFLOW`) documented
- [ ] `containerConcurrency` paired with pool size
- [ ] Transaction-mode compatibility if using managed pooling
