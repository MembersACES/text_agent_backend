# Climate integration — deploy & test checklist

Use this after deploying **text_agent_backend** and **text_agent_interface** with the Wave 0.5 climate changes.

## Services to deploy

| Service | Cloud Run name (typical) | Required? |
|---------|--------------------------|-----------|
| Backend | `text-agent-backend` | **Yes** — APIs, SQL tables, webhook, ETL |
| Interface | `acesagentinterface` | **Yes** — Climate tab + ETL sync UI |
| Prograde platform | `prograde-climate-dev` | Optional — iframe only (defaults to dev URL) |
| B4 (`aces-climate-api`) | — | **No** — scaffold only; not in this test path |

## Backend env vars

| Variable | Required | Purpose |
|----------|----------|---------|
| `DB_TYPE` | `postgresql` | Cloud SQL |
| `DATABASE_URL` | ✓ | Same CRM DB; creates `climate_*` tables on startup |
| `AIRTABLE_API_KEY` | ✓ for ETL | Invoice row fetch |
| `USE_AIRTABLE_DIRECT` | `true` | Direct Airtable mode |
| `PROGRADE_WEBHOOK_SECRET` | ✓ for drift test | HMAC verify on `/api/webhooks/prograde-drift` |
| `CLIMATE_ROSTER_SERVICE_KEY` | ✓ for Prograde launcher | Shared with `ACES_ROSTER_SERVICE_KEY` on `prograde-sustainability-dev` |

```powershell
gcloud run services update text-agent-backend `
  --region australia-southeast2 `
  --update-env-vars PROGRADE_WEBHOOK_SECRET=whsec_your_shared_secret
```

Tables created automatically on first startup after deploy:

- `climate_drift_events`
- `climate_activity_records`
- `climate_ingest_runs`

## Interface env (Climate iframe only — **not** backend)

`NEXT_PUBLIC_SUSTAINABILITY_PLATFORM_URL` is **interface only**. The backend does not use it.

**Cloud Run:** baked in at **Docker build** via `cloudbuild.yaml` + `Dockerfile` (runtime env on Cloud Run is too late for `NEXT_PUBLIC_*`).

**Local:** `.env.local`:

```bash
NEXT_PUBLIC_SUSTAINABILITY_PLATFORM_URL=https://prograde-sustainability-dev-63gwbzzcdq-km.a.run.app
```

Current dev Prograde service: `prograde-sustainability-dev` → URL above (`/ready` must return `ok`).

---

## Test plan

### 1. Climate tab loads

1. Sign in to deployed interface.
2. Open **Member Profile** → **Climate** tab (`?tab=climate`).
3. Expect: management-grade badge, drift status, reporting entity panel, ETL card, staged records, optional iframe.

### 2. Reporting entity + iframe

1. **Strategy & WIP** → set `reporting_entity` (e.g. `parramatta-leagues-club`).
2. Reload Climate tab → entity shown, AASB S2 iframe loads.

**Pilot:** Parramatta — NMI `NEEE001316`, LOA `recafZHjICWMdueoo`.

### 3. ETL sync (UI)

On Climate tab, **Sync from Airtable** card:

1. Choose utility type (e.g. `C&I Electricity`).
2. Enter identifier (NMI/MRIN) — auto-filled from Utilities if business info loaded.
3. **Preview (dry run)** — no DB writes; check toast summary.
4. **Sync to SQL** — writes `climate_activity_records`.
5. Staged records list should refresh.

### 4. Drift webhook

Point PC3 mock `fire_drift` at:

```
POST https://<backend-host>/api/webhooks/prograde-drift
```

Secret must match `PROGRADE_WEBHOOK_SECRET`.

Or run locally:

```powershell
cd "c:\My Projects\sustainability_reporting\PROGRA_2\MORGAN_RC2_BUNDLE\scripts"
.\smoke_pc3.ps1
```

**Expect:** `200` → Climate tab shows drift badge + event list.

### 5. API reference (if debugging)

| Method | Path | Auth |
|--------|------|------|
| POST | `/api/webhooks/prograde-drift` | HMAC header |
| GET | `/api/climate/drift-events` | Google Bearer |
| GET | `/api/clients/{id}/climate/drift-events` | Google Bearer |
| GET | `/api/clients/{id}/climate/activity-records` | Google Bearer |
| POST | `/api/clients/{id}/climate/etl/sync` | Google Bearer |

ETL body:

```json
{
  "utility_type": "C&I Electricity",
  "identifier": "NEEE001316",
  "reporting_period_label": "FY26",
  "max_records": 100,
  "dry_run": false
}
```

---

## Common failures

| Symptom | Fix |
|---------|-----|
| `503 PROGRADE_WEBHOOK_SECRET not configured` | Set env on backend Cloud Run |
| `400 no reporting_entity` | Strategy tab → save A1 slug |
| `503 Airtable integration is not configured` | `AIRTABLE_API_KEY` + `USE_AIRTABLE_DIRECT=true` |
| ETL preview: all skipped | Invoice rows missing kWh/GJ columns for that identifier |
| Empty drift on tab but webhook OK | `reporting_entity` must match event `affected.entity_ids` or scope is `global` |
| Iframe blank | Check `NEXT_PUBLIC_SUSTAINABILITY_PLATFORM_URL` / entity exists on Prograde dev |

---

## Not in scope for this deploy

- B4 full ingest (`aces-climate-api` POST `/api/climate/activity`)
- n8n GHG WIP cutover
- Drift acknowledge button
- Post-Tuesday PC1 workbook emit
