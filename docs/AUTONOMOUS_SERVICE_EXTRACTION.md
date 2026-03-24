# Autonomous agent service — extraction guide

This document describes how to split autonomous follow-up sequences into a **dedicated FastAPI service** (separate repo / deploy) without changing the **HTTP contract** the dashboard and Base 2 already use. Use it as a mechanical checklist.

The **frontend** can point at a split service via `NEXT_PUBLIC_AUTONOMOUS_API_BASE_URL` (see `text_agent_interface` `getAutonomousApiBaseUrl()`).

---

## 1. Goals

- **Isolate** scheduler, n8n/Retell integration, and sequence schema changes from the main CRM `text_agent_backend`.
- **Keep** the existing UI (`/autonomous-agent`, run detail, Base 2 start call) by preserving paths and JSON shapes—or use the dedicated base URL env.
- **Align** with meeting notes: autonomous service owns the sequence object, steps, events, and tick processing.

---

## 2. Proposed new repository layout

```
autonomous-agent-backend/
├── README.md
├── .env.example
├── Dockerfile
├── requirements.txt          # fastapi, uvicorn, sqlalchemy, httpx, pydantic, apscheduler (optional)
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI app, CORS, lifespan (scheduler), mount routers
│   ├── config.py             # pydantic-settings from env
│   ├── database.py           # engine, SessionLocal, init_db (autonomous tables only)
│   ├── dependencies.py       # get_db, get_current_user_* (JWT) or API-key for tick
│   ├── models.py             # AutonomousSequenceRun, Step, Event only (see §6)
│   ├── schemas.py            # AutonomousSequence* Pydantic models (+ to_melbourne_iso)
│   ├── routers/
│   │   └── autonomous.py     # all /api/autonomous/* routes
│   └── services/
│       └── autonomous_sequence.py
├── utils/
│   └── timezone.py           # to_melbourne_iso (copy from text_agent_backend)
└── docs/
    └── OPENAPI.md            # optional: export OpenAPI JSON for n8n/CRM consumers
```

You can flatten `app/` if you prefer a smaller tree; the important part is **one router module** and **one service module** lifted from the monolith.

---

## 3. HTTP API to lift (preserve for UI compatibility)

Keep the same **path prefix** `/api/autonomous` so the Next.js app only swaps the host via `NEXT_PUBLIC_AUTONOMOUS_API_BASE_URL`.

| Method | Path | Auth | Source today (`text_agent_backend/main.py`) |
|--------|------|------|-----------------------------------------------|
| `POST` | `/api/autonomous/sequences/start` | Bearer (`get_current_user_with_db`) | `autonomous_sequence_start` — **queries `Offer`** for existence + `client_id` default |
| `GET` | `/api/autonomous/sequences/runs` | Bearer | `autonomous_sequence_list_runs` — **queries `Offer`** for `business_name` in list items |
| `GET` | `/api/autonomous/sequences/runs/{run_id}` | Bearer | `autonomous_sequence_get_run` — **queries `Offer`** for `business_name` in detail |
| `POST` | `/api/autonomous/sequences/runs/{run_id}/stop` | Bearer | `autonomous_sequence_stop_run` |
| `PATCH` | `/api/autonomous/sequences/runs/{run_id}` | Bearer | `autonomous_sequence_patch_run` |
| `POST` | `/api/autonomous/sequences/inbound` | Header `X-Autonomous-Inbound-Secret` (if env set) | `autonomous_sequence_inbound` |
| `POST` | `/api/autonomous/internal/tick` | Bearer | `autonomous_manual_tick` — **protect** with secret or restrict to Cloud Scheduler in prod |

**Helpers to move with the router:**

- `verify_autonomous_inbound_secret`
- `_autonomous_list_item`
- `_autonomous_run_detail`

**Startup scheduler** (optional in new service): same logic as `main.py` `on_startup` / `on_shutdown`: `AUTONOMOUS_SCHEDULER_ENABLED`, `AUTONOMOUS_SCHEDULER_INTERVAL_SECONDS`, APScheduler calling `execute_due_steps_sync`.

---

## 4. Python modules to copy or split

| Monolith path | Action |
|---------------|--------|
| `services/autonomous_sequence.py` | **Copy wholesale**; fix imports to local `models` |
| `schemas.py` (classes from `AutonomousSequenceStartRequest` through `AutonomousSequenceInboundRequest`) | **Copy** into `app/schemas.py` |
| `utils/timezone.py` | **Copy** (`to_melbourne_iso` used by step serializers) |
| `models.py` — `AutonomousSequenceRun`, `AutonomousSequenceStep`, `AutonomousSequenceEvent` | **Copy** into dedicated `models.py` (see §6) |
| `main.py` — autonomous block only | **Move** into `app/routers/autonomous.py` + wire in `app/main.py` |

**Not copied** into autonomous service: CRM `Offer`, `Client`, bulk of `main.py`, unrelated schemas.

---

## 5. Environment variables

### Required for behaviour (parity with monolith)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | SQLAlchemy URL for **autonomous DB** (or shared DB — see §6) |
| `N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL` | Email step execution (`autonomous_sequence.py`) |
| `N8N_AUTONOMOUS_SMS_WEBHOOK_URL` | SMS step execution |
| `RETELL_API_BASE_URL` | Default `https://api.retellai.com` |
| `RETELL_API_KEY` | Voice steps |

### Security

| Variable | Purpose |
|----------|---------|
| `AUTONOMOUS_INBOUND_SECRET` | If set, inbound webhook must send matching `X-Autonomous-Inbound-Secret` |

### Scheduler

| Variable | Purpose |
|----------|---------|
| `AUTONOMOUS_SCHEDULER_ENABLED` | `1` / `true` to run APScheduler tick |
| `AUTONOMOUS_SCHEDULER_INTERVAL_SECONDS` | Default tick interval (monolith uses 60; min 15 in code) |

### Auth (dashboard + tick)

Replicate the monolith’s **Google JWT / Bearer** validation used by `get_current_user_with_db`, **or**:

- Use a **service API key** for `POST /api/autonomous/internal/tick` (Cloud Scheduler) and keep JWT for human-facing routes.

Document the chosen approach in the new service `README.md`.

---

## 6. Database and CRM decoupling

### Current monolith schema constraints

`AutonomousSequenceRun` uses:

```text
offer_id → ForeignKey("offers.id")
client_id → ForeignKey("clients.id")
```

Those tables live in the **CRM** database.

### Option A — **Dedicated database** (recommended for isolation)

1. Create a new database (e.g. Cloud SQL instance or separate SQLite for dev).
2. **Drop FK constraints** to `offers` / `clients`; keep `offer_id` and `client_id` as **plain integers** (references only).
3. **`POST /sequences/start`**: stop querying `Offer` locally. Either:
   - **Trust the caller** (Base 2 / CRM already validated), or
   - Call CRM **`GET /api/offers/{id}`** (or internal endpoint) with a **server-to-server token** before creating a run.
4. **`business_name` in list/detail**: stop joining `Offer`. Instead:
   - Require `business_name` inside **`context`** on start (Base 2 already sends it), **or**
   - Add optional `business_name` on the start request body, **or**
   - Enrich from CRM HTTP when rendering (cache optional).

### Option B — **Shared database** with CRM

- Same `DATABASE_URL` as `text_agent_backend`; keep FKs.
- **Fastest migration** but **does not** isolate DB blast radius; only process/deploy isolation.

---

## 7. Frontend (`text_agent_interface`) wiring

Implemented in `src/lib/utils.ts`: **`getAutonomousApiBaseUrl()`**

- If `NEXT_PUBLIC_AUTONOMOUS_API_BASE_URL` is set → use it (trim trailing slash).
- Server-side only: if `AUTONOMOUS_API_URL` is set → use it (for future server components / API routes).
- Else → **`getApiBaseUrl()`** (monolith).

**Call sites:** `autonomous-agent/page.tsx`, `autonomous-agent/[runId]/page.tsx`, Base 2 sequence `start` only.

**CORS:** allow the Next.js origin on the new service.

**Cookies / JWT:** if the new host is different, ensure the **same Bearer token** is accepted (same Google client / audience validation as monolith, or proxy through Next.js API routes).

---

## 8. Rollout checklist

1. [ ] Create repo from §2 layout; copy modules per §4.
2. [ ] Implement §6 Option A or B; run `init_db` / Alembic for autonomous tables only.
3. [ ] Port routes per §3; run OpenAPI diff against monolith for autonomous tags.
4. [ ] Configure env per §5; deploy to staging.
5. [ ] Set `NEXT_PUBLIC_AUTONOMOUS_API_BASE_URL` in Vercel/Cloud Build; smoke-test Base 2 + dashboard.
6. [ ] Point n8n/Retell inbound URLs to **new** host + secret.
7. [ ] Enable scheduler on new service; disable autonomous scheduler on monolith (remove env or strip routes).
8. [ ] Migrate historical rows: `pg_dump` / SQLite copy of `autonomous_sequence_*` tables if staying on shared DB; or backfill from export if new DB.
9. [ ] Remove autonomous routes + models + scheduler from `text_agent_backend` **after** traffic cutover (single PR to avoid dual-writes).

---

## 9. Sequence types (unchanged contract)

Allowed `sequence_type` values in `POST /api/autonomous/sequences/start`:

- `gas_base2_followup_v1`
- `ci_electricity_base2_followup_v1`

Logic remains in `start_gas_base2_sequence` / `plan_gas_base2_followup_times`.

---

## 10. Related UI features (keep when pointing to new backend)

- List runs with tabs (running / finished).
- Run detail: steps table, timezone, link to offer (still CRM URL on frontend).
- Editable **context** + **Save** (`PATCH`).
- **Stop sequence** (`POST .../stop`).

No API changes required if paths and bodies stay the same.

---

## 11. Quick reference — monolith file map

| Concern | Location |
|---------|----------|
| Scheduler | `main.py` — search `AUTONOMOUS_SCHEDULER_ENABLED` |
| Inbound secret + routes | `main.py` — search `verify_autonomous_inbound_secret`, `autonomous_sequence_` |
| Pydantic | `schemas.py` — block starting `# --- Autonomous follow-up` |
| ORM | `models.py` — `AutonomousSequenceRun` / `Step` / `Event` |
| Business logic | `services/autonomous_sequence.py` |
| ISO timestamps | `utils/timezone.py` — `to_melbourne_iso` |

*Line numbers drift over time; use search.*
