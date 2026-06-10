# IMMEDIATE_ACTION — ACES climate × Prograde integration

**Owner:** Morgan / ACES engineering  
**Repos:** `text_agent_backend`, `text_agent_interface` only  
**Not in scope here:** `sustainability_reporting` (Marcus — see handoff section at end)  
**Date:** 10 June 2026

This is the **execution plan** to complete all ACES-side work needed before sending Marcus an updated integration brief against live Prograde `dev` (registry merge + export gate already shipped).

**Reference docs (read-only context):**

| Doc | Purpose |
|-----|---------|
| [`frankston-rsl-integration-audit.md`](./frankston-rsl-integration-audit.md) | Frankston gap analysis |
| [`ENTITY_GROUP_CLIMATE_INTEGRATION.md`](./ENTITY_GROUP_CLIMATE_INTEGRATION.md) | Group ↔ disclosure design |
| [`morgan-integration.md`](./morgan-integration.md) | Detailed technical spec |
| [`../sustainability_reporting/docs/marcus-integration.md`](../sustainability_reporting/docs/marcus-integration.md) | Marcus-side (do not implement here) |

**Validation entity:** `frankston-rsl` · CRM group id `1` · members Frankston (client `1`) + Box Hill (client `21`)

---

## What Marcus already has (live on Prograde `dev`)

Do **not** re-implement these on ACES:

- Dynamic A1 registry merge (`GET /data/registry/entity-registry.json`)
- ACES roster + activity-sources **proxy** (same-origin → backend)
- `_aces_dynamic` export gate (preview-only; no defensible BoP / engagement pack)
- BoP lock state + zero-confidence fixes

ACES work below **feeds** those endpoints with correct, complete data.

---

## Execution overview

| Phase | Focus | Repos | Blocker for Marcus handoff? |
|-------|--------|-------|----------------------------|
| **0** | Prep + baseline smoke | both | No |
| **1** | LOA data fix (Oil/Waste/retailers) | backend | **Yes** — incomplete activity-sources |
| **2** | Entity group ↔ disclosure | backend + interface | **Yes** — multisite rollup |
| **3** | CRM Climate UX alignment | interface | **Yes** — operator workflow |
| **4** | Deploy dev + verify + handoff pack | both | **Yes** — send to Marcus |
| **Later** | B4 ingest (post-Tuesday) | backend | No (separate track) |

**Recommended order:** Phase 0 → 1 → 2 (backend first) → 3 → 4.  
Phase 1 and Phase 2 backend can be one PR if preferred, but **deploy and verify Phase 1 before closing Phase 2 UI**.

---

## Phase 0 — Prep (≈½ day)

### 0.1 Confirm environment

- [ ] Backend dev Cloud Run has `USE_AIRTABLE_DIRECT=true`, `AIRTABLE_API_KEY` set
- [ ] `CLIMATE_ROSTER_SERVICE_KEY` matches Prograde `ACES_ROSTER_SERVICE_KEY`
- [ ] Interface `NEXT_PUBLIC_SUSTAINABILITY_PLATFORM_URL` points at Prograde dev
- [ ] Local SQLite or dev DB: Frankston group id `1`, client `1` has `reporting_entity=frankston-rsl`

### 0.2 Baseline capture (before any code)

Run and save output for comparison after Phase 1–2:

```bash
# Backend (service key or JWT)
curl -s "$BACKEND/api/climate/entities/frankston-rsl/activity-sources?period=FY26" \
  -H "X-ACES-Service-Key: $KEY" | jq '{site_count, staged: .staged_activity_record_count, keys: .raw_loa.linked_utilities | keys, aces_client_ids}'

curl -s "$BACKEND/api/climate/roster?period=FY26" \
  -H "X-ACES-Service-Key: $KEY" | jq '.clients | length'

curl -s "$BACKEND/api/entity-groups/frankston-rsl/summary" \
  -H "Authorization: Bearer $JWT" | jq .
```

**Expected today:** 4 sites, Oil/Waste missing, roster may list one row per client with slug, Box Hill not in activity-sources.

### 0.3 Branch strategy

- [ ] Single integration branch (e.g. `feature/climate-integration-v2`) spanning backend + interface, or paired PRs with clear merge order (backend first)

---

## Phase 1 — LOA & activity-sources data (P0) · `text_agent_backend`

**Goal:** Prograde receives **6 utility sites** for Frankston with **retailers** and correct staged attachment.

### 1.1 Fix Oil/Waste in `UTILITY_CONFIG`

**File:** `services/airtable_client.py`

| Utility | Change |
|---------|--------|
| **Oil** | `table_name` → `Oil Clients`; `identifier_field` → `Client Name` |
| **Waste** | `loa_link_field` → `Link to Waste Clients` (+ fallback `7th Sheet - Waste`); `table_name` → `Waste Clients`; `identifier_field` → `Account Number or Customer Number`; `retailer_field` → `Provider` |

- [ ] Implement config changes
- [ ] Add unit test: mock LOA → `get_linked_utility_records` includes `Oil` and `Waste`

### 1.2 Retailer fallback

**File:** `services/climate_entity_sources.py` and/or `airtable_client.py`

- [ ] When utility-row retailer empty, use LOA rollups (`Retailer C&I Gas`, `Retailers Oil Clients`, etc.)
- [ ] Verify Frankston sites show Origin, Alinta, Trojan, J.J. Richards

### 1.3 C&I Gas link field fallback (if needed)

**File:** `services/airtable_client.py`

- [ ] Try `Link to C&I Gas Client` when `4th Sheet - Large Gas` empty on LOA

### 1.4 Orphaned staged records (safety net)

**File:** `services/climate_entity_sources.py`

- [ ] Add `orphaned_staged_records` / count for staged rows not matched to a site bundle (can remove once Oil fix verified)

### 1.5 Tests & local verify

- [ ] `pytest tests/test_climate_entity_sources.py tests/test_climate_entity_merge.py` (+ new Oil/Waste test)
- [ ] Local script: `build_entity_activity_sources(db, 'frankston-rsl')` → `site_count >= 6`, keys include Oil, Waste
- [ ] Sync Oil on Frankston in CRM → staged diesel rows attach to Oil site bundle

**Phase 1 exit criteria:**

- [ ] `GET /api/climate/entities/frankston-rsl/activity-sources` → ≥6 sites, retailers non-empty
- [ ] Waste: ~19 invoices, 0 staged (expected)
- [ ] Oil: ~50 staged after sync

---

## Phase 2 — Entity groups ↔ disclosure (P1) · `text_agent_backend`

**Goal:** One disclosure slug rolls up **all group members** (Box Hill without manual member slug). Roster deduped for Prograde launcher.

**Design:** [`ENTITY_GROUP_CLIMATE_INTEGRATION.md`](./ENTITY_GROUP_CLIMATE_INTEGRATION.md)

### 2.1 Schema

**Files:** `models.py`, `database.py`, `schemas.py`

- [ ] Add `entity_groups.reporting_entity` (nullable, indexed, kebab-case validation)
- [ ] Startup migration in `database.py`
- [ ] `EntityGroupCreate` / `EntityGroupResponse` / PATCH body include `reporting_entity`

### 2.2 Resolution helper

**New or in:** `services/entity_groups.py` or `services/climate_entity_sources.py`

- [ ] `effective_reporting_entity(client, group) -> str | None`
- [ ] `clients_in_disclosure_rollup(db, slug) -> list[Client]` (member slug match + group inherit; exclude conflicting member slugs)

### 2.3 Expand `build_entity_activity_sources`

**File:** `services/climate_entity_sources.py`

- [ ] Use `clients_in_disclosure_rollup` instead of `Client.reporting_entity == slug` only
- [ ] Response additions:
  - `entity_group_slug`
  - `members[]`: `{ aces_client_id, business_name, loa_record_id, disclosure_source }`
  - Site bundles: `member_aces_client_id`, `member_business_name` where applicable
- [ ] Extend `test_build_entity_activity_sources_merges_multiple_clients` + new group-inherit test (Box Hill pattern)

### 2.4 Entity group APIs

**File:** `main.py`, `services/entity_groups.py`

- [ ] `PATCH /api/entity-groups/{slug}` — accept `reporting_entity`
- [ ] `GET /api/entity-groups/{slug}/summary` — add `group_reporting_entity`, `members_in_climate_rollup`, `staged_activity_total`
- [ ] Optional: `POST /api/entity-groups/{slug}/apply-reporting-entity` (bulk copy slug to members — explicit operator action)

### 2.5 Climate roster v2

**File:** `services/climate_store.py`, `main.py` (`GET /api/climate/roster`)

- [ ] Return **one row per disclosure slug** (deduped), not one per client
- [ ] Fields per row:
  - `reporting_entity`, `display_name`, `entity_group_slug`, `member_count`
  - `aces_client_ids[]`, `loa_record_ids[]`
  - `activity_record_count` (sum across rollup members)
  - `primary_abn` (from `entity_groups.primary_abn` when set)
  - `stage`, `deep_link`, `period` (unchanged pattern)
- [ ] Backward compatibility: document breaking change for Prograde if roster was per-client (coordinate in handoff — Marcus dedupes interim)

### 2.6 Seed / ops data (Frankston)

- [ ] Set `entity_groups.id=1.reporting_entity = frankston-rsl`
- [ ] Set `entity_groups.id=1.primary_abn = 12 643 054 953` (optional P2 but do now if easy)
- [ ] Leave Box Hill `clients.reporting_entity` null to prove inherit path

**Phase 2 exit criteria:**

- [ ] activity-sources `members` includes Frankston + Box Hill
- [ ] Roster returns **one** `frankston-rsl` entry, `member_count: 2`, summed activity count
- [ ] Solo site with own slug still separate roster entry

---

## Phase 3 — Interface (P1) · `text_agent_interface`

**Goal:** Operators manage disclosure at **group or site** level; Climate tab matches backend truth.

### 3.1 Group hub (`/crm-groups/[slug]`)

**File:** `src/app/crm-groups/[slug]/page.tsx`, `src/lib/entity-groups.ts`

- [ ] Edit + save group `reporting_entity` (PATCH API)
- [ ] Show `members_in_climate_rollup` vs total members
- [ ] **Open Prograde workspace** button → `{NEXT_PUBLIC_SUSTAINABILITY_PLATFORM_URL}/?entity={slug}&period=FY26`
- [ ] **Sync all sites in rollup** — loop ETL sync per member in rollup (reuse Climate tab sync logic)
- [ ] Upgrade amber banner: mixed member slugs vs members excluded from rollup

### 3.2 Member profile — entity group section

**File:** `src/components/crm-member/EntityGroupSection.tsx`

- [ ] Show group `reporting_entity` when assigned
- [ ] Indicate **inherit** vs **site override** for member slug

### 3.3 Climate tab

**Files:** `src/components/crm-member/tabs/ClimateTab.tsx`, optional new API hook

- [ ] Display **effective** reporting entity (inherit vs override)
- [ ] **Option A (recommended):** `GET /api/clients/{id}/climate/linked-utilities` — new backend endpoint wrapping `get_linked_utility_records`
- [ ] Replace or merge `parseAllLinkedUtilities(businessInfo)` with backend response (6 utilities match Prograde after Phase 1)
- [ ] Optional dev banner: n8n utility count ≠ backend count

### 3.4 Types & API client

**Files:** `src/lib/entity-groups.ts`, `src/components/crm-member/types.ts`

- [ ] Extend types for group `reporting_entity`, summary fields, roster v2 shape if interface consumes roster directly

**Phase 3 exit criteria:**

- [ ] Group hub: set reporting entity, open Prograde, sync all
- [ ] Member Climate tab: effective slug + linked utilities match activity-sources

---

## Phase 4 — Deploy, verify, Marcus handoff

### 4.1 Deploy to dev

- [ ] Merge backend PR → deploy `text-agent-backend-dev`
- [ ] Merge interface PR → deploy ACES interface dev
- [ ] Confirm Prograde dev still has `ACES_API_URL` + service key pointed at backend dev

### 4.2 End-to-end smoke (Frankston)

| # | Check | Pass? |
|---|--------|-------|
| 1 | CRM group hub shows `reporting_entity=frankston-rsl` | |
| 2 | Box Hill in group, no member slug → in activity-sources `members` | |
| 3 | activity-sources: 6 utility types, retailers filled | |
| 4 | Sync all from group hub → staged count includes all rollup members | |
| 5 | Roster: one `frankston-rsl`, `member_count: 2` | |
| 6 | Prograde `/?entity=frankston-rsl&period=FY26` opens (registry merge) | |
| 7 | Prograde activity-sources proxy returns expanded JSON (curl via Prograde) | |
| 8 | Export links greyed (dynamic stub) — expected | |

```bash
# Prograde proxy (after ACES deploy)
curl -s "$PROGRADE/api/aces/entities/frankston-rsl/activity-sources?period=FY26" \
  -H "X-ACES-Service-Key: $KEY" | jq '{site_count, members: .members, member_count: (.members|length)}'

curl -s "$PROGRADE/api/aces/clients?period=FY26" \
  -H "X-ACES-Service-Key: $KEY" | jq '[.clients[] | select(.reporting_entity=="frankston-rsl")]'
```

### 4.3 Update Marcus integration file

After Phase 4 passes, send Marcus an **updated integration brief**. Suggested deliverable:

1. **Copy or symlink** refreshed [`marcus-integration.md`](../sustainability_reporting/docs/marcus-integration.md) with a dated **“ACES live contract”** appendix, **or**
2. New email/doc: **`ACES_INTEGRATION_HANDOFF_<date>.md`** (can live in `text_agent_backend/docs/`) containing:

| Section | Content |
|---------|---------|
| **Summary** | What ACES shipped; Prograde unchanged URL model |
| **Roster v2 schema** | Full JSON example (deduped disclosure entity) |
| **Activity-sources v2 schema** | `members[]`, `entity_group_slug`, site `member_aces_client_id` |
| **Breaking changes** | Roster no longer one-row-per-client (if true) |
| **Marcus action items** | Launcher dedupe, panel member breakdown, stub fields — from `marcus-integration.md` |
| **Smoke URLs** | Dev backend, Prograde, Frankston deep link |
| **Out of scope** | B4 ingest timeline |

- [ ] Draft handoff doc
- [ ] Attach sample JSON payloads (real Frankston curl output, redact if needed)
- [ ] Send to Marcus with link to Prograde `marcus-integration.md` on `dev`

### 4.4 Internal sign-off

- [ ] Morgan confirms Frankston pilot complete on dev
- [ ] Note any clubs beyond Frankston needing `entity_groups.reporting_entity` seeding

---

## Later — B4 ingest (post-Tuesday) · `text_agent_backend` only

Not required for Marcus UI handoff; track separately:

- [ ] POST staged `climate_activity_records.body_json` to Prograde B4 adapter
- [ ] Preserve `aces_client_id` per member in ingest payload
- [ ] Update `integration_notes.b4_boundary` in activity-sources

See [`morgan-integration.md`](./morgan-integration.md) P3 and Marcus pack Wave 1 appendix.

---

## Explicitly out of scope (ACES immediate action)

| Item | Where |
|------|--------|
| Prograde panel UX, launcher badge, registry stub mapping | Marcus — `sustainability_reporting` |
| Static A1 promotion of Frankston | Ops / Marcus / ASRS data entry |
| Waste Scope 3 tonnes ETL | Blocked — bins only in Airtable |
| Cleaning ETL adapter | Backlog |
| Full n8n deprecation | [`N8N_DEPRECATION_PLAN.md`](./N8N_DEPRECATION_PLAN.md) — separate programme |
| `sustainability_reporting` code changes | Do not touch in this plan |

---

## Task checklist (master)

Copy for project tracking:

```
Phase 0
[ ] 0.1 Environment confirmed
[ ] 0.2 Baseline curls saved
[ ] 0.3 Branch created

Phase 1 — backend
[ ] 1.1 Oil/Waste UTILITY_CONFIG
[ ] 1.2 Retailer fallback
[ ] 1.3 Gas link fallback (if needed)
[ ] 1.4 Orphaned staged (optional)
[ ] 1.5 Tests + local verify

Phase 2 — backend
[ ] 2.1 entity_groups.reporting_entity schema
[ ] 2.2 effective_reporting_entity helper
[ ] 2.3 activity-sources rollup + members[]
[ ] 2.4 Entity group API updates
[ ] 2.5 Climate roster v2
[ ] 2.6 Frankston seed data

Phase 3 — interface
[ ] 3.1 Group hub climate controls
[ ] 3.2 EntityGroupSection inherit/override
[ ] 3.3 Climate tab + linked-utilities API
[ ] 3.4 Types updated

Phase 4
[ ] 4.1 Deploy dev
[ ] 4.2 E2E smoke table
[ ] 4.3 Marcus handoff doc sent
[ ] 4.4 Sign-off
```

---

## Estimated effort (rough)

| Phase | Effort |
|-------|--------|
| 0 | 0.5 day |
| 1 | 1–2 days |
| 2 | 2–3 days |
| 3 | 2–3 days |
| 4 | 0.5–1 day |
| **Total** | **~6–9 days** |

Phase 1 unblocks accurate Prograde data immediately; Phase 2–3 unblocks multisite operators and clean Marcus roster consumption.
