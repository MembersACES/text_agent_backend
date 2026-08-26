# ACES ↔ Prograde Integration — Post-Stabilisation Review & Next-Steps Plan

**Date:** 2026-06-16
**Author:** review pass (planning only — nothing in here has been actioned)
**Scope:** `text_agent_backend` (FastAPI/Cloud Run) + `sustainability_reporting` (Prograde AASB S2 frontend)
**Status of the thing we just fixed:** ✅ Shipped to dev **and** prod, verified healthy.

---

## 1. Executive summary

The "ACES activity sources" panel was timing out (HTTP 504) on large reporting
entities and, worse, exhausting the DB connection pool so *unrelated* screens
500'd. Root causes were identified from production logs (not guessed):

1. A single request did a long, synchronous **live-Airtable fan-out while holding
   a DB connection** for ~250s+ (`build_entity_activity_sources`).
2. The Cloud Run DB pool was effectively **1 + 1 = 2 connections**; one slow
   request starved everything else → `QueuePool ... timeout` 500s.
3. The largest entity (`aligned-leisure`) exceeded even a 600s request timeout as
   a single call — no timeout setting could fix that.

**Fixes shipped and verified in prod (2026-06-16):**
- Release the DB session *before* the Airtable work.
- Pool sized via env (`DB_POOL_SIZE`/`DB_MAX_OVERFLOW`), with a hardened parser
  (`database.py:_int_env`) so a malformed value warns instead of crashing boot.
- New **progressive** endpoints — `/activity-sources/manifest` (fast site list)
  and `/activity-sources/site` (one site) — plus a frontend rewrite that loads the
  manifest then fans out per-site calls (5 concurrent) with a live status line.

**Prod evidence (logs, 2026-06-16):** `aligned-leisure` now resolves as ~30
per-site calls, all HTTP 200, 3.7–18s each, fanned 5 at a time. Zero 5xx, zero
`QueuePool` errors. The 504 is gone.

This document is the honest backlog of what's *left* — prioritised, with evidence,
effort, and a recommendation for each. **Nothing here is urgent enough to undo the
win; most is hardening and hygiene.**

---

## 2. Current-state assessment (honest)

| Area | State | Note |
|---|---|---|
| Activity-sources feature | ✅ Working in prod | Progressive load, no 504, no pool exhaustion |
| DB pool | ✅ Adequate | 30 concurrent site calls, no QueuePool errors |
| Per-site latency | 🟡 OK but slow | C&I Electricity/Gas sites 15–18s (live Airtable); SME/Oil ~3.7s |
| Airtable rate-limiting | 🔴 No handling | No 429/backoff/Retry-After anywhere in `airtable_client.py` |
| Secrets | 🔴 Exposed + plaintext | Creds dumped in a chat `describe`; most are plaintext Cloud Run env, not Secret Manager |
| CI / automated tests | 🔴 None | Tests exist; no GitHub Actions to run them before deploy |
| Repo hygiene | 🟡 Noisy | No `.gitattributes`; working tree has a repo-wide CRLF flip that pollutes diffs |
| "Twin" endpoints | 🟡 Same anti-pattern | `linked-utilities`, `etl/sync`, `get-business-info`, `data-request` still hold DB across Airtable (milder) |
| defra emission-factor 404s | ✅ Expected by design | `server/index.js:170` says so; factors load from inline/CZA bundle. **Not a bug.** |

---

## 3. Prioritised backlog

### P0 — Security (do first)

**S1. Rotate the exposed secrets and move plaintext env → Secret Manager.**
- *Why:* During troubleshooting a `gcloud run services describe` dumped live
  credentials into chat: dev DB password, a **GCP service-account private key**,
  OpenAI key, Airtable PAT, Pudu keys, and webhook/roster secrets. They're now in
  a transcript. Several are stored as **plaintext env values** on Cloud Run
  (visible to anyone who can `describe` the service); only Google client id/secret
  use `secretKeyRef`.
- *Action:* Rotate each exposed credential; migrate the plaintext ones to Secret
  Manager and reference via `--set-secrets`. Verify prod still boots after.
- *Effort:* M · *Risk if skipped:* high (credential compromise, now in prod).

### P1 — Robustness of the new feature

**R1. Add Airtable rate-limit handling (retry + backoff).**
- *Why:* `services/airtable_client.py` has **no** 429/backoff logic (only a
  field-name typo retry around line ~1021). Airtable's limit is ~5 req/s/base. The
  frontend fans out 5 concurrent, and each site makes multiple Airtable calls — so
  multiple users/tabs or bigger entities will trip 429s, which currently surface as
  per-site "⚠ failed" rows.
- *Action:* Wrap the GET helpers with retry + exponential backoff honouring
  `Retry-After`; optionally a small process-wide limiter. Keep frontend
  concurrency at 5.
- *Effort:* M.

**R2. Apply the session-release pattern to the "twin" endpoints.**
- *Why:* Same "hold DB across live Airtable" shape, milder: `linked-utilities`
  (main.py:968, single-client LOA fan-out), `etl/sync` (999, write), `get-business-info`
  (1131), `data-request` (3257). Lower concurrency/duration than activity-sources,
  but the same failure mode under load.
- *Action:* `linked-utilities` first (closest twin, low effort); assess the others.
- *Effort:* S–M.

**R3. Verify prod pool sizing against Cloud SQL limits.**
- *Why:* We set prod `DB_POOL_SIZE=5`/`DB_MAX_OVERFLOW=10` = 15/instance. Safe only
  if `(15) × maxScale ≤ ~80% of aces-tasks-db (g1-small) max_connections`.
- *Action:* Confirm prod `maxScale` and run `SHOW max_connections;` on the prod DB;
  adjust if the product exceeds budget.
- *Effort:* S (verification).

### P2 — Hygiene & tech-debt

**H1. Add `.gitattributes` and normalise line endings.**
- *Why:* No `.gitattributes`; the backend working tree shows a repo-wide CRLF↔LF
  flip that turns every diff into thousands of phantom lines (and contributed to the
  confusion this week).
- *Action:* Add `* text=auto eol=lf`, run a one-time `renormalize` commit
  deliberately (separate from feature work).
- *Effort:* S (but a large, one-time noisy commit — do it on its own).

**H2. Add CI (GitHub Actions).**
- *Why:* No workflows; the pytest suite isn't run automatically before Cloud Build
  deploys. A bad push reaches dev/prod unchecked.
- *Action:* Workflow on PRs to `dev`/`main`: backend `pytest`, frontend
  `node --check` / lint.
- *Effort:* M.

**H3. Resolve the `/api/diagnostics` reference.**
- *Why:* The old error banner pointed users to `/api/diagnostics`, which never
  existed. The new widget banner no longer references it — confirm nothing else does,
  or build a real lightweight diagnostics endpoint.
- *Effort:* S.

**H4. Decide the fate of the old monolithic endpoint.**
- *Why:* `/api/climate/entities/{id}/activity-sources` (main.py:840) +
  `build_entity_activity_sources` are now superseded by manifest+site. Keeping them
  is fine as a fallback, but they carry the original 504 risk if anything still calls
  them.
- *Action:* Confirm nothing calls the old route, then keep-as-documented-fallback or
  remove. **Decision needed.**
- *Effort:* S.

**H5. Revert dev Cloud Run timeout 600 → 300.**
- *Why:* We bumped dev to 600 as a band-aid; progressive loading removed the need.
  Reverting keeps dev/prod consistent and lets latency regressions surface instead of
  hiding under a long timeout. (Prod was left at default — good.)
- *Effort:* trivial.

**H6. Add CSS for the new `aces-as-site*` classes.**
- *Why:* The progressive rows work but are unstyled (new classes the stylesheet
  doesn't define yet).
- *Effort:* S.

### P3 — Strategic bets (bigger, optional)

**B1. Decouple Airtable → SQL (scheduled sync + serve from SQL).**
- *Why:* The real performance + resilience win. C&I sites are still 15–18s because
  the data is fetched **live** from Airtable on every load. A scheduled sync into SQL
  gives sub-second dashboards, immunity to Airtable outages/rate-limits, and enables
  historical snapshots/point-in-time disclosure (valuable for an assurance product).
- *Action:* Design a sync job (Cloud Scheduler → ingest endpoint), a freshness
  indicator, and a read path from SQL. Progressive endpoints stay as the live/refresh
  path.
- *Effort:* L · *This is the one worth scoping properly if the feature gets heavy use.*

**B2. Observability & alerting.**
- *Why:* We only found the root cause by manually reading logs. No alerting on 5xx
  rate, Airtable error rate, or p95 latency.
- *Action:* Log-based metrics + alerts in Cloud Monitoring.
- *Effort:* M.

**B3. Broaden automated test coverage.**
- *Why:* Good unit tests exist for the new functions; no integration tests hitting
  the manifest/site routes via `TestClient` with mocked Airtable, and no frontend
  smoke test.
- *Effort:* M.

---

## 4. Explicitly NOT recommended (challenge / watch-only)

- **Chasing the defra/emission-factor 404s** — expected by design (`server/index.js:170`);
  factors load from the inline/CZA bundle (`EFACTORS_LIB overridden` log). *Watch-only:*
  spot-check that a known factor value actually applies in a Scope calc; otherwise leave it.
- **Raising Cloud Run timeouts further** — unnecessary; progressive loading removed the
  need. Higher timeouts just hide latency and cost more.
- **Over-provisioning the DB pool** — bounded by Cloud SQL `max_connections`; bigger
  isn't better. Size to the formula in R3.

---

## 5. Open decisions for you

1. **Secrets (S1):** who owns rotation, and what's the timeline? (Recommend this week.)
2. **Old endpoint (H4):** keep `/activity-sources` as a fallback, or remove it?
3. **Caching project (B1):** is the feature used enough to justify the Airtable→SQL sync,
   or is "live but progressive" good enough for now?
4. **Line-ending normalisation (H1):** OK to do the one-time noisy renormalise commit?
5. **CI (H2):** add now, or defer?

---

## 6. Suggested sequencing

- **This week (small, high-value):** S1 (secrets) · H5 (revert dev timeout) ·
  R3 (verify prod pool) · H3 (diagnostics ref).
- **Next (robustness):** R1 (Airtable backoff) · R2 (linked-utilities) · H2 (CI) ·
  H1 (.gitattributes).
- **Later (bets):** B1 (caching) · B2 (observability) · B3 (tests) · H4 / H6.

---

## 7. What I'm unsure about

- Whether anything other than the new widget still calls the **old** `/activity-sources`
  route — needs a quick caller search across both repos before retiring it (H4).
- Prod `max_connections` and `maxScale` — not yet confirmed; needed to bless the
  current 5/10 pool (R3).
- Whether the C&I latency (15–18s) is Airtable-side or our pagination — a quick profile
  would tell us, and informs whether B1 is worth it.
