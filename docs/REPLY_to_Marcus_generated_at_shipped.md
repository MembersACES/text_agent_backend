# Re: ACES ↔ Prograde — `generated_at` / freshness stamp shipped to dev

**To:** Marcus (Prograde)
**From:** Morgan (ACES)
**Date:** 2026-06-19

Done — the freshness anchor you asked for is live on **dev**, verified against a real `/site` response. No Airtable schema change was needed (turned out both fields are server-side / built-in). Summary below so you can point your ESG ingest at dev and validate.

## Single source of truth — work from `dev`

Both repos are now consolidated on their **`dev`** branch — that's the one thing to build and validate against:

- **`sustainability_reporting` `dev`** now contains your **`fix/registry-404-cleanup` (PR #6)** — merged in, so the dead `_canonical` / `entity-registry` console 404s are cleaned up — **alongside** our progressive activity-sources loader and the new freshness fields. No conflicts; they sit together cleanly.
- **`text_agent_backend` `dev`** is the latest backend and has everything (progressive `manifest`/`site` endpoints, the DB-session fix, and the new `generated_at` / `airtable_created_time`).

So: dev = current truth for both sides. Prod follows once we're happy (see bottom).

## What shipped (dev)

Two additions to the activity-sources responses, **both additive — nothing you already read changed shape**:

1. **`generated_at`** — top-level on **both** `/activity-sources/manifest` and `/activity-sources/site` responses. ISO-8601 UTC. This is your as-at / re-pull anchor; use the manifest's `generated_at` as the timestamp for a pull.
2. **`airtable_created_time`** — on **each invoice row** inside `airtable_invoices.sample_rows[].row`. ISO-8601, sourced from Airtable's built-in record `createdTime`. May be `null` on older rows that predate it. (No Airtable column was added — this is Airtable's native per-record timestamp, which we now pass through instead of dropping.)

## Verified live on dev

From an actual `/site` response (`C&I Electricity` / NMI `VEEE0U1Y2S`, Frankston RSL):

```
"generated_at": "2026-06-19T02:50:55.063482+00:00",
...
"airtable_invoices": {
    "total_count": 28,
    "sample_rows": [ { "row": { ... "airtable_created_time": "2026-01-01T04:01:24.000Z" } }, ... ],
    "diagnostics": { "linked_invoice_ids_count": 28, "linked_invoice_ids_returned_count": 3 }
}
```

Confirming the points from our last exchange, now with live data:
- **`total_count` is the coverage denominator** — `28` invoices exist for the meter while only `3` sample rows were returned. It's the full linked-invoice count, independent of sample size, per `(utility_type, identifier)`. Build coverage on it.
- **`airtable_created_time`** is present on every row (also listed in `field_keys`).
- **`etl_preview`** is producing clean records on supported utilities (`supported: true`, `would_produce: 28`, `would_skip: 0`, `electricity_grid` / kWh / Scope 2).
- Real-world key note: this site came back with **`loa_record_id: null`** alongside `member_aces_client_id: 1` — exactly the case we flagged. Confirms: **key on the meter `identifier` (NMI/MRIN)**, treat `member_aces_client_id` as the stable owning-member ref, and `member_loa_record_id` as a nullable soft-hint, never a key.

## Also already live on dev (recap)

The progressive read path you're building against is in place and tested on a large entity (per-site fan-out at ~5 concurrent, no 504): `/manifest` (fast site list) → `/site` per meter. Auth and the `/api/aces/*` proxy (forwards your Google Bearer, read-only) are unchanged.

## Promotion to prod

We'll **merge to prod once we're happy with dev** — same code, no contract changes between dev and prod, for **both** repos (`dev → main`). I'll give you a heads-up when prod is live so you can repoint. Until then, **build and validate against dev**; the contract you see there is exactly what prod will serve.

## Still deferred (no action, your court)

- **Document-proxy** for invoice/contract preview — still reconciliation-driven, not ESG-ingest. Circle back when you need it; today previews use the viewer's own Drive/Airtable access.
- **Write-back** — out of v1 scope, as agreed.

That's everything for the ingest. Validate on dev and shout if the freshness stamp doesn't give you what you need for the incremental re-pull. — Morgan
