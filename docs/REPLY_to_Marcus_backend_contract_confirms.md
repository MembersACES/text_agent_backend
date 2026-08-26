# Re: ACES ↔ Prograde — backend contract confirms

**To:** Marcus (Prograde)
**From:** Morgan (ACES)
**Date:** 2026-06-16

Thanks Marcus — clear note. I've confirmed each point against the actual backend code (both the API layer and the Airtable/CRM internals) rather than from memory, so you can build on these without guessing. Short version: your understanding is right on most of it; two small clarifications below, and one genuinely missing thing (a freshness timestamp) that we're happy to add if you want it.

Your framing is correct: read-only API, reached via our `/api/aces/*` proxy which forwards your Google OAuth Bearer; ~5 concurrent `/site` calls is what our loader does too. No data-shape changes needed from us.

---

## The four confirms

**1. Waste → read it via `/activity-sources/site`, same as energy.**
Waste is a first-class utility on our side — `/site?utility_type=Waste&identifier=...` returns `airtable_invoices` exactly like an NMI/MIRN energy site, so use that for reconciliation. The `ETL→0` you saw on Frankston is expected and correct: waste *is* ETL-supported, but our Airtable tracks **bin pickups, not tonnes**, so the ETL preview skips rows for missing mass. The invoices themselves are there via `airtable_invoices`.

Do **not** use `POST /api/get-waste-info` for this — that's a separate "latest invoice" lookup wired to an internal automation, not the Airtable invoice rows you want. Treat waste like energy through `/site`; just expect no tonnage until mass data exists upstream.

**2. `total_count` is canonical, and it's per-meter.**
`airtable_invoices.total_count` is the count of **all** linked invoice records for that account/meter — not the size of the returned sample. Our small sample limit (default 3) only caps how many `sample_rows` we return; `total_count` still reflects every invoice that exists. Scope is **per `(utility_type, identifier)`** (i.e. per meter), so sum across sites for an entity-level figure. It's a sound basis for your coverage calc.

One edge case to be aware of: the account lookup behind it caps at 8 matching rows, so a *non-unique* identifier that matches >8 accounts would return `total_count: 0`. That won't happen for unique NMI/MRIN/account numbers — just don't key coverage off a deliberately ambiguous identifier.

**3. Documents need the viewer's own access — there's no backend proxy today.**
The invoice rows we return are the raw Airtable fields passed straight through. So any document link is whatever's in the cell:
- an Airtable **attachment** field → a signed, **short-lived URL that expires**;
- a **Drive link** field → a normal URL that needs the **viewer's own Google Drive permission**.

We do **not** currently serve these through the backend (no document-proxy). So as it stands, previewing relies on the viewer's own access, and you shouldn't cache attachment URLs (they expire). If you need reliable in-dashboard preview regardless of the viewer's Drive access, that would be a new backend document-proxy on our side — see "Things we can do" below.

**4. Write-back: read-only today (confirmed). Feasible later, rails already exist.**
Nothing on the `/activity-sources/*` path writes. If/when you want a user to confirm a site mapping or upload a missing invoice, we already have the internal building blocks for it (an activity-record upsert path, an Airtable field-PATCH path, and a Drive-upload path used elsewhere), so it's a clean addition rather than a rebuild. It's not on the committed roadmap yet — happy to scope it when you're ready. Design read-only for v1 as you planned.

---

## For the ESG real-data pull (B4)

**Per-meter keys — key on the NMI/MRIN, not the member IDs.**
- `member_aces_client_id` is a stable database primary key. Our CRM sync **updates client rows in place** — it does not delete-and-recreate them — so this ID survives re-syncs. The only way it changes is a manual admin delete + re-add of a client.
- `member_loa_record_id` (the Airtable `rec…` LOA id) is **mutable**: a re-sync or manual edit can re-point it, clear it, or it can be shared across members. Treat it as a soft hint, not a key.
- The truly stable natural key for a meter is its own **`identifier` (NMI/MRIN)**. Recommendation: key your stored meters on `identifier`; keep `member_aces_client_id` as the (stable) owning-member reference; reconcile `member_loa_record_id` loosely since it can drift.

**As-at / re-sync — this is the one gap, and we can close it.**
Right now there's **no server-generated timestamp** on the manifest or site responses, and the live Airtable rows don't carry a modified/synced time. You *can* read `updated_at` on each staged record (`staged_activity_records[].updated_at`), but there's nothing on the response to anchor a re-pull of the *live* data. We can fix that cleanly — see below.

---

## Things we can do on our side — your call (not built yet)

We haven't changed anything; flagging these so you can confirm before we action:

1. **Add an as-at stamp.** We can add a `generated_at` (UTC) to the manifest and `/site` responses, and optionally surface Airtable's per-record `created_time` on each invoice row. That gives you a clean freshness anchor for incremental re-pulls. Small change — **confirm and we'll do it.**
2. **Document-proxy (only if you need reliable preview).** If viewer-independent invoice/contract preview matters, we can add a backend endpoint that brokers the document so it doesn't depend on each viewer's Drive access or on expiring Airtable URLs. Larger than #1 — **tell us if it's in scope** and we'll scope it.

Neither is built; both wait on your confirmation.

---

## One thing only Airtable can confirm

Whether a given invoice table (incl. the waste table) actually has an attachment column vs a Drive-link column — and whether it carries an internal "last modified" field — is an Airtable-base schema question, not visible in our code. If your reconciliation depends on a specific document field being present, let us know which and we'll verify it exists in the base.

---

Net: build against the contract as-is for waste, `total_count`, keys, and read-only — all confirmed. Just confirm whether you want the `generated_at`/`created_time` stamp (easy) and whether the document-proxy is in scope, and we'll line those up. — Morgan
