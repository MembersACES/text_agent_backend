# n8n: Agreement Follow Up first-touch (PDF to the member)

Sequence type: `agreement_followup_v1`

This is **not** the Alinta agreement-request flow (that emails the retailer). This emails the **member** with the agreement PDF attached, then the autonomous runner chases a signature on the same Gmail thread.

## What you need to set up

Two webhooks. You already have the second one.

### 1. New: first-touch with attachment (`N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL`)

The CRM POSTs **multipart/form-data** (not JSON) so n8n receives a real PDF.

Production webhook (already created):

`https://membersaces.app.n8n.cloud/webhook/aces-autonomous-agent/agreement-followup-email`

Do not use the `/webhook-test/` URL except while clicking **Listen for test event**. The app uses the production URL.

| Field | Source | Use |
|---|---|---|
| `to` | form | Gmail To |
| `subject` | form | Gmail subject |
| `body_html` / `body_text` | form | Gmail body (already includes the ACES signature) |
| `file` | binary | Attach this PDF. Do not upload it to Drive and link it. |
| `business_name`, `agreement_label`, `agreement_type` | form | logging / Drive filing if you want a copy |
| `offer_id`, `client_id`, `sequence_type` | form | metadata only |
| `first_touch` | `"true"` | branch: this is a **new** email, not a reply |

**Gmail node:** Send a **new** message (not Reply). Attach the webhook binary (`file`).

**Respond to Webhook** must return Gmail ids so follow-ups can reply in-thread:

```json
{
  "gmail_message_id": "={{ $json.id }}",
  "gmail_thread_id": "={{ $json.threadId }}",
  "email_id": "={{ $json.id }}",
  "thread_id": "={{ $json.threadId }}"
}
```

(Adjust `$json` to your Gmail node output.)

Without `id` / `threadId`, later steps will send as new emails instead of sitting on the original thread.

Do **not** reuse `/webhook/email-supplier` for this. That workflow emails the retailer and files a signed lodgement.

### 2. Existing: follow-ups (`N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL`)

After the first send, the runner uses the same autonomous email webhook as campaigns / solar. Payload is JSON:

```json
{
  "channel": "email",
  "sequence_type": "agreement_followup_v1",
  "step_index": 1,
  "reply_in_thread": true,
  "gmail_message_id": "<from first-touch>",
  "gmail_thread_id": "<from first-touch>",
  "omit_validity": true,
  "omit_document_links": true,
  "initial_email_subject": "<subject of the first email>",
  "signature_html": "<p>…The Team…</p>",
  "use_html_signature": true
}
```

Same rules as solar engagement follow-ups:

1. Reply on the thread (`threadId` / In-Reply-To). Do not start a new email.
2. Do not attach the PDF again.
3. Do not include Drive links or invented “valid until” dates.
4. Step 0 is already marked complete (that was the first-touch). Follow-ups are step_index 1, 2, 3.

## Env

On the CRM backend:

```
N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL=https://membersaces.app.n8n.cloud/webhook/agreement-followup-email
```

If this is unset, the CRM logs a placeholder and still starts the sequence (same as campaigns in local/dev). Set it in Cloud Run before staff use the page live.

## Cadence

| When | What | Who sends it |
|---|---|---|
| On **Start follow-up** | Email + PDF | CRM → this new n8n webhook |
| +2 business days | Email reply | Autonomous runner → existing email webhook |
| +4 business days | Email reply | same |
| +6 business days | Email reply | same |

Stops when inbound mail is classified as `agreement_signed` (or negative sentiment / human review).
