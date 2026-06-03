# n8n: Solar engagement form autonomous follow-up emails

Sequence type: `solar_panel_cleaning_engagement_form_v1`

The **initial** outreach (engagement form + testimonial PDFs) is sent by the **send-eoi** workflow from Document Generation. Autonomous steps are **three follow-up emails** only.

## Required: return Gmail ids from send-eoi

After **Send a message1** (Gmail), the **Respond to Webhook** body must include the sent message metadata so the UI can start the autonomous sequence on the correct thread:

```json
{
  "gmail_message_id": "={{ $json.id }}",
  "gmail_thread_id": "={{ $json.threadId }}",
  "email_id": "={{ $json.id }}",
  "thread_id": "={{ $json.threadId }}"
}
```

(Adjust `$json` to the Gmail node output path in your workflow.)

Without `id` / `threadId`, follow-ups cannot reply in-thread and will send as new emails.

## Required: autonomous email webhook (`N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL`)

Each due step POSTs a payload like:

```json
{
  "channel": "email",
  "sequence_type": "solar_panel_cleaning_engagement_form_v1",
  "step_index": 0,
  "reply_in_thread": true,
  "gmail_message_id": "<from run>",
  "gmail_thread_id": "<from run>",
  "omit_validity": true,
  "omit_document_links": true,
  "initial_email_subject": "Solar cleaning — quick win to protect performance and your solar investment",
  "signature_html": "<p>…Amelia Williams…</p>",
  "use_html_signature": true,
  "context": { … }
}
```

### Gmail node settings for follow-ups

1. **Do not** use “Send” (new message). Use **Reply** to thread or set:
   - `threadId` = `{{ $json.gmail_thread_id }}` (or from `context`)
   - `In-Reply-To` / `References` = `{{ $json.gmail_message_id }}` if your node supports it
2. **Subject**: `Re: Solar cleaning — quick win to protect performance and your solar investment` (or keep thread subject)
3. **Do not** include:
   - “Valid until …” / expiry dates
   - Links to Google Drive / “access the document here”
4. **Do not** attach files again — they remain on the first message in the thread
5. **Body**: use `signature_html` from payload when `use_html_signature` is true (matches initial Amelia signature)

### Branch on `step_index`

| step_index | Intent |
|------------|--------|
| 0 | Gentle follow-up |
| 1 | Polite reminder |
| 2 | Final nudge; offer to close out if not proceeding |

Use `context.step_prompt` or prompts in `autonomous_sequence_type` for this sequence type.

## Prompts in CRM

On backend startup, defaults are synced into `autonomous_sequence_type` for `solar_panel_cleaning_engagement_form_v1` unless you have already customised prompts without “valid until” / Drive link language.

Edit in the app: **Autonomous Agent → Templates → Solar Panel Cleaning — Engagement Form v1 → Prompt examples**.
