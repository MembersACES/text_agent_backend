# Unified n8n file upload (testimonials + 1st month savings invoices)

Both manual testimonial upload and 1st month savings invoice PDF upload call the **same** n8n webhook. Branch in n8n on `upload_type`.

## URLs

| Environment | URL |
|-------------|-----|
| **Test** (Listen for test event) | `https://membersaces.app.n8n.cloud/webhook-test/file-upload` |
| **Production** (after workflow active) | `https://membersaces.app.n8n.cloud/webhook/file-upload` |

**Backend env:** `N8N_FILE_UPLOAD_WEBHOOK` (defaults to test URL for local/dev)

**Ops checklist (required for Drive file_id):**

1. Create/activate the n8n workflow with webhook path `file-upload` (production URL returns 404 until active).
2. Deploy `text_agent_backend` with `tools/n8n_file_upload.py` wired in `POST /api/one-month-savings/upload-pdf`.
3. Set Cloud Run env `N8N_FILE_UPLOAD_WEBHOOK=https://membersaces.app.n8n.cloud/webhook/file-upload`.
4. Respond to webhook with `{ "success": true, "file_id": "…" }`.

## Code paths

| Feature | API | `upload_type` |
|---------|-----|----------------|
| CRM → Upload testimonial | `POST /api/testimonials/upload` | `testimonial` |
| One Month Savings → Generate invoice | `POST /api/one-month-savings/upload-pdf` | `one_month_savings_invoice` |

Implementation: `tools/n8n_file_upload.py`

## Request (multipart/form-data)

| Field | Required | testimonial | invoice |
|-------|----------|-------------|---------|
| `file` | Yes | PDF/Word | PDF |
| `upload_type` | Yes | `testimonial` | `one_month_savings_invoice` |
| `business_name` | Yes | ✓ | ✓ |
| `drive_folder` | Yes | Client folder URL/ID or `TESTIMONIAL_STORAGE_FOLDER_ID` | `ONE_MONTH_SAVINGS_DRIVE_FOLDER_ID` |
| `filename` | Yes | Original filename | e.g. `Business - RA5711.pdf` |
| `invoice_number` | No | Optional | ✓ (match key for sheet row) |
| `due_date` | No | — | ✓ `YYYY-MM-DD` |
| `invoice_date` | No | — | ✓ `YYYY-MM-DD` |
| `status` | No | — | ✓ e.g. `Generated` |
| `solution` | No | — | ✓ First line solution label |
| `savings_amount` | No | — | ✓ Ex-GST (first line) |
| `gst` | No | — | ✓ First line GST |
| `total_invoice` | No | — | ✓ First line total (inc GST) |
| `subtotal` | No | — | ✓ Invoice subtotal (ex GST) |
| `total_gst` | No | — | ✓ Invoice total GST |
| `total_amount` | No | — | ✓ Invoice total (inc GST) |
| `line_items` | No | — | ✓ JSON array (all lines — use in n8n loop) |
| `testimonial_type` | No | ✓ | — |
| `testimonial_solution_type_id` | No | ✓ | — |
| `testimonial_savings` | No | ✓ | — |
| `requested_by` | No | Portal user email | ✓ |
| `request_id` | No | UUID for logs | ✓ |

## Response

### Success — HTTP 200

```json
{
  "success": true,
  "file_id": "1abc…",
  "file_url": "https://drive.google.com/file/d/1abc…/view"
}
```

**`file_id` is required** (alias `fileId` also accepted by backend).

### Failure

```json
{
  "success": false,
  "error_code": "DRIVE_UPLOAD_FAILED",
  "message": "Human-readable reason"
}
```

## Logging

```
textPayload:"[FILE_UPLOAD]"
textPayload:"[OMS_UPLOAD]"
```

## n8n workflow

```
Webhook (POST multipart)
  → Switch on {{ $json.upload_type }}
      → testimonial → Drive upload
      → one_month_savings_invoice → Drive upload → update tracking sheet
  → Respond to Webhook { success, file_id, file_url }
```

### Sheet columns (`one_month_savings_invoice`)

Match / update rows by **`invoice_number`** (column F). Portal sends these form fields:

| Sheet column | n8n expression (webhook body) |
|--------------|-------------------------------|
| Invoice Number (match) | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.invoice_number }}` |
| Member | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.business_name }}` |
| Solution | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.solution }}` |
| Savings Amount | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.savings_amount }}` |
| GST | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.gst }}` |
| Total Invoice | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.total_invoice }}` |
| Due Date | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.due_date }}` |
| Invoice ID | Set after Drive upload — `{{ $json.id }}` from Drive node, or return as `file_id` in Respond |
| Status | `{{ $('File Upload - Testimoinal & 1st Month Savings').item.json.body.status }}` |

**Multi-line invoices:** parse `line_items` JSON and loop — each object has `solution_label`, `savings_amount`, `gst`, `total`.

**Amounts:** `savings_amount` is **ex-GST**; `gst` is 10% on top; `total_invoice` is inc-GST for that line.
