# Testimonial document template

## Template ID

The testimonial Google Doc template ID is configured via:

- **Env:** `TESTIMONIAL_TEMPLATE_DOC_ID` (default: `1Q1kVW8F3ahYK6nVIoIcPdCWkmU6t0eFj4Le0nCl5FpA`)

## Placeholders

The live Google Doc uses **single braces**: `{key}`, not `{{key}}`.

n8n find-and-replace nodes search for `{key}` and replace with the matching field from the backend payload (`data.<key>`). Placeholder **names must not be renamed** — the Doc template and both n8n nodes bind to the existing keys.

Narrative fields (section labels are static text in the Doc; keys are unchanged):

| Doc section label | Placeholder key |
|---|---|
| The Service Received: | `{key_approach_of_solution}` |
| The Problem We Solved: | `{key_challenge_of_solution}` |
| What Made the Experience Great: | `{key_outcome_of_solution}` |

Other payload keys include `{business_name}`, `{position}`, `{monthly_savings}`, `{annual_savings}`, `{key_outcome_metrics}`, `{key_outcome_dotpoints_1}`–`{key_outcome_dotpoints_5}`, and the solar extras when `solution_type` is `solar_panel_cleaning`.

## n8n workflow

Production webhook (used by `tools/document_generation.py`):

`https://membersaces.app.n8n.cloud/webhook/testimonial-generation`

The n8n workflow copies the Google Doc template, fills `{placeholders}`, and saves the new doc (e.g. into the client’s Drive folder).

### Known bug — `{position}` never replaced

Both find-and-replace nodes currently have `"text": "={position}"` (a stray leading `=`). n8n then evaluates the **search** string as an expression, so `{position}` is never found in the Doc. Fix in n8n: set the search text to `{position}` with no leading `=`. Do not change the placeholder name.

## Do I need to give access to the doc?

**Yes.** The EOI and Engagement Form generators use the same pattern:

1. The backend sends a payload (template ID + placeholder data) to the **n8n** webhook above.
2. The **n8n workflow** copies the Google Doc template, fills `{placeholders}`, and saves the new doc.

So the **Google Doc template** must be **shared with the same Google account that the n8n testimonial-generation workflow uses** (the Google account connected in that n8n workflow). Typically that is:

- A **service account** (e.g. from your project’s service account key), or
- The **Google account** you connected to n8n for Drive/Docs access.

**What to do:**

1. Open the testimonial template in Google Docs:  
   `https://docs.google.com/document/d/1Q1kVW8F3ahYK6nVIoIcPdCWkmU6t0eFj4Le0nCl5FpA/edit`
2. **Share** the doc with the same identity that runs the testimonial-generation workflow (e.g. the service account email or the n8n Google connection). Grant at least **Viewer** (so n8n can copy it); **Editor** if the workflow needs to write into the same file.
