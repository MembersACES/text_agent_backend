# Testimonial document template

## Template ID

The testimonial Google Doc template ID is configured via:

- **Env:** `TESTIMONIAL_TEMPLATE_DOC_ID` (default: `1sFquN7bpWskP8NPNwSGFS7T82fCp2f1yG0weVl_Vkxw`)

## Do I need to give access to the doc?

**Yes.** The EOI and Engagement Form generators use the same pattern:

1. The backend sends a payload (template ID + placeholder data) to the **n8n** webhook:  
   `https://membersaces.app.n8n.cloud/webhook-test/testimonial-generation`
2. The **n8n workflow** copies the Google Doc template, fills placeholders, and saves the new doc (e.g. into the client’s Drive folder).

So the **Google Doc template** must be **shared with the same Google account that the n8n testimonial-generation workflow uses** (the Google account connected in that n8n workflow). Typically that is:

- A **service account** (e.g. from your project’s service account key), or  
- The **Google account** you connected to n8n for Drive/Docs access.

**What to do:**

1. Open the testimonial template in Google Docs:  
   `https://docs.google.com/document/d/1sFquN7bpWskP8NPNwSGFS7T82fCp2f1yG0weVl_Vkxw/edit`
2. **Share** the doc with the same identity that runs the testimonial-generation workflow (e.g. the service account email or the n8n Google connection). Grant at least **Viewer** (so n8n can copy it); **Editor** if the workflow needs to write into the same file.

Once the template is shared, testimonial document generation (when implemented) will use it the same way EOI/EF use their template IDs in `tools/document_generation.py`.
