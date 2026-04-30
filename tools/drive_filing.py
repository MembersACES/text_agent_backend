import requests
from datetime import datetime
import mimetypes
from typing import List, Tuple, Optional


def drive_filing(
    file_payloads: List[Tuple[bytes, str]],
    business_name: str,
    gdrive_url: str,
    filing_type: str,
    contract_status: Optional[str] = None,
    contract_update_mode: Optional[str] = None,
) -> dict:
    """Process drive filing data and send to n8n webhook.

    Args:
        file_payloads: List of (file_bytes, original_filename). One or more files per request.
        business_name: Name of the business
        gdrive_url: Google Drive folder URL
        filing_type: Type of filing (loa, savings, site_map_upload, signed_CI_E, etc.)
        contract_status: For signed contracts (e.g. "Signed via ACES", "Existing Contract")
        contract_update_mode: For signed contracts: "replace" | "append" | "append_batch"
            (n8n merges file IDs / status into comma-separated cells)
    """
    try:
        if not file_payloads:
            return {"status": "error", "message": "At least one file is required"}
        if not gdrive_url:
            return {"status": "error", "message": "Google Drive folder URL is required"}

        signed_contract_display_names = {
            'signed_CI_E': 'Signed Contract - C&I Electricity',
            'signed_SME_E': 'Signed Contract - SME Electricity',
            'signed_CI_G': 'Signed Contract - C&I Gas',
            'signed_SME_G': 'Signed Contract - SME Gas',
            'signed_WASTE': 'Signed Contract - Waste',
            'signed_OIL': 'Signed Contract - Oil',
            'signed_DMA': 'Signed Contract - DMA',
        }

        invoice_display_names = {
            'cleaning_invoice_upload': 'Cleaning Invoice',
            'telecommunication_invoice_upload': 'Telecommunication Invoice',
            'water_invoice_upload': 'Water Invoice',
        }

        if filing_type in ['savings', 'revenue']:
            display_name = 'Service Fee Agreement'
        elif filing_type in signed_contract_display_names:
            display_name = signed_contract_display_names[filing_type]
        elif filing_type in invoice_display_names:
            display_name = invoice_display_names[filing_type]
        else:
            display_name = filing_type.replace('_', ' ').title()

        is_signed_contract = filing_type in signed_contract_display_names

        webhook_urls = {
            'loa': 'https://membersaces.app.n8n.cloud/webhook/loa_upload',
            'savings': 'https://membersaces.app.n8n.cloud/webhook/savings_upload',
            'revenue': 'https://membersaces.app.n8n.cloud/webhook/revenue_upload',
            'site_profiling': 'https://membersaces.app.n8n.cloud/webhook/site_profiling',
            'cleaning_invoice_upload': 'https://membersaces.app.n8n.cloud/webhook/cleaning_invoice_upload',
            'telecommunication_invoice_upload': 'https://membersaces.app.n8n.cloud/webhook/telecommunication_invoice_upload',
            'water_invoice_upload': 'https://membersaces.app.n8n.cloud/webhook/water_invoice_upload',
            'site_map_upload': 'https://membersaces.app.n8n.cloud/webhook/site_map_upload',
            'signed_CI_E': 'https://membersaces.app.n8n.cloud/webhook/signed_C&IE',
            'signed_SME_E': 'https://membersaces.app.n8n.cloud/webhook/signed_SMEE',
            'signed_CI_G': 'https://membersaces.app.n8n.cloud/webhook/signed_C&IG',
            'signed_SME_G': 'https://membersaces.app.n8n.cloud/webhook/signed_SMEG',
            'signed_WASTE': 'https://membersaces.app.n8n.cloud/webhook/signed_Waste',
            'signed_OIL': 'https://membersaces.app.n8n.cloud/webhook/signed_Oil',
            'signed_DMA': 'https://membersaces.app.n8n.cloud/webhook/signed_DMA',
            'amortisation_excel': 'https://membersaces.app.n8n.cloud/webhook/amortisation_excel',
            'amortisation_pdf': 'https://membersaces.app.n8n.cloud/webhook/amortisation_pdf'
        }

        if filing_type not in webhook_urls:
            return {"status": "error", "message": f"Invalid filing type. Must be one of: {', '.join(webhook_urls.keys())}"}

        webhook_url = webhook_urls[filing_type]

        # Non–signed-contract uploads: only one file supported (merge on client if needed)
        if not is_signed_contract and len(file_payloads) > 1:
            return {"status": "error", "message": "Only one file allowed for this filing type"}

        headers = {'Accept': 'application/json'}
        last_filename = ""
        n = len(file_payloads)

        for idx, (file_bytes, filename) in enumerate(file_payloads):
            ext = (filename or "file").split('.')[-1]
            new_filename = f"{business_name} - {display_name}.{ext}"
            if n > 1:
                base = f"{business_name} - {display_name}"
                new_filename = f"{base} ({idx + 1} of {n}).{ext}"

            mime_type, _ = mimetypes.guess_type(filename or "")
            if not mime_type:
                mime_type = "application/octet-stream"

            files = {
                'file': (new_filename, file_bytes, mime_type)
            }

            data = {
                'business_name': business_name,
                'gdrive_url': gdrive_url,
                'timestamp': datetime.now().isoformat(),
                'new_filename': new_filename,
            }

            if contract_status:
                data['contract_status'] = contract_status

            if is_signed_contract:
                mode = contract_update_mode or 'replace'
                if n > 1:
                    mode = 'append_batch'
                elif mode == 'append_multiple':
                    mode = 'append'
                data['contract_update_mode'] = mode
                data['batch_index'] = str(idx)
                data['batch_total'] = str(n)

            response = requests.post(
                webhook_url,
                files=files,
                data=data,
                headers=headers
            )
            last_filename = new_filename

            if response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Failed to process {display_name} filing (part {idx + 1}/{n}). Status code: {response.status_code}, Response: {response.text}"
                }

        return {
            "status": "success",
            "message": f"Successfully processed {display_name} filing for {business_name}",
            "filename": last_filename,
            "parts_uploaded": n,
        }

    except Exception as e:
        if filing_type in ['savings', 'revenue']:
            display_name = 'Service Fee Agreement'
        elif filing_type in invoice_display_names:
            display_name = invoice_display_names[filing_type]
        else:
            display_name = filing_type.replace('_', ' ').title()
        return {"status": "error", "message": f"Error processing {display_name} filing: {str(e)}"}
