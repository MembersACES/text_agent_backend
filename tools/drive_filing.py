import json
import requests
from datetime import datetime
import os
import mimetypes

def drive_filing(file_bytes: bytes, filename: str, business_name: str, gdrive_url: str, filing_type: str) -> dict:
    """Process drive filing data and send to n8n webhook
    Args:
        file_bytes (bytes): File content in bytes
        filename (str): Name of the file
        business_name (str): Name of the business
        gdrive_url (str): Google Drive folder URL
        filing_type (str): Type of filing (loa, savings, revenue, site_profiling, etc.)
    Returns:
        dict: Result of the filing process
    """
    try:
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

        if filing_type in ['savings', 'revenue']:
            display_name = 'Service Fee Agreement'
        elif filing_type in signed_contract_display_names:
            display_name = signed_contract_display_names[filing_type]
        else:
            display_name = filing_type.upper()

        # Preserve original extension
        ext = filename.split('.')[-1]
        new_filename = f"{business_name} - {display_name}.{ext}"

        # Detect the MIME type dynamically
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type:
            mime_type = "application/octet-stream"  # safe fallback

        files = {
            'file': (new_filename, file_bytes, mime_type)
        }

        data = {
            'business_name': business_name,
            'gdrive_url': gdrive_url,
            'timestamp': datetime.now().isoformat(),
            'new_filename': new_filename
        }

        webhook_urls = {
            'loa': 'https://membersaces.app.n8n.cloud/webhook/loa_upload',
            'savings': 'https://membersaces.app.n8n.cloud/webhook/savings_upload',
            'revenue': 'https://membersaces.app.n8n.cloud/webhook/revenue_upload',
            'site_profiling': 'https://membersaces.app.n8n.cloud/webhook/site_profiling',
            'cleaning_invoice_upload': 'https://membersaces.app.n8n.cloud/webhook/cleaning_invoice_upload',
            'telecommunication_invoice_upload': 'https://membersaces.app.n8n.cloud/webhook/telecommunication_invoice_upload',
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

        headers = {'Accept': 'application/json'}
        response = requests.post(
            webhook_urls[filing_type],
            files=files,
            data=data,
            headers=headers
        )

        if response.status_code == 200:
            return {
                "status": "success",
                "message": f"Successfully processed {display_name} filing for {business_name}",
                "filename": new_filename
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to process {display_name} filing. Status code: {response.status_code}, Response: {response.text}"
            }

    except Exception as e:
        if filing_type in ['savings', 'revenue']:
            display_name = 'Service Fee Agreement'
        else:
            display_name = filing_type.upper()
        return {"status": "error", "message": f"Error processing {display_name} filing: {str(e)}"}
