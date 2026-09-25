import os

import requests

from tools.sme_gas_sheet import get_sme_gas_invoice_from_sheet

N8N_SME_GAS_URL = "https://membersaces.app.n8n.cloud/webhook/search-gas-sme-info"


def get_gas_sme_latest_invoice_information(
    business_name: str = None, mrin: str = None
) -> dict:
    """
    Get the latest gas SME invoice information as JSON.

    Reads "5th Sheet - Small Gas" directly (tools/sme_gas_sheet.py). Set SME_GAS_SOURCE=n8n
    to fall back to the old n8n webhook.
    Args:
        business_name (str, optional): The name of the business to search for
        mrin (str, optional): The MRIN of the business to search for
    Returns:
        dict: {"gas_sme_invoicedetails": ..., "normalised": ..., "source": ...}, or an error dict.
    """
    if not business_name and not mrin:
        return {"error": "Please provide either a business name or mrin"}

    if os.getenv("SME_GAS_SOURCE", "sheet").strip().lower() != "n8n":
        try:
            return get_sme_gas_invoice_from_sheet(business_name=business_name, mrin=mrin)
        except Exception as e:
            return {"error": f"Failed to read SME gas sheet: {str(e)}"}

    payload = {}
    if business_name:
        payload["mrin"] = ""
        payload["business_name"] = business_name
    if mrin:
        payload["business_name"] = ""
        payload["mrin"] = mrin

    response = requests.post(N8N_SME_GAS_URL, json=payload)

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find gas SME invoice information for that business and MRIN"}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"}
