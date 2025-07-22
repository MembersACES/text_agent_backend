from langchain_core.tools import tool
import requests


@tool
def telco_comparaison_generation(
    record_id: str,
    current_provider: str,
    new_provider: str,
    current_annual_cost: str,
    new_annual_cost: str,
) -> str:
    """Generate an telco comparison document. You will need to get the latest invoice ID information first and the user needs to confirm the invoice details.
    Then, You will use this tool to generate the telco comparison document.

    Args:
        record_id: business record id. Use get_business_information to get the business information
        current_provider: Current Provider
        new_provider: New Provider
        current_annual_cost: Current Annual Cost without GST
        new_annual_cost: New Annual Cost without GST
    """
    payload = {
        "record_id": record_id,
        "current_provider": current_provider,
        "new_provider": new_provider,
        "current_annual_cost": current_annual_cost,
        "new_annual_cost": new_annual_cost,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-telco-comparison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate telco comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    pdf_document_id = data.get("pdf_document_id")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Telco Comparison document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}. The telco comparison document id is {pdf_document_id}"
